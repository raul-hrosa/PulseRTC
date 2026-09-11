package auth

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// wsProtocolPrefix carries a token through the browser WebSocket handshake,
// which cannot set an Authorization header. The client offers two subprotocols:
//
//	["pulsertc", "pulsertc.token.<raw-jwt>"]
//
// and the server negotiates the plain "pulsertc". Preferred over a query
// parameter because subprotocols are not logged by proxies the way URLs are
// ("avoid putting long-lived tokens in URLs").
const (
	WSProtocol      = "pulsertc"
	wsTokenPrefix   = "pulsertc.token."
	bearerPrefix    = "Bearer "
	queryTokenParam = "access_token"
)

// Authenticator is the composed trust boundary used by the signaling layer:
// validator + rate limiters + metrics + config. One instance per server.
type Authenticator struct {
	cfg       Config
	validator *Validator
	connRL    *RateLimiter
	metrics   Metrics
	logger    *slog.Logger
}

// Build assembles an Authenticator. With Enabled=true and an empty secret it
// returns an error: the server must fail to start rather than run wide open.
func Build(cfg Config, logger *slog.Logger) (*Authenticator, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Enabled && strings.TrimSpace(cfg.Secret) == "" {
		return nil, errors.New("auth: PULSERTC_AUTH_ENABLED=true but PULSERTC_JWT_SECRET is empty")
	}
	if !cfg.Enabled {
		logger.Warn("auth_disabled",
			"detail", "PULSERTC_AUTH_ENABLED=false — every connection is trusted; do not use in production")
	}
	return &Authenticator{
		cfg:       cfg,
		validator: NewValidator(cfg),
		connRL:    NewRateLimiter(cfg.ConnRatePerMin, time.Minute),
		logger:    logger,
	}, nil
}

// Enabled reports whether tokens are enforced.
func (a *Authenticator) Enabled() bool { return a.cfg.Enabled }

// MaxMessageSize is the inbound WebSocket frame cap.
func (a *Authenticator) MaxMessageSize() int64 { return a.cfg.MaxMessageSize }

// MsgRatePerSec is the per-connection signaling message budget. 0 = off.
func (a *Authenticator) MsgRatePerSec() int { return a.cfg.MsgRatePerSec }

// MetricsSnapshot is the /metrics "auth" block.
func (a *Authenticator) MetricsSnapshot() Snapshot { return a.metrics.Snapshot() }

// SweepRateLimiters drops idle per-IP buckets. Call from a slow background loop.
func (a *Authenticator) SweepRateLimiters() { a.connRL.Sweep(10 * time.Minute) }

// TokenFromRequest extracts a raw token from, in order: the Authorization
// bearer header, the "pulsertc.token.*" WebSocket subprotocol, or the
// access_token query parameter. Returns "" when none is present.
func TokenFromRequest(r *http.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, bearerPrefix) {
		return strings.TrimSpace(h[len(bearerPrefix):])
	}
	for _, proto := range parseWSProtocols(r.Header.Get("Sec-WebSocket-Protocol")) {
		if strings.HasPrefix(proto, wsTokenPrefix) {
			return strings.TrimPrefix(proto, wsTokenPrefix)
		}
	}
	return strings.TrimSpace(r.URL.Query().Get(queryTokenParam))
}

func parseWSProtocols(h string) []string {
	if h == "" {
		return nil
	}
	parts := strings.Split(h, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// clientIP is the rate-limit key: the left-most X-Forwarded-For entry when
// present (single trusted reverse proxy), else the TCP peer address.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// AuthenticateRequest is the WebSocket gate. It applies the per-IP connection
// rate limit, then validates the token. When auth is disabled it returns a
// synthetic identity that grants everything so the rest of the pipeline is
// identical in both modes.
func (a *Authenticator) AuthenticateRequest(r *http.Request) (*Identity, *Error) {
	ip := clientIP(r)
	if !a.connRL.Allow(ip) {
		a.metrics.recordRateLimit()
		a.logger.Warn("auth_rate_limited", "ip", ip)
		return nil, &Error{CodeRateLimited, "too many connection attempts"}
	}

	if !a.cfg.Enabled {
		return &Identity{
			Subject:     "anonymous-" + uuid.NewString(),
			Name:        "anonymous",
			Permissions: Permissions{Join: true, Publish: true, Subscribe: true, Control: true},
			ExpiresAt:   time.Now().Add(24 * time.Hour).Unix(),
		}, nil
	}

	id, err := a.validator.Validate(TokenFromRequest(r))
	if err != nil {
		e := AsError(err)
		a.metrics.recordFailure(e.Code)
		a.logger.Warn("auth_failed", "ip", ip, "reason", e.Code)
		return nil, e
	}
	a.metrics.recordSuccess()
	a.logger.Info("auth_success", "subject", id.Subject, "room", id.Room)
	return id, nil
}

// RecordAuthzFailure bumps the right counters for an authorization denial that
// happens after the connection is established (publish/subscribe/control/room).
func (a *Authenticator) RecordAuthzFailure(code string) { a.metrics.recordFailure(code) }

// ParticipantID derives a per-connection participant id from an authenticated
// identity. It is prefixed with the user id (token subject) so logs and stats
// stay legible, and suffixed with a short random token so the same user opening
// several connections gets distinct, non-colliding participant ids
// (userId is the account, participantId is the session).
func ParticipantID(id *Identity) string {
	suffix := uuid.NewString()[:8]
	if id == nil || strings.TrimSpace(id.Subject) == "" {
		return "anon." + suffix
	}
	return id.Subject + "." + suffix
}

// ProtectHTTP wraps a handler so it requires a valid bearer token unless
// MetricsPublic is set or auth is disabled. Health is never wrapped.
func (a *Authenticator) ProtectHTTP(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.cfg.Enabled || a.cfg.MetricsPublic {
			next(w, r)
			return
		}
		if _, err := a.validator.Validate(TokenFromRequest(r)); err != nil {
			e := AsError(err)
			a.metrics.recordFailure(e.Code)
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, `{"error":"`+e.Code+`"}`, http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
