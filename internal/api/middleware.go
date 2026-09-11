package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

type ctxKey int

const (
	ctxRequestID ctxKey = iota
	ctxAPIKey
)

// requestIDFrom returns the request id stashed by the middleware ("" if none).
func requestIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(ctxRequestID).(string)
	return v
}

func apiKeyFrom(ctx context.Context) *APIKey {
	v, _ := ctx.Value(ctxAPIKey).(*APIKey)
	return v
}

func newRequestID() string {
	var b [10]byte
	_, _ = rand.Read(b[:])
	return "req_" + hex.EncodeToString(b[:])
}

// middleware chain applied to every /v1 route (design):
//
//	recover -> request id -> API key auth -> per-key rate limit
type middleware struct {
	cfg     Config
	keys    map[string]*APIKey
	limiter *auth.RateLimiter
	logger  *slog.Logger
}

func newMiddleware(cfg Config, logger *slog.Logger) *middleware {
	if logger == nil {
		logger = slog.Default()
	}
	keys := make(map[string]*APIKey, len(cfg.Keys))
	for i := range cfg.Keys {
		k := cfg.Keys[i]
		keys[k.Key] = &k
	}
	return &middleware{
		cfg:     cfg,
		keys:    keys,
		limiter: auth.NewRateLimiter(cfg.RateLimitPerMin, time.Minute),
		logger:  logger,
	}
}

// wrap applies the chain and enforces that the caller holds `scope`.
func (m *middleware) wrap(scope Scope, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if reqID == "" {
			reqID = newRequestID()
		}
		w.Header().Set("X-Request-Id", reqID)

		defer func() {
			if rec := recover(); rec != nil {
				m.logger.Error("api_panic", "path", r.URL.Path, "err", rec, "requestId", reqID)
				writeError(w, reqID, apiErr(CodeInternalError, "internal error"))
			}
		}()

		ctx := context.WithValue(r.Context(), ctxRequestID, reqID)

		// Auth. When disabled the middleware is a no-op that grants every scope.
		if m.cfg.Enabled {
			key := m.authenticate(r)
			if key == nil {
				writeError(w, reqID, apiErr(CodeUnauthorized, "missing or invalid API key"))
				return
			}
			if !key.Has(scope) {
				m.logger.Warn("api_forbidden", "scope", scope, "path", r.URL.Path, "requestId", reqID)
				writeError(w, reqID, apiErr(CodeForbidden, "API key lacks required scope: "+string(scope)))
				return
			}
			if !m.limiter.Allow(key.Key) {
				writeError(w, reqID, apiErr(CodeRateLimited, "rate limit exceeded"))
				return
			}
			ctx = context.WithValue(ctx, ctxAPIKey, key)
		}

		h(w, r.WithContext(ctx))
	}
}

// authenticate resolves the presented credential to a configured APIKey, in
// constant time against every known key. It never accepts a participant JWT or
// the cluster secret (they are not in m.keys).
func (m *middleware) authenticate(r *http.Request) *APIKey {
	presented := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if presented == "" {
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			presented = strings.TrimSpace(h[len("Bearer "):])
		}
	}
	if presented == "" {
		return nil
	}
	var match *APIKey
	for stored, k := range m.keys {
		if subtle.ConstantTimeCompare([]byte(stored), []byte(presented)) == 1 {
			match = k
		}
	}
	return match
}
