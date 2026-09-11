package api

import (
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Scope is an administrative API permission (design). These are entirely
// separate from the participant WebRTC permissions (JOIN/PUBLISH/SUBSCRIBE/
// CONTROL) carried by a JWT.
type Scope string

const (
	ScopeRoomsCreate      Scope = "rooms:create"
	ScopeRoomsRead        Scope = "rooms:read"
	ScopeRoomsClose       Scope = "rooms:close"
	ScopeTokensCreate     Scope = "tokens:create"
	ScopeParticipantsRead Scope = "participants:read"
	ScopeQualityRead      Scope = "quality:read"
)

// AllScopes is the set granted by the "*" wildcard.
var AllScopes = []Scope{
	ScopeRoomsCreate, ScopeRoomsRead, ScopeRoomsClose,
	ScopeTokensCreate, ScopeParticipantsRead, ScopeQualityRead,
}

var knownScopes = func() map[Scope]bool {
	m := make(map[Scope]bool, len(AllScopes))
	for _, s := range AllScopes {
		m[s] = true
	}
	return m
}()

// APIKey is one credential and the scopes it grants.
type APIKey struct {
	Key    string
	Scopes map[Scope]bool
}

// Has reports whether the key grants scope.
func (k APIKey) Has(s Scope) bool { return k.Scopes[s] }

// Config is the integration-API configuration, entirely environment-driven.
// No secret is ever hardcoded and none is echoed in any response.
type Config struct {
	Enabled bool

	Keys []APIKey // parsed PULSERTC_API_KEYS

	RateLimitPerMin int // per-key request budget

	PublicWSURL string // advertised to the app: wss://host/ws

	TokenTTL           time.Duration
	TokenTTLMax        time.Duration
	AllowControlTokens bool

	RoomRetention time.Duration // how long a CLOSED/idle room record survives

	WebhookURL    string
	WebhookSecret string
}

const (
	defaultRateLimitPerMin = 600
	defaultTokenTTL        = time.Hour
	defaultTokenTTLMax     = 12 * time.Hour
	defaultRoomRetention   = 5 * time.Minute
)

// FromEnv reads the PULSERTC_API_* / PULSERTC_WEBHOOK_* variables. Enabled
// defaults to true to mirror the fail-closed posture; set
// PULSERTC_API_ENABLED=false explicitly for local development.
func FromEnv() Config {
	c := Config{
		Enabled:            envBool("PULSERTC_API_ENABLED", true),
		Keys:               ParseAPIKeys(os.Getenv("PULSERTC_API_KEYS")),
		RateLimitPerMin:    envInt("PULSERTC_API_RATE_LIMIT", defaultRateLimitPerMin),
		PublicWSURL:        strings.TrimSpace(os.Getenv("PULSERTC_PUBLIC_WS_URL")),
		TokenTTL:           envDuration("PULSERTC_API_TOKEN_TTL", defaultTokenTTL),
		TokenTTLMax:        envDuration("PULSERTC_API_TOKEN_TTL_MAX", defaultTokenTTLMax),
		AllowControlTokens: envBool("PULSERTC_API_ALLOW_CONTROL_TOKENS", false),
		RoomRetention:      envDuration("PULSERTC_API_ROOM_TTL", defaultRoomRetention),
		WebhookURL:         strings.TrimSpace(os.Getenv("PULSERTC_WEBHOOK_URL")),
		WebhookSecret:      strings.TrimSpace(os.Getenv("PULSERTC_WEBHOOK_SECRET")),
	}
	if c.TokenTTLMax < c.TokenTTL {
		c.TokenTTLMax = c.TokenTTL
	}
	return c
}

// ParseAPIKeys parses "key:scopeA,scopeB;key2:*" into APIKeys. Entries are
// separated by ';' or newlines; within an entry the key and a comma-separated
// scope list are separated by the FIRST ':'. "*" grants every scope. Unknown
// scope tokens are ignored. Blank / malformed entries are skipped.
func ParseAPIKeys(raw string) []APIKey {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	fields := strings.FieldsFunc(raw, func(r rune) bool { return r == ';' || r == '\n' })
	out := make([]APIKey, 0, len(fields))
	for _, entry := range fields {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		key, scopeStr := entry, ""
		if i := strings.IndexByte(entry, ':'); i >= 0 {
			key = strings.TrimSpace(entry[:i])
			scopeStr = entry[i+1:]
		}
		if key == "" {
			continue
		}
		scopes := make(map[Scope]bool)
		for _, s := range strings.Split(scopeStr, ",") {
			s = strings.TrimSpace(s)
			switch {
			case s == "":
			case s == "*":
				for _, all := range AllScopes {
					scopes[all] = true
				}
			case knownScopes[Scope(s)]:
				scopes[Scope(s)] = true
			}
		}
		out = append(out, APIKey{Key: key, Scopes: scopes})
	}
	return out
}

// scopeList renders a key's scopes, sorted, for logging / diagnostics.
func (k APIKey) scopeList() []string {
	out := make([]string, 0, len(k.Scopes))
	for s := range k.Scopes {
		out = append(out, string(s))
	}
	sort.Strings(out)
	return out
}

func envBool(key string, def bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
