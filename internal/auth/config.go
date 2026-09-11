package auth

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the security configuration, entirely environment-driven.
// No secret is ever hardcoded; a nil/zero secret with Enabled=true is a
// fatal misconfiguration (see Build).
type Config struct {
	Enabled bool

	Secret   string
	Issuer   string
	Audience string
	Algs     []string // accepted "alg" header values

	// Leeway absorbs small clock skew between the token issuer and this server
	// when checking exp / iat.
	Leeway time.Duration

	// MaxTokenAge is the replay window: a token whose iat is
	// older than this is rejected even if exp is still in the future. Keep it
	// short. 0 disables the check.
	MaxTokenAge time.Duration

	// MaxMessageSize caps a single inbound WebSocket frame. The largest
	// legitimate PulseRTC message is an SDP offer, typically 2-8 KiB; 64 KiB is
	// ~8x headroom and still small enough that a flood cannot exhaust memory.
	MaxMessageSize int64

	// ConnRatePerMin limits new WebSocket connections + auth attempts per client
	// IP per minute. MsgRatePerSec limits signaling messages per
	// connection per second. 0 disables the respective limiter.
	ConnRatePerMin int
	MsgRatePerSec  int

	// MetricsPublic keeps /metrics and /sfu/stats reachable without a token
	// (development / trusted network only). Health is always public.
	MetricsPublic bool
}

const (
	defaultMaxMessageSize = 64 * 1024
	defaultConnRatePerMin = 120
	defaultMsgRatePerSec  = 100
	defaultLeeway         = 60 * time.Second
	defaultMaxTokenAge    = 1 * time.Hour
)

// FromEnv reads the PULSERTC_* security variables. Enabled defaults to TRUE:
// an unconfigured deployment fails closed rather than open. Set
// PULSERTC_AUTH_ENABLED=false explicitly for local development.
func FromEnv() Config {
	c := Config{
		Enabled:        envBool("PULSERTC_AUTH_ENABLED", true),
		Secret:         strings.TrimSpace(os.Getenv("PULSERTC_JWT_SECRET")),
		Issuer:         envString("PULSERTC_JWT_ISSUER", "pulsertc"),
		Audience:       envString("PULSERTC_JWT_AUDIENCE", "pulsertc"),
		Algs:           envList("PULSERTC_JWT_ALGS", []string{AlgHS256}),
		Leeway:         defaultLeeway,
		MaxTokenAge:    envDuration("PULSERTC_TOKEN_MAX_AGE", defaultMaxTokenAge),
		MaxMessageSize: int64(envInt("PULSERTC_MAX_MESSAGE_SIZE", defaultMaxMessageSize)),
		ConnRatePerMin: envInt("PULSERTC_RATE_LIMIT", defaultConnRatePerMin),
		MsgRatePerSec:  envInt("PULSERTC_MSG_RATE", defaultMsgRatePerSec),
		MetricsPublic:  envBool("PULSERTC_METRICS_PUBLIC", false),
	}
	return c
}

func envString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func envList(key string, fallback []string) []string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	out := make([]string, 0, 3)
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}
