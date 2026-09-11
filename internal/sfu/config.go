package sfu

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/pion/webrtc/v4"
)

// Config is the SFU's environment-driven tuning, resolved once in New().
type Config struct {
	ICEServers             []webrtc.ICEServer
	NackBufferSize         uint16
	NegotiationConcurrency int
	SubQueueAudio          int
	SubQueueVideo          int
	UDPPort                int // 0 = ephemeral
	NAT1To1IPs             []string
}

func configFromEnv(logger *slog.Logger) Config {
	audio, video := subQueueCaps(logger)
	return Config{
		ICEServers:             iceServersFromEnv(logger),
		NackBufferSize:         nackResponderSize(logger),
		NegotiationConcurrency: negotiationConcurrencyFromEnv(logger),
		SubQueueAudio:          audio,
		SubQueueVideo:          video,
		UDPPort:                envPositiveInt("SFU_UDP_PORT", 0, logger),
		NAT1To1IPs:             splitCSV(os.Getenv("SFU_NAT_1TO1_IP")),
	}
}

// negotiationConcurrencyFromEnv picks a queue width that tracks the machine's
// core count (where the SDP-parsing CPU cost is actually spent), overridable
// for experimentation via SFU_NEGOTIATION_CONCURRENCY.
func negotiationConcurrencyFromEnv(logger *slog.Logger) int {
	n := runtime.GOMAXPROCS(0)
	if n < 2 {
		n = 2
	}
	n = envPositiveInt("SFU_NEGOTIATION_CONCURRENCY", n, logger)
	logger.Info("sfu_negotiation_concurrency", "limit", n)
	return n
}

func (c Config) Validate() error {
	var errs []error
	if c.NegotiationConcurrency < 1 {
		errs = append(errs, fmt.Errorf("NegotiationConcurrency must be >= 1, got %d", c.NegotiationConcurrency))
	}
	if c.SubQueueAudio < 1 {
		errs = append(errs, fmt.Errorf("SubQueueAudio must be >= 1, got %d", c.SubQueueAudio))
	}
	if c.SubQueueVideo < 1 {
		errs = append(errs, fmt.Errorf("SubQueueVideo must be >= 1, got %d", c.SubQueueVideo))
	}
	if !validNackBufferSizes[int(c.NackBufferSize)] {
		errs = append(errs, fmt.Errorf("NackBufferSize %d is not a valid power of two", c.NackBufferSize))
	}
	return errors.Join(errs...)
}

// All SFU tuning comes from the environment, resolved once at New(). The
// helpers here are the single place that reads SFU_* / PULSERTC_* ICE settings.

// splitCSV splits a comma-separated env value, trimming spaces and dropping
// empties.
func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// iceServersFromEnv builds the ICE server list the SFU offers to browsers:
//
//	PULSERTC_STUN_URLS     - comma-separated STUN URLs (default: Google STUN)
//	PULSERTC_TURN_URLS     - comma-separated TURN/TURNS URLs
//	PULSERTC_TURN_USERNAME - long-term credential username (required for TURN)
//	PULSERTC_TURN_PASSWORD - long-term credential password (required for TURN)
//
// A TURN URL set without both credentials is skipped with a warning — an
// unauthenticated TURN entry is never useful.
func iceServersFromEnv(logger *slog.Logger) []webrtc.ICEServer {
	var servers []webrtc.ICEServer

	stun := splitCSV(os.Getenv("PULSERTC_STUN_URLS"))
	if len(stun) == 0 {
		stun = []string{"stun:stun.l.google.com:19302"}
	}
	servers = append(servers, webrtc.ICEServer{URLs: stun})

	turn := splitCSV(os.Getenv("PULSERTC_TURN_URLS"))
	if len(turn) > 0 {
		user := strings.TrimSpace(os.Getenv("PULSERTC_TURN_USERNAME"))
		pass := strings.TrimSpace(os.Getenv("PULSERTC_TURN_PASSWORD"))
		if user == "" || pass == "" {
			logger.Error("PULSERTC_TURN_URLS set without PULSERTC_TURN_USERNAME/PASSWORD; ignoring TURN")
		} else {
			servers = append(servers, webrtc.ICEServer{
				URLs:           turn,
				Username:       user,
				Credential:     pass,
				CredentialType: webrtc.ICECredentialTypePassword,
			})
			logger.Info("sfu_turn_configured", "urls", turn)
		}
	}
	return servers
}

// ICEServerConfig is the browser-facing shape of one RTCIceServer, sent to the
// client in the `welcome` message so it uses the same STUN/TURN the SFU does.
type ICEServerConfig struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

// ICEServers returns the configured ICE servers in browser-friendly form.
func (s *SFU) ICEServers() []ICEServerConfig {
	out := make([]ICEServerConfig, 0, len(s.config.ICEServers))
	for _, srv := range s.config.ICEServers {
		c := ICEServerConfig{URLs: srv.URLs, Username: srv.Username}
		if p, ok := srv.Credential.(string); ok {
			c.Credential = p
		}
		out = append(out, c)
	}
	return out
}

func envPositiveInt(key string, def int, logger *slog.Logger) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		logger.Error("invalid env int, using default", "key", key, "value", v, "default", def)
		return def
	}
	return n
}

// Default per-subscriber send-queue depths (ADR 012). Audio ~1.3s at 50 pkt/s;
// video large enough to ride out a few hundred ms of subscriber stall at a
// typical 2.5 Mbps / ~1400-byte packet rate before dropping the backlog.
const (
	defaultSubQueueAudio = 64
	defaultSubQueueVideo = 512
)

func subQueueCaps(logger *slog.Logger) (audio, video int) {
	audio = envPositiveInt("SFU_SUBSCRIBER_QUEUE_AUDIO", defaultSubQueueAudio, logger)
	video = envPositiveInt("SFU_SUBSCRIBER_QUEUE_VIDEO", defaultSubQueueVideo, logger)
	return
}

// validNackBufferSizes are the only sizes Pion's nack.ResponderInterceptor
// accepts (rtpbuffer requires a power of two).
var validNackBufferSizes = map[int]bool{
	1: true, 2: true, 4: true, 8: true, 16: true, 32: true, 64: true,
	128: true, 256: true, 512: true, 1024: true, 2048: true, 4096: true,
	8192: true, 16384: true, 32768: true,
}

// defaultNackBufferSize is a quarter of Pion's own default (1024). It still
// covers roughly 8s of audio (50 pkt/s) or several seconds of 30fps video —
// comfortably more than one RTT of retransmission history on a LAN/WAN call —
// while cutting the dominant heap consumer identified in by ~4x per
// subscription.
const defaultNackBufferSize = 256

// nackResponderSize resolves the NACK retained-packet buffer size, allowing
// SFU_NACK_BUFFER_SIZE to override it for experimentation (7B.3). Falls back
// to defaultNackBufferSize on an invalid or non-power-of-two value.
func nackResponderSize(logger *slog.Logger) uint16 {
	size := defaultNackBufferSize
	if v := strings.TrimSpace(os.Getenv("SFU_NACK_BUFFER_SIZE")); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && validNackBufferSizes[parsed] {
			size = parsed
		} else {
			logger.Error("invalid SFU_NACK_BUFFER_SIZE, using default",
				"value", v, "default", defaultNackBufferSize)
		}
	}
	logger.Info("sfu_nack_buffer_size", "size", size)
	return uint16(size)
}

// configureNetwork wires optional, env-driven ICE settings:
//
//	SFU_UDP_PORT     - bind all ICE traffic to this single UDP port (port mux).
//	SFU_NAT_1TO1_IP  - comma-separated public IP(s) to advertise as host
//	                   candidates instead of the container's private address.
//
// Both are optional; with neither set the SFU behaves like a plain Pion peer
// (ephemeral ports, real interface IPs) which is fine for same-host tests.
func configureNetwork(se *webrtc.SettingEngine, logger *slog.Logger) error {
	if ips := strings.TrimSpace(os.Getenv("SFU_NAT_1TO1_IP")); ips != "" {
		if list := splitCSV(ips); len(list) > 0 {
			se.SetNAT1To1IPs(list, webrtc.ICECandidateTypeHost)
			logger.Info("sfu_nat_1to1", "ips", list)
		}
	}

	if p := strings.TrimSpace(os.Getenv("SFU_UDP_PORT")); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil {
			return err
		}
		udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: port})
		if err != nil {
			return err
		}
		se.SetICEUDPMux(webrtc.NewICEUDPMux(nil, udp))
		logger.Info("sfu_ice_udp_mux", "port", port)
	}
	return nil
}
