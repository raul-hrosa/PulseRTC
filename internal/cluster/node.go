// Package cluster is the PulseRTC multi-node foundation.
//
// It teaches the server that other nodes may exist — node identity, a node
// registry, room ownership and participant location — WITHOUT moving any media
// between nodes. Every RTP packet, PeerConnection, Track and Subscription stays
// local to the node that owns the room (see docs/architecture/distributed-architecture.md).
//
// The guiding rule: distribute state and ownership first, media later.
//
// Everything here is an interface with an in-memory implementation. A Redis (or
// other) backend can be dropped in later without touching callers:
//
//	NodeRegistry / RoomLocator / ParticipantLocator / NodeTransport
//	    ├── in-memory  → development, tests, single node
//	    └── redis      → shared, distributed state
package cluster

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// NodeState is the lifecycle phase of a node.
type NodeState string

const (
	NodeStarting     NodeState = "STARTING"
	NodeReady        NodeState = "READY"
	NodeActive       NodeState = "ACTIVE"
	NodeShuttingDown NodeState = "SHUTTING_DOWN"
	NodeOffline      NodeState = "OFFLINE"
)

// AcceptsSessions reports whether a node in this state may take new rooms /
// participants. A node that is shutting down keeps serving its existing
// sessions but is never assigned new ones.
func (s NodeState) AcceptsSessions() bool { return s == NodeReady || s == NodeActive }

// NodeInfo is the shared, distributable description of one PulseRTC instance.
// It carries no session state — only identity and location.
type NodeInfo struct {
	ID        string    `json:"id"`
	Host      string    `json:"host"`
	Port      int       `json:"port"`
	Version   string    `json:"version"`
	State     NodeState `json:"state"`
	StartedAt time.Time `json:"startedAt"`
	// LastSeen is maintained by the registry from heartbeats, not by the node.
	LastSeen time.Time `json:"lastSeen"`
	// MediaAddr is the "host:port" this node listens on for inter-SFU media.
	// Empty when cross-node media is disabled.
	MediaAddr string `json:"mediaAddr,omitempty"`
}

// BaseURL is the address other nodes use to reach this one's /internal API.
func (n NodeInfo) BaseURL() string {
	host := n.Host
	if host == "" {
		host = "localhost"
	}
	return "http://" + host + ":" + strconv.Itoa(n.Port)
}

// Config is the cluster configuration, entirely environment-driven.
// Defaults keep the earlier single-node behavior.
type Config struct {
	Enabled bool
	NodeID  string
	Host    string
	Port    int
	Version string
	// Secret authenticates inter-node /internal calls. Required when
	// Enabled is true.
	Secret string
	// Peers are the base URLs of the other nodes this one can query
	// (PULSERTC_CLUSTER_PEERS, comma separated). Static discovery.
	Peers []string

	HeartbeatInterval time.Duration
	StaleAfter        time.Duration

	// Cross-node signaling transport.
	RequestTimeout time.Duration // per node→node request, default 2s
	MaxMessageSize int           // cluster message payload/body limit, default 64 KiB
	MessageMaxAge  time.Duration // reject messages older than this, default 30s
	MaxRetries     int           // bounded retry attempts, default 2

	// Cross-node media (SFU ↔ SFU RTP/RTCP over a dedicated UDP
	// transport). Disabled by default: single-node behavior is unchanged.
	Media MediaConfig

	// Load balancing / room routing.
	LoadReportInterval time.Duration
	Capacity           CapacityConfig

	// Failure detection & recovery.
	FailureCheckInterval time.Duration // how often the FailureDetector scans nodes
	FailureGracePeriod   time.Duration // extra wait after a missed heartbeat before OFFLINE
	RecoveryConcurrency  int           // max rooms recovered in parallel, default 4
	RecoveryDisabled     bool          // turn OFF room recovery; recovery is on by default

	// Redis, when Redis.Enabled, makes the cluster use a RedisClusterState
	// instead of the in-memory one. Everything else is unchanged.
	Redis RedisConfig
}

// RedisConfig is the shared-state backend configuration. All
// keys are PULSERTC_REDIS_*. Disabled by default so tests, benchmarks and
// local development keep the in-memory backend.
type RedisConfig struct {
	Enabled  bool
	Addr     string
	Password string
	DB       int
	Prefix   string
	// Required makes Redis a hard dependency: the node refuses to become READY
	// and fails joins closed while Redis is unreachable. When false,
	// an unreachable Redis still degrades the node to NOT READY during
	// operation but the process starts.
	Required    bool
	Timeout     time.Duration
	PoolSize    int
	NodeTTL     time.Duration // node-registry key TTL
	ParticipTTL time.Duration // participant-location key TTL
	RoomTTL     time.Duration // room-ownership key TTL; 0 = no expiry
}

// MediaConfig is the inter-SFU media transport configuration. All keys are
// PULSERTC_MEDIA_*.
type MediaConfig struct {
	Enabled           bool
	BindAddr          string        // UDP bind address, default 0.0.0.0
	Port              int           // UDP port for RTP+RTCP (multiplexed), default 10000
	AdvertiseHost     string        // host peers use to reach this node's media port; defaults to Config.Host
	RequestTimeout    time.Duration // media session handshake timeout, default 2s
	MaxPacketSize     int           // largest inter-node media datagram payload, default 1500
	SessionTimeout    time.Duration // close a session with no traffic for this long, default 10s
	HeartbeatInterval time.Duration // media session heartbeat cadence, default 2s
	SendQueue         int           // per-session outbound queue depth before drop, default 1024
}

// CapacityConfig bounds how much a single node is asked to host. A 0 max
// means "no explicit limit".
type CapacityConfig struct {
	MaxRooms        int
	MaxParticipants int
	MaxPublications int
	// SoftLimitFraction: above this fraction of a max, the router deprioritises
	// the node (still eligible). HardLimitFraction: at/above it, the node is
	// excluded from new rooms. Defaults: 0.8 / 1.0.
	SoftLimitFraction float64
	HardLimitFraction float64
}

func (m MediaConfig) bindAddr() string {
	if strings.TrimSpace(m.BindAddr) == "" {
		return "0.0.0.0"
	}
	return m.BindAddr
}

func (r RedisConfig) prefix() string {
	if strings.TrimSpace(r.Prefix) == "" {
		return "pulsertc"
	}
	return strings.TrimRight(strings.TrimSpace(r.Prefix), ":")
}

// Validate checks the cross-node timing and auth invariants. A disabled
// (single-node) cluster has no inter-node timing to validate, so it always
// passes — this keeps cluster.New(cluster.Config{}, logger) working.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	var errs []error
	if strings.TrimSpace(c.Secret) == "" {
		errs = append(errs, errors.New("PULSERTC_CLUSTER_SECRET is required when the cluster is enabled"))
	}
	if c.HeartbeatInterval <= 0 {
		errs = append(errs, fmt.Errorf("HeartbeatInterval must be > 0, got %v", c.HeartbeatInterval))
	}
	if c.StaleAfter <= c.HeartbeatInterval {
		errs = append(errs, fmt.Errorf("StaleAfter (%v) must exceed HeartbeatInterval (%v)", c.StaleAfter, c.HeartbeatInterval))
	}
	return errors.Join(errs...)
}

// FromEnv reads PULSERTC_NODE_* / PULSERTC_CLUSTER_*. A missing node id is
// generated so a node always has one; for real clusters it should be set
// explicitly.
func FromEnv() Config {
	port, _ := strconv.Atoi(strings.TrimSpace(os.Getenv("PULSERTC_NODE_PORT")))
	if port == 0 {
		if p, err := strconv.Atoi(strings.TrimSpace(os.Getenv("PORT"))); err == nil {
			port = p
		}
	}
	if port == 0 {
		port = 8090
	}

	nodeID := strings.TrimSpace(os.Getenv("PULSERTC_NODE_ID"))
	if nodeID == "" {
		nodeID = "node-" + uuid.NewString()[:8]
	}

	var peers []string
	for _, p := range strings.Split(os.Getenv("PULSERTC_CLUSTER_PEERS"), ",") {
		if p = strings.TrimSpace(p); p != "" {
			peers = append(peers, strings.TrimRight(p, "/"))
		}
	}

	version := strings.TrimSpace(os.Getenv("PULSERTC_VERSION"))
	if version == "" {
		version = "dev"
	}

	return Config{
		Enabled:           envBool("PULSERTC_CLUSTER_ENABLED", false),
		NodeID:            nodeID,
		Host:              strings.TrimSpace(os.Getenv("PULSERTC_NODE_HOST")),
		Port:              port,
		Version:           version,
		Secret:            strings.TrimSpace(os.Getenv("PULSERTC_CLUSTER_SECRET")),
		Peers:             peers,
		HeartbeatInterval: envDuration("PULSERTC_CLUSTER_HEARTBEAT", 5*time.Second),
		StaleAfter: envDuration("PULSERTC_CLUSTER_STALE_AFTER",
			envDuration("PULSERTC_NODE_TTL", 15*time.Second)),

		FailureCheckInterval: envDuration("PULSERTC_FAILURE_CHECK_INTERVAL", 5*time.Second),
		FailureGracePeriod:   envDuration("PULSERTC_FAILURE_GRACE_PERIOD", 5*time.Second),
		RecoveryConcurrency:  envInt("PULSERTC_RECOVERY_CONCURRENCY", 4),
		RecoveryDisabled:     !envBool("PULSERTC_RECOVERY_ENABLED", true),
		RequestTimeout:       envDuration("PULSERTC_CLUSTER_REQUEST_TIMEOUT", 2*time.Second),
		MaxMessageSize:       envInt("PULSERTC_CLUSTER_MAX_MESSAGE_SIZE", 64*1024),
		MessageMaxAge:        envDuration("PULSERTC_CLUSTER_MESSAGE_MAX_AGE", 30*time.Second),
		MaxRetries:           envInt("PULSERTC_CLUSTER_MAX_RETRIES", 2),
		Media:                mediaFromEnv(),
		Redis:                redisFromEnv(),

		LoadReportInterval: envDuration("PULSERTC_LOAD_REPORT_INTERVAL", 2*time.Second),
		Capacity: CapacityConfig{
			MaxRooms:          envInt("PULSERTC_NODE_MAX_ROOMS", 0),
			MaxParticipants:   envInt("PULSERTC_NODE_MAX_PARTICIPANTS", 0),
			MaxPublications:   envInt("PULSERTC_NODE_MAX_PUBLICATIONS", 0),
			SoftLimitFraction: envFloat("PULSERTC_NODE_SOFT_LIMIT", 0.8),
			HardLimitFraction: envFloat("PULSERTC_NODE_HARD_LIMIT", 1.0),
		},
	}
}

func envFloat(key string, fallback float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func mediaFromEnv() MediaConfig {
	return MediaConfig{
		Enabled:           envBool("PULSERTC_MEDIA_ENABLED", false),
		BindAddr:          strings.TrimSpace(os.Getenv("PULSERTC_MEDIA_BIND_ADDR")),
		Port:              envInt("PULSERTC_MEDIA_PORT", 10000),
		AdvertiseHost:     strings.TrimSpace(os.Getenv("PULSERTC_MEDIA_ADVERTISE_HOST")),
		RequestTimeout:    envDuration("PULSERTC_MEDIA_REQUEST_TIMEOUT", 2*time.Second),
		MaxPacketSize:     envInt("PULSERTC_MEDIA_MAX_PACKET_SIZE", 1500),
		SessionTimeout:    envDuration("PULSERTC_MEDIA_SESSION_TIMEOUT", 10*time.Second),
		HeartbeatInterval: envDuration("PULSERTC_MEDIA_HEARTBEAT_INTERVAL", 2*time.Second),
		SendQueue:         envInt("PULSERTC_MEDIA_SEND_QUEUE", 1024),
	}
}

func envInt(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func redisFromEnv() RedisConfig {
	db, _ := strconv.Atoi(strings.TrimSpace(os.Getenv("PULSERTC_REDIS_DB")))
	poolSize, _ := strconv.Atoi(strings.TrimSpace(os.Getenv("PULSERTC_REDIS_POOL_SIZE")))
	addr := strings.TrimSpace(os.Getenv("PULSERTC_REDIS_ADDR"))
	if addr == "" {
		addr = "redis:6379"
	}
	return RedisConfig{
		Enabled:     envBool("PULSERTC_REDIS_ENABLED", false),
		Addr:        addr,
		Password:    os.Getenv("PULSERTC_REDIS_PASSWORD"),
		DB:          db,
		Prefix:      strings.TrimSpace(os.Getenv("PULSERTC_REDIS_PREFIX")),
		Required:    envBool("PULSERTC_REDIS_REQUIRED", true),
		Timeout:     envDuration("PULSERTC_REDIS_TIMEOUT", 3*time.Second),
		PoolSize:    poolSize,
		NodeTTL:     envDuration("PULSERTC_REDIS_NODE_TTL", 15*time.Second),
		ParticipTTL: envDuration("PULSERTC_REDIS_PARTICIPANT_TTL", 60*time.Second),
		RoomTTL:     envDuration("PULSERTC_REDIS_ROOM_TTL", 0),
	}
}

func envBool(key string, fallback bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
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
