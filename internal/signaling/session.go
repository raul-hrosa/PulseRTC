package signaling

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Session Recovery & Reconnection.
//
// A Session is the server's explicit handle on one participant's *logical*
// presence in a room, independent of the physical WebSocket / PeerConnection
// that currently backs it. When a node dies the client reconnects (to whatever
// node now owns the room), builds a brand-new PeerConnection, and rejoins — the
// Session model carries the identity, generation and the publish/subscribe
// *intent* across that gap. PeerConnections, ICE/DTLS/SRTP state and Pion
// objects are never migrated.

// SessionState is the lifecycle of a Session.
type SessionState string

const (
	SessionConnecting  SessionState = "CONNECTING"
	SessionConnected   SessionState = "CONNECTED"
	SessionRecovering  SessionState = "RECOVERING"
	SessionReconnected SessionState = "RECONNECTED"
	SessionClosed      SessionState = "CLOSED"
)

// TrackIntent is the logical publish state the client wants restored after a
// reconnect. trackId is a client-chosen *logical* id ("audio-main",
// "video-camera") — stable across sessions — not the physical Pion track id.
type TrackIntent struct {
	TrackID string `json:"trackId"`
	Kind    string `json:"kind"`
	Muted   bool   `json:"muted"`
}

// SubIntent is one desired subscription.
type SubIntent struct {
	PublicationID string `json:"publicationId"`
	Enabled       bool   `json:"enabled"`
}

// Session is one logical presence. It is node-local state: it is never
// written to Redis, and it holds no Pion objects.
type Session struct {
	SessionID     string
	LogicalID     string // JWT subject — the stable identity
	ParticipantID string // current *physical* participant id (changes on reconnect)
	RoomID        string
	NodeID        string
	Generation    uint64
	State         SessionState

	CreatedAt         time.Time
	RecoveryStartedAt time.Time
	ReconnectedAt     time.Time

	// desired client state, updated as the client publishes/subscribes and
	// replayed by the client on reconnect. Keyed by logical track id / publication id.
	Publish   map[string]TrackIntent
	Subscribe map[string]SubIntent
}

// ResumeInfo is the client's claim in a `session.resume` / `join` message.
// The server never trusts it blindly — auth + room authorization still run, and
// the generation is validated against the registry.
type ResumeInfo struct {
	SessionID  string `json:"sessionId"`
	Generation uint64 `json:"generation"`
}

// SessionError is a typed recovery-path failure with a stable code.
type SessionError struct {
	Code    string
	Message string
}

func (e *SessionError) Error() string { return e.Code + ": " + e.Message }

const (
	CodeStaleSession     = "STALE_SESSION"
	CodeSessionReplaced  = "SESSION_REPLACED"
	CodeRecoveryDisabled = "SESSION_RECOVERY_DISABLED"
	CodeRecoveryTimeout  = "SESSION_RECOVERY_TIMEOUT"
)

// SessionRecoveryConfig is env-driven. Recovery is on by default; set
// PULSERTC_SESSION_RECOVERY_ENABLED=false for earlier behavior.
type SessionRecoveryConfig struct {
	Enabled         bool
	RecoveryTimeout time.Duration
	ReconnectHints  ReconnectHints
}

// ReconnectHints are advisory backoff parameters the server sends to the client
// so every client in a deployment reconnects with the same discipline.
type ReconnectHints struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	JitterFrac   float64
}

func sessionRecoveryConfigFromEnv() SessionRecoveryConfig {
	return SessionRecoveryConfig{
		Enabled:         envBool("PULSERTC_SESSION_RECOVERY_ENABLED", true),
		RecoveryTimeout: envDuration("PULSERTC_SESSION_RECOVERY_TIMEOUT", 30*time.Second),
		ReconnectHints: ReconnectHints{
			InitialDelay: envDuration("PULSERTC_RECONNECT_INITIAL_DELAY", 500*time.Millisecond),
			MaxDelay:     envDuration("PULSERTC_RECONNECT_MAX_DELAY", 10*time.Second),
			JitterFrac:   envPercent("PULSERTC_RECONNECT_JITTER", 0.2),
		},
	}
}

func envBool(key string, def bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
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

// envPercent parses "20%" or "0.2" into a fraction.
func envPercent(key string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	if strings.HasSuffix(v, "%") {
		if f, err := strconv.ParseFloat(strings.TrimSuffix(v, "%"), 64); err == nil {
			return f / 100
		}
		return def
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	return def
}

// sessionKey identifies a logical presence: one participant identity in one room.
func sessionKey(logicalID, roomID string) string { return logicalID + "\x00" + roomID }

// SessionRegistry tracks logical sessions on THIS node. Storage is keyed
// by the physical participant id; a secondary index maps (identity, room) to the
// current primary session so a `session.resume` can find the prior one and take
// it over with generation fencing. It stores no Pion objects.
//
// A brand-new connection with no resume claim gets its own fresh session and
// never disturbs an existing one — only an explicit resume (or a resume-style
// takeover) invalidates the previous session.
type SessionRegistry struct {
	cfg     SessionRecoveryConfig
	now     func() time.Time
	mu      sync.Mutex
	byPhys  map[string]*Session // physicalID -> session
	primary map[string]string   // sessionKey -> physicalID of the current primary

	// metric hooks (set by the server).
	onStarted   func()
	onReplaced  func()
	onTimeout   func()
	onReconnect func(d time.Duration)
}

func newSessionRegistry(cfg SessionRecoveryConfig) *SessionRegistry {
	return &SessionRegistry{
		cfg: cfg, now: time.Now,
		byPhys:  map[string]*Session{},
		primary: map[string]string{},
	}
}

// OpenResult is what Open returns to the caller.
type OpenResult struct {
	Session *Session
	// Kick, when non-nil, is the physical participant id of a still-registered
	// session this Open replaced — the caller must close its socket with
	// SESSION_REPLACED.
	Kick        string
	Reconnected bool          // true when this Open resumed an existing session
	RecoveryDur time.Duration // set when Reconnected
}

// Open registers a physical connection for (logicalID, roomID).
//
//   - resume == nil: a fresh session (generation 1). An existing session for the
//     same identity+room is left alone — two live connections coexist.
//   - resume matches the current primary session: a reconnect. The session's
//     generation advances, it re-binds to the new physical id, and its declared
//     intent is preserved. If the previous socket is still live it is kicked.
//   - resume present but stale/unknown: STALE_SESSION, nothing changes.
func (r *SessionRegistry) Open(logicalID, roomID, physicalID, nodeID string, resume *ResumeInfo) (OpenResult, *SessionError) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := sessionKey(logicalID, roomID)

	if resume != nil {
		prevPhys, ok := r.primary[key]
		prev := r.byPhys[prevPhys]
		if !ok || prev == nil || prev.SessionID != resume.SessionID {
			return OpenResult{}, &SessionError{Code: CodeStaleSession, Message: "no such session to resume"}
		}
		if resume.Generation != 0 && resume.Generation < prev.Generation {
			return OpenResult{}, &SessionError{Code: CodeStaleSession, Message: "session generation is stale"}
		}
		res := OpenResult{}
		if prev.State == SessionConnected && prevPhys != physicalID {
			res.Kick = prevPhys // a live duplicate is replaced
			if r.onReplaced != nil {
				r.onReplaced()
			}
		}
		delete(r.byPhys, prevPhys)
		prev.Generation++
		prev.ParticipantID = physicalID
		prev.NodeID = nodeID
		prev.State = SessionConnected
		prev.ReconnectedAt = r.now()
		if !prev.RecoveryStartedAt.IsZero() {
			res.RecoveryDur = prev.ReconnectedAt.Sub(prev.RecoveryStartedAt)
		}
		prev.RecoveryStartedAt = time.Time{}
		r.byPhys[physicalID] = prev
		r.primary[key] = physicalID
		res.Session = prev
		res.Reconnected = true
		if r.onReconnect != nil {
			r.onReconnect(res.RecoveryDur)
		}
		return res, nil
	}

	sess := &Session{
		SessionID:     "sess-" + uuid.NewString()[:12],
		LogicalID:     logicalID,
		ParticipantID: physicalID,
		RoomID:        roomID,
		NodeID:        nodeID,
		Generation:    1,
		State:         SessionConnected,
		CreatedAt:     r.now(),
		Publish:       map[string]TrackIntent{},
		Subscribe:     map[string]SubIntent{},
	}
	r.byPhys[physicalID] = sess
	r.primary[key] = physicalID
	if r.onStarted != nil {
		r.onStarted()
	}
	return OpenResult{Session: sess}, nil
}

// MarkRecovering is called when a connection drops but recovery is enabled: the
// session is parked in RECOVERING with a deadline. Returns false when
// there is nothing to park (already superseded).
func (r *SessionRegistry) MarkRecovering(logicalID, roomID, physicalID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur, ok := r.byPhys[physicalID]
	if !ok || cur.State == SessionClosed {
		return false
	}
	cur.State = SessionRecovering
	cur.RecoveryStartedAt = r.now()
	return true
}

// Get returns the current primary session for an identity+room.
func (r *SessionRegistry) Get(logicalID, roomID string) (*Session, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	phys, ok := r.primary[sessionKey(logicalID, roomID)]
	if !ok {
		return nil, false
	}
	s, ok := r.byPhys[phys]
	return s, ok
}

// Close removes the session bound to physicalID (marks it CLOSED). Idempotent.
func (r *SessionRegistry) Close(logicalID, roomID, physicalID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur, ok := r.byPhys[physicalID]
	if !ok {
		return
	}
	cur.State = SessionClosed
	delete(r.byPhys, physicalID)
	key := sessionKey(logicalID, roomID)
	if r.primary[key] == physicalID {
		delete(r.primary, key)
	}
}

// SweepExpired closes sessions stuck in RECOVERING past the timeout. Call
// it periodically. Returns the number closed.
func (r *SessionRegistry) SweepExpired() int {
	if r.cfg.RecoveryTimeout <= 0 {
		return 0
	}
	cutoff := r.now().Add(-r.cfg.RecoveryTimeout)
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for phys, s := range r.byPhys {
		if s.State == SessionRecovering && s.RecoveryStartedAt.Before(cutoff) {
			s.State = SessionClosed
			delete(r.byPhys, phys)
			if r.primary[sessionKey(s.LogicalID, s.RoomID)] == phys {
				delete(r.primary, sessionKey(s.LogicalID, s.RoomID))
			}
			n++
			if r.onTimeout != nil {
				r.onTimeout()
			}
		}
	}
	return n
}

// UpdatePublishIntent / RemovePublishIntent / UpdateSubIntent record what the
// client wants restored after a reconnect. Idempotent.
func (r *SessionRegistry) UpdatePublishIntent(logicalID, roomID string, ti TrackIntent) {
	r.withSession(logicalID, roomID, func(s *Session) {
		if ti.TrackID == "" {
			return
		}
		s.Publish[ti.TrackID] = ti
	})
}

func (r *SessionRegistry) RemovePublishIntent(logicalID, roomID, trackID string) {
	r.withSession(logicalID, roomID, func(s *Session) { delete(s.Publish, trackID) })
}

func (r *SessionRegistry) UpdateSubIntent(logicalID, roomID string, si SubIntent) {
	r.withSession(logicalID, roomID, func(s *Session) {
		if si.PublicationID == "" {
			return
		}
		if si.Enabled {
			s.Subscribe[si.PublicationID] = si
		} else {
			delete(s.Subscribe, si.PublicationID)
		}
	})
}

func (r *SessionRegistry) withSession(logicalID, roomID string, fn func(*Session)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if phys, ok := r.primary[sessionKey(logicalID, roomID)]; ok {
		if s, ok := r.byPhys[phys]; ok {
			fn(s)
		}
	}
}

// RoomStats returns how many sessions this node tracks for roomID and how many
// of them are currently RECOVERING (drives the public room status).
func (r *SessionRegistry) RoomStats(roomID string) (total, recovering int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.byPhys {
		if s.RoomID != roomID {
			continue
		}
		total++
		if s.State == SessionRecovering {
			recovering++
		}
	}
	return
}

// Count returns how many sessions are tracked (for /metrics).
func (r *SessionRegistry) Count() (total, recovering int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.byPhys {
		total++
		if s.State == SessionRecovering {
			recovering++
		}
	}
	return
}
