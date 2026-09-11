package signaling

import (
	"sync/atomic"

	"github.com/raulhrosa/pulsertc/internal/cluster"
)

// buildRoomSnapshot assembles the current LOGICAL state of a room for a
// reconnecting client. It reads the live media plane on this node —
// which already includes cross-node mirrored publications — so it
// reflects the state AFTER a takeover, never the dead node's memory.
func (s *Server) buildRoomSnapshot(roomID string) RoomSnapshot {
	snap := RoomSnapshot{
		Type:         TypeRoomSnapshot,
		RoomID:       roomID,
		Participants: []SnapshotParticipant{},
	}
	if rr, ok := s.cluster.Rooms().(cluster.RecoverableRooms); ok {
		if _, gen, ok := rr.Ownership(roomID); ok {
			snap.RoomGeneration = gen
		}
	}
	for _, p := range s.sfu.Stats(roomID).Participants {
		sp := SnapshotParticipant{ParticipantID: p.ID, Name: s.participantName(p.ID), Tracks: []SnapshotTrack{}}
		for _, pub := range p.Publications {
			sp.Tracks = append(sp.Tracks, SnapshotTrack{
				PublicationID: pub.ID,
				Kind:          pub.Kind,
				Source:        pub.Source,
				Muted:         pub.Muted,
			})
		}
		if len(sp.Tracks) > 0 {
			snap.Participants = append(snap.Participants, sp)
		}
	}
	return snap
}

// roomGeneration returns the ownership generation of roomID (0 when the
// backend has no recovery support).
func (s *Server) roomGeneration(roomID string) int64 {
	if rr, ok := s.cluster.Rooms().(cluster.RecoverableRooms); ok {
		if _, gen, ok := rr.Ownership(roomID); ok {
			return gen
		}
	}
	return 0
}

// sessionMetrics are the counters. Low cardinality: no
// participant / room / session / track labels.
type sessionMetrics struct {
	started   atomic.Int64
	attempts  atomic.Int64
	success   atomic.Int64
	failed    atomic.Int64
	timeout   atomic.Int64
	replaced  atomic.Int64
	stale     atomic.Int64
	durSumMs  atomic.Int64
	durCount  atomic.Int64
	wsReconn  atomic.Int64
	rtcReconn atomic.Int64
}

func (m *sessionMetrics) observeDuration(ms int64) {
	if ms <= 0 {
		return
	}
	m.durSumMs.Add(ms)
	m.durCount.Add(1)
}

// SessionMetricsSnapshot is the JSON block under "session" in /metrics.
type SessionMetricsSnapshot struct {
	Active           int     `json:"active"`
	Recovering       int     `json:"recovering"`
	RecoveryStarted  int64   `json:"recoveryStarted"`
	RecoveryAttempts int64   `json:"recoveryAttempts"`
	RecoverySuccess  int64   `json:"recoverySuccess"`
	RecoveryFailed   int64   `json:"recoveryFailed"`
	RecoveryTimeout  int64   `json:"recoveryTimeout"`
	RecoveryReplaced int64   `json:"recoveryReplaced"`
	StaleRejected    int64   `json:"staleRejected"`
	RecoveryDurAvgMs float64 `json:"recoveryDurationMsAvg"`
	ReconnectWebSock int64   `json:"reconnectWebsocket"`
	ReconnectWebRTC  int64   `json:"reconnectWebrtc"`
}

func (s *Server) sessionMetricsSnapshot() SessionMetricsSnapshot {
	m := s.sessionMx
	total, recovering := 0, 0
	if s.sessions != nil {
		total, recovering = s.sessions.Count()
	}
	avg := 0.0
	if n := m.durCount.Load(); n > 0 {
		avg = float64(m.durSumMs.Load()) / float64(n)
	}
	return SessionMetricsSnapshot{
		Active:           total,
		Recovering:       recovering,
		RecoveryStarted:  m.started.Load(),
		RecoveryAttempts: m.attempts.Load(),
		RecoverySuccess:  m.success.Load(),
		RecoveryFailed:   m.failed.Load(),
		RecoveryTimeout:  m.timeout.Load(),
		RecoveryReplaced: m.replaced.Load(),
		StaleRejected:    m.stale.Load(),
		RecoveryDurAvgMs: avg,
		ReconnectWebSock: m.wsReconn.Load(),
		ReconnectWebRTC:  m.rtcReconn.Load(),
	}
}
