package quality

import (
	"sort"
	"sync"
	"time"
)

// Engine holds per-stream state and turns a stream of Samples into stable
// verdicts and transition events. Safe for concurrent use.
//
// It knows nothing about rooms, WebSockets or the SFU — callers feed it Samples
// and ask for Snapshots. This keeps QoE logic out of signaling and out of the
// UI (principles).
type Engine struct {
	cfg Config
	now func() time.Time

	mu         sync.Mutex
	streams    map[StreamKey]*streamState
	recovering map[string]bool // Participants mid session-recovery
}

type streamState struct {
	prev    *Sample
	win     *window
	stab    *stabilizer
	last    Analysis
	history []HistoryPoint
	updated time.Time
}

// New builds an Engine. A zero Config falls back to DefaultConfig.
func New(cfg Config) *Engine {
	if cfg.WindowSamples == 0 {
		cfg = DefaultConfig()
	}
	return &Engine{
		cfg:        cfg,
		now:        time.Now,
		streams:    make(map[StreamKey]*streamState),
		recovering: make(map[string]bool),
	}
}

// SetRecovering marks (or clears) a participant as mid session-recovery.
// While set, that participant's Snapshot reports
// status=WARNING with reason SESSION_RECOVERY instead of a POOR verdict driven
// by the torn-down connection's last samples.
func (e *Engine) SetRecovering(participantID string, recovering bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if recovering {
		e.recovering[participantID] = true
	} else {
		delete(e.recovering, participantID)
	}
}

// Ingest processes one Sample and returns any stable-status transition events
// it triggered (usually none).
func (e *Engine) Ingest(s Sample) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	st, ok := e.streams[s.Key]
	if !ok {
		st = &streamState{
			win:  newWindow(e.cfg.WindowSamples),
			stab: newStabilizer(e.cfg),
			last: Analysis{Status: Unknown, Problems: []string{}, Metrics: map[string]float64{}},
		}
		e.streams[s.Key] = st
	}

	sample := s
	d := derive(st.prev, &sample)
	st.prev = &sample
	st.updated = e.now()

	if !d.HasData && d.Enabled {
		return nil
	}

	st.win.push(d)
	verdict := analyze(s.Key.Kind, st.win.mean(), e.cfg)

	prevStable := st.stab.stable
	stable, changed := st.stab.observe(verdict.Status)

	st.last = Analysis{
		Status:   stable,
		Score:    verdict.Score,
		Problems: verdict.Problems,
		Metrics:  verdict.Metrics,
	}

	if !changed {
		return nil
	}

	at := e.now()
	st.history = append(st.history, HistoryPoint{At: at, Status: stable})
	if len(st.history) > e.cfg.HistorySize {
		st.history = st.history[len(st.history)-e.cfg.HistorySize:]
	}
	return []Event{makeEvent(s.Key, prevStable, stable, verdict, at)}
}

// Forget drops every stream belonging to a participant (called on leave).
func (e *Engine) Forget(participantID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for k := range e.streams {
		if k.Participant == participantID {
			delete(e.streams, k)
		}
	}
	delete(e.recovering, participantID)
}

// Prune drops streams that have not been updated within maxAge (e.g. a track
// that was unpublished). Call it periodically.
func (e *Engine) Prune(maxAge time.Duration) {
	cutoff := e.now().Add(-maxAge)
	e.mu.Lock()
	defer e.mu.Unlock()
	for k, st := range e.streams {
		if st.updated.Before(cutoff) {
			delete(e.streams, k)
		}
	}
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

// StreamQuality is the current verdict for one tracked stream.
type StreamQuality struct {
	Direction Direction          `json:"direction"`
	Kind      Kind               `json:"kind"`
	TrackID   string             `json:"trackId,omitempty"`
	Status    Status             `json:"status"`
	Score     int                `json:"score"`
	Problems  []string           `json:"problems"`
	Metrics   map[string]float64 `json:"metrics"`
}

// ParticipantQuality is the aggregated QoE view for one participant.
type ParticipantQuality struct {
	ParticipantID string          `json:"participantId"`
	Connection    StreamQuality   `json:"connection"`
	Outbound      []StreamQuality `json:"outbound"`
	Inbound       []StreamQuality `json:"inbound"`
	Overall       Status          `json:"overall"`
	Reason        string          `json:"reason,omitempty"`
	Timeline      []HistoryPoint  `json:"timeline"`
}

// Snapshot returns the current aggregated verdict for one participant.
func (e *Engine) Snapshot(participantID string) ParticipantQuality {
	e.mu.Lock()
	defer e.mu.Unlock()

	pq := ParticipantQuality{
		ParticipantID: participantID,
		Connection:    StreamQuality{Kind: Connection, Status: Unknown, Problems: []string{}, Metrics: map[string]float64{}},
		Outbound:      []StreamQuality{},
		Inbound:       []StreamQuality{},
		Overall:       Unknown,
	}

	for k, st := range e.streams {
		if k.Participant != participantID {
			continue
		}
		sq := StreamQuality{
			Direction: k.Direction, Kind: k.Kind, TrackID: k.TrackID,
			Status: st.last.Status, Score: st.last.Score,
			Problems: st.last.Problems, Metrics: st.last.Metrics,
		}
		switch {
		case k.Kind == Connection:
			pq.Connection = sq
			pq.Timeline = append([]HistoryPoint(nil), st.history...)
		case k.Direction == Outbound:
			pq.Outbound = append(pq.Outbound, sq)
		default:
			pq.Inbound = append(pq.Inbound, sq)
		}
	}

	sortStreams(pq.Outbound)
	sortStreams(pq.Inbound)
	pq.Overall = overall(pq)

	// A participant mid-recovery is not a network failure — the
	// real cause is the SFU takeover. Cap at WARNING and name the reason so the
	// UI does not flash "POOR_NETWORK" during the reconnect gap.
	if e.recovering[participantID] {
		if pq.Overall == Poor || pq.Overall == Unknown {
			pq.Overall = Warning
		}
		pq.Reason = "SESSION_RECOVERY"
	}
	return pq
}

// SnapshotRoom returns snapshots for a given set of participant ids, ordered.
func (e *Engine) SnapshotRoom(participantIDs []string) []ParticipantQuality {
	out := make([]ParticipantQuality, 0, len(participantIDs))
	for _, id := range participantIDs {
		out = append(out, e.Snapshot(id))
	}
	return out
}

func sortStreams(s []StreamQuality) {
	sort.Slice(s, func(i, j int) bool {
		if s[i].Kind != s[j].Kind {
			return s[i].Kind < s[j].Kind
		}
		return s[i].TrackID < s[j].TrackID
	})
}

// overall combines the legs. It is NOT a plain average:
//   - a bad connection or bad audio drags the whole call down (→ POOR);
//   - bad video alone, with audio and connection fine, is only WARNING —
//
// the conversation still works.
func overall(pq ParticipantQuality) Status {
	base := pq.Connection.Status // audio + connection
	video := Status(Unknown)

	for _, s := range append(append([]StreamQuality{}, pq.Outbound...), pq.Inbound...) {
		switch s.Kind {
		case Audio:
			base = worst(base, s.Status)
		case Video:
			video = worst(video, s.Status)
		}
	}

	if video == Poor {
		return worst(base, Warning) // video POOR caps at WARNING on its own
	}
	return worst(base, video) // video GOOD/WARNING contributes normally
}
