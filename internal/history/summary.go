package history

import "time"

// RoomSummary is the aggregated record of one call. It is built incrementally
// while the room is live and finalized (EndedAt set, negotiation deltas
// computed, quality intervals closed) exactly once, when the room is torn down.
type RoomSummary struct {
	RoomID              string
	StartedAt           time.Time
	EndedAt             time.Time
	ParticipantsPeak    int
	Reconnects          int
	NegotiationsTotal   int64
	NegotiationFailures int64
	Participants        map[string]*ParticipantSummary

	// internal bookkeeping (not part of the emitted record)
	activeCount  int
	negBaseStart int64
	negBaseFail  int64
}

// Duration is the wall-clock life of the room. Zero until finalized.
func (rs *RoomSummary) Duration() time.Duration {
	if rs.EndedAt.IsZero() {
		return 0
	}
	return rs.EndedAt.Sub(rs.StartedAt)
}

// ParticipantSummary is the aggregated record of one logical participant across
// every physical connection it used during the call (a reconnect does not start
// a new record — Reconnects is incremented instead).
type ParticipantSummary struct {
	ParticipantID string // logical identity (token subject), stable across reconnects
	Name          string
	FirstJoinedAt time.Time
	LastLeftAt    time.Time
	Reconnects    int

	// QoE, aggregated from stable quality transitions — never from raw samples.
	QualityGoodDur    time.Duration
	QualityWarningDur time.Duration
	QualityPoorDur    time.Duration
	DegradationCount  int
	Degradations      map[string]int // reason -> count, reasons are the Quality Engine's own
	Recoveries        int
	LastStatus        string

	// Media, aggregated from the per-second browser samples. nil when no
	// sample carried the value — a nil is not the same as 0 (§7).
	RTTMsAvg    *float64
	RTTMsMax    *float64
	JitterMsAvg *float64
	JitterMsMax *float64

	// internal bookkeeping
	connected     bool
	qualityStatus string
	qualitySince  time.Time
	rttSum        float64
	rttN          int
	rttMax        float64
	rttSeen       bool
	jitterSum     float64
	jitterN       int
	jitterMax     float64
	jitterSeen    bool
}

// Duration is the span from the first join to the last leave. Zero while the
// participant is still connected and the room has not been finalized.
func (p *ParticipantSummary) Duration() time.Duration {
	if p.LastLeftAt.IsZero() {
		return 0
	}
	return p.LastLeftAt.Sub(p.FirstJoinedAt)
}
