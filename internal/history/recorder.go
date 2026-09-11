// Package history keeps a lightweight, in-memory summary of every call (room)
// and its participants. The summary is finalized and emitted as a structured
// log the moment the room is torn down — before the live Room / SFU / Quality
// Engine state is forgotten — so a finished call is not lost just because
// /quality and /sfu/stats only report live rooms.
//
// It stores aggregates and event counts only, never individual quality_report
// samples, and it adds no database: a finalized summary is logged and then
// released from memory (§16). Durable, queryable history is a later phase.
package history

import (
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Quality status labels. history takes these as plain strings to avoid
// importing the Quality Engine; they mirror quality.Status.
const (
	statusGood    = "GOOD"
	statusWarning = "WARNING"
	statusPoor    = "POOR"
)

// NegotiationProbe reports the process-global SFU negotiation counters
// (started, failed). history diffs a snapshot taken when a room opens against
// one taken when it closes. With multiple rooms open at once the per-room
// figure is approximate — it is deliberately derived from the official
// counters (§8) rather than a parallel counter that could drift.
type NegotiationProbe func() (started, failed int64)

type counters struct {
	summaries        atomic.Int64
	reconnects       atomic.Int64
	qualityDegraded  atomic.Int64
	qualityRecovered atomic.Int64
}

// MetricsSnapshot is the low-cardinality view fed to /metrics. No roomId /
// participantId labels (§17).
type MetricsSnapshot struct {
	RoomSummariesTotal    int64 `json:"roomSummariesTotal"`
	ReconnectsTotal       int64 `json:"reconnectsTotal"`
	QualityDegradedTotal  int64 `json:"qualityDegradedTotal"`
	QualityRecoveredTotal int64 `json:"qualityRecoveredTotal"`
}

// Recorder is safe for concurrent use. Every method is a no-op on a nil
// Recorder so callers need not guard the wiring.
type Recorder struct {
	logger *slog.Logger
	now    func() time.Time
	neg    NegotiationProbe

	// onFinalize, when set, receives every finalized summary. Nil in
	// production; tests use it to inspect the record without parsing logs.
	onFinalize func(*RoomSummary)

	mu    sync.Mutex
	rooms map[string]*RoomSummary

	metrics counters
}

// NewRecorder builds a Recorder. A nil logger falls back to slog.Default(); a
// nil probe reports zero negotiations.
func NewRecorder(logger *slog.Logger, neg NegotiationProbe) *Recorder {
	if logger == nil {
		logger = slog.Default()
	}
	if neg == nil {
		neg = func() (int64, int64) { return 0, 0 }
	}
	return &Recorder{
		logger: logger,
		now:    time.Now,
		neg:    neg,
		rooms:  make(map[string]*RoomSummary),
	}
}

// EnsureRoom opens a summary for roomID if none is currently open. Idempotent.
func (r *Recorder) EnsureRoom(roomID string) {
	if r == nil || roomID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rooms[roomID]; ok {
		return
	}
	started, failed := r.neg()
	r.rooms[roomID] = &RoomSummary{
		RoomID:       roomID,
		StartedAt:    r.now(),
		Participants: make(map[string]*ParticipantSummary),
		negBaseStart: started,
		negBaseFail:  failed,
	}
}

// ParticipantJoined records a (re)join of the logical participant. reconnect is
// the server's own hint (session recovery); a rejoin of a participant we
// already know is also treated as a reconnect.
func (r *Recorder) ParticipantJoined(roomID, logicalID, name string, reconnect bool) {
	if r == nil || roomID == "" || logicalID == "" {
		return
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	rs := r.rooms[roomID]
	if rs == nil {
		return
	}
	p := rs.Participants[logicalID]
	wasConnected := p != nil && p.connected
	if p == nil {
		p = &ParticipantSummary{
			ParticipantID: logicalID,
			Name:          name,
			FirstJoinedAt: now,
			Degradations:  make(map[string]int),
			qualityStatus: statusGood,
			qualitySince:  now,
			LastStatus:    statusGood,
		}
		rs.Participants[logicalID] = p
	} else {
		if name != "" && p.Name == "" {
			p.Name = name
		}
		if reconnect || !p.connected {
			p.Reconnects++
			rs.Reconnects++
			r.metrics.reconnects.Add(1)
		}
		p.LastLeftAt = time.Time{}
		p.qualitySince = now
	}
	p.connected = true
	if !wasConnected {
		rs.activeCount++
		if rs.activeCount > rs.ParticipantsPeak {
			rs.ParticipantsPeak = rs.activeCount
		}
	}
}

// ParticipantLeft records a disconnect. The quality interval in progress is
// closed; it resumes on the next join.
func (r *Recorder) ParticipantLeft(roomID, logicalID string) {
	if r == nil || roomID == "" || logicalID == "" {
		return
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	rs := r.rooms[roomID]
	if rs == nil {
		return
	}
	p := rs.Participants[logicalID]
	if p == nil || !p.connected {
		return
	}
	r.accrueQuality(p, now)
	p.connected = false
	p.LastLeftAt = now
	rs.activeCount--
}

// RecordQualityEvent folds one stable Quality Engine transition into the
// summary. evType is "quality_degraded" / "quality_recovered" / "quality_changed";
// reason is the engine's own reason string; status is the target status.
func (r *Recorder) RecordQualityEvent(roomID, logicalID, evType, reason, status string) {
	if r == nil || roomID == "" || logicalID == "" {
		return
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	rs := r.rooms[roomID]
	if rs == nil {
		return
	}
	p := rs.Participants[logicalID]
	if p == nil {
		return
	}
	if p.connected {
		r.accrueQuality(p, now)
	}
	if s := normStatus(status); s != "" {
		p.qualityStatus = s
		p.LastStatus = s
	}
	switch evType {
	case "quality_degraded":
		p.DegradationCount++
		if reason == "" {
			reason = "UNKNOWN"
		}
		p.Degradations[reason]++
		r.metrics.qualityDegraded.Add(1)
	case "quality_recovered":
		p.Recoveries++
		r.metrics.qualityRecovered.Add(1)
	}
}

// RecordMediaSample folds one per-second browser sample's RTT / jitter into the
// running aggregates. Both values are optional.
func (r *Recorder) RecordMediaSample(roomID, logicalID string, rttMs, jitterMs *float64) {
	if r == nil || roomID == "" || logicalID == "" || (rttMs == nil && jitterMs == nil) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rs := r.rooms[roomID]
	if rs == nil {
		return
	}
	p := rs.Participants[logicalID]
	if p == nil {
		return
	}
	if rttMs != nil {
		p.rttSum += *rttMs
		p.rttN++
		if !p.rttSeen || *rttMs > p.rttMax {
			p.rttMax = *rttMs
		}
		p.rttSeen = true
	}
	if jitterMs != nil {
		p.jitterSum += *jitterMs
		p.jitterN++
		if !p.jitterSeen || *jitterMs > p.jitterMax {
			p.jitterMax = *jitterMs
		}
		p.jitterSeen = true
	}
}

// RoomClosed finalizes the summary for roomID, emits it, and releases it from
// memory. No-op if the room has no open summary.
func (r *Recorder) RoomClosed(roomID string) {
	if r == nil || roomID == "" {
		return
	}
	r.mu.Lock()
	rs := r.rooms[roomID]
	if rs == nil {
		r.mu.Unlock()
		return
	}
	delete(r.rooms, roomID)

	end := r.now()
	rs.EndedAt = end
	started, failed := r.neg()
	rs.NegotiationsTotal = nonNeg(started - rs.negBaseStart)
	rs.NegotiationFailures = nonNeg(failed - rs.negBaseFail)
	for _, p := range rs.Participants {
		if p.connected {
			r.accrueQuality(p, end)
			p.connected = false
		}
		if p.LastLeftAt.IsZero() {
			p.LastLeftAt = end
		}
		finalizeMedia(p)
	}
	r.metrics.summaries.Add(1)
	onFinalize := r.onFinalize
	r.mu.Unlock()

	r.emit(rs)
	if onFinalize != nil {
		onFinalize(rs)
	}
}

// accrueQuality adds the time since the last quality checkpoint to the bucket
// for the status that was in effect over that interval. Caller holds r.mu.
func (r *Recorder) accrueQuality(p *ParticipantSummary, at time.Time) {
	if p.qualitySince.IsZero() {
		p.qualitySince = at
		return
	}
	d := at.Sub(p.qualitySince)
	if d < 0 {
		d = 0
	}
	switch p.qualityStatus {
	case statusWarning:
		p.QualityWarningDur += d
	case statusPoor:
		p.QualityPoorDur += d
	default:
		p.QualityGoodDur += d
	}
	p.qualitySince = at
}

// MetricsSnapshot returns the process-wide history counters.
func (r *Recorder) MetricsSnapshot() MetricsSnapshot {
	if r == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{
		RoomSummariesTotal:    r.metrics.summaries.Load(),
		ReconnectsTotal:       r.metrics.reconnects.Load(),
		QualityDegradedTotal:  r.metrics.qualityDegraded.Load(),
		QualityRecoveredTotal: r.metrics.qualityRecovered.Load(),
	}
}

func (r *Recorder) emit(rs *RoomSummary) {
	r.logger.Info("room_summary",
		"roomId", rs.RoomID,
		"startedAt", rs.StartedAt.UTC().Format(time.RFC3339),
		"endedAt", rs.EndedAt.UTC().Format(time.RFC3339),
		"durationSeconds", int64(rs.Duration().Seconds()),
		"participantsPeak", rs.ParticipantsPeak,
		"participants", len(rs.Participants),
		"reconnects", rs.Reconnects,
		"negotiations", rs.NegotiationsTotal,
		"negotiationFailures", rs.NegotiationFailures,
	)
	for _, id := range sortedKeys(rs.Participants) {
		p := rs.Participants[id]
		attrs := []any{
			"roomId", rs.RoomID,
			"participantId", p.ParticipantID,
			"durationSeconds", int64(p.Duration().Seconds()),
			"reconnects", p.Reconnects,
			"qualityGoodSeconds", int64(p.QualityGoodDur.Seconds()),
			"qualityWarningSeconds", int64(p.QualityWarningDur.Seconds()),
			"qualityPoorSeconds", int64(p.QualityPoorDur.Seconds()),
			"degradationCount", p.DegradationCount,
			"degradations", degradationsString(p.Degradations),
			"recoveries", p.Recoveries,
			"lastStatus", p.LastStatus,
		}
		if p.RTTMsAvg != nil {
			attrs = append(attrs, "rttMsAvg", round1(*p.RTTMsAvg), "rttMsMax", round1(*p.RTTMsMax))
		}
		if p.JitterMsAvg != nil {
			attrs = append(attrs, "jitterMsAvg", round1(*p.JitterMsAvg), "jitterMsMax", round1(*p.JitterMsMax))
		}
		r.logger.Info("participant_summary", attrs...)
	}
}

func finalizeMedia(p *ParticipantSummary) {
	if p.rttN > 0 {
		avg := p.rttSum / float64(p.rttN)
		mx := p.rttMax
		p.RTTMsAvg = &avg
		p.RTTMsMax = &mx
	}
	if p.jitterN > 0 {
		avg := p.jitterSum / float64(p.jitterN)
		mx := p.jitterMax
		p.JitterMsAvg = &avg
		p.JitterMsMax = &mx
	}
}

func normStatus(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case statusGood:
		return statusGood
	case statusWarning:
		return statusWarning
	case statusPoor:
		return statusPoor
	default:
		return ""
	}
}

func degradationsString(m map[string]int) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+strconv.Itoa(m[k]))
	}
	return strings.Join(parts, ",")
}

func sortedKeys(m map[string]*ParticipantSummary) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func nonNeg(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

func round1(v float64) float64 { return float64(int64(v*10+0.5)) / 10 }
