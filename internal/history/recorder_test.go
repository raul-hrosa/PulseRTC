package history

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// clock is a deterministic time source.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestRecorder returns a recorder wired to a deterministic clock and a
// channel-free capture of the finalized summary.
func newTestRecorder(t *testing.T, neg NegotiationProbe) (*Recorder, *clock, *[]*RoomSummary) {
	t.Helper()
	clk := newClock()
	r := NewRecorder(slog.New(slog.NewTextHandler(io.Discard, nil)), neg)
	r.now = clk.now
	var got []*RoomSummary
	var mu sync.Mutex
	r.onFinalize = func(rs *RoomSummary) {
		mu.Lock()
		got = append(got, rs)
		mu.Unlock()
	}
	return r, clk, &got
}

func lastSummary(t *testing.T, got *[]*RoomSummary) *RoomSummary {
	t.Helper()
	if len(*got) == 0 {
		t.Fatal("no summary was finalized")
	}
	return (*got)[len(*got)-1]
}

func TestRoomLifecycle(t *testing.T) {
	r, clk, got := newTestRecorder(t, nil)

	r.EnsureRoom("room-1")
	r.EnsureRoom("room-1") // idempotent
	r.ParticipantJoined("room-1", "alice", "Alice", false)
	clk.advance(2 * time.Second)
	r.ParticipantJoined("room-1", "bob", "Bob", false)
	clk.advance(10 * time.Second)
	r.ParticipantLeft("room-1", "bob")
	clk.advance(3 * time.Second)
	r.ParticipantLeft("room-1", "alice")
	r.RoomClosed("room-1")

	rs := lastSummary(t, got)
	if rs.ParticipantsPeak != 2 {
		t.Errorf("peak = %d, want 2", rs.ParticipantsPeak)
	}
	if got, want := rs.Duration(), 15*time.Second; got != want {
		t.Errorf("room duration = %s, want %s", got, want)
	}
	if len(rs.Participants) != 2 {
		t.Fatalf("participants = %d, want 2", len(rs.Participants))
	}
	if d := rs.Participants["alice"].Duration(); d != 15*time.Second {
		t.Errorf("alice duration = %s, want 15s", d)
	}
	if d := rs.Participants["bob"].Duration(); d != 10*time.Second {
		t.Errorf("bob duration = %s, want 10s", d)
	}
}

func TestRoomClosedUnknownRoomIsNoop(t *testing.T) {
	r, _, got := newTestRecorder(t, nil)
	r.RoomClosed("never-opened")
	if len(*got) != 0 {
		t.Fatalf("finalized %d summaries, want 0", len(*got))
	}
}

func TestEmptyRoomClose(t *testing.T) {
	r, clk, got := newTestRecorder(t, nil)
	r.EnsureRoom("room-1")
	clk.advance(time.Second)
	r.RoomClosed("room-1")

	rs := lastSummary(t, got)
	if len(rs.Participants) != 0 {
		t.Errorf("participants = %d, want 0", len(rs.Participants))
	}
	if rs.EndedAt.IsZero() {
		t.Error("EndedAt not set")
	}
}

func TestQuickJoinLeave(t *testing.T) {
	r, _, got := newTestRecorder(t, nil)
	r.EnsureRoom("r")
	r.ParticipantJoined("r", "a", "", false)
	r.ParticipantLeft("r", "a")
	r.RoomClosed("r")

	rs := lastSummary(t, got)
	if rs.ParticipantsPeak != 1 {
		t.Errorf("peak = %d, want 1", rs.ParticipantsPeak)
	}
	if rs.Participants["a"].Duration() != 0 {
		t.Errorf("duration = %s, want 0", rs.Participants["a"].Duration())
	}
}

func TestReconnectCounting(t *testing.T) {
	r, clk, got := newTestRecorder(t, nil)
	r.EnsureRoom("r")
	r.ParticipantJoined("r", "a", "A", false)
	clk.advance(5 * time.Second)
	r.ParticipantLeft("r", "a")
	clk.advance(2 * time.Second)
	r.ParticipantJoined("r", "a", "A", false) // rejoin -> reconnect
	clk.advance(5 * time.Second)
	r.ParticipantLeft("r", "a")
	r.RoomClosed("r")

	rs := lastSummary(t, got)
	if len(rs.Participants) != 1 {
		t.Fatalf("participants = %d, want 1 (logical identity preserved)", len(rs.Participants))
	}
	p := rs.Participants["a"]
	if p.Reconnects != 1 {
		t.Errorf("reconnects = %d, want 1", p.Reconnects)
	}
	if rs.Reconnects != 1 {
		t.Errorf("room reconnects = %d, want 1", rs.Reconnects)
	}
	if p.Duration() != 12*time.Second {
		t.Errorf("duration = %s, want 12s (first join to last leave)", p.Duration())
	}
	if rs.ParticipantsPeak != 1 {
		t.Errorf("peak = %d, want 1", rs.ParticipantsPeak)
	}
}

func TestMultipleReconnects(t *testing.T) {
	r, _, got := newTestRecorder(t, nil)
	r.EnsureRoom("r")
	for i := 0; i < 3; i++ {
		r.ParticipantJoined("r", "a", "", false)
		r.ParticipantLeft("r", "a")
	}
	r.RoomClosed("r")

	if p := lastSummary(t, got).Participants["a"]; p.Reconnects != 2 {
		t.Errorf("reconnects = %d, want 2", p.Reconnects)
	}
}

func TestReconnectHintCountsEvenIfStillConnected(t *testing.T) {
	r, _, got := newTestRecorder(t, nil)
	r.EnsureRoom("r")
	r.ParticipantJoined("r", "a", "", false)
	// server says this is a recovery even though we never saw the leave
	r.ParticipantJoined("r", "a", "", true)
	r.ParticipantLeft("r", "a")
	r.RoomClosed("r")

	if p := lastSummary(t, got).Participants["a"]; p.Reconnects != 1 {
		t.Errorf("reconnects = %d, want 1", p.Reconnects)
	}
}

func TestQoEDurationsAndEvents(t *testing.T) {
	r, clk, got := newTestRecorder(t, nil)
	r.EnsureRoom("r")
	r.ParticipantJoined("r", "a", "", false)

	clk.advance(10 * time.Second)
	r.RecordQualityEvent("r", "a", "quality_degraded", "HIGH_PACKET_LOSS", "WARNING")
	clk.advance(5 * time.Second)
	r.RecordQualityEvent("r", "a", "quality_degraded", "LOW_BITRATE", "POOR")
	clk.advance(5 * time.Second)
	r.RecordQualityEvent("r", "a", "quality_recovered", "", "GOOD")
	clk.advance(10 * time.Second)
	r.ParticipantLeft("r", "a")
	r.RoomClosed("r")

	p := lastSummary(t, got).Participants["a"]
	if p.QualityGoodDur != 20*time.Second {
		t.Errorf("good = %s, want 20s", p.QualityGoodDur)
	}
	if p.QualityWarningDur != 5*time.Second {
		t.Errorf("warning = %s, want 5s", p.QualityWarningDur)
	}
	if p.QualityPoorDur != 5*time.Second {
		t.Errorf("poor = %s, want 5s", p.QualityPoorDur)
	}
	if p.DegradationCount != 2 {
		t.Errorf("degradationCount = %d, want 2", p.DegradationCount)
	}
	if p.Degradations["HIGH_PACKET_LOSS"] != 1 || p.Degradations["LOW_BITRATE"] != 1 {
		t.Errorf("degradations by reason = %v", p.Degradations)
	}
	if p.Recoveries != 1 {
		t.Errorf("recoveries = %d, want 1", p.Recoveries)
	}
	if p.LastStatus != "GOOD" {
		t.Errorf("lastStatus = %q, want GOOD", p.LastStatus)
	}
}

func TestQoENoSamplesStaysGood(t *testing.T) {
	r, clk, got := newTestRecorder(t, nil)
	r.EnsureRoom("r")
	r.ParticipantJoined("r", "a", "", false)
	clk.advance(30 * time.Second)
	r.ParticipantLeft("r", "a")
	r.RoomClosed("r")

	p := lastSummary(t, got).Participants["a"]
	if p.QualityGoodDur != 30*time.Second {
		t.Errorf("good = %s, want 30s", p.QualityGoodDur)
	}
	if p.QualityWarningDur != 0 || p.QualityPoorDur != 0 {
		t.Errorf("warning/poor should be zero, got %s / %s", p.QualityWarningDur, p.QualityPoorDur)
	}
	if p.RTTMsAvg != nil || p.JitterMsAvg != nil {
		t.Error("media aggregates should be nil with no samples")
	}
}

func TestQoEDurationsNotAccruedWhileDisconnected(t *testing.T) {
	r, clk, got := newTestRecorder(t, nil)
	r.EnsureRoom("r")
	r.ParticipantJoined("r", "a", "", false)
	clk.advance(10 * time.Second)
	r.ParticipantLeft("r", "a")
	clk.advance(100 * time.Second) // gap while away must not count
	r.ParticipantJoined("r", "a", "", false)
	clk.advance(10 * time.Second)
	r.ParticipantLeft("r", "a")
	r.RoomClosed("r")

	p := lastSummary(t, got).Participants["a"]
	if p.QualityGoodDur != 20*time.Second {
		t.Errorf("good = %s, want 20s (gap excluded)", p.QualityGoodDur)
	}
}

func TestMediaAggregates(t *testing.T) {
	r, _, got := newTestRecorder(t, nil)
	r.EnsureRoom("r")
	r.ParticipantJoined("r", "a", "", false)
	for _, v := range []float64{10, 20, 30} {
		v := v
		r.RecordMediaSample("r", "a", &v, nil)
	}
	j := 4.0
	r.RecordMediaSample("r", "a", nil, &j)
	r.ParticipantLeft("r", "a")
	r.RoomClosed("r")

	p := lastSummary(t, got).Participants["a"]
	if p.RTTMsAvg == nil || *p.RTTMsAvg != 20 {
		t.Errorf("rtt avg = %v, want 20", p.RTTMsAvg)
	}
	if p.RTTMsMax == nil || *p.RTTMsMax != 30 {
		t.Errorf("rtt max = %v, want 30", p.RTTMsMax)
	}
	if p.JitterMsAvg == nil || *p.JitterMsAvg != 4 {
		t.Errorf("jitter avg = %v, want 4", p.JitterMsAvg)
	}
}

func TestNegotiationDelta(t *testing.T) {
	started, failed := int64(5), int64(1)
	r, _, got := newTestRecorder(t, func() (int64, int64) { return started, failed })
	r.EnsureRoom("r")
	r.ParticipantJoined("r", "a", "", false)
	started, failed = 55, 3
	r.ParticipantLeft("r", "a")
	r.RoomClosed("r")

	rs := lastSummary(t, got)
	if rs.NegotiationsTotal != 50 {
		t.Errorf("negotiations = %d, want 50", rs.NegotiationsTotal)
	}
	if rs.NegotiationFailures != 2 {
		t.Errorf("negotiation failures = %d, want 2", rs.NegotiationFailures)
	}
}

func TestNegotiationDeltaNeverNegative(t *testing.T) {
	n := int64(100)
	r, _, got := newTestRecorder(t, func() (int64, int64) { return n, 0 })
	r.EnsureRoom("r")
	r.ParticipantJoined("r", "a", "", false)
	n = 90 // counter went backwards (e.g. process restarted between calls)
	r.ParticipantLeft("r", "a")
	r.RoomClosed("r")
	if rs := lastSummary(t, got); rs.NegotiationsTotal != 0 {
		t.Errorf("negotiations = %d, want 0", rs.NegotiationsTotal)
	}
}

func TestMetricsSnapshot(t *testing.T) {
	r, _, _ := newTestRecorder(t, nil)
	r.EnsureRoom("r")
	r.ParticipantJoined("r", "a", "", false)
	r.ParticipantLeft("r", "a")
	r.ParticipantJoined("r", "a", "", false)
	r.RecordQualityEvent("r", "a", "quality_degraded", "LOW_BITRATE", "POOR")
	r.RecordQualityEvent("r", "a", "quality_recovered", "", "GOOD")
	r.ParticipantLeft("r", "a")
	r.RoomClosed("r")

	m := r.MetricsSnapshot()
	if m.RoomSummariesTotal != 1 {
		t.Errorf("summaries = %d, want 1", m.RoomSummariesTotal)
	}
	if m.ReconnectsTotal != 1 {
		t.Errorf("reconnects = %d, want 1", m.ReconnectsTotal)
	}
	if m.QualityDegradedTotal != 1 || m.QualityRecoveredTotal != 1 {
		t.Errorf("degraded/recovered = %d/%d, want 1/1", m.QualityDegradedTotal, m.QualityRecoveredTotal)
	}
}

func TestNilRecorderIsSafe(t *testing.T) {
	var r *Recorder
	r.EnsureRoom("r")
	r.ParticipantJoined("r", "a", "", false)
	r.ParticipantLeft("r", "a")
	r.RecordQualityEvent("r", "a", "quality_degraded", "X", "POOR")
	r.RecordMediaSample("r", "a", nil, nil)
	r.RoomClosed("r")
	if got := r.MetricsSnapshot(); got != (MetricsSnapshot{}) {
		t.Errorf("nil snapshot = %+v", got)
	}
}

func TestDegradationsString(t *testing.T) {
	got := degradationsString(map[string]int{"LOW_BITRATE": 3, "HIGH_RTT": 1})
	if got != "HIGH_RTT=1,LOW_BITRATE=3" {
		t.Errorf("got %q", got)
	}
	if degradationsString(nil) != "" {
		t.Error("nil map should render empty")
	}
}

func TestConcurrentAccess(t *testing.T) {
	r := NewRecorder(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			room := "room-" + string(rune('a'+g%5))
			for i := 0; i < 200; i++ {
				r.EnsureRoom(room)
				r.ParticipantJoined(room, "p", "", false)
				r.RecordMediaSample(room, "p", ptr(12.5), ptr(1.0))
				r.RecordQualityEvent(room, "p", "quality_degraded", "LOW_BITRATE", "WARNING")
				r.RecordQualityEvent(room, "p", "quality_recovered", "", "GOOD")
				r.ParticipantLeft(room, "p")
				r.RoomClosed(room)
			}
		}(g)
	}
	wg.Wait()

	if r.MetricsSnapshot().RoomSummariesTotal == 0 {
		t.Error("expected some summaries to be finalized")
	}
}

func ptr(f float64) *float64 { return &f }
