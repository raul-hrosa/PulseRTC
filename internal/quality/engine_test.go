package quality

import "testing"

func testEngine() *Engine {
	c := DefaultConfig()
	c.WindowSamples = 1
	c.DegradeSamples = 2
	c.RecoverSamples = 2
	return New(c)
}

// feeder emits cumulative video samples for one stream.
type feeder struct {
	e    *Engine
	key  StreamKey
	t    int64
	recv float64
	lost float64
	byts float64
}

func (f *feeder) step(lossPct, jitterMs, rttMs, bitrateBps float64) []Event {
	f.t += 1000
	dRecv := 1000.0
	dLost := 0.0
	if lossPct > 0 {
		dLost = lossPct / 100 * dRecv / (1 - lossPct/100)
	}
	f.recv += dRecv
	f.lost += dLost
	f.byts += bitrateBps / 8
	return f.e.Ingest(Sample{
		Key: f.key, AtMillis: f.t,
		PacketsReceived: ptr(f.recv), PacketsLost: ptr(f.lost), BytesReceived: ptr(f.byts),
		JitterMs: ptr(jitterMs), RTTMs: ptr(rttMs), FPS: ptr(30.0), Enabled: ptr(true),
	})
}

func (f *feeder) stepN(n int, lossPct float64) (ev []Event) {
	for i := 0; i < n; i++ {
		ev = append(ev, f.step(lossPct, 10, 40, 1_000_000)...)
	}
	return
}

func lastType(ev []Event) string {
	if len(ev) == 0 {
		return ""
	}
	return ev[len(ev)-1].Type
}

func TestEngineDegradeAndRecoverEvents(t *testing.T) {
	e := testEngine()
	f := &feeder{e: e, key: StreamKey{Participant: "A", Direction: Inbound, Kind: Video, TrackID: "pub-1"}}

	// acquire GOOD
	f.stepN(3, 0)
	if s := e.Snapshot("A"); len(s.Inbound) != 1 || s.Inbound[0].Status != Good {
		t.Fatalf("expected inbound video GOOD, got %+v", s.Inbound)
	}

	// consistent 3% loss -> WARNING (degraded)
	ev := f.stepN(3, 3)
	if lastType(ev) != EventDegraded {
		t.Fatalf("expected quality_degraded, got events %+v", ev)
	}

	// consistent 8% loss -> POOR (degraded)
	ev = f.stepN(3, 8)
	if lastType(ev) != EventDegraded {
		t.Fatalf("expected quality_degraded to POOR, got %+v", ev)
	}
	if e.Snapshot("A").Inbound[0].Status != Poor {
		t.Fatalf("expected POOR, got %s", e.Snapshot("A").Inbound[0].Status)
	}

	// back to 3% -> WARNING (partial: quality_changed)
	ev = f.stepN(3, 3)
	if lastType(ev) != EventChanged {
		t.Fatalf("POOR->WARNING should be quality_changed, got %+v", ev)
	}

	// back to 0% -> GOOD (recovered)
	ev = f.stepN(3, 0)
	if lastType(ev) != EventRecovered {
		t.Fatalf("WARNING->GOOD should be quality_recovered, got %+v", ev)
	}
}

func TestEngineNoEventWithoutStableChange(t *testing.T) {
	e := testEngine()
	f := &feeder{e: e, key: StreamKey{Participant: "A", Direction: Inbound, Kind: Video, TrackID: "v"}}
	f.stepN(3, 0) // acquire GOOD (fires one quality_changed)

	// alternate 0% / 4% loss: never 2 consecutive at a new status -> no events
	var events []Event
	for i := 0; i < 8; i++ {
		loss := 0.0
		if i%2 == 0 {
			loss = 4
		}
		events = append(events, f.step(loss, 10, 40, 1_000_000)...)
	}
	if len(events) != 0 {
		t.Fatalf("oscillation produced events: %+v", events)
	}
}

func TestEngineParticipantsAreIsolated(t *testing.T) {
	e := testEngine()
	a := &feeder{e: e, key: StreamKey{Participant: "A", Direction: Inbound, Kind: Video, TrackID: "va"}}
	b := &feeder{e: e, key: StreamKey{Participant: "B", Direction: Inbound, Kind: Video, TrackID: "vb"}}

	a.stepN(3, 0)
	b.stepN(3, 0)
	a.stepN(4, 12) // A degrades hard

	if got := e.Snapshot("B").Inbound[0].Status; got != Good {
		t.Fatalf("B should be unaffected by A's degradation, got %s", got)
	}
	if got := e.Snapshot("A").Inbound[0].Status; got != Poor {
		t.Fatalf("A should be POOR, got %s", got)
	}
}

func TestEngineMultiTrackVideoPoorAudioGood(t *testing.T) {
	e := testEngine()
	vid := &feeder{e: e, key: StreamKey{Participant: "A", Direction: Outbound, Kind: Video, TrackID: "pv"}}
	aud := &feeder{e: e, key: StreamKey{Participant: "A", Direction: Outbound, Kind: Audio, TrackID: "pa"}}

	for i := 0; i < 3; i++ {
		vid.step(9, 10, 40, 800_000) // video: high loss -> POOR
		aud.step(0, 5, 40, 24_000)   // audio: clean -> GOOD
	}

	s := e.Snapshot("A")
	var audioSt, videoSt Status
	for _, st := range s.Outbound {
		if st.Kind == Audio {
			audioSt = st.Status
		}
		if st.Kind == Video {
			videoSt = st.Status
		}
	}
	if audioSt != Good {
		t.Fatalf("audio should stay GOOD despite video loss, got %s", audioSt)
	}
	if videoSt != Poor {
		t.Fatalf("video should be POOR, got %s", videoSt)
	}
	if s.Overall != Warning {
		t.Fatalf("overall should be WARNING (audio GOOD, video POOR), got %s", s.Overall)
	}
}

func TestEngineForget(t *testing.T) {
	e := testEngine()
	f := &feeder{e: e, key: StreamKey{Participant: "A", Direction: Inbound, Kind: Video, TrackID: "v"}}
	f.stepN(3, 0)
	e.Forget("A")
	s := e.Snapshot("A")
	if len(s.Inbound) != 0 || s.Overall != Unknown {
		t.Fatalf("Forget did not clear participant state: %+v", s)
	}
}

func TestEngineUnknownParticipant(t *testing.T) {
	e := testEngine()
	s := e.Snapshot("ghost")
	if s.Overall != Unknown || s.Connection.Status != Unknown {
		t.Fatalf("unknown participant should be all UNKNOWN, got %+v", s)
	}
}
