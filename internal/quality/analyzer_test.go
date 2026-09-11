package quality

import "testing"

func cfg() Config { return DefaultConfig() }

// derivedVideo builds a Derived directly, for analyzer-level tests.
func derivedVideo(loss, jitter, rtt, bitrate, fps, drop float64) Derived {
	return Derived{
		HasData: true, Enabled: true,
		LossPct: ptr(loss), JitterMs: ptr(jitter), RTTMs: ptr(rtt),
		BitrateBps: ptr(bitrate), FPS: ptr(fps), FrameDropPct: ptr(drop),
	}
}

func TestAnalyzeVideoGood(t *testing.T) {
	a := analyze(Video, derivedVideo(0.1, 10, 40, 1_200_000, 30, 0.5), cfg())
	if a.Status != Good {
		t.Fatalf("expected GOOD, got %s (score %d, problems %v)", a.Status, a.Score, a.Problems)
	}
	if len(a.Problems) != 0 {
		t.Fatalf("expected no problems, got %v", a.Problems)
	}
}

func TestAnalyzeVideoHighPacketLoss(t *testing.T) {
	a := analyze(Video, derivedVideo(8, 10, 40, 1_200_000, 30, 0.5), cfg())
	if a.Status != Poor {
		t.Fatalf("8%% loss should be POOR, got %s (score %d)", a.Status, a.Score)
	}
	if !contains(a.Problems, ProblemHighPacketLoss) {
		t.Fatalf("expected HIGH_PACKET_LOSS, got %v", a.Problems)
	}
}

func TestAnalyzeVideoModeratePacketLoss(t *testing.T) {
	a := analyze(Video, derivedVideo(3, 10, 40, 1_200_000, 30, 0.5), cfg())
	if a.Status != Warning {
		t.Fatalf("3%% loss should be WARNING, got %s (score %d)", a.Status, a.Score)
	}
}

func TestAnalyzeAudioNoData(t *testing.T) {
	a := analyze(Audio, Derived{HasData: false, Enabled: true}, cfg())
	if a.Status != Unknown {
		t.Fatalf("expected UNKNOWN with no data, got %s", a.Status)
	}
}

func TestAnalyzeTrackDisabled(t *testing.T) {
	a := analyze(Audio, Derived{HasData: true, Enabled: false}, cfg())
	if a.Status != Unknown || !contains(a.Problems, ProblemTrackDisabled) {
		t.Fatalf("disabled track: got %s %v", a.Status, a.Problems)
	}
}

func TestAnalyzeVideoLowFPSandBitrate(t *testing.T) {
	a := analyze(Video, derivedVideo(0.1, 10, 40, 40_000, 6, 0.5), cfg())
	if a.Status != Poor {
		t.Fatalf("low fps + low bitrate should be POOR, got %s (score %d)", a.Status, a.Score)
	}
	if !contains(a.Problems, ProblemLowFPS) || !contains(a.Problems, ProblemLowBitrate) {
		t.Fatalf("expected LOW_FPS and LOW_BITRATE, got %v", a.Problems)
	}
}

func TestAnalyzeConnectionRTT(t *testing.T) {
	good := analyzeConnection(Derived{HasData: true, Enabled: true, RTTMs: ptr(50.0), ConnState: "connected"}, cfg())
	if good.Status != Good {
		t.Fatalf("50ms RTT should be GOOD, got %s", good.Status)
	}
	warn := analyzeConnection(Derived{HasData: true, Enabled: true, RTTMs: ptr(260.0), ConnState: "connected"}, cfg())
	if warn.Status != Warning {
		t.Fatalf("260ms RTT should be WARNING, got %s (score %d)", warn.Status, warn.Score)
	}
	poorState := analyzeConnection(Derived{HasData: true, Enabled: true, ConnState: "failed"}, cfg())
	if poorState.Status != Poor || !contains(poorState.Problems, ProblemConnectionUnstable) {
		t.Fatalf("failed transport should be POOR/CONNECTION_UNSTABLE, got %s %v", poorState.Status, poorState.Problems)
	}
}

func TestSingleSpikeAveragedOut(t *testing.T) {
	// "0 0 0 8 0 0" must not be POOR.
	w := newWindow(5)
	for _, l := range []float64{0, 0, 0, 8, 0} {
		w.push(Derived{HasData: true, Enabled: true, LossPct: ptr(l),
			JitterMs: ptr(10.0), RTTMs: ptr(40.0), BitrateBps: ptr(1_000_000.0), FPS: ptr(30.0)})
	}
	a := analyze(Video, w.mean(), cfg())
	if a.Status == Poor {
		t.Fatalf("a single 8%% spike in a 5-sample window should not be POOR (got score %d)", a.Score)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
