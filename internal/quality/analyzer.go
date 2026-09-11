package quality

// Analysis is the verdict for one stream over the current window.
type Analysis struct {
	Status   Status             `json:"status"`
	Score    int                `json:"score"`
	Problems []string           `json:"problems"`
	Metrics  map[string]float64 `json:"metrics"`
}

// analyze routes to the per-kind analyzer. The three analyzers share the same
// rule-driven core (evalRules); only their rule sets differ, which keeps every
// threshold in Config and out of the code path.
func analyze(k Kind, d Derived, cfg Config) Analysis {
	if !d.HasData {
		return Analysis{Status: Unknown, Problems: []string{}, Metrics: map[string]float64{}}
	}
	if !d.Enabled {
		return Analysis{Status: Unknown, Score: 0,
			Problems: []string{ProblemTrackDisabled}, Metrics: map[string]float64{}}
	}
	switch k {
	case Connection:
		return analyzeConnection(d, cfg)
	default:
		return evalRules(mediaRules(k, d, cfg.thresholds(k)), d, cfg)
	}
}

type rule struct {
	problem    string
	value      *float64
	warn, poor float64
	pen        float64
	lowerWorse bool
	metricKey  string
}

func mediaRules(k Kind, d Derived, t MediaThresholds) []rule {
	rules := []rule{
		{ProblemHighPacketLoss, d.LossPct, t.LossWarn, t.LossPoor, penLoss, false, "packetLossPct"},
		{ProblemHighJitter, d.JitterMs, t.JitterWarn, t.JitterPoor, penJitter, false, "jitterMs"},
		{ProblemHighRTT, d.RTTMs, t.RTTWarn, t.RTTPoor, penRTT, false, "rttMs"},
		{ProblemLowBitrate, d.BitrateBps, t.BitrateWarn, t.BitratePoor, penBitrate, true, "bitrateBps"},
	}
	if k == Video {
		rules = append(rules,
			rule{ProblemLowFPS, d.FPS, t.FPSWarn, t.FPSPoor, penFPS, true, "fps"},
			rule{ProblemHighFrameDrop, d.FrameDropPct, t.FrameDropWarn, t.FrameDropPoor, penFrameDrop, false, "frameDropPct"},
		)
	}
	return rules
}

func evalRules(rules []rule, d Derived, cfg Config) Analysis {
	score := 100.0
	problems := []string{}
	worstBand := 0
	metrics := map[string]float64{}

	for _, r := range rules {
		v, ok := fval(r.value)
		if !ok {
			continue
		}
		metrics[r.metricKey] = round2(v)

		var b int
		if r.lowerWorse {
			score -= penaltyLow(v, r.warn, r.poor, r.pen)
			b = bandLow(v, r.warn, r.poor)
		} else {
			score -= penalty(v, r.warn, r.poor, r.pen)
			b = band(v, r.warn, r.poor)
		}
		if b >= 1 {
			problems = append(problems, r.problem)
		}
		if b > worstBand {
			worstBand = b
		}
	}

	if d.Width != nil && d.Height != nil {
		metrics["width"] = *d.Width
		metrics["height"] = *d.Height
	}

	s := clampScore(score)
	return Analysis{
		Status:   worst(cfg.statusFromScore(s), statusFromBand(worstBand)),
		Score:    s,
		Problems: problems,
		Metrics:  metrics,
	}
}

func analyzeConnection(d Derived, cfg Config) Analysis {
	t := cfg.Connection

	// A non-connected transport is decisive and needs no window.
	if d.ConnState != "" && d.ConnState != "connected" {
		return Analysis{
			Status:   Poor,
			Score:    0,
			Problems: []string{ProblemConnectionUnstable},
			Metrics:  map[string]float64{},
		}
	}
	if d.ICEState == "disconnected" || d.ICEState == "failed" {
		return Analysis{
			Status:   Poor,
			Score:    10,
			Problems: []string{ProblemConnectionUnstable},
			Metrics:  map[string]float64{},
		}
	}

	rules := []rule{
		{ProblemHighRTT, d.RTTMs, t.RTTWarn, t.RTTPoor, penRTT + 10, false, "rttMs"},
		{ProblemHighPacketLoss, d.LossPct, t.LossWarn, t.LossPoor, penLoss, false, "packetLossPct"},
		{ProblemHighJitter, d.JitterMs, t.JitterWarn, t.JitterPoor, penJitter, false, "jitterMs"},
	}
	return evalRules(rules, d, cfg)
}

func round2(v float64) float64 {
	return float64(int(v*100+0.5)) / 100
}
