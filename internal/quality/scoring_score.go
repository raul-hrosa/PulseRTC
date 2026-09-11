package quality

import "math"

// Scoring strategy (documented in docs/perf/quality-model.md):
//
// Every stream starts at 100. Each evaluated metric subtracts a penalty that is
// zero up to its "warn" threshold, then ramps linearly to a per-metric maximum
// at its "poor" threshold (and stays there beyond). The penalty maxima are the
// weights — they are chosen so that a single metric at "poor" cannot by itself
// pull a stream below the POOR band, but two can. Packet loss carries the
// heaviest weight because it is the metric users notice first.
//
// Final status = worst( status-from-score , worst-single-metric-band ). So one
// metric genuinely in the POOR band forces at least WARNING even if the score
// maths would round up.
const (
	penLoss      = 45
	penJitter    = 25
	penRTT       = 25
	penBitrate   = 30
	penFPS       = 30
	penFrameDrop = 25
)

// band returns 0 (good), 1 (warn), 2 (poor) for a "higher is worse" metric.
func band(v, warn, poor float64) int {
	switch {
	case v >= poor:
		return 2
	case v >= warn:
		return 1
	default:
		return 0
	}
}

// bandLow is band for a "lower is worse" metric (bitrate, fps).
func bandLow(v, warn, poor float64) int {
	switch {
	case v <= poor:
		return 2
	case v <= warn:
		return 1
	default:
		return 0
	}
}

// penalty ramps 0 → max between warn and poor for a "higher is worse" metric.
func penalty(v, warn, poor, max float64) float64 {
	if warn <= 0 && poor <= 0 {
		return 0
	}
	if v <= warn {
		return 0
	}
	if v >= poor {
		return max
	}
	return max * (v - warn) / (poor - warn)
}

// penaltyLow ramps 0 → max between warn and poor for a "lower is worse" metric.
func penaltyLow(v, warn, poor, max float64) float64 {
	if warn <= 0 && poor <= 0 {
		return 0
	}
	if v >= warn {
		return 0
	}
	if v <= poor {
		return max
	}
	return max * (warn - v) / (warn - poor)
}

func clampScore(s float64) int {
	if math.IsNaN(s) {
		return 0
	}
	return int(math.Round(math.Max(0, math.Min(100, s))))
}

func (c Config) statusFromScore(score int) Status {
	switch {
	case score >= c.GoodScore:
		return Good
	case score >= c.WarningScore:
		return Warning
	default:
		return Poor
	}
}

func statusFromBand(b int) Status {
	switch b {
	case 2:
		return Poor
	case 1:
		return Warning
	default:
		return Good
	}
}
