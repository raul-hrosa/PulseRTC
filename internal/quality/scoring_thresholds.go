// Package quality is the PulseRTC Media Quality / QoE engine.
//
// It turns raw WebRTC metrics into an explainable verdict:
//
//	raw sample ─► derive (deltas) ─► window (mean of recent) ─► analyze
//	          ─► score + status + problems ─► hysteresis ─► stable status ─► events
//
// Design rules:
//   - raw metrics and business rules are kept apart: Sample/Derived carry
//     numbers, Config carries every threshold, analyzer.go carries the rules;
//   - no verdict from a single sample — a short window plus hysteresis guard
//     against false positives and flapping;
//   - every classification can name the metric that caused it (Problems).
package quality

import "time"

// Config centralises every tunable. Nothing in the engine hard-codes a
// threshold; tests build a Config and assert against it. See
// docs/perf/quality-model.md for the rationale of each number.
type Config struct {
	// SamplingInterval is the cadence the browser is expected to report at.
	// The engine is robust to jitter around it (it works on real deltas).
	SamplingInterval time.Duration

	// WindowSamples is how many recent Derived samples are averaged before a
	// verdict is computed (false-positive guard).
	WindowSamples int

	// HistorySize caps the per-stream status history kept in memory.
	HistorySize int

	// DegradeSamples / RecoverSamples: consecutive windowed verdicts at a new
	// status required before the *stable* status moves (hysteresis).
	// Recovery is deliberately slower than degradation.
	DegradeSamples int
	RecoverSamples int

	// Score bands: status from the 0..100 score.
	GoodScore    int // score >= GoodScore  -> GOOD
	WarningScore int // score >= WarningScore -> WARNING, else POOR

	Audio      MediaThresholds
	Video      MediaThresholds
	Connection MediaThresholds
}

// MediaThresholds holds the warn/poor boundary for each metric of one media
// kind. A zero value for a pair means "do not evaluate this metric".
//
// Units: Loss in %, Jitter/RTT in ms, Bitrate in bits/s, FPS in frames/s,
// FrameDrop in %.
type MediaThresholds struct {
	LossWarn, LossPoor           float64
	JitterWarn, JitterPoor       float64
	RTTWarn, RTTPoor             float64
	BitrateWarn, BitratePoor     float64 // lower is worse
	FPSWarn, FPSPoor             float64 // lower is worse
	FrameDropWarn, FrameDropPoor float64
}

// DefaultConfig is the shipped configuration. See docs/perf/quality-model.md.
func DefaultConfig() Config {
	return Config{
		SamplingInterval: time.Second,
		WindowSamples:    5,
		HistorySize:      60,
		DegradeSamples:   3,
		RecoverSamples:   5,
		GoodScore:        80,
		WarningScore:     55,

		Audio: MediaThresholds{
			LossWarn: 2, LossPoor: 5,
			JitterWarn: 30, JitterPoor: 60,
			RTTWarn: 200, RTTPoor: 400,
			BitrateWarn: 16_000, BitratePoor: 8_000,
		},
		Video: MediaThresholds{
			LossWarn: 2, LossPoor: 5,
			JitterWarn: 40, JitterPoor: 80,
			RTTWarn: 250, RTTPoor: 450,
			BitrateWarn: 150_000, BitratePoor: 60_000,
			FPSWarn: 15, FPSPoor: 8,
			FrameDropWarn: 5, FrameDropPoor: 15,
		},
		Connection: MediaThresholds{
			LossWarn: 3, LossPoor: 7,
			JitterWarn: 40, JitterPoor: 80,
			RTTWarn: 200, RTTPoor: 400,
		},
	}
}

func (c Config) thresholds(k Kind) MediaThresholds {
	switch k {
	case Audio:
		return c.Audio
	case Video:
		return c.Video
	default:
		return c.Connection
	}
}
