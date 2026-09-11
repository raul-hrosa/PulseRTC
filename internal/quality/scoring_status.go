package quality

// Status is the quality verdict. Colour is a presentation concern only — the
// status is the source of truth.
type Status string

const (
	Unknown Status = "UNKNOWN" // not enough data, or the track is disabled
	Good    Status = "GOOD"
	Warning Status = "WARNING"
	Poor    Status = "POOR"
)

// rank orders the known statuses from best to worst. Unknown sits apart (-1):
// it is not "worse than GOOD", it means "no verdict".
func rank(s Status) int {
	switch s {
	case Good:
		return 0
	case Warning:
		return 1
	case Poor:
		return 2
	default:
		return -1
	}
}

// worst returns the more severe of two statuses. Unknown is ignored unless
// both are Unknown.
func worst(a, b Status) Status {
	if rank(a) < 0 {
		return b
	}
	if rank(b) < 0 {
		return a
	}
	if rank(a) >= rank(b) {
		return a
	}
	return b
}

// Problem codes name the metric behind a WARNING/POOR verdict.
const (
	ProblemHighPacketLoss     = "HIGH_PACKET_LOSS"
	ProblemHighJitter         = "HIGH_JITTER"
	ProblemHighRTT            = "HIGH_RTT"
	ProblemLowBitrate         = "LOW_BITRATE"
	ProblemLowFPS             = "LOW_FPS"
	ProblemHighFrameDrop      = "HIGH_FRAME_DROP"
	ProblemConnectionUnstable = "CONNECTION_UNSTABLE"
	ProblemTrackDisabled      = "TRACK_DISABLED"
)
