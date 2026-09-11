package auth

import "sync/atomic"

// Metrics are the authentication observability counters. All
// monotonic; exposed as a snapshot under "auth" in /metrics. Tokens and secrets
// never appear here or in any log line.
type Metrics struct {
	success          atomic.Int64
	failed           atomic.Int64
	tokenExpired     atomic.Int64
	permissionDenied atomic.Int64
	roomDenied       atomic.Int64
	rateLimited      atomic.Int64
}

// Snapshot is the JSON shape reported by /metrics.
type Snapshot struct {
	Success          int64 `json:"success"`
	Failed           int64 `json:"failed"`
	TokenExpired     int64 `json:"tokenExpired"`
	PermissionDenied int64 `json:"permissionDenied"`
	RoomDenied       int64 `json:"roomDenied"`
	RateLimited      int64 `json:"rateLimited"`
}

func (m *Metrics) recordSuccess()   { m.success.Add(1) }
func (m *Metrics) recordRateLimit() { m.rateLimited.Add(1) }

// recordFailure classifies a validation/authorization failure by its code.
func (m *Metrics) recordFailure(code string) {
	m.failed.Add(1)
	switch code {
	case CodeExpiredToken:
		m.tokenExpired.Add(1)
	case CodeRoomAccessDenied:
		m.roomDenied.Add(1)
	case CodeJoinDenied, CodePublishDenied, CodeSubscribeDenied, CodeControlDenied:
		m.permissionDenied.Add(1)
	}
}

// Snapshot returns the current counter values.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		Success:          m.success.Load(),
		Failed:           m.failed.Load(),
		TokenExpired:     m.tokenExpired.Load(),
		PermissionDenied: m.permissionDenied.Load(),
		RoomDenied:       m.roomDenied.Load(),
		RateLimited:      m.rateLimited.Load(),
	}
}
