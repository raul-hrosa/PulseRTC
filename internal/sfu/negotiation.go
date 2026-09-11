package sfu

import "time"

// negotiationDebounce is how long negotiate() waits after being requested
// before it actually runs CreateOffer/SetLocalDescription. Multiple
// subscribe()/unsubscribe() calls landing on the same Participant within this
// window (e.g. several publishers coming online at once) collapse into a
// single renegotiation instead of one per call.
//
// Chosen empirically — small enough not to add perceptible
// latency to a lone subscribe, large enough to catch the handful of calls
// that land within the same event-loop tick during a join burst. See
// docs/perf-negotiation.md for the before/after benchmark.
const negotiationDebounce = 8 * time.Millisecond

// negotiationLimiter bounds how many SDP operations (CreateOffer/CreateAnswer,
// SetLocalDescription, SetRemoteDescription) run concurrently across the whole
// SFU, regardless of which Participant they belong to.
//
// A CPU profile under load showed ~24% of process CPU in
// PeerConnection.SetRemoteDescription -> updateFromRemoteDescription (SDP
// parsing). That cost is paid once per negotiation round; with dozens of
// PeerConnections renegotiating within the same second (a join burst), all of
// them compete for the same handful of CPU cores at once. This limiter does
// not change per-PeerConnection semantics — exactly one negotiation is still
// in flight per Participant (unchanged), Perfect Negotiation is unchanged —
// it only caps how many participants' SDP work happens in parallel
// server-wide, turning a thundering herd into a controlled queue.
type negotiationLimiter struct {
	sem chan struct{}
}

func newNegotiationLimiter(n int) *negotiationLimiter {
	if n < 1 {
		n = 1
	}
	return &negotiationLimiter{sem: make(chan struct{}, n)}
}

func (l *negotiationLimiter) acquire() { l.sem <- struct{}{} }
func (l *negotiationLimiter) release() { <-l.sem }
