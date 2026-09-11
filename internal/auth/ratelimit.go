package auth

import (
	"sync"
	"time"
)

// RateLimiter is a per-key token bucket. Deliberately minimal:
// the goal is to blunt accidental floods and trivial abuse from a single
// source, not to be a DDoS mitigation. Keys are usually client IPs (connection
// limiter) or a fixed string (per-connection message limiter).
type RateLimiter struct {
	rate  float64 // tokens added per second
	burst float64 // bucket capacity

	mu      sync.Mutex
	buckets map[string]*bucket
	now     func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewRateLimiter allows `perWindow` events per `window`, with a burst equal to
// perWindow. A non-positive perWindow yields a limiter that allows everything.
func NewRateLimiter(perWindow int, window time.Duration) *RateLimiter {
	rl := &RateLimiter{buckets: make(map[string]*bucket), now: time.Now}
	if perWindow > 0 && window > 0 {
		rl.rate = float64(perWindow) / window.Seconds()
		rl.burst = float64(perWindow)
	}
	return rl
}

// Allow consumes one token for key and reports whether it was available.
func (rl *RateLimiter) Allow(key string) bool {
	if rl.rate == 0 {
		return true
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := rl.now()
	b, ok := rl.buckets[key]
	if !ok {
		b = &bucket{tokens: rl.burst, last: now}
		rl.buckets[key] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * rl.rate
	if b.tokens > rl.burst {
		b.tokens = rl.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Sweep drops buckets untouched for longer than maxIdle so memory does not grow
// with the number of distinct IPs seen. Call it periodically.
func (rl *RateLimiter) Sweep(maxIdle time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	cutoff := rl.now().Add(-maxIdle)
	for k, b := range rl.buckets {
		if b.last.Before(cutoff) {
			delete(rl.buckets, k)
		}
	}
}
