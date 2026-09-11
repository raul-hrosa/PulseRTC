package signaling

import (
	"math"
	"time"
)

// nextReconnectDelay computes the wait before reconnect attempt n (0-based),
// mirroring the algorithm the browser client uses:
//
//	base  = initial * 2^n     (capped at max)
//	delay = base + rand[0, base*jitter)
//
// rnd returns a value in [0,1); pass nil for the zero-jitter (deterministic)
// delay, which is also the lower bound of the jittered range.
func nextReconnectDelay(n int, h ReconnectHints, rnd func() float64) time.Duration {
	initial := h.InitialDelay
	if initial <= 0 {
		initial = 500 * time.Millisecond
	}
	max := h.MaxDelay
	if max <= 0 {
		max = 10 * time.Second
	}
	if n < 0 {
		n = 0
	}

	base := float64(initial) * math.Pow(2, float64(n))
	if base > float64(max) {
		base = float64(max)
	}

	delay := base
	if rnd != nil && h.JitterFrac > 0 {
		delay += rnd() * base * h.JitterFrac
	}
	return time.Duration(delay)
}
