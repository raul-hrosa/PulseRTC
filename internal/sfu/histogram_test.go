package sfu

import (
	"testing"
	"time"
)

func TestDurationHistogram(t *testing.T) {
	h := newDurationHistogram(0.01, 0.1, 1)
	h.Observe(5 * time.Millisecond)   // bucket 0
	h.Observe(50 * time.Millisecond)  // bucket 1
	h.Observe(500 * time.Millisecond) // bucket 2
	h.Observe(2 * time.Second)        // overflow

	bounds, counts, sum, total := h.Snapshot()
	if len(bounds) != 3 || total != 4 {
		t.Fatalf("bounds=%v total=%d", bounds, total)
	}
	if counts[0] != 1 || counts[1] != 1 || counts[2] != 1 {
		t.Fatalf("counts=%v", counts)
	}
	if sum < 2.55 || sum > 2.56 { // 0.005+0.05+0.5+2.0
		t.Fatalf("sum=%v", sum)
	}
}
