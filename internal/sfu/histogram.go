package sfu

import (
	"slices"
	"sync/atomic"
	"time"
)

type durationHistogram struct {
	bounds    []float64 // seconds, ascending
	counts    []atomic.Uint64
	sumMicros atomic.Uint64
	total     atomic.Uint64
}

func newDurationHistogram(bounds ...float64) *durationHistogram {
	return &durationHistogram{bounds: bounds, counts: make([]atomic.Uint64, len(bounds))}
}

func (h *durationHistogram) Observe(d time.Duration) {
	s := d.Seconds()
	for i, b := range h.bounds {
		if s <= b {
			h.counts[i].Add(1)
			break
		}
	}
	h.sumMicros.Add(uint64(d.Microseconds()))
	h.total.Add(1)
}

func (h *durationHistogram) Snapshot() (bounds []float64, counts []uint64, sumSeconds float64, total uint64) {
	bounds = slices.Clone(h.bounds)
	counts = make([]uint64, len(h.counts))
	for i := range h.counts {
		counts[i] = h.counts[i].Load()
	}
	return bounds, counts, float64(h.sumMicros.Load()) / 1e6, h.total.Load()
}
