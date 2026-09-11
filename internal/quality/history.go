package quality

import "time"

// HistoryPoint is one stable-status transition, timestamped.
type HistoryPoint struct {
	At     time.Time `json:"at"`
	Status Status    `json:"status"`
}

// window keeps the last N Derived samples and averages them, so a single spike
// cannot drive a verdict. Averaging (not max) is deliberate: the example
// "0 0 0 8 0 0" must stay GOOD.
type window struct {
	buf  []Derived
	size int
}

func newWindow(size int) *window {
	if size < 1 {
		size = 1
	}
	return &window{size: size}
}

func (w *window) push(d Derived) {
	w.buf = append(w.buf, d)
	if len(w.buf) > w.size {
		w.buf = w.buf[len(w.buf)-w.size:]
	}
}

// mean averages each metric over the samples that reported it. Non-numeric
// fields (state) come from the most recent sample.
func (w *window) mean() Derived {
	last := w.buf[len(w.buf)-1]
	out := Derived{
		HasData:   true,
		Enabled:   last.Enabled,
		ConnState: last.ConnState,
		ICEState:  last.ICEState,
		Width:     last.Width,
		Height:    last.Height,
	}
	out.LossPct = avg(w.buf, func(d Derived) *float64 { return d.LossPct })
	out.BitrateBps = avg(w.buf, func(d Derived) *float64 { return d.BitrateBps })
	out.JitterMs = avg(w.buf, func(d Derived) *float64 { return d.JitterMs })
	out.RTTMs = avg(w.buf, func(d Derived) *float64 { return d.RTTMs })
	out.FPS = avg(w.buf, func(d Derived) *float64 { return d.FPS })
	out.FrameDropPct = avg(w.buf, func(d Derived) *float64 { return d.FrameDropPct })
	return out
}

func avg(buf []Derived, get func(Derived) *float64) *float64 {
	var sum float64
	var n int
	for _, d := range buf {
		if v, ok := fval(get(d)); ok {
			sum += v
			n++
		}
	}
	if n == 0 {
		return nil
	}
	return ptr(sum / float64(n))
}
