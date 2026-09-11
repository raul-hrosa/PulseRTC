package sfu

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func TestPublicationWriteRTPFansToAllSinks(t *testing.T) {
	pub := &Publication{id: "pub-x", kind: webrtc.RTPCodecTypeAudio}
	q1 := newSendQueue(queueKindAudio, 8)
	q2 := newSendQueue(queueKindAudio, 8)
	pub.attachSink(q1)
	pub.attachSink(q2)

	pub.writeRTP([]byte{1})
	pub.writeRTP([]byte{2})

	for _, q := range []*sendQueue{q1, q2} {
		for _, want := range []byte{1, 2} {
			bp, _, ok := q.pop()
			if !ok || (*bp)[0] != want {
				t.Fatalf("pop = %v ok=%v, want %d", bp, ok, want)
			}
			releaseBuf(bp)
		}
	}
}

func TestPublicationDetachStopsDelivery(t *testing.T) {
	pub := &Publication{id: "pub-x", kind: webrtc.RTPCodecTypeAudio}
	q := newSendQueue(queueKindAudio, 8)
	detach := pub.attachSink(q)

	pub.writeRTP([]byte{1})
	detach()
	pub.writeRTP([]byte{2})

	bp, _, ok := q.pop()
	if !ok || (*bp)[0] != 1 {
		t.Fatalf("first pop = %v ok=%v, want 1", bp, ok)
	}
	releaseBuf(bp)

	q.close()
	if bp, _, ok := q.pop(); ok {
		t.Fatalf("got %v after detach+close, want nothing", *bp)
	}
}

// slowWriter blocks every Write until release is called.
type slowWriter struct {
	mu       sync.Mutex
	count    int
	gate     chan struct{}
	released bool
}

func newSlowWriter() *slowWriter { return &slowWriter{gate: make(chan struct{})} }

func (w *slowWriter) Write(p []byte) (int, error) {
	<-w.gate
	w.mu.Lock()
	w.count++
	w.mu.Unlock()
	return len(p), nil
}
func (w *slowWriter) release() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.released {
		w.released = true
		close(w.gate)
	}
}
func (w *slowWriter) writes() int { w.mu.Lock(); defer w.mu.Unlock(); return w.count }

// recordWriter counts every successful Write and never blocks.
type recordWriter struct {
	mu    sync.Mutex
	count int
}

func (w *recordWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.count++
	w.mu.Unlock()
	return len(p), nil
}
func (w *recordWriter) writes() int { w.mu.Lock(); defer w.mu.Unlock(); return w.count }

func TestSubWriteLoopSlowWriterDropsInsteadOfBlockingFanOut(t *testing.T) {
	pub := &Publication{id: "pub-v", kind: webrtc.RTPCodecTypeVideo}

	fast := &recordWriter{}
	slow := newSlowWriter()
	defer slow.release()

	qFast := newSendQueue(queueKindVideo, 64)
	qSlow := newSendQueue(queueKindVideo, 64)
	pub.attachSink(qFast)
	pub.attachSink(qSlow)

	go runSubWriteLoop(qFast, fast, nil, nil)
	go runSubWriteLoop(qSlow, slow, nil, nil)

	// Produce at a realistic packet cadence. The fast subscriber consumes
	// instantly; the slow one is blocked on every Write for the whole test.
	const n = 300
	start := time.Now()
	for i := 0; i < n; i++ {
		pub.writeRTP([]byte{byte(i)})
		time.Sleep(time.Millisecond)
	}
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("producing %d packets took %v — the slow subscriber is back-pressuring the producer", n, elapsed)
	}

	deadline := time.Now().Add(2 * time.Second)
	for fast.writes() < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fast.writes() != n {
		t.Fatalf("fast subscriber got %d/%d packets while a peer was slow", fast.writes(), n)
	}
	if slow.writes() != 0 {
		t.Fatalf("slow writer completed %d writes, expected it to stay blocked", slow.writes())
	}
	if qSlow.dropped() == 0 {
		t.Fatalf("slow subscriber queue reported no drops, expected its backlog to overflow")
	}
}

func TestSubWriteLoopCallsOnResyncAfterVideoOverflow(t *testing.T) {
	q := newSendQueue(queueKindVideo, 2)
	w := &recordWriter{}
	resyncs := make(chan struct{}, 4)

	go runSubWriteLoop(q, w, func() { resyncs <- struct{}{} }, nil)

	// Overflow the queue before the writer drains it.
	for i := 0; i < 10; i++ {
		q.push([]byte{byte(i)})
	}

	select {
	case <-resyncs:
	case <-time.After(time.Second):
		t.Fatal("onResync was not called after a video overflow")
	}
}

func TestSubWriteLoopStopsOnClosedPipe(t *testing.T) {
	q := newSendQueue(queueKindAudio, 4)
	done := make(chan struct{})
	go func() { runSubWriteLoop(q, pipeErrWriter{}, nil, nil); close(done) }()

	q.push([]byte{1})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("write loop did not exit on io.ErrClosedPipe")
	}
}

type pipeErrWriter struct{}

func (pipeErrWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
