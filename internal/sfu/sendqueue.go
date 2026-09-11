package sfu

import (
	"sync"
	"sync/atomic"
)

// queueKind selects the drop policy of a sendQueue.
type queueKind int

const (
	queueKindAudio queueKind = iota // drop the oldest packet on overflow
	queueKindVideo                  // drop the whole backlog on overflow, flag a resync
)

const maxRTPPacketSize = 1500

// rtpBufPool recycles the fixed-size buffers that carry one RTP packet through a
// sendQueue. A buffer is sliced to the packet length while in flight and
// restored to full capacity when returned.
var rtpBufPool = sync.Pool{New: func() any { b := make([]byte, maxRTPPacketSize); return &b }}

func getBuf(pkt []byte) *[]byte {
	bp := rtpBufPool.Get().(*[]byte)
	b := (*bp)[:cap(*bp)]
	n := copy(b, pkt)
	*bp = b[:n]
	return bp
}

// releaseBuf returns a buffer obtained from getBuf (or handed out by pop).
func releaseBuf(bp *[]byte) {
	*bp = (*bp)[:cap(*bp)]
	rtpBufPool.Put(bp)
}

// sendQueue is a bounded, single-producer / single-consumer packet queue that
// isolates one subscriber's send path from the shared publisher ingest loop: a
// slow consumer causes drops on its own queue, never back-pressure on the
// producer. pop blocks; push never does.
type sendQueue struct {
	kind queueKind
	cap  int

	mu      sync.Mutex
	cond    *sync.Cond
	ring    []*[]byte
	closed  bool
	resync  bool // a video overflow happened; surfaced once on the next pop
	dropCnt atomic.Int64

	// onDrop, when set, is called with the number of packets discarded by an
	// overflow. Used to roll drops into SFU-wide metrics.
	onDrop func(n int64)
}

func newSendQueue(kind queueKind, capacity int) *sendQueue {
	if capacity < 1 {
		capacity = 1
	}
	q := &sendQueue{kind: kind, cap: capacity, ring: make([]*[]byte, 0, capacity)}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// push enqueues a copy of pkt. It never blocks. On overflow it applies the
// queue's drop policy and counts every discarded packet.
func (q *sendQueue) push(pkt []byte) {
	bp := getBuf(pkt)

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		releaseBuf(bp)
		return
	}

	if len(q.ring) >= q.cap {
		var dropped int64
		switch q.kind {
		case queueKindVideo:
			for _, old := range q.ring {
				releaseBuf(old)
			}
			dropped = int64(len(q.ring))
			q.ring = q.ring[:0]
			q.resync = true
		default: // queueKindAudio
			releaseBuf(q.ring[0])
			dropped = 1
			q.ring = q.ring[1:]
		}
		q.dropCnt.Add(dropped)
		if q.onDrop != nil {
			q.onDrop(dropped)
		}
	}
	q.ring = append(q.ring, bp)
	q.cond.Signal()
}

// pop returns the next packet, blocking until one is available or the queue is
// closed. resync is true exactly once after a video overflow, telling the
// consumer to request a fresh keyframe. ok is false once the queue is closed
// and drained. The returned buffer must be handed to releaseBuf when done.
func (q *sendQueue) pop() (bp *[]byte, resync bool, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.ring) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.ring) == 0 {
		return nil, false, false
	}
	bp = q.ring[0]
	q.ring = q.ring[1:]
	resync = q.resync
	q.resync = false
	return bp, resync, true
}

func (q *sendQueue) close() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		for _, bp := range q.ring {
			releaseBuf(bp)
		}
		q.ring = nil
	}
	q.mu.Unlock()
	q.cond.Broadcast()
}

func (q *sendQueue) dropped() int64 { return q.dropCnt.Load() }
