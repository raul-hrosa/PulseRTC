package sfu

import (
	"errors"
	"io"
)

const subWriteErrorBudget = 50

// runSubWriteLoop drains one subscriber's send queue into its own fan-out track
// (w). It is the only writer of that track. The loop ends when the queue is
// closed (unsubscribe / participant teardown) or the track is gone
// (io.ErrClosedPipe). onResync, when non-nil, is called after the queue dropped
// a video backlog so the caller can request a fresh keyframe.
//
// onFatal, when non-nil, is called once after subWriteErrorBudget consecutive
// non-ErrClosedPipe write errors (e.g. a broken SRTP context) before the loop
// returns, so the caller can tear the subscription down. A single successful
// write resets the streak.
func runSubWriteLoop(q *sendQueue, w io.Writer, onResync func(), onFatal func()) {
	streak := 0
	for {
		bp, resync, ok := q.pop()
		if !ok {
			return
		}
		_, err := w.Write(*bp)
		releaseBuf(bp)
		if err != nil {
			if errors.Is(err, io.ErrClosedPipe) {
				return
			}
			if streak++; streak >= subWriteErrorBudget {
				if onFatal != nil {
					onFatal()
				}
				return
			}
			continue
		}
		streak = 0
		if resync && onResync != nil {
			onResync()
		}
	}
}
