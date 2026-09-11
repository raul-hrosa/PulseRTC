package sfu

import (
	"testing"
	"time"
)

func TestSendQueueAudioDropsOldestWhenFull(t *testing.T) {
	q := newSendQueue(queueKindAudio, 2)

	q.push([]byte{1})
	q.push([]byte{2})
	q.push([]byte{3}) // queue was full -> oldest ({1}) is dropped

	bp, _, ok := q.pop()
	if !ok || len(*bp) != 1 || (*bp)[0] != 2 {
		t.Fatalf("first pop = %v ok=%v, want {2}", bp, ok)
	}
	releaseBuf(bp)
	bp, _, ok = q.pop()
	if !ok || len(*bp) != 1 || (*bp)[0] != 3 {
		t.Fatalf("second pop = %v ok=%v, want {3}", bp, ok)
	}
	releaseBuf(bp)
	if got := q.dropped(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
}

func TestSendQueuePopBlocksUntilPush(t *testing.T) {
	q := newSendQueue(queueKindAudio, 4)

	done := make(chan byte, 1)
	go func() {
		bp, _, _ := q.pop()
		done <- (*bp)[0]
	}()

	select {
	case <-done:
		t.Fatal("pop returned before any push")
	case <-time.After(20 * time.Millisecond):
	}

	q.push([]byte{9})
	select {
	case b := <-done:
		if b != 9 {
			t.Fatalf("pop = %d, want 9", b)
		}
	case <-time.After(time.Second):
		t.Fatal("pop did not return after push")
	}
}

func TestSendQueueCloseUnblocksPop(t *testing.T) {
	q := newSendQueue(queueKindVideo, 4)

	done := make(chan bool, 1)
	go func() {
		_, _, ok := q.pop()
		done <- ok
	}()

	q.close()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("pop after close returned ok=true, want false")
		}
	case <-time.After(time.Second):
		t.Fatal("pop did not return after close")
	}
}

func TestSendQueueVideoOverflowDropsAllAndFlagsResync(t *testing.T) {
	q := newSendQueue(queueKindVideo, 2)

	q.push([]byte{1})
	q.push([]byte{2})
	q.push([]byte{3}) // full -> drop {1} and {2}, enqueue {3}, mark resync

	bp, resync, ok := q.pop()
	if !ok || len(*bp) != 1 || (*bp)[0] != 3 {
		t.Fatalf("pop = %v ok=%v, want {3}", bp, ok)
	}
	releaseBuf(bp)
	if !resync {
		t.Fatal("pop resync = false, want true after a video overflow")
	}
	if got := q.dropped(); got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}
}

func TestSendQueueResyncFlagClearsAfterOnePop(t *testing.T) {
	q := newSendQueue(queueKindVideo, 1)

	q.push([]byte{1})
	q.push([]byte{2}) // overflow -> resync

	bp, resync, _ := q.pop()
	releaseBuf(bp)
	if !resync {
		t.Fatal("first pop resync = false, want true")
	}
	q.push([]byte{3})
	bp, resync, _ = q.pop()
	releaseBuf(bp)
	if resync {
		t.Fatal("second pop resync = true, want false (flag should clear)")
	}
}
