package sfu

import (
	"errors"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

type alwaysErrWriter struct{ n int }

func (a *alwaysErrWriter) Write(p []byte) (int, error) {
	a.n++
	return 0, errors.New("srtp: context is gone")
}

func TestSubWriteLoopGivesUpAfterErrorBudget(t *testing.T) {
	// queueKindAudio drops the oldest on overflow, so a steady feeder keeps the
	// queue non-empty and the write loop accrues a continuous error streak.
	q := newSendQueue(queueKindAudio, 8)
	w := &alwaysErrWriter{}
	fatal := make(chan struct{}, 1)
	done := make(chan struct{})

	go runSubWriteLoop(q, w, nil, func() { fatal <- struct{}{} })
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				q.push([]byte{1})
				time.Sleep(time.Millisecond)
			}
		}
	}()

	select {
	case <-fatal:
	case <-time.After(2 * time.Second):
		close(done)
		t.Fatal("onFatal never fired after the error budget was exhausted")
	}
	close(done)

	if w.n > subWriteErrorBudget*4 {
		t.Fatalf("kept writing well past the budget: %d attempts", w.n)
	}
}

func TestOnSubscriptionFatalUnsubscribesAndFiresHook(t *testing.T) {
	s := newTestSFU(t)
	var got [3]string
	s.SetSubscriptionFailedHook(func(room, sub, pub string) { got = [3]string{room, sub, pub} })

	r := s.Room("demo")
	pubP, _ := r.Join("pub", nopTransport{})
	subP, _ := r.Join("sub", nopTransport{})

	pub := &Publication{id: "pub-x", kind: webrtc.RTPCodecTypeVideo, participantID: "pub", room: r}
	subP.mu.Lock()
	subP.subscriptions["pub-x"] = &Subscription{
		subscriberID: "sub",
		publication:  pub,
		queue:        newSendQueue(queueKindVideo, 4),
	}
	subP.mu.Unlock()

	before := s.subscriptionsFailed.Load()
	subP.onSubscriptionFatal("pub-x")

	if s.subscriptionsFailed.Load() != before+1 {
		t.Fatal("counter not incremented")
	}
	if got != [3]string{"demo", "sub", "pub-x"} {
		t.Fatalf("hook args = %v", got)
	}
	subP.mu.Lock()
	_, still := subP.subscriptions["pub-x"]
	subP.mu.Unlock()
	if still {
		t.Fatal("subscription not removed")
	}
	_ = pubP
}
