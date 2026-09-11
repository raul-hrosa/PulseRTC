package sfu

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- 7B.1: negotiation concurrency limiter -------------------------------

func TestNegotiationLimiterCapsConcurrency(t *testing.T) {
	const limit = 3
	l := newNegotiationLimiter(limit)

	var inFlight, peak atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.acquire()
			n := inFlight.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			inFlight.Add(-1)
			l.release()
		}()
	}
	wg.Wait()

	if peak.Load() > limit {
		t.Fatalf("limiter allowed %d concurrent holders, want <= %d", peak.Load(), limit)
	}
	if peak.Load() == 0 {
		t.Fatal("limiter never admitted anyone")
	}
}

func TestNegotiationLimiterZeroBecomesOne(t *testing.T) {
	l := newNegotiationLimiter(0)
	l.acquire()
	done := make(chan struct{})
	go func() { l.acquire(); close(done) }()
	select {
	case <-done:
		t.Fatal("second acquire should have blocked with limit 1")
	case <-time.After(20 * time.Millisecond):
	}
	l.release()
	<-done
	l.release()
}

// --- 7B.1: debounced negotiation ---------------------------------------

func TestScheduleNegotiateCoalesces(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")
	p, err := r.Join("alice", nopTransport{})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	defer p.close()

	// A fresh PeerConnection is in signalling state "stable", so the one
	// negotiate() that eventually fires runs its full body (started++,
	// completed++). The other 19 burst calls are absorbed by the pending
	// timer (coalesced++).
	for i := 0; i < 20; i++ {
		p.scheduleNegotiate()
	}
	time.Sleep(negotiationDebounce * 4)

	n := s.Metrics().Negotiations
	if n.Started != 1 {
		t.Fatalf("20 scheduleNegotiate calls in a burst started %d negotiations, want 1", n.Started)
	}
	if n.Coalesced != 19 {
		t.Fatalf("expected 19 coalesced negotiations, got %d", n.Coalesced)
	}
}

// --- 7B.2: subscriptions.removed counter on subscriber leave -----------

func TestCloseAccountsForOwnSubscriptions(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")
	sub, err := r.Join("bob", nopTransport{})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	// Inject subscriptions directly (no real PeerConnection tracks needed for
	// the counter path). Each carries a send queue, as a real subscription does.
	sub.mu.Lock()
	for _, id := range []string{"pub-a", "pub-b", "pub-c"} {
		sub.subscriptions[id] = &Subscription{subscriberID: "bob", queue: newSendQueue(queueKindAudio, 4)}
	}
	sub.mu.Unlock()

	before := s.subscriptionsRemoved.Load()
	sub.close()
	after := s.subscriptionsRemoved.Load()

	if after-before != 3 {
		t.Fatalf("close() accounted %d removed subscriptions, want 3", after-before)
	}
	if snap := s.Metrics().Subscriptions; snap.Active != 0 {
		t.Fatalf("active subscriptions gauge = %d, want 0", snap.Active)
	}
}

// --- 7B.3: NACK buffer size resolution -------------------------------

func TestNackResponderSizeDefaultsAndClamps(t *testing.T) {
	if got := nackResponderSize(testLogger()); got != defaultNackBufferSize {
		t.Fatalf("default nack size = %d, want %d", got, defaultNackBufferSize)
	}
	t.Setenv("SFU_NACK_BUFFER_SIZE", "512")
	if got := nackResponderSize(testLogger()); got != 512 {
		t.Fatalf("SFU_NACK_BUFFER_SIZE=512 -> %d, want 512", got)
	}
	t.Setenv("SFU_NACK_BUFFER_SIZE", "500") // not a power of two
	if got := nackResponderSize(testLogger()); got != defaultNackBufferSize {
		t.Fatalf("invalid SFU_NACK_BUFFER_SIZE should fall back to %d, got %d", defaultNackBufferSize, got)
	}
}
