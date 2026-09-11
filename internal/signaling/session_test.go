package signaling

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testRecoveryCfg() SessionRecoveryConfig {
	return SessionRecoveryConfig{
		Enabled:         true,
		RecoveryTimeout: 30 * time.Second,
		ReconnectHints:  ReconnectHints{InitialDelay: 500 * time.Millisecond, MaxDelay: 10 * time.Second, JitterFrac: 0.2},
	}
}

func TestSessionOpenCreatesSessionWithGeneration1(t *testing.T) {
	r := newSessionRegistry(testRecoveryCfg())
	res, err := r.Open("alice", "room-1", "alice.p1", "node-a", nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if res.Session.Generation != 1 || res.Session.State != SessionConnected {
		t.Fatalf("session = %+v", res.Session)
	}
	if res.Reconnected || res.Kick != "" {
		t.Fatalf("fresh open should not reconnect/kick: %+v", res)
	}
	if res.Session.SessionID == "" {
		t.Fatal("empty session id")
	}
}

func TestSessionResumeBumpsGenerationAndKeepsIntent(t *testing.T) {
	r := newSessionRegistry(testRecoveryCfg())
	res, _ := r.Open("alice", "room-1", "alice.p1", "node-a", nil)
	sid := res.Session.SessionID
	r.UpdatePublishIntent("alice", "room-1", TrackIntent{TrackID: "video", Kind: "video"})
	r.UpdateSubIntent("alice", "room-1", SubIntent{PublicationID: "bob-cam", Enabled: true})
	r.MarkRecovering("alice", "room-1", "alice.p1")

	res2, err := r.Open("alice", "room-1", "alice.p2", "node-b", &ResumeInfo{SessionID: sid, Generation: 1})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !res2.Reconnected {
		t.Fatal("resume not flagged as reconnected")
	}
	if res2.Session.Generation != 2 {
		t.Fatalf("generation = %d, want 2", res2.Session.Generation)
	}
	if res2.Session.ParticipantID != "alice.p2" || res2.Session.NodeID != "node-b" {
		t.Fatalf("session not rebound: %+v", res2.Session)
	}
	if res2.Session.SessionID != sid {
		t.Fatalf("session id changed on resume: %s -> %s", sid, res2.Session.SessionID)
	}
	if _, ok := res2.Session.Publish["video"]; !ok {
		t.Fatal("publish intent lost across resume")
	}
	if _, ok := res2.Session.Subscribe["bob-cam"]; !ok {
		t.Fatal("subscribe intent lost across resume")
	}
}

func TestSessionResumeStaleGenerationRejected(t *testing.T) {
	r := newSessionRegistry(testRecoveryCfg())
	res, _ := r.Open("alice", "room-1", "alice.p1", "node-a", nil)
	sid := res.Session.SessionID
	// bump to generation 2
	r.Open("alice", "room-1", "alice.p2", "node-a", &ResumeInfo{SessionID: sid, Generation: 1})

	// a straggler from generation 1 tries to resume
	_, err := r.Open("alice", "room-1", "alice.p3", "node-a", &ResumeInfo{SessionID: sid, Generation: 1})
	if err == nil || err.Code != CodeStaleSession {
		t.Fatalf("stale resume = %v, want STALE_SESSION", err)
	}
	// state unchanged: current generation is still 2
	cur, _ := r.Get("alice", "room-1")
	if cur.Generation != 2 || cur.ParticipantID != "alice.p2" {
		t.Fatalf("stale resume mutated state: %+v", cur)
	}
}

func TestSessionResumeUnknownSessionRejected(t *testing.T) {
	r := newSessionRegistry(testRecoveryCfg())
	r.Open("alice", "room-1", "alice.p1", "node-a", nil)
	_, err := r.Open("alice", "room-1", "alice.p2", "node-a", &ResumeInfo{SessionID: "sess-bogus", Generation: 1})
	if err == nil || err.Code != CodeStaleSession {
		t.Fatalf("unknown-session resume = %v, want STALE_SESSION", err)
	}
}

func TestSessionResumeKicksLiveDuplicate(t *testing.T) {
	r := newSessionRegistry(testRecoveryCfg())
	res, _ := r.Open("alice", "room-1", "alice.p1", "node-a", nil) // still CONNECTED
	res2, err := r.Open("alice", "room-1", "alice.p2", "node-a", &ResumeInfo{SessionID: res.Session.SessionID})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res2.Kick != "alice.p1" {
		t.Fatalf("live duplicate not kicked, Kick=%q", res2.Kick)
	}
}

func TestSessionFreshOpenDoesNotDisturbExisting(t *testing.T) {
	r := newSessionRegistry(testRecoveryCfg())
	a, _ := r.Open("alice", "room-1", "alice.p1", "node-a", nil)
	b, _ := r.Open("alice", "room-1", "alice.p2", "node-a", nil) // second tab, no resume
	if b.Kick != "" {
		t.Fatal("a fresh (non-resume) open must not kick the existing session")
	}
	if b.Session.SessionID == a.Session.SessionID {
		t.Fatal("two independent connections share a session id")
	}
	total, _ := r.Count()
	if total != 2 {
		t.Fatalf("session count = %d, want 2", total)
	}
}

func TestSessionSweepExpiredClosesRecovering(t *testing.T) {
	cfg := testRecoveryCfg()
	cfg.RecoveryTimeout = 10 * time.Millisecond
	r := newSessionRegistry(cfg)
	var timeouts atomic.Int64
	r.onTimeout = func() { timeouts.Add(1) }

	r.Open("alice", "room-1", "alice.p1", "node-a", nil)
	r.MarkRecovering("alice", "room-1", "alice.p1")
	time.Sleep(25 * time.Millisecond)

	if n := r.SweepExpired(); n != 1 {
		t.Fatalf("SweepExpired closed %d, want 1", n)
	}
	if timeouts.Load() != 1 {
		t.Fatal("onTimeout not fired")
	}
	if _, ok := r.Get("alice", "room-1"); ok {
		t.Fatal("expired session still resolvable")
	}
}

func TestSessionRegistryConcurrentOpen(t *testing.T) {
	r := newSessionRegistry(testRecoveryCfg())
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			_, _ = r.Open("u", "room", "u.p"+string(rune('A'+id%26))+string(rune('0'+id/26)), "n", nil)
		}()
	}
	wg.Wait()
	total, _ := r.Count()
	if total == 0 {
		t.Fatal("no sessions registered under concurrency")
	}
}

// ---- reconnect backoff ----

func TestReconnectBackoffGrowsAndCaps(t *testing.T) {
	h := ReconnectHints{InitialDelay: 500 * time.Millisecond, MaxDelay: 8 * time.Second, JitterFrac: 0}
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}
	for n, w := range want {
		if got := nextReconnectDelay(n, h, nil); got != w {
			t.Fatalf("attempt %d: got %v want %v", n, got, w)
		}
	}
}

func TestReconnectBackoffJitterWithinBounds(t *testing.T) {
	h := ReconnectHints{InitialDelay: time.Second, MaxDelay: 30 * time.Second, JitterFrac: 0.2}
	for _, frac := range []float64{0, 0.5, 0.999} {
		d := nextReconnectDelay(2, h, func() float64 { return frac })
		base := 4 * time.Second // 1s * 2^2
		lo, hi := base, base+time.Duration(0.2*float64(base))
		if d < lo || d > hi {
			t.Fatalf("jitter frac=%.3f: delay %v outside [%v,%v]", frac, d, lo, hi)
		}
	}
}

func TestReconnectBackoffDefaults(t *testing.T) {
	// zero hints fall back to 500ms / 10s.
	if got := nextReconnectDelay(0, ReconnectHints{}, nil); got != 500*time.Millisecond {
		t.Fatalf("default initial = %v", got)
	}
	if got := nextReconnectDelay(20, ReconnectHints{}, nil); got != 10*time.Second {
		t.Fatalf("default cap = %v", got)
	}
}
