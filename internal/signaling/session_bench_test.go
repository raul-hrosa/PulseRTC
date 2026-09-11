package signaling

import (
	"strconv"
	"testing"
	"time"
)

// Session recovery is control-plane only — none of it runs on
// the RTP path. These guard that the primitives stay cheap.

func BenchmarkSessionCreate(b *testing.B) {
	r := newSessionRegistry(testRecoveryCfg())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = r.Open("u"+strconv.Itoa(i), "room", "u.p"+strconv.Itoa(i), "node", nil)
	}
}

func BenchmarkResumeValidation(b *testing.B) {
	r := newSessionRegistry(testRecoveryCfg())
	res, _ := r.Open("alice", "room", "alice.p0", "node", nil)
	sid := res.Session.SessionID
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, _ := r.Open("alice", "room", "alice.p"+strconv.Itoa(i+1), "node",
			&ResumeInfo{SessionID: sid, Generation: uint64(i + 1)})
		sid = out.Session.SessionID
	}
}

func BenchmarkRecoveryStateTransition(b *testing.B) {
	r := newSessionRegistry(testRecoveryCfg())
	r.Open("alice", "room", "alice.p0", "node", nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.MarkRecovering("alice", "room", "alice.p0")
		if s, ok := r.Get("alice", "room"); ok {
			s.State = SessionConnected
		}
	}
}

func BenchmarkReconnectBackoff(b *testing.B) {
	h := ReconnectHints{InitialDelay: 500 * time.Millisecond, MaxDelay: 10 * time.Second, JitterFrac: 0.2}
	rnd := func() float64 { return 0.5 }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = nextReconnectDelay(i%10, h, rnd)
	}
}
