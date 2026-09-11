package sfu

import "testing"

// Performance regression guards. These are microbenchmarks of the
// isolated hot paths the work touched — cheap and deterministic,
// unlike a full ICE/DTLS handshake. End-to-end SFU throughput is measured by
// cmd/loadtest, whose JSON results are the real before/after evidence.
//
//	go test ./internal/sfu/ -run x -bench . -benchmem

// BenchmarkNegotiationLimiter measures acquire/release overhead of the
// SFU-wide negotiation limiter. It sits on the renegotiation path, so its
// per-op cost must stay negligible next to SDP parsing.
func BenchmarkNegotiationLimiter(b *testing.B) {
	l := newNegotiationLimiter(4)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.acquire()
			l.release()
		}
	})
}

// BenchmarkScheduleNegotiateCoalesced measures the cost of a coalesced
// scheduleNegotiate() call (the common case during a subscribe burst: a timer
// is already pending, so this is just a lock + counter bump).
func BenchmarkScheduleNegotiateCoalesced(b *testing.B) {
	s, err := New(testLogger())
	if err != nil {
		b.Fatal(err)
	}
	p, err := s.Room("bench").Join("p", nopTransport{})
	if err != nil {
		b.Fatal(err)
	}
	defer p.close()

	p.scheduleNegotiate() // arm the timer once; every later call coalesces
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.scheduleNegotiate()
	}
}

// BenchmarkMetricsSnapshot guards the /metrics aggregation, polled once per
// second by the load client and by any external monitoring.
func BenchmarkMetricsSnapshot(b *testing.B) {
	s, err := New(testLogger())
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, _ = s.Room("r").Join(string(rune('a'+i)), nopTransport{})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Metrics()
	}
}
