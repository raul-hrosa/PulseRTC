package auth

import (
	"testing"
	"time"
)

func TestRateLimiterAllowsBurstThenBlocks(t *testing.T) {
	rl := NewRateLimiter(3, time.Minute)
	for i := 0; i < 3; i++ {
		if !rl.Allow("ip") {
			t.Fatalf("burst token %d should be allowed", i)
		}
	}
	if rl.Allow("ip") {
		t.Fatalf("4th call should be blocked")
	}
	if !rl.Allow("other") {
		t.Fatalf("a different key has its own bucket")
	}
}

func TestRateLimiterRefills(t *testing.T) {
	rl := NewRateLimiter(60, time.Minute) // 1/sec
	now := time.Now()
	rl.now = func() time.Time { return now }
	for i := 0; i < 60; i++ {
		rl.Allow("ip")
	}
	if rl.Allow("ip") {
		t.Fatalf("bucket should be empty")
	}
	now = now.Add(2 * time.Second)
	if !rl.Allow("ip") || !rl.Allow("ip") {
		t.Fatalf("2 tokens should have refilled")
	}
}

func TestRateLimiterDisabled(t *testing.T) {
	rl := NewRateLimiter(0, time.Minute)
	for i := 0; i < 1000; i++ {
		if !rl.Allow("ip") {
			t.Fatalf("disabled limiter must allow everything")
		}
	}
}

func TestRateLimiterSweep(t *testing.T) {
	rl := NewRateLimiter(10, time.Minute)
	now := time.Now()
	rl.now = func() time.Time { return now }
	rl.Allow("ip")
	now = now.Add(time.Hour)
	rl.Sweep(10 * time.Minute)
	if len(rl.buckets) != 0 {
		t.Fatalf("idle bucket should be swept")
	}
}
