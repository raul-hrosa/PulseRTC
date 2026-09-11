package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Quantify the cost of each coordination operation against the
// Redis backend so we know the price of distributed consistency. Run:
//
//	go test ./internal/cluster/ -run=^$ -bench=Redis -benchmem
//
// Numbers use miniredis over loopback TCP — a real Redis adds network RTT but
// the relative shape (SETNX vs GET vs Lua) holds. Compare with the in-memory
// figures from bench_test.go (ClaimRoom local hit ~48ns/0alloc).
func benchRedisState(b *testing.B) *RedisClusterState {
	b.Helper()
	mr := miniredis.RunT(b)
	st := newRedisClusterStateWith(
		newRedisClientFrom(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "pulsertc"),
		RedisConfig{Addr: mr.Addr(), Prefix: "pulsertc", Timeout: time.Second,
			NodeTTL: 15 * time.Second, ParticipTTL: 60 * time.Second}, testLogger())
	_ = st.Ping(context.Background())
	return st
}

func BenchmarkRedisClaimRoom(b *testing.B) {
	rl := benchRedisState(b).Rooms()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rl.ClaimOwner(fmt.Sprintf("room-%d", i), "n1")
	}
}

func BenchmarkRedisResolveRoomHit(b *testing.B) {
	rl := benchRedisState(b).Rooms()
	rl.ClaimOwner("room", "n1")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rl.GetOwner("room")
	}
}

func BenchmarkRedisReleaseRoom(b *testing.B) {
	rl := benchRedisState(b).Rooms()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("room-%d", i)
		rl.ClaimOwner(k, "n1")
		rl.ReleaseOwner(k, "n1")
	}
}

func BenchmarkRedisRegisterParticipant(b *testing.B) {
	pl := benchRedisState(b).Participants()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pl.Register(ParticipantLocation{ParticipantID: fmt.Sprintf("p-%d", i), RoomID: "r", NodeID: "n1"})
	}
}

func BenchmarkRedisResolveParticipant(b *testing.B) {
	pl := benchRedisState(b).Participants()
	pl.Register(ParticipantLocation{ParticipantID: "p", RoomID: "r", NodeID: "n1"})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pl.Lookup("p")
	}
}

func BenchmarkRedisRegisterNode(b *testing.B) {
	nr := benchRedisState(b).Nodes()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = nr.Register(NodeInfo{ID: "n1", State: NodeReady})
	}
}

func BenchmarkRedisHeartbeat(b *testing.B) {
	nr := benchRedisState(b).Nodes()
	_ = nr.Register(NodeInfo{ID: "n1", State: NodeReady})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nr.Heartbeat("n1")
	}
}
