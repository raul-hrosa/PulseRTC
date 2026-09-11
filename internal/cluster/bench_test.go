package cluster

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// benchClusterCfg is an enabled-cluster config whose heartbeat is effectively
// disabled for the duration of a benchmark.
func benchClusterCfg(nodeID string) Config {
	return Config{
		Enabled: true, NodeID: nodeID, Secret: "s",
		HeartbeatInterval: time.Hour, StaleAfter: 2 * time.Hour,
	}
}

// These benchmarks quantify the cost of the coordination layer so we
// know it before ever putting it near the media path. Run:
//
//	go test ./internal/cluster/ -run=^$ -bench=. -benchmem

func BenchmarkClaimRoomLocalHit(b *testing.B) {
	c, _ := New(benchClusterCfg("n1"), testLogger())
	c.Start()
	c.rooms.SetOwner("room", "n1")
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.ClaimRoom(ctx, "room"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkResolveRoomLocalMiss(b *testing.B) {
	// Enabled but no peers: the miss path with nothing to query — the pure
	// local-map-lookup cost.
	c, _ := New(benchClusterCfg("n1"), testLogger())
	c.Start()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.ResolveRoom(ctx, "nope")
	}
}

func BenchmarkDisabledClaimRoom(b *testing.B) {
	c, _ := New(Config{Enabled: false}, testLogger())
	c.Start()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.ClaimRoom(ctx, fmt.Sprintf("r%d", i&1023))
	}
}

func BenchmarkInternalRoomLookupHTTP(b *testing.B) {
	owner, _ := New(benchClusterCfg("owner"), testLogger())
	owner.Start()
	owner.rooms.SetOwner("room", "owner")
	mux := http.NewServeMux()
	owner.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	tr := NewHTTPTransport("s", 0)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := tr.RoomOwner(ctx, ts.URL, "room"); err != nil || !ok {
			b.Fatalf("ok=%v err=%v", ok, err)
		}
	}
}

func BenchmarkParticipantRegister(b *testing.B) {
	c, _ := New(benchClusterCfg("n1"), testLogger())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.RegisterParticipant(fmt.Sprintf("p%d", i&4095), "room")
	}
}
