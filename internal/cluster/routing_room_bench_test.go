package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Run:
//
//	go test ./internal/cluster/ -run=^$ -bench='RoomSelection|LoadSnapshot|RoomRouting' -benchmem
//
// The local scoring algorithm must be cheap and must not do network I/O.

func benchRouter(b *testing.B, nodes int) *roomRouter {
	b.Helper()
	reg := NewInMemoryNodeRegistry(time.Minute)
	load := NewInMemoryLoadStore()
	for i := 0; i < nodes; i++ {
		id := fmt.Sprintf("node-%d", i)
		_ = reg.Register(NodeInfo{ID: id, State: NodeReady})
		_ = load.PublishLoad(NodeLoad{NodeID: id, State: NodeReady, Participants: i * 3, ReportedAt: time.Now()}, time.Minute)
	}
	return newRoomRouter(func() NodeInfo { return NodeInfo{ID: "node-0", State: NodeReady} },
		reg, load, NewInMemoryRoomLocator(),
		LeastLoadedPolicy{Weights: defaultLoadWeights()}, CapacityConfig{}, &Metrics{}, nil,
		func() bool { return true }, time.Minute)
}

func BenchmarkRoomSelection(b *testing.B) {
	// The pure policy, no store access.
	p := LeastLoadedPolicy{Weights: defaultLoadWeights(), LocalNodeID: "node-2"}
	cands := make([]NodeLoad, 8)
	for i := range cands {
		cands[i] = NodeLoad{NodeID: fmt.Sprintf("node-%d", i), Participants: i * 5}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := p.Select(cands); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLoadSnapshot(b *testing.B) {
	load := NewInMemoryLoadStore()
	for i := 0; i < 8; i++ {
		_ = load.PublishLoad(NodeLoad{NodeID: fmt.Sprintf("n%d", i), ReportedAt: time.Now()}, time.Minute)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := load.LoadSnapshot(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRoomRouting(b *testing.B) {
	rr := benchRouter(b, 5)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := rr.SelectNode(ctx, fmt.Sprintf("room-%d", i)); err != nil {
			b.Fatal(err)
		}
	}
}
