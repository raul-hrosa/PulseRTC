package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func routerHarness(t *testing.T, cap CapacityConfig) (*roomRouter, *InMemoryNodeRegistry, *InMemoryLoadStore) {
	t.Helper()
	reg := NewInMemoryNodeRegistry(time.Minute)
	load := NewInMemoryLoadStore()
	self := func() NodeInfo { return NodeInfo{ID: "n1", State: NodeReady} }
	rr := newRoomRouter(self, reg, load, NewInMemoryRoomLocator(),
		LeastLoadedPolicy{Weights: defaultLoadWeights(), PreferLocalMargin: 4},
		cap, &Metrics{}, nil, func() bool { return true }, 10*time.Second)
	return rr, reg, load
}

func regNode(reg *InMemoryNodeRegistry, id string, st NodeState) {
	_ = reg.Register(NodeInfo{ID: id, State: st})
}
func pubLoad(load *InMemoryLoadStore, id string, participants int) {
	_ = load.PublishLoad(NodeLoad{NodeID: id, State: NodeReady, Participants: participants, ReportedAt: time.Now()}, 10*time.Second)
}

func TestRouter_IgnoresDrainingOfflineStale(t *testing.T) {
	rr, reg, load := routerHarness(t, CapacityConfig{})
	regNode(reg, "n1", NodeReady)
	regNode(reg, "n2", NodeShuttingDown) // draining
	regNode(reg, "n3", NodeOffline)
	regNode(reg, "n4", NodeReady)
	pubLoad(load, "n1", 90)
	pubLoad(load, "n4", 5)
	// n4 is the only healthy less-loaded option beyond n1's local margin.
	got, reason, err := rr.SelectNode(context.Background(), "room-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "n4" {
		t.Fatalf("draining/offline nodes must be excluded; want n4, got %s (%s)", got.ID, reason)
	}
}

func TestRouter_StaleNodeExcluded(t *testing.T) {
	reg := NewInMemoryNodeRegistry(50 * time.Millisecond)
	load := NewInMemoryLoadStore()
	rr := newRoomRouter(func() NodeInfo { return NodeInfo{ID: "n1", State: NodeReady} },
		reg, load, NewInMemoryRoomLocator(),
		LeastLoadedPolicy{Weights: defaultLoadWeights()}, CapacityConfig{}, &Metrics{}, nil,
		func() bool { return true }, 10*time.Second)

	_ = reg.Register(NodeInfo{ID: "n1", State: NodeReady})
	_ = reg.Register(NodeInfo{ID: "n2", State: NodeReady})
	pubLoad(load, "n1", 100)
	pubLoad(load, "n2", 1)
	time.Sleep(80 * time.Millisecond)                      // both go stale
	_ = reg.Register(NodeInfo{ID: "n1", State: NodeReady}) // refresh only n1

	got, _, err := rr.SelectNode(context.Background(), "r")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "n1" {
		t.Fatalf("stale n2 must be excluded even though it is least-loaded, got %s", got.ID)
	}
}

func TestRouter_CapacityHardLimitAndNoCapacity(t *testing.T) {
	rr, reg, load := routerHarness(t, CapacityConfig{MaxParticipants: 100, HardLimitFraction: 1.0})
	regNode(reg, "n1", NodeReady)
	regNode(reg, "n2", NodeReady)
	pubLoad(load, "n1", 100) // full
	pubLoad(load, "n2", 40)

	got, _, err := rr.SelectNode(context.Background(), "r")
	if err != nil || got.ID != "n2" {
		t.Fatalf("full node must be skipped; want n2, got %s err=%v", got.ID, err)
	}

	pubLoad(load, "n2", 100) // now both full
	_, _, err = rr.SelectNode(context.Background(), "r")
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeNoCapacity {
		t.Fatalf("all nodes full must return NO_CAPACITY, got %v", err)
	}
}

func TestRouter_FailClosedWhenStateUnavailable(t *testing.T) {
	reg := NewInMemoryNodeRegistry(time.Minute)
	regNode(reg, "n1", NodeReady)
	rr := newRoomRouter(func() NodeInfo { return NodeInfo{ID: "n1", State: NodeReady} },
		reg, NewInMemoryLoadStore(), NewInMemoryRoomLocator(),
		LeastLoadedPolicy{}, CapacityConfig{}, &Metrics{}, nil,
		func() bool { return false }, // state NOT healthy
		10*time.Second)
	_, _, err := rr.SelectNode(context.Background(), "r")
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeClusterStateUnavailable {
		t.Fatalf("must fail closed, got %v", err)
	}
}

// TestRouter_DistributesRooms is: many rooms over 3 nodes land roughly
// evenly. The router's local-pending correction spreads a burst within one
// report interval.
func TestRouter_DistributesRooms(t *testing.T) {
	reg := NewInMemoryNodeRegistry(time.Minute)
	load := NewInMemoryLoadStore()
	for _, id := range []string{"n1", "n2", "n3"} {
		regNode(reg, id, NodeReady)
		pubLoad(load, id, 0)
	}
	// Route from a neutral perspective (no local preference) so distribution is
	// purely least-loaded + pending.
	rr := newRoomRouter(func() NodeInfo { return NodeInfo{ID: "router", State: NodeReady} },
		reg, load, NewInMemoryRoomLocator(),
		LeastLoadedPolicy{Weights: defaultLoadWeights()}, CapacityConfig{}, &Metrics{}, nil,
		func() bool { return true }, 10*time.Second)

	counts := map[string]int{}
	for i := 0; i < 300; i++ {
		got, _, err := rr.SelectNode(context.Background(), fmt.Sprintf("room-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		counts[got.ID]++
	}
	max, min := 0, 1<<30
	for _, id := range []string{"n1", "n2", "n3"} {
		if counts[id] > max {
			max = counts[id]
		}
		if counts[id] < min {
			min = counts[id]
		}
	}
	if max-min > 60 { // 300/3 = 100; allow 20% spread
		t.Fatalf("uneven distribution: %v (spread %d)", counts, max-min)
	}
}

func TestRouter_ConcurrentSelections(t *testing.T) {
	rr, reg, load := routerHarness(t, CapacityConfig{})
	for _, id := range []string{"n1", "n2", "n3"} {
		regNode(reg, id, NodeReady)
		pubLoad(load, id, 10)
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, _, err := rr.SelectNode(context.Background(), fmt.Sprintf("r%d", i)); err != nil {
				t.Errorf("select %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
}
