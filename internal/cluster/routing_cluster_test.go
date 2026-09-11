package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type routedFixture struct {
	nodes map[string]*Cluster
	st    *InMemoryClusterState
	mu    sync.Mutex
	load  map[string]int
}

func (f *routedFixture) set(id string, participants int) {
	f.mu.Lock()
	f.load[id] = participants
	f.mu.Unlock()
	// Push it now so a test that immediately routes sees it.
	_ = f.st.Load().PublishLoad(NodeLoad{
		NodeID: id, State: NodeReady, Participants: participants, ReportedAt: time.Now(),
	}, time.Minute)
}

func routedCluster(t *testing.T, ids ...string) *routedFixture {
	t.Helper()
	f := &routedFixture{
		nodes: map[string]*Cluster{},
		st:    NewInMemoryClusterState(time.Minute),
		load:  map[string]int{},
	}
	for _, id := range ids {
		c, err := NewWithState(Config{
			Enabled: true, NodeID: id, Secret: "s", Host: "127.0.0.1", Port: 8090,
			HeartbeatInterval: time.Hour, StaleAfter: time.Minute,
			RequestTimeout: time.Second, MessageMaxAge: 30 * time.Second, MaxMessageSize: 64 * 1024,
			LoadReportInterval: time.Hour, // no automatic overwrite in tests
		}, testLogger(), f.st)
		if err != nil {
			t.Fatal(err)
		}
		id := id
		c.SetLoadProvider(func() NodeLoad {
			f.mu.Lock()
			defer f.mu.Unlock()
			return NodeLoad{Participants: f.load[id]}
		})
		f.nodes[id] = c
	}
	for _, c := range f.nodes {
		c.UseInMemoryMessageTransport(f.nodes)
		c.Start()
	}
	t.Cleanup(func() {
		for _, c := range f.nodes {
			c.Shutdown()
		}
	})
	return f
}

func TestClusterClaimRoom_RoutesNewRoomToLeastLoaded(t *testing.T) {
	f := routedCluster(t, "node-a", "node-b", "node-c")
	f.set("node-a", 200)
	f.set("node-b", 10)
	f.set("node-c", 90)

	_, err := f.nodes["node-a"].ClaimRoom(context.Background(), "fresh-room")
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeRoomOnOtherNode || ce.NodeID != "node-b" {
		t.Fatalf("new room on the heavy node should be routed to node-b, got %v", err)
	}

	res, err := f.nodes["node-b"].ClaimRoom(context.Background(), "fresh-room")
	if err != nil || !res.Local {
		t.Fatalf("node-b claim: %+v %v", res, err)
	}
	if owner, _ := f.st.Rooms().GetOwner("fresh-room"); owner != "node-b" {
		t.Fatalf("owner = %s, want node-b", owner)
	}
}

func TestClusterClaimRoom_ExistingRoomNeverReRouted(t *testing.T) {
	f := routedCluster(t, "node-a", "node-b")
	f.set("node-a", 5)
	f.set("node-b", 5)

	if _, err := f.nodes["node-a"].ClaimRoom(context.Background(), "X"); err != nil {
		t.Fatalf("initial claim: %v", err)
	}
	f.set("node-a", 500) // now node-a looks overloaded
	res, err := f.nodes["node-b"].ClaimRoom(context.Background(), "X")
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeRoomOnOtherNode || ce.NodeID != "node-a" {
		t.Fatalf("join to existing room must resolve to its owner node-a, got %+v %v", res, err)
	}
}

// TestClusterClaimRoom_JoinStorm is: many concurrent new-room creations
// following redirects end up with exactly one owner each and a spread of owners.
func TestClusterClaimRoom_JoinStorm(t *testing.T) {
	f := routedCluster(t, "node-a", "node-b", "node-c")
	f.set("node-a", 0)
	f.set("node-b", 0)
	f.set("node-c", 0)
	nodes := f.nodes
	st := f.st
	entry := nodes["node-a"] // every client happens to hit node-a first

	const n = 120
	var wg sync.WaitGroup
	var mu sync.Mutex
	failures := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			room := fmt.Sprintf("room-%d", i)
			ctx := context.Background()
			res, err := entry.ClaimRoom(ctx, room)
			for hop := 0; hop < 4; hop++ {
				if err == nil {
					return
				}
				ce, ok := err.(*Error)
				if !ok || ce.Code != CodeRoomOnOtherNode {
					break
				}
				target := nodes[ce.NodeID]
				if target == nil {
					break
				}
				res, err = target.ClaimRoom(ctx, room)
			}
			if err != nil {
				mu.Lock()
				failures++
				t.Logf("room %s failed: %v", room, err)
				mu.Unlock()
			}
			_ = res
		}(i)
	}
	wg.Wait()

	if failures != 0 {
		t.Fatalf("%d rooms failed to be created", failures)
	}
	owners := st.Rooms().All()
	if len(owners) != n {
		t.Fatalf("want %d rooms owned, got %d", n, len(owners))
	}
	dist := map[string]int{}
	for _, o := range owners {
		dist[o]++
	}
	t.Logf("room distribution: %v", dist)
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		if dist[id] == 0 {
			t.Fatalf("node %s got no rooms — scheduler is not distributing (%v)", id, dist)
		}
	}
}

func TestClusterClaimRoom_AllDrainingIsNoCapacity(t *testing.T) {
	f := routedCluster(t, "node-a", "node-b")
	f.nodes["node-a"].BeginShutdown()
	f.nodes["node-b"].BeginShutdown()

	_, err := f.nodes["node-a"].ClaimRoom(context.Background(), "r")
	ce, _ := err.(*Error)
	// A draining node rejects with NODE_SHUTTING_DOWN before routing is reached.
	if ce == nil || ce.Code != CodeNodeShuttingDown {
		t.Fatalf("got %v", err)
	}
}
