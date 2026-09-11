package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// backendFactory builds a fresh ClusterState plus, for Redis, a handle to
// fast-forward its clock for TTL assertions.
type backendFactory struct {
	name    string
	make    func(t *testing.T) (ClusterState, *miniredis.Miniredis)
	ttlSkip bool // in-memory has no key TTL — skip expiry assertions
}

func backends() []backendFactory {
	return []backendFactory{
		{
			name:    "inmemory",
			ttlSkip: true,
			make: func(t *testing.T) (ClusterState, *miniredis.Miniredis) {
				return NewInMemoryClusterState(time.Second), nil
			},
		},
		{
			name: "redis",
			make: func(t *testing.T) (ClusterState, *miniredis.Miniredis) {
				mr := miniredis.RunT(t)
				rc := RedisConfig{
					Addr: mr.Addr(), Prefix: "pulsertc",
					Timeout: time.Second, NodeTTL: 15 * time.Second,
					ParticipTTL: 30 * time.Second,
				}
				st := newRedisClusterStateWith(
					newRedisClientFrom(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "pulsertc"),
					rc, testLogger())
				if err := st.Ping(context.Background()); err != nil {
					t.Fatalf("ping: %v", err)
				}
				return st, mr
			},
		},
	}
}

func TestContract_NodeRegistry(t *testing.T) {
	for _, bf := range backends() {
		t.Run(bf.name, func(t *testing.T) {
			st, _ := bf.make(t)
			nr := st.Nodes()

			if err := nr.Register(NodeInfo{ID: "node-1", State: NodeReady, StartedAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
			// Idempotent.
			_ = nr.Register(NodeInfo{ID: "node-1", State: NodeReady})
			if got := nr.List(); len(got) != 1 {
				t.Fatalf("want 1 node, got %d", len(got))
			}
			if n, ok := nr.Get("node-1"); !ok || n.State != NodeReady {
				t.Fatalf("get: %+v %v", n, ok)
			}
			if err := nr.Register(NodeInfo{ID: ""}); err == nil {
				t.Fatal("empty node id must error")
			}
			nr.Unregister("node-1")
			nr.Unregister("node-1") // no-op
			if _, ok := nr.Get("node-1"); ok {
				t.Fatal("node still present after unregister")
			}
		})
	}
}

func TestContract_NodeTTLExpiry(t *testing.T) {
	for _, bf := range backends() {
		if bf.ttlSkip {
			continue
		}
		t.Run(bf.name, func(t *testing.T) {
			st, mr := bf.make(t)
			nr := st.Nodes()
			_ = nr.Register(NodeInfo{ID: "n", State: NodeReady})

			mr.FastForward(10 * time.Second)
			nr.Heartbeat("n") // refresh TTL
			mr.FastForward(10 * time.Second)
			if _, ok := nr.Get("n"); !ok {
				t.Fatal("heartbeat should have kept the node alive")
			}
			mr.FastForward(20 * time.Second) // no heartbeat -> TTL expires
			if _, ok := nr.Get("n"); ok {
				t.Fatal("stale node should have expired")
			}
		})
	}
}

func TestContract_RoomOwnership(t *testing.T) {
	for _, bf := range backends() {
		t.Run(bf.name, func(t *testing.T) {
			st, _ := bf.make(t)
			rl := st.Rooms()

			owner, claimed := rl.ClaimOwner("room-x", "node-a")
			if owner != "node-a" || !claimed {
				t.Fatalf("first claim: owner=%s claimed=%v", owner, claimed)
			}
			// Idempotent re-claim by the same node.
			owner, claimed = rl.ClaimOwner("room-x", "node-a")
			if owner != "node-a" || claimed {
				t.Fatalf("re-claim: owner=%s claimed=%v", owner, claimed)
			}
			// A different node loses.
			owner, claimed = rl.ClaimOwner("room-x", "node-b")
			if owner != "node-a" || claimed {
				t.Fatalf("conflicting claim: owner=%s claimed=%v", owner, claimed)
			}
			// Compare-and-delete: wrong node cannot release.
			if rl.ReleaseOwner("room-x", "node-b") {
				t.Fatal("node-b must not be able to release node-a's room")
			}
			if got, _ := rl.GetOwner("room-x"); got != "node-a" {
				t.Fatalf("owner changed after bogus release: %s", got)
			}
			// The owner releases.
			if !rl.ReleaseOwner("room-x", "node-a") {
				t.Fatal("owner should release")
			}
			if _, ok := rl.GetOwner("room-x"); ok {
				t.Fatal("room still owned after release")
			}
		})
	}
}

// TestContract_ClaimRace is: hundreds of concurrent claims for one room,
// exactly one winner, on both backends.
func TestContract_ClaimRace(t *testing.T) {
	for _, bf := range backends() {
		t.Run(bf.name, func(t *testing.T) {
			st, _ := bf.make(t)
			rl := st.Rooms()
			const n = 200
			var wg sync.WaitGroup
			var wins int64
			var mu sync.Mutex
			winner := ""
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					node := fmt.Sprintf("node-%d", id)
					if owner, claimed := rl.ClaimOwner("hot-room", node); claimed {
						mu.Lock()
						wins++
						winner = owner
						mu.Unlock()
					}
				}(i)
			}
			wg.Wait()
			if wins != 1 {
				t.Fatalf("want exactly 1 winner, got %d", wins)
			}
			if got, _ := rl.GetOwner("hot-room"); got != winner {
				t.Fatalf("stored owner %s != winner %s", got, winner)
			}
		})
	}
}

func TestContract_ParticipantLocation(t *testing.T) {
	for _, bf := range backends() {
		t.Run(bf.name, func(t *testing.T) {
			st, mr := bf.make(t)
			pl := st.Participants()

			pl.Register(ParticipantLocation{ParticipantID: "u1", RoomID: "r1", NodeID: "node-a"})
			if loc, ok := pl.Lookup("u1"); !ok || loc.NodeID != "node-a" || loc.RoomID != "r1" {
				t.Fatalf("lookup: %+v %v", loc, ok)
			}
			// Reconnect updates location consistently.
			pl.Register(ParticipantLocation{ParticipantID: "u1", RoomID: "r1", NodeID: "node-b"})
			if loc, _ := pl.Lookup("u1"); loc.NodeID != "node-b" {
				t.Fatalf("reconnect should move location, got %s", loc.NodeID)
			}
			if pl.CountByNode("node-b") != 1 {
				t.Fatalf("count by node-b = %d", pl.CountByNode("node-b"))
			}
			pl.Remove("u1")
			pl.Remove("u1") // idempotent
			if _, ok := pl.Lookup("u1"); ok {
				t.Fatal("still located after remove")
			}
			if !bf.ttlSkip {
				pl.Register(ParticipantLocation{ParticipantID: "u2", RoomID: "r1", NodeID: "node-a"})
				mr.FastForward(31 * time.Second)
				if _, ok := pl.Lookup("u2"); ok {
					t.Fatal("participant location should have expired")
				}
			}
		})
	}
}
