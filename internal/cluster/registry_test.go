package cluster

import (
	"testing"
	"time"
)

func node(id string) NodeInfo {
	return NodeInfo{ID: id, Host: "h", Port: 8090, State: NodeReady, StartedAt: time.Now()}
}

func TestRegistryRegisterGetListUnregister(t *testing.T) {
	r := NewInMemoryNodeRegistry(time.Minute)
	if err := r.Register(node("a")); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(node("b")); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Get("a"); !ok {
		t.Fatal("a missing")
	}
	if got := r.List(); len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("list = %v", got)
	}
	r.Unregister("a")
	if _, ok := r.Get("a"); ok {
		t.Fatal("a still present")
	}
	r.Unregister("missing") // no-op
}

func TestRegistryRegisterIsIdempotent(t *testing.T) {
	r := NewInMemoryNodeRegistry(time.Minute)
	_ = r.Register(node("a"))
	_ = r.Register(node("a"))
	if len(r.List()) != 1 {
		t.Fatalf("duplicate register created a second node")
	}
}

func TestRegistryRegisterRequiresID(t *testing.T) {
	if err := NewInMemoryNodeRegistry(0).Register(NodeInfo{}); err == nil {
		t.Fatal("empty id should be rejected")
	}
}

func TestRegistryHeartbeatAndStale(t *testing.T) {
	r := NewInMemoryNodeRegistry(10 * time.Second)
	now := time.Now()
	r.now = func() time.Time { return now }

	_ = r.Register(node("a"))
	if r.Heartbeat("missing") {
		t.Fatal("heartbeat for unknown node should be false")
	}

	n, _ := r.Get("a")
	if r.IsStale(n) {
		t.Fatal("fresh node should not be stale")
	}

	now = now.Add(30 * time.Second)
	n, _ = r.Get("a")
	if !r.IsStale(n) {
		t.Fatal("node should be stale after 30s with a 10s window")
	}

	if !r.Heartbeat("a") {
		t.Fatal("heartbeat for known node should be true")
	}
	n, _ = r.Get("a")
	if r.IsStale(n) {
		t.Fatal("heartbeat should refresh LastSeen")
	}
}

func TestRegistryCounts(t *testing.T) {
	r := NewInMemoryNodeRegistry(10 * time.Second)
	now := time.Now()
	r.now = func() time.Time { return now }
	_ = r.Register(node("a"))
	_ = r.Register(node("b"))
	drain := node("c")
	drain.State = NodeShuttingDown
	_ = r.Register(drain)

	total, active, stale := r.Counts()
	if total != 3 || active != 2 || stale != 0 {
		t.Fatalf("got total=%d active=%d stale=%d", total, active, stale)
	}
	now = now.Add(time.Minute)
	total, active, stale = r.Counts()
	if total != 3 || active != 0 || stale != 3 {
		t.Fatalf("after staleness: total=%d active=%d stale=%d", total, active, stale)
	}
}
