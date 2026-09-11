package cluster

import (
	"context"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Task F2: control-plane recovery end to end over REAL *Cluster nodes.
//
// recovery_test.go already covers the recoveryManager in isolation (fake
// detector, bare locators). This file exercises the wiring: three real
// clusters on one shared state, a node that stops heartbeating like a crashed
// process, the real FailureDetector noticing, the real Cluster.onNodeStale
// bridge firing, and the real RoomRouter electing the new owner (trigger A).
// ---------------------------------------------------------------------------

// crashFixture is routedCluster's sibling with sub-second failure timings: the
// shared registry TTL, the heartbeat and the detector scan all have to be short
// enough for a node death to be observed inside a unit test.
type crashFixture struct {
	nodes map[string]*Cluster
	st    *InMemoryClusterState
	ttl   time.Duration
}

func crashCluster(t *testing.T, ids ...string) *crashFixture {
	t.Helper()
	const ttl = 300 * time.Millisecond
	f := &crashFixture{
		nodes: map[string]*Cluster{},
		st:    NewInMemoryClusterState(ttl),
		ttl:   ttl,
	}
	for _, id := range ids {
		c, err := NewWithState(Config{
			Enabled: true, NodeID: id, Secret: "s", Host: "127.0.0.1", Port: 8090,
			HeartbeatInterval: 25 * time.Millisecond, StaleAfter: ttl,
			FailureCheckInterval: 20 * time.Millisecond, FailureGracePeriod: 0,
			RecoveryConcurrency: 4,
			RequestTimeout:      time.Second, MessageMaxAge: 30 * time.Second, MaxMessageSize: 64 * 1024,
			LoadReportInterval: time.Hour, // no automatic load overwrite in tests
		}, testLogger(), f.st)
		if err != nil {
			t.Fatal(err)
		}
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

// crash makes a node look like a killed process: its heartbeat and its own
// failure detector stop, but its registry entry stays behind and goes stale on
// TTL. A clean Shutdown would Unregister the node, which the peers' detectors
// never even scan — the interesting case is the stale leftover.
func (f *crashFixture) crash(id string) {
	c := f.nodes[id]
	c.stopOnce.Do(func() { close(c.stopHB) })
	if c.fdCancel != nil {
		c.fdCancel()
	}
	if c.failureDet != nil {
		_ = c.failureDet.Stop()
	}
}

func waitForCond(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", d, what)
}

func TestRoomReclaimedAfterOwnerDeath(t *testing.T) {
	ctx := context.Background()
	f := crashCluster(t, "node-0", "node-1", "node-2")
	n0, n1, n2 := f.nodes["node-0"], f.nodes["node-1"], f.nodes["node-2"]

	// node-0 owns "demo" and hosts a participant in it.
	res, err := n0.ClaimRoom(ctx, "demo")
	if err != nil || !res.Local || res.OwnerID != "node-0" {
		t.Fatalf("node-0 claim demo: res=%+v err=%v", res, err)
	}
	n0.RegisterParticipant("p-1", "demo")

	// node-1 owns "safe": a healthy node's room must survive recovery untouched.
	if res, err := n1.ClaimRoom(ctx, "safe"); err != nil || !res.Local {
		t.Fatalf("node-1 claim safe: res=%+v err=%v", res, err)
	}

	// The whole cluster agrees on where p-1 lives.
	if loc, ok := n1.LocateParticipant(ctx, "p-1"); !ok || loc.NodeID != "node-0" {
		t.Fatalf("before crash LocateParticipant(p-1) = %+v ok=%v, want node-0", loc, ok)
	}

	// --- node-0 dies ------------------------------------------------------
	f.crash("node-0")

	// Trigger A: the real detector on a peer must notice by itself, which is
	// what fires Cluster.onNodeStale -> RecoveryManager.RecoverNode.
	waitForCond(t, "node-1's detector marks node-0 stale", 3*time.Second, func() bool {
		return n1.FailureDetector().IsStale("node-0")
	})

	// (a) "demo" moves to a live node and the generation bumps exactly once.
	var newOwner string
	waitForCond(t, "demo reclaimed by a live node", 3*time.Second, func() bool {
		owner, gen, ok := f.st.rooms.Ownership("demo")
		if !ok || owner == "node-0" || gen != 1 {
			return false
		}
		newOwner = owner
		return true
	})
	if newOwner != "node-1" && newOwner != "node-2" {
		t.Fatalf("demo recovered to unexpected owner %q", newOwner)
	}

	// (b) A join for the existing room resolves to the NEW owner, never node-0.
	res, err = n2.ClaimRoom(ctx, "demo")
	if newOwner == "node-2" {
		if err != nil || !res.Local {
			t.Fatalf("node-2 recovered demo but claim says res=%+v err=%v", res, err)
		}
	} else {
		ce, _ := err.(*Error)
		if ce == nil || ce.Code != CodeRoomOnOtherNode || ce.NodeID != newOwner {
			t.Fatalf("join to recovered demo = %+v %v, want redirect to %s", res, err, newOwner)
		}
	}

	// (c) The stale participant record for the dead node is gone.
	if loc, ok := n1.LocateParticipant(ctx, "p-1"); ok && loc.NodeID == "node-0" {
		t.Fatalf("participant p-1 still resolves to the dead node: %+v", loc)
	}

	// (d) The healthy node's room was never touched.
	if owner, ok := f.st.rooms.GetOwner("safe"); !ok || owner != "node-1" {
		t.Fatalf("safe owner = %q ok=%v, want node-1", owner, ok)
	}
	if _, gen, _ := f.st.rooms.Ownership("safe"); gen != 0 {
		t.Fatalf("safe generation moved to %d — a healthy node's room was recovered", gen)
	}
}
