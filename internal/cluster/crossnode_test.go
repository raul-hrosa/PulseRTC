package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeDelivery stands in for the signaling layer: it knows which participant ids
// are "connected" on its node and records what was delivered.
type fakeDelivery struct {
	mu         sync.Mutex
	local      map[string]bool
	received   []ClusterMessage
	roomEvents []ClusterMessage
}

func newFakeDelivery() *fakeDelivery { return &fakeDelivery{local: map[string]bool{}} }

func (f *fakeDelivery) addLocal(id string)    { f.mu.Lock(); f.local[id] = true; f.mu.Unlock() }
func (f *fakeDelivery) removeLocal(id string) { f.mu.Lock(); delete(f.local, id); f.mu.Unlock() }

func (f *fakeDelivery) DeliverToParticipant(_ context.Context, msg ClusterMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.local[msg.ParticipantID] {
		return &Error{Code: CodeParticipantNotFound, Message: "not here", ParticipantID: msg.ParticipantID}
	}
	f.received = append(f.received, msg)
	return nil
}

func (f *fakeDelivery) DeliverRoomEvent(_ context.Context, msg ClusterMessage) error {
	f.mu.Lock()
	f.roomEvents = append(f.roomEvents, msg)
	f.mu.Unlock()
	return nil
}

func (f *fakeDelivery) count() int      { f.mu.Lock(); defer f.mu.Unlock(); return len(f.received) }
func (f *fakeDelivery) roomEventN() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.roomEvents) }

// miniCluster is N in-process nodes sharing one ClusterState and an in-memory
// message transport — the multi-node scenario without HTTP.
type miniCluster struct {
	nodes map[string]*Cluster
	del   map[string]*fakeDelivery
}

func newMiniCluster(t *testing.T, ids ...string) *miniCluster {
	t.Helper()
	st := NewInMemoryClusterState(time.Minute)
	mc := &miniCluster{nodes: map[string]*Cluster{}, del: map[string]*fakeDelivery{}}
	for _, id := range ids {
		c, err := NewWithState(Config{
			Enabled: true, NodeID: id, Secret: "s", Host: "localhost", Port: 8090,
			HeartbeatInterval: time.Hour, StaleAfter: time.Minute,
			RequestTimeout: 200 * time.Millisecond, MessageMaxAge: 30 * time.Second,
			MaxMessageSize: 64 * 1024,
		}, testLogger(), st)
		if err != nil {
			t.Fatal(err)
		}
		d := newFakeDelivery()
		c.SetLocalDelivery(d)
		mc.nodes[id] = c
		mc.del[id] = d
	}
	for _, c := range mc.nodes {
		c.UseInMemoryMessageTransport(mc.nodes)
		c.Start()
	}
	t.Cleanup(func() {
		for _, c := range mc.nodes {
			c.Shutdown()
		}
	})
	return mc
}

func (mc *miniCluster) connect(node, participantID, roomID string) {
	mc.nodes[node].RegisterParticipant(participantID, roomID)
	mc.del[node].addLocal(participantID)
}

func (mc *miniCluster) send(t *testing.T, from, target string) error {
	t.Helper()
	c := mc.nodes[from]
	msg := c.NewClusterMessage(MsgParticipantMessage, "room-1", target, json.RawMessage(`{"hi":true}`))
	return c.RouteMessage(context.Background(), target, msg)
}

func TestCrossNode_RouteBothWays(t *testing.T) {
	mc := newMiniCluster(t, "n1", "n2")
	mc.connect("n1", "A", "room-1")
	mc.connect("n2", "B", "room-1")

	if err := mc.send(t, "n1", "B"); err != nil {
		t.Fatalf("A->B: %v", err)
	}
	if mc.del["n2"].count() != 1 {
		t.Fatalf("B should have received 1, got %d", mc.del["n2"].count())
	}
	if err := mc.send(t, "n2", "A"); err != nil {
		t.Fatalf("B->A: %v", err)
	}
	if mc.del["n1"].count() != 1 {
		t.Fatalf("A should have received 1, got %d", mc.del["n1"].count())
	}
}

func TestCrossNode_LocalDeliveryDoesNotUseTransport(t *testing.T) {
	mc := newMiniCluster(t, "n1", "n2")
	mc.connect("n1", "A", "room-1")
	mc.connect("n1", "C", "room-1")

	before := mc.nodes["n1"].metrics.msgSentC.Load()
	if err := mc.send(t, "n1", "C"); err != nil {
		t.Fatalf("local route: %v", err)
	}
	if mc.nodes["n1"].metrics.routeLocalC.Load() == 0 {
		t.Fatal("expected a local routing hit")
	}
	if mc.nodes["n1"].metrics.msgSentC.Load() != before {
		t.Fatal("local delivery must not send over the transport")
	}
	if mc.del["n1"].count() != 1 {
		t.Fatal("C should have received the message locally")
	}
}

func TestCrossNode_ParticipantNotFound(t *testing.T) {
	mc := newMiniCluster(t, "n1", "n2")
	err := mc.send(t, "n1", "ghost")
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeParticipantNotFound {
		t.Fatalf("want PARTICIPANT_NOT_FOUND, got %v", err)
	}
	if mc.nodes["n1"].metrics.routeNotFoundC.Load() == 0 {
		t.Fatal("routing.not_found metric not incremented")
	}
}

func TestCrossNode_LoopPreventionTargetMismatch(t *testing.T) {
	mc := newMiniCluster(t, "n1", "n2")
	// A message addressed to n1 but handed to n2 must be rejected, not forwarded.
	msg := mc.nodes["n1"].NewClusterMessage(MsgParticipantMessage, "room-1", "x", nil)
	msg.TargetNodeID = "n1"
	err := mc.nodes["n2"].handleInboundClusterMessage(context.Background(), msg)
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeClusterTargetMismatch {
		t.Fatalf("want CLUSTER_TARGET_MISMATCH, got %v", err)
	}
}

func TestCrossNode_Idempotency(t *testing.T) {
	mc := newMiniCluster(t, "n1", "n2")
	mc.connect("n2", "B", "room-1")

	msg := mc.nodes["n1"].NewClusterMessage(MsgParticipantMessage, "room-1", "B", json.RawMessage(`{}`))
	for i := 0; i < 3; i++ {
		if err := mc.nodes["n1"].RouteMessage(context.Background(), "B", msg); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if got := mc.del["n2"].count(); got != 1 {
		t.Fatalf("duplicate requestId must be processed once, got %d", got)
	}
	if mc.nodes["n2"].metrics.msgDuplicatedC.Load() != 2 {
		t.Fatalf("expected 2 duplicates counted, got %d", mc.nodes["n2"].metrics.msgDuplicatedC.Load())
	}
}

func TestCrossNode_NodeOffline(t *testing.T) {
	mc := newMiniCluster(t, "n1", "n2")
	mc.connect("n2", "B", "room-1")
	mc.nodes["n2"].BeginShutdown() // DRAINING -> not accepting new operations

	done := make(chan error, 1)
	go func() { done <- mc.send(t, "n1", "B") }()
	select {
	case err := <-done:
		ce, _ := err.(*Error)
		if ce == nil || ce.Code != CodeClusterNodeUnavailable {
			t.Fatalf("want CLUSTER_NODE_UNAVAILABLE, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send to an offline node blocked — no timeout")
	}
}

func TestCrossNode_StaleLocation(t *testing.T) {
	mc := newMiniCluster(t, "n1", "n2")
	mc.connect("n2", "B", "room-1")

	// Warm n1's location cache: B is on n2.
	if err := mc.send(t, "n1", "B"); err != nil {
		t.Fatalf("initial send: %v", err)
	}
	// B reconnects on n1. Shared state now points to n1.
	mc.del["n2"].removeLocal("B")
	mc.connect("n1", "B", "room-1")

	// n1 still has a stale cache entry for n2; the router must recover.
	if err := mc.send(t, "n1", "B"); err != nil {
		t.Fatalf("send after move: %v", err)
	}
	if mc.del["n1"].count() != 1 {
		t.Fatalf("message should now be delivered locally on n1, got %d", mc.del["n1"].count())
	}
}

func TestCrossNode_RoomEventFanOut(t *testing.T) {
	mc := newMiniCluster(t, "n1", "n2", "n3")
	mc.connect("n1", "A", "room-1")
	mc.connect("n2", "B", "room-1")
	mc.connect("n3", "C", "room-1")

	mc.nodes["n1"].BroadcastRoomEvent(context.Background(), "room-1", []byte(`{"type":"participant_joined"}`))
	if mc.del["n2"].roomEventN() != 1 || mc.del["n3"].roomEventN() != 1 {
		t.Fatalf("room event should reach n2 and n3: n2=%d n3=%d",
			mc.del["n2"].roomEventN(), mc.del["n3"].roomEventN())
	}
	if mc.del["n1"].roomEventN() != 0 {
		t.Fatal("origin node must not receive its own room event over the transport")
	}
}

func TestCrossNode_ConcurrentMessages(t *testing.T) {
	mc := newMiniCluster(t, "n1", "n2")
	mc.connect("n2", "B", "room-1")

	const n = 100
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := mc.nodes["n1"]
			msg := c.NewClusterMessage(MsgParticipantMessage, "room-1", "B",
				json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)))
			if err := c.RouteMessage(context.Background(), "B", msg); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent send failed: %v", err)
	}
	if got := mc.del["n2"].count(); got != n {
		t.Fatalf("want %d delivered, got %d", n, got)
	}
}
