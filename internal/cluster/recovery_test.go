package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// test doubles
// ---------------------------------------------------------------------------

type fakeDetector struct {
	mu    sync.Mutex
	stale map[string]bool
}

func newFakeDetector(stale ...string) *fakeDetector {
	m := map[string]bool{}
	for _, s := range stale {
		m[s] = true
	}
	return &fakeDetector{stale: m}
}

func (f *fakeDetector) Start(context.Context) error { return nil }
func (f *fakeDetector) Stop() error                 { return nil }
func (f *fakeDetector) IsHealthy(n string) bool     { return !f.IsStale(n) }
func (f *fakeDetector) IsStale(n string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stale[n]
}
func (f *fakeDetector) Failures() []NodeFailure { return nil }
func (f *fakeDetector) BackendKnown() bool      { return true }

type fakeRouter struct {
	pick  string
	err   error
	calls atomic.Int64
}

func (f *fakeRouter) SelectNode(context.Context, string) (NodeInfo, string, error) {
	f.calls.Add(1)
	if f.err != nil {
		return NodeInfo{}, "", f.err
	}
	return NodeInfo{ID: f.pick}, "test", nil
}

func selfFn(id string) func() NodeInfo {
	return func() NodeInfo { return NodeInfo{ID: id, State: NodeReady} }
}

// ---------------------------------------------------------------------------
// FailureDetector
// ---------------------------------------------------------------------------

func TestFailureDetectorMarksStaleAfterGrace(t *testing.T) {
	reg := NewInMemoryNodeRegistry(20 * time.Millisecond)
	now := time.Now()
	reg.now = func() time.Time { return now }
	_ = reg.Register(NodeInfo{ID: "node-x", State: NodeReady})

	var staleSeen atomic.Value
	staleSeen.Store("")
	d := newFailureDetector(reg, func() string { return "self" }, func() bool { return true },
		time.Hour, 30*time.Millisecond, &Metrics{}, testLogger(),
		func(id string) { staleSeen.Store(id) })
	d.now = func() time.Time { return now }

	// Heartbeat fresh -> not stale.
	d.scan()
	if d.IsStale("node-x") {
		t.Fatal("fresh node marked stale")
	}

	// Advance past TTL but still inside the grace window.
	now = now.Add(25 * time.Millisecond)
	d.scan()
	if d.IsStale("node-x") {
		t.Fatal("node marked stale during grace period")
	}

	// Advance past TTL + grace.
	now = now.Add(40 * time.Millisecond)
	d.scan()
	if !d.IsStale("node-x") {
		t.Fatal("node not marked stale after grace period expired")
	}
	if staleSeen.Load().(string) != "node-x" {
		t.Fatalf("onStale not fired, got %q", staleSeen.Load())
	}
}

func TestFailureDetectorReadyStateButHeartbeatExpired(t *testing.T) {
	reg := NewInMemoryNodeRegistry(10 * time.Millisecond)
	_ = reg.Register(NodeInfo{ID: "zombie", State: NodeReady})
	time.Sleep(20 * time.Millisecond)

	d := newFailureDetector(reg, func() string { return "self" }, func() bool { return true },
		time.Hour, 0, &Metrics{}, testLogger(), nil)
	d.scan()
	if d.IsHealthy("zombie") {
		t.Fatal("node with State=READY but expired heartbeat must not be healthy")
	}
	if !d.IsStale("zombie") {
		t.Fatal("expired-heartbeat node must be stale regardless of stored State")
	}
}

func TestFailureDetectorFailClosedWhenBackendDown(t *testing.T) {
	reg := NewInMemoryNodeRegistry(1 * time.Millisecond)
	_ = reg.Register(NodeInfo{ID: "node-x", State: NodeReady})
	time.Sleep(5 * time.Millisecond)

	backend := atomic.Bool{} // false = down
	fired := atomic.Int64{}
	d := newFailureDetector(reg, func() string { return "self" }, backend.Load,
		time.Hour, 0, &Metrics{}, testLogger(), func(string) { fired.Add(1) })
	d.scan()
	if d.BackendKnown() {
		t.Fatal("backend reported known while it is down")
	}
	if fired.Load() != 0 {
		t.Fatal("failure emitted while state backend unavailable")
	}

	backend.Store(true)
	d.scan()
	if fired.Load() != 1 {
		t.Fatal("failure not emitted once backend recovered")
	}
}

func TestFailureDetectorNeverMarksSelf(t *testing.T) {
	reg := NewInMemoryNodeRegistry(1 * time.Millisecond)
	_ = reg.Register(NodeInfo{ID: "self", State: NodeReady})
	time.Sleep(5 * time.Millisecond)
	d := newFailureDetector(reg, func() string { return "self" }, func() bool { return true },
		time.Hour, 0, &Metrics{}, testLogger(), nil)
	d.scan()
	if d.IsStale("self") {
		t.Fatal("detector marked its own node stale")
	}
}

// ---------------------------------------------------------------------------
// Atomic ownership recovery + generation
// ---------------------------------------------------------------------------

func TestRecoverOwnerTransfersAndBumpsGeneration(t *testing.T) {
	l := NewInMemoryRoomLocator()
	l.ClaimOwner("room-x", "node-a")

	_, gen, _ := l.Ownership("room-x")
	if gen != 0 {
		t.Fatalf("initial generation = %d, want 0", gen)
	}

	res := l.RecoverOwner("room-x", "node-a", "node-b")
	if res.Outcome != RecoverOK || res.Owner != "node-b" || res.Generation != 1 {
		t.Fatalf("RecoverOwner = %+v", res)
	}
	owner, gen, _ := l.Ownership("room-x")
	if owner != "node-b" || gen != 1 {
		t.Fatalf("after recovery owner=%s gen=%d", owner, gen)
	}
}

func TestRecoverOwnerIdempotent(t *testing.T) {
	l := NewInMemoryRoomLocator()
	l.ClaimOwner("room-x", "node-a")
	l.RecoverOwner("room-x", "node-a", "node-b")

	res := l.RecoverOwner("room-x", "node-a", "node-b")
	if res.Outcome != RecoverAlready {
		t.Fatalf("second recovery outcome = %v, want RecoverAlready", res.Outcome)
	}
	if _, gen, _ := l.Ownership("room-x"); gen != 1 {
		t.Fatalf("generation bumped twice: %d", gen)
	}
}

func TestRecoverOwnerConflict(t *testing.T) {
	l := NewInMemoryRoomLocator()
	l.ClaimOwner("room-x", "node-c") // someone else already took it

	res := l.RecoverOwner("room-x", "node-a", "node-b")
	if res.Outcome != RecoverConflict || res.Owner != "node-c" {
		t.Fatalf("RecoverOwner = %+v, want conflict/node-c", res)
	}
}

func TestConcurrentRecoverOwnerSingleWinner(t *testing.T) {
	l := NewInMemoryRoomLocator()
	l.ClaimOwner("room-x", "node-dead")

	const goroutines = 100
	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	var wins int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		newOwner := nodes[i%len(nodes)]
		go func() {
			defer wg.Done()
			if l.RecoverOwner("room-x", "node-dead", newOwner).Outcome == RecoverOK {
				atomic.AddInt64(&wins, 1)
			}
		}()
	}
	wg.Wait()

	if wins != 1 {
		t.Fatalf("recovery winners = %d, want exactly 1", wins)
	}
	owner, gen, _ := l.Ownership("room-x")
	if gen != 1 {
		t.Fatalf("generation incremented %d times, want once", gen)
	}
	found := false
	for _, n := range nodes {
		if n == owner {
			found = true
		}
	}
	if !found {
		t.Fatalf("owner %q is not one of the candidates", owner)
	}
}

// ---------------------------------------------------------------------------
// RecoveryManager
// ---------------------------------------------------------------------------

func newTestRecovery(t *testing.T, self string, rooms *InMemoryRoomLocator, det FailureDetector, pick string) (*recoveryManager, *fakeRouter, *InMemoryParticipantLocator) {
	t.Helper()
	rtr := &fakeRouter{pick: pick}
	parts := NewInMemoryParticipantLocator()
	rm := newRecoveryManager(selfFn(self), rooms, rooms.All, rtr, det, parts, nil,
		func() bool { return true }, true, 4, &Metrics{}, testLogger())
	return rm, rtr, parts
}

func TestRecoveryManagerRecoversStaleNodeRooms(t *testing.T) {
	rooms := NewInMemoryRoomLocator()
	rooms.ClaimOwner("room-x", "node-a")
	rooms.ClaimOwner("room-y", "node-a")
	rooms.ClaimOwner("room-z", "node-b") // healthy owner, must be untouched

	det := newFakeDetector("node-a")
	rm, _, parts := newTestRecovery(t, "node-b", rooms, det, "node-b")
	parts.Register(ParticipantLocation{ParticipantID: "p1", RoomID: "room-x", NodeID: "node-a"})

	if err := rm.RecoverNode(context.Background(), "node-a"); err != nil {
		t.Fatalf("RecoverNode: %v", err)
	}

	for _, r := range []string{"room-x", "room-y"} {
		owner, gen, _ := rooms.Ownership(r)
		if owner != "node-b" || gen != 1 {
			t.Fatalf("%s owner=%s gen=%d, want node-b/1", r, owner, gen)
		}
	}
	if owner, _ := rooms.GetOwner("room-z"); owner != "node-b" {
		t.Fatalf("healthy node's room-z owner changed to %s", owner)
	}
	if _, ok := parts.Lookup("p1"); ok {
		t.Fatal("participant on dead node not cleaned up")
	}
}

func TestRecoveryManagerSkipsHealthyOwner(t *testing.T) {
	rooms := NewInMemoryRoomLocator()
	rooms.ClaimOwner("room-x", "node-a")
	det := newFakeDetector() // nobody stale

	rm, _, _ := newTestRecovery(t, "node-b", rooms, det, "node-b")
	if err := rm.RecoverNode(context.Background(), "node-a"); err != nil {
		t.Fatalf("RecoverNode: %v", err)
	}
	if owner, _ := rooms.GetOwner("room-x"); owner != "node-a" {
		t.Fatalf("room-x taken from healthy node-a (owner=%s)", owner)
	}
}

func TestRecoveryManagerIdempotent(t *testing.T) {
	rooms := NewInMemoryRoomLocator()
	rooms.ClaimOwner("room-x", "node-a")
	det := newFakeDetector("node-a")
	rm, _, _ := newTestRecovery(t, "node-b", rooms, det, "node-b")

	_ = rm.RecoverRoom(context.Background(), "room-x")
	err := rm.RecoverRoom(context.Background(), "room-x")
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeAlreadyRecovered {
		t.Fatalf("second RecoverRoom = %v, want ALREADY_RECOVERED", err)
	}
	if _, gen, _ := rooms.Ownership("room-x"); gen != 1 {
		t.Fatalf("generation moved twice: %d", gen)
	}
}

func TestRecoveryManagerFailClosedWhenBackendDown(t *testing.T) {
	rooms := NewInMemoryRoomLocator()
	rooms.ClaimOwner("room-x", "node-a")
	det := newFakeDetector("node-a")
	rtr := &fakeRouter{pick: "node-b"}
	rm := newRecoveryManager(selfFn("node-b"), rooms, rooms.All, rtr, det,
		NewInMemoryParticipantLocator(), nil, func() bool { return false }, // backend down
		true, 4, &Metrics{}, testLogger())

	err := rm.RecoverRoom(context.Background(), "room-x")
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeClusterStateUnavailable {
		t.Fatalf("recovery must fail closed, got %v", err)
	}
	if owner, _ := rooms.GetOwner("room-x"); owner != "node-a" {
		t.Fatalf("ownership changed during backend outage: %s", owner)
	}
}

func TestRecoveryManagerDefersToElectedNode(t *testing.T) {
	rooms := NewInMemoryRoomLocator()
	rooms.ClaimOwner("room-x", "node-a")
	det := newFakeDetector("node-a")
	// Router elects node-c, but this manager runs as node-b.
	rm, _, _ := newTestRecovery(t, "node-b", rooms, det, "node-c")

	if err := rm.RecoverRoom(context.Background(), "room-x"); err != nil {
		t.Fatalf("RecoverRoom: %v", err)
	}
	if owner, _ := rooms.GetOwner("room-x"); owner != "node-a" {
		t.Fatalf("node-b took a room elected to node-c (owner=%s)", owner)
	}
}

func TestRecoveryManagerConcurrentNodesOneWinner(t *testing.T) {
	rooms := NewInMemoryRoomLocator()
	rooms.ClaimOwner("room-x", "node-dead")
	det := newFakeDetector("node-dead")

	const managers = 10
	var conflicts int64
	var wg sync.WaitGroup
	for i := 0; i < managers; i++ {
		self := "node-" + string(rune('a'+i))
		rm, _, _ := newTestRecovery(t, self, rooms, det, self)
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := rm.RecoverRoom(context.Background(), "room-x")
			if ce, ok := err.(*Error); ok && (ce.Code == CodeOwnershipConflict || ce.Code == CodeAlreadyRecovered) {
				atomic.AddInt64(&conflicts, 1)
			}
		}()
	}
	wg.Wait()

	_ = conflicts
	// Exactly one atomic takeover: generation moves once and the room
	// leaves the dead node. Managers that lost the race either saw a conflict or
	// found a healthy new owner and stood down — never a second swap.
	owner, gen, _ := rooms.Ownership("room-x")
	if gen != 1 {
		t.Fatalf("generation = %d, want exactly 1", gen)
	}
	if owner == "node-dead" {
		t.Fatal("room still owned by the dead node")
	}
}

// ---------------------------------------------------------------------------
// Participant routing rejects stale nodes
// ---------------------------------------------------------------------------

func TestParticipantRouterRejectsStaleNode(t *testing.T) {
	loc := NewInMemoryParticipantLocator()
	loc.Register(ParticipantLocation{ParticipantID: "alice", RoomID: "r", NodeID: "node-a"})
	det := newFakeDetector("node-a")
	m := &Metrics{}
	r := &participantRouter{
		self:         "node-self",
		cache:        newLocationCache(time.Second),
		locator:      loc,
		delivery:     func() LocalDelivery { return nil },
		metrics:      m,
		stateHealthy: func() bool { return true },
		nodeStale:    det.IsStale,
	}

	err := r.Route(context.Background(), "alice", ClusterMessage{})
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeParticipantStale {
		t.Fatalf("Route to stale node = %v, want PARTICIPANT_STALE", err)
	}
	if m.routingStaleNodeC.Load() == 0 {
		t.Fatal("routing.stale_node metric not incremented")
	}
}

// ---------------------------------------------------------------------------
// End-to-end over shared (Redis) state: node dies, peer recovers, new joins
// land on the new owner.
// ---------------------------------------------------------------------------

func TestClusterRecoveryEndToEndRedis(t *testing.T) {
	a, mr := redisCluster(t, "node-a")
	a.Start()
	b := clusterOnSameRedis(t, mr, "node-b")
	b.Start()

	if _, err := a.ClaimRoom(context.Background(), "room-1"); err != nil {
		t.Fatalf("A claim: %v", err)
	}

	// node-a dies for good: its registry key goes away.
	a.Shutdown()

	if !b.FailureDetector().IsStale("node-a") {
		t.Fatal("node-a not detected stale on b after it left the registry")
	}
	if err := b.RecoveryManager().RecoverNode(context.Background(), "node-a"); err != nil {
		t.Fatalf("b RecoverNode: %v", err)
	}

	owner, gen, ok := b.rooms.(RecoverableRooms).Ownership("room-1")
	if !ok || owner != "node-b" || gen != 1 {
		t.Fatalf("room-1 after recovery: owner=%s gen=%d ok=%v", owner, gen, ok)
	}

	// A fresh join for room-1 now resolves locally on b — a second recovery is
	// an idempotent no-op.
	res, err := b.ClaimRoom(context.Background(), "room-1")
	if err != nil || !res.Local {
		t.Fatalf("post-recovery join: res=%+v err=%v", res, err)
	}
	if err := b.RecoveryManager().RecoverRoom(context.Background(), "room-1"); err != nil {
		if ce, _ := err.(*Error); ce == nil || ce.Code != CodeAlreadyRecovered {
			t.Fatalf("idempotent re-recovery: %v", err)
		}
	}
	if _, gen, _ := b.rooms.(RecoverableRooms).Ownership("room-1"); gen != 1 {
		t.Fatalf("generation moved on idempotent re-recovery: %d", gen)
	}
}

// With Redis unavailable, no recovery, no takeover.
func TestClusterRecoveryFailClosedRedisDown(t *testing.T) {
	a, mr := redisCluster(t, "node-a")
	a.Start()
	b := clusterOnSameRedis(t, mr, "node-b")
	b.Start()
	if _, err := a.ClaimRoom(context.Background(), "room-1"); err != nil {
		t.Fatalf("claim: %v", err)
	}

	mr.Close() // authoritative state gone
	_ = b.state.Ping(context.Background())

	err := b.RecoveryManager().RecoverNode(context.Background(), "node-a")
	if ce, _ := err.(*Error); ce == nil || ce.Code != CodeClusterStateUnavailable {
		t.Fatalf("recovery during Redis outage = %v, want CLUSTER_STATE_UNAVAILABLE", err)
	}
}
