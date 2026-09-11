package cluster

import (
	"testing"
	"time"
)

func TestInMemoryLoadStore_TTL(t *testing.T) {
	s := NewInMemoryLoadStore()
	base := time.Now()
	s.now = func() time.Time { return base }

	_ = s.PublishLoad(NodeLoad{NodeID: "n1", Participants: 5}, 100*time.Millisecond)
	if snap, _ := s.LoadSnapshot(); len(snap) != 1 {
		t.Fatalf("want 1, got %d", len(snap))
	}

	s.now = func() time.Time { return base.Add(200 * time.Millisecond) }
	if snap, _ := s.LoadSnapshot(); len(snap) != 0 {
		t.Fatal("stale report must not appear in the snapshot")
	}
}

func TestInMemoryLoadStore_RoomAssignmentTTL(t *testing.T) {
	s := NewInMemoryLoadStore()
	base := time.Now()
	s.now = func() time.Time { return base }
	_ = s.PutRoomAssignment("room-1", "node-b", 50*time.Millisecond)

	if n, ok := s.RoomAssignment("room-1"); !ok || n != "node-b" {
		t.Fatalf("assignment lookup: %s %v", n, ok)
	}
	s.now = func() time.Time { return base.Add(time.Second) }
	if _, ok := s.RoomAssignment("room-1"); ok {
		t.Fatal("assignment must expire")
	}
}

func TestLoadReporter_PublishesOnStart(t *testing.T) {
	store := NewInMemoryLoadStore()
	self := NodeInfo{ID: "n1", State: NodeReady}
	provider := LoadProvider(func() NodeLoad { return NodeLoad{Participants: 42} })
	r := newLoadReporter(store, 20*time.Millisecond, &Metrics{}, nil,
		func() LoadProvider { return provider },
		func() NodeInfo { return self })
	go r.run()
	defer r.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap, _ := store.LoadSnapshot()
		if len(snap) == 1 && snap[0].NodeID == "n1" && snap[0].Participants == 42 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("load reporter never published")
}
