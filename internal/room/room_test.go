package room

import (
	"sync"
	"testing"
)

type fakeParticipant struct {
	id string

	mu       sync.Mutex
	received [][]byte
}

func (f *fakeParticipant) ID() string { return f.id }

func (f *fakeParticipant) Send(data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	f.received = append(f.received, cp)
}

func (f *fakeParticipant) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.received)
}

func TestManagerCreatesRoomOnce(t *testing.T) {
	m := NewManager()
	r1 := m.GetOrCreate("a")
	r2 := m.GetOrCreate("a")
	if r1 != r2 {
		t.Fatal("GetOrCreate returned different rooms for the same id")
	}
	if m.Count() != 1 {
		t.Fatalf("expected 1 room, got %d", m.Count())
	}
}

func TestRoomAddAndListParticipants(t *testing.T) {
	r := newRoom("a")
	r.Add(&fakeParticipant{id: "p1"})
	r.Add(&fakeParticipant{id: "p2"})
	if r.Size() != 2 {
		t.Fatalf("expected 2 participants, got %d", r.Size())
	}
	if len(r.ParticipantIDs()) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(r.ParticipantIDs()))
	}
}

func TestRoomRemoveReportsEmpty(t *testing.T) {
	r := newRoom("a")
	r.Add(&fakeParticipant{id: "p1"})
	if empty := r.Remove("p1"); !empty {
		t.Fatal("expected room to be empty after removing last participant")
	}
}

func TestManagerDeletesEmptyRoom(t *testing.T) {
	m := NewManager()
	r := m.GetOrCreate("a")
	r.Add(&fakeParticipant{id: "p1"})
	m.RemoveParticipant("a", "p1")
	if m.Count() != 0 {
		t.Fatalf("expected empty room to be deleted, still have %d", m.Count())
	}
}

func TestBroadcastExcludesSender(t *testing.T) {
	r := newRoom("a")
	sender := &fakeParticipant{id: "p1"}
	other := &fakeParticipant{id: "p2"}
	r.Add(sender)
	r.Add(other)

	r.Broadcast([]byte("hello"), "p1")

	if sender.count() != 0 {
		t.Fatal("sender should not receive its own broadcast")
	}
	if other.count() != 1 {
		t.Fatalf("other participant should have received 1 message, got %d", other.count())
	}
}

func TestRoomsAreIsolated(t *testing.T) {
	m := NewManager()
	ra := m.GetOrCreate("a")
	rb := m.GetOrCreate("b")
	ra.Add(&fakeParticipant{id: "p1"})
	rb.Add(&fakeParticipant{id: "p2"})

	if _, ok := ra.Get("p2"); ok {
		t.Fatal("participant from room b must not be visible in room a")
	}
}

func TestConcurrentAccess(t *testing.T) {
	m := NewManager()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			r := m.GetOrCreate("room")
			p := &fakeParticipant{id: string(rune('a' + n%26))}
			r.Add(p)
			r.Broadcast([]byte("x"), "")
			r.ParticipantIDs()
			m.RemoveParticipant("room", p.ID())
		}(i)
	}
	wg.Wait()
}
