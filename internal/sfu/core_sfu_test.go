package sfu

import (
	"io"
	"log/slog"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type nopTransport struct{}

func (nopTransport) SendSFU(any) {}

func newTestSFU(t *testing.T) *SFU {
	t.Helper()
	s, err := New(testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestSFURoomLifecycle(t *testing.T) {
	s := newTestSFU(t)

	r := s.Room("demo")
	if r != s.Room("demo") {
		t.Fatal("Room returned a different instance for the same id")
	}
	if s.RoomCount() != 1 {
		t.Fatalf("expected 1 room, got %d", s.RoomCount())
	}
	if _, ok := s.GetRoom("nope"); ok {
		t.Fatal("GetRoom returned an unknown room")
	}
}

func TestSFUJoinAndLeave(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")

	p, err := r.Join("alice", nopTransport{})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	defer p.close()

	if got, ok := r.participant("alice"); !ok || got != p {
		t.Fatal("participant not registered after Join")
	}
	if r.size() != 1 {
		t.Fatalf("expected room size 1, got %d", r.size())
	}

	r.Leave("alice")
	if _, ok := r.participant("alice"); ok {
		t.Fatal("participant still present after Leave")
	}
	if _, ok := s.GetRoom("demo"); ok {
		t.Fatal("empty room was not garbage collected")
	}
}

func TestSFURoomsAreIsolated(t *testing.T) {
	s := newTestSFU(t)
	ra := s.Room("a")
	rb := s.Room("b")

	pa, _ := ra.Join("p1", nopTransport{})
	pb, _ := rb.Join("p2", nopTransport{})
	defer pa.close()
	defer pb.close()

	if _, ok := ra.participant("p2"); ok {
		t.Fatal("participant from room b is visible in room a")
	}
	if others := ra.othersOf("p1"); len(others) != 0 {
		t.Fatalf("room a should have no other participants, got %d", len(others))
	}
	if _, ok := ra.findPublication("pub-does-not-exist"); ok {
		t.Fatal("findPublication matched a non-existent publication")
	}
}

func TestSubscribeToUnknownPublicationFails(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")
	p, _ := r.Join("alice", nopTransport{})
	defer p.close()

	if err := p.Subscribe("pub-ghost"); err != errNotFound {
		t.Fatalf("expected errNotFound, got %v", err)
	}
}

func TestStatsUnknownRoom(t *testing.T) {
	s := newTestSFU(t)
	st := s.Stats("ghost")
	if st.Room != "ghost" || len(st.Participants) != 0 {
		t.Fatalf("unexpected stats for unknown room: %+v", st)
	}
}
