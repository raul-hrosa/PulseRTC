package cluster

import "testing"

func TestParticipantLocator(t *testing.T) {
	p := NewInMemoryParticipantLocator()

	p.Register(ParticipantLocation{ParticipantID: "u1", RoomID: "r1", NodeID: "n1"})
	p.Register(ParticipantLocation{ParticipantID: "u2", RoomID: "r1", NodeID: "n1"})
	p.Register(ParticipantLocation{ParticipantID: "u3", RoomID: "r2", NodeID: "n2"})

	if loc, ok := p.Lookup("u1"); !ok || loc.NodeID != "n1" || loc.RoomID != "r1" {
		t.Fatalf("Lookup(u1) = %+v,%v", loc, ok)
	}
	if _, ok := p.Lookup("nobody"); ok {
		t.Fatal("unknown participant should not be found")
	}
	if got := p.InRoom("r1"); len(got) != 2 {
		t.Fatalf("InRoom(r1) = %v", got)
	}
	if n := p.CountByNode("n1"); n != 2 {
		t.Fatalf("CountByNode(n1) = %d", n)
	}

	// Move: same id, new node.
	p.Register(ParticipantLocation{ParticipantID: "u1", RoomID: "r1", NodeID: "n2"})
	if loc, _ := p.Lookup("u1"); loc.NodeID != "n2" {
		t.Fatalf("after move, u1 node = %s", loc.NodeID)
	}
	if p.CountByNode("n1") != 1 {
		t.Fatalf("n1 count after move = %d", p.CountByNode("n1"))
	}

	p.Remove("u1")
	if _, ok := p.Lookup("u1"); ok {
		t.Fatal("u1 still present after Remove")
	}
}
