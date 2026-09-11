package cluster

import "testing"

func nl(id string, participants, rooms int) NodeLoad {
	return NodeLoad{NodeID: id, State: NodeReady, Participants: participants, Rooms: rooms}
}

func TestLeastLoadedPolicy(t *testing.T) {
	p := LeastLoadedPolicy{Weights: defaultLoadWeights()}

	if _, _, err := p.Select(nil); err == nil {
		t.Fatal("empty candidate set must error")
	}

	// Single node.
	got, reason, err := p.Select([]NodeLoad{nl("n1", 10, 2)})
	if err != nil || got.NodeID != "n1" || reason != "least_loaded" {
		t.Fatalf("single: %+v %s %v", got, reason, err)
	}

	// Different load -> least loaded wins.
	got, _, _ = p.Select([]NodeLoad{nl("n1", 80, 4), nl("n2", 30, 1), nl("n3", 50, 2)})
	if got.NodeID != "n2" {
		t.Fatalf("least-loaded should pick n2, got %s", got.NodeID)
	}

	// Equal load -> deterministic tie-break by node id.
	got1, _, _ := p.Select([]NodeLoad{nl("n2", 10, 1), nl("n1", 10, 1)})
	got2, _, _ := p.Select([]NodeLoad{nl("n1", 10, 1), nl("n2", 10, 1)})
	if got1.NodeID != "n1" || got2.NodeID != "n1" {
		t.Fatalf("tie-break must be deterministic (n1), got %s / %s", got1.NodeID, got2.NodeID)
	}
}

func TestLeastLoadedPreferLocalMargin(t *testing.T) {
	p := LeastLoadedPolicy{Weights: defaultLoadWeights(), PreferLocalMargin: 4, LocalNodeID: "n1"}

	// n1 slightly heavier than n2 but within the margin -> stay local.
	got, reason, _ := p.Select([]NodeLoad{nl("n1", 12, 0), nl("n2", 10, 0)})
	if got.NodeID != "n1" || reason != "local" {
		t.Fatalf("within margin should stay local, got %s/%s", got.NodeID, reason)
	}

	// n1 well beyond the margin -> redirect to n2.
	got, reason, _ = p.Select([]NodeLoad{nl("n1", 40, 0), nl("n2", 10, 0)})
	if got.NodeID != "n2" || reason != "least_loaded" {
		t.Fatalf("beyond margin should redirect, got %s/%s", got.NodeID, reason)
	}
}

func TestWeightedLoadPrioritisesLightestNode(t *testing.T) {
	p := LeastLoadedPolicy{Weights: defaultLoadWeights()}
	got, _, _ := p.Select([]NodeLoad{nl("n1", 10, 0), nl("n2", 50, 0), nl("n3", 20, 0)})
	if got.NodeID != "n1" {
		t.Fatalf("new rooms should prioritise n1, got %s", got.NodeID)
	}
}
