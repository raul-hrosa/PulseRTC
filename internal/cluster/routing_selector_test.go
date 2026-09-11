package cluster

import (
	"context"
	"testing"
)

func TestLocalNodeSelector(t *testing.T) {
	c := enabledCluster(t, "node-1")
	c.Start()
	sel := NewLocalNodeSelector(c)

	// New room → this node.
	n, err := sel.SelectNode(context.Background(), "fresh")
	if err != nil || n.ID != "node-1" {
		t.Fatalf("new room selection: %+v %v", n, err)
	}

	// Existing room → its owner.
	c.rooms.SetOwner("owned", "node-2")
	_ = c.registry.Register(NodeInfo{ID: "node-2", State: NodeReady})
	n, err = sel.SelectNode(context.Background(), "owned")
	if err != nil || n.ID != "node-2" {
		t.Fatalf("existing room selection: %+v %v", n, err)
	}

	// Draining node → error for a new room.
	c.BeginShutdown()
	if _, err := sel.SelectNode(context.Background(), "another-fresh"); err == nil {
		t.Fatal("draining node should not be selected for a new room")
	}
}
