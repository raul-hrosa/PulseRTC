package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// serveCluster exposes a cluster's routes on an httptest server and returns its
// base URL.
func serveCluster(t *testing.T, c *Cluster) string {
	t.Helper()
	mux := http.NewServeMux()
	c.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestInternalAPIRequiresClusterCredential(t *testing.T) {
	c := enabledCluster(t, "node-1")
	c.Start()
	c.rooms.SetOwner("room-x", "node-1")
	base := serveCluster(t, c)

	// No credential → 401.
	resp, err := http.Get(base + "/internal/rooms/room-x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /internal call: status %d", resp.StatusCode)
	}

	// Wrong credential → 401.
	if got := doStatus(t, base+"/internal/rooms/room-x", "Bearer wrong"); got != http.StatusUnauthorized {
		t.Fatalf("wrong-credential /internal call: status %d", got)
	}
	// Correct credential → 200.
	if got := doStatus(t, base+"/internal/rooms/room-x", "Bearer cluster-secret"); got != http.StatusOK {
		t.Fatalf("authenticated /internal call: status %d", got)
	}
}

func doStatus(t *testing.T, url, auth string) int {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestInternalAPIDisabledWhenClusterOff(t *testing.T) {
	c := disabledCluster(t)
	base := serveCluster(t, c)
	req, _ := http.NewRequest("GET", base+"/internal/nodes", nil)
	req.Header.Set("Authorization", "Bearer anything")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("disabled cluster /internal should 404, got %d", resp.StatusCode)
	}
}

func TestReadyEndpoint(t *testing.T) {
	c := enabledCluster(t, "node-1")
	c.Start()
	base := serveCluster(t, c)

	resp, _ := http.Get(base + "/ready")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ready node: /ready = %d", resp.StatusCode)
	}

	c.BeginShutdown()
	resp2, err := http.Get(base + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("draining node: /ready = %d", resp2.StatusCode)
	}
}

// integration test: two nodes over real HTTP. Node A owns a room; node B
// detects the remote ownership and refuses to create a duplicate.
func TestTwoNodesOverHTTP(t *testing.T) {
	nodeA := enabledCluster(t, "node-A")
	nodeA.Start()
	baseA := serveCluster(t, nodeA)

	nodeB := enabledCluster(t, "node-B", baseA)
	nodeB.transport = NewHTTPTransport("cluster-secret", 0)
	nodeB.Start()
	_ = nodeB.registry.Register(NodeInfo{ID: "node-A", Host: strings.TrimPrefix(baseA, "http://")})

	// A creates the room.
	if res, err := nodeA.ClaimRoom(context.Background(), "consultation-123"); err != nil || !res.Local {
		t.Fatalf("A claim: %+v %v", res, err)
	}

	// B tries the same room → ROOM_ON_OTHER_NODE pointing at node-A.
	_, err := nodeB.ClaimRoom(context.Background(), "consultation-123")
	ce, ok := err.(*Error)
	if !ok || ce.Code != CodeRoomOnOtherNode || ce.NodeID != "node-A" {
		t.Fatalf("B should be redirected to node-A, got %v", err)
	}
	if _, owned := nodeB.rooms.GetOwner("consultation-123"); owned {
		t.Fatal("node-B created a duplicate room")
	}

	// B can create a different room of its own.
	if res, err := nodeB.ClaimRoom(context.Background(), "consultation-999"); err != nil || !res.Local {
		t.Fatalf("B own room: %+v %v", res, err)
	}
}
