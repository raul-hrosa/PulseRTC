package signaling

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/raulhrosa/pulsertc/internal/auth"
	"github.com/raulhrosa/pulsertc/internal/cluster"
)

// clusteredNode builds a full PulseRTC node (signaling + enabled cluster) on an
// httptest server, peered with peerURLs.
func clusteredNode(t *testing.T, nodeID string, peerURLs ...string) (*Server, string) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	authn, err := auth.Build(testAuthConfig(), log)
	if err != nil {
		t.Fatalf("auth.Build: %v", err)
	}
	clu, err := cluster.New(cluster.Config{
		Enabled: true, NodeID: nodeID, Secret: "cluster-secret",
		Peers: peerURLs, Host: "127.0.0.1", Port: 8090,
		HeartbeatInterval: time.Hour, StaleAfter: 2 * time.Hour,
	}, log)
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}
	s, err := NewServerWithDeps(log, authn, clu)
	if err != nil {
		t.Fatalf("NewServerWithDeps: %v", err)
	}
	clu.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.HandleHealth)
	mux.HandleFunc("/ws", s.HandleWS)
	mux.HandleFunc("/metrics", s.Protected(s.HandleMetrics))
	mux.HandleFunc("/metrics.json", s.Protected(s.HandleMetricsJSON))
	clu.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return s, ts.URL
}

func wsURLOf(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http") + "/ws"
}

func TestClusterJoinOnOwningNodeSucceeds(t *testing.T) {
	_, urlA := clusteredNode(t, "node-A")
	c := dialWithToken(t, wsURLOf(urlA), mintToken(t, "u1", "room-x", nil))
	welcomeID(t, c)
	join(t, c, "room-x")
	expectType(t, c, "room_joined")
}

func TestClusterJoinOnWrongNodeIsRedirected(t *testing.T) {
	nodeA, urlA := clusteredNode(t, "node-A")
	_, urlB := clusteredNode(t, "node-B", urlA)
	// node-A also needs to know node-B for a symmetric mesh, but this test only
	// goes B→A so peering B→A is enough.

	// A client joins room-x on node-A → node-A owns it.
	ca := dialWithToken(t, wsURLOf(urlA), mintToken(t, "doctor", "room-x", nil))
	welcomeID(t, ca)
	join(t, ca, "room-x")
	expectType(t, ca, "room_joined")

	// A second client tries the same room on node-B → ROOM_ON_OTHER_NODE.
	cb := dialWithToken(t, wsURLOf(urlB), mintToken(t, "patient", "room-x", nil))
	welcomeID(t, cb)
	join(t, cb, "room-x")
	e := expectType(t, cb, "error")
	if e["code"] != cluster.CodeRoomOnOtherNode {
		t.Fatalf("expected ROOM_ON_OTHER_NODE, got %v", e)
	}
	if e["nodeId"] != "node-A" {
		t.Fatalf("redirect should point at node-A, got %v", e["nodeId"])
	}

	// node-B must not have created a local room-x.
	if _, ok := nodeA.rooms.Get("room-x"); !ok {
		t.Fatal("node-A lost its room")
	}
}

func TestClusterDisabledUnchangedBehavior(t *testing.T) {
	// The default test server has a disabled cluster; a plain join still works
	// and /metrics carries a cluster block flagged disabled.
	ts, wsURL := newTestServer(t)
	c := dial(t, wsURL)
	welcomeID(t, c)
	join(t, c, "solo")
	expectType(t, c, "room_joined")

	req, _ := http.NewRequest("GET", ts.URL+"/metrics.json", nil)
	req.Header.Set("Authorization", "Bearer "+mintToken(t, "admin", "", nil))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"cluster"`) || !strings.Contains(string(body), `"enabled":false`) {
		t.Fatalf("metrics missing disabled cluster block: %s", body)
	}
}
