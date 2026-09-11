package signaling

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/raulhrosa/pulsertc/internal/api"
	"github.com/raulhrosa/pulsertc/internal/auth"
	"github.com/raulhrosa/pulsertc/internal/cluster"
)

type apiNode struct {
	srv    *Server
	clu    *cluster.Cluster
	ts     *httptest.Server
	wsURL  string
	client *http.Client
}

func (n apiNode) get(t *testing.T, path string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest("GET", n.ts.URL+path, nil)
	req.Header.Set("Authorization", "Bearer itest")
	resp, err := n.client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m
}

func (n apiNode) post(t *testing.T, path, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest("POST", n.ts.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer itest")
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m
}

func buildAPICluster(t *testing.T) (a, b apiNode) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := cluster.NewInMemoryClusterState(time.Minute)

	mk := func(id string) *cluster.Cluster {
		c, err := cluster.NewWithState(cluster.Config{
			Enabled: true, NodeID: id, Secret: "cluster-secret", Host: "127.0.0.1", Port: 8090,
			HeartbeatInterval: time.Hour, StaleAfter: time.Minute,
		}, log, st)
		if err != nil {
			t.Fatalf("cluster.NewWithState: %v", err)
		}
		return c
	}
	cluA, cluB := mk("node-A"), mk("node-B")
	nodes := map[string]*cluster.Cluster{"node-A": cluA, "node-B": cluB}
	cluA.UseInMemoryMessageTransport(nodes)
	cluB.UseInMemoryMessageTransport(nodes)

	authn, err := auth.Build(testAuthConfig(), log)
	if err != nil {
		t.Fatalf("auth.Build: %v", err)
	}
	sA, err := NewServerWithDeps(log, authn, cluA)
	if err != nil {
		t.Fatalf("server A: %v", err)
	}
	sB, err := NewServerWithDeps(log, authn, cluB)
	if err != nil {
		t.Fatalf("server B: %v", err)
	}
	cluA.Start()
	cluB.Start()
	t.Cleanup(func() { cluA.Shutdown(); cluB.Shutdown() })

	apiCfg := api.Config{
		Enabled: true, Keys: api.ParseAPIKeys("itest:*"), RateLimitPerMin: 100000,
		PublicWSURL: "wss://pulsertc.test/ws", TokenTTL: time.Hour, TokenTTLMax: 2 * time.Hour,
		RoomRetention: time.Minute,
	}
	issuer := api.NewTokenIssuer(testAuthConfig(), apiCfg)

	mount := func(s *Server, clu *cluster.Cluster) apiNode {
		svc := api.NewService(apiCfg, NewAPICore(s), issuer, nil, log)
		mux := http.NewServeMux()
		mux.HandleFunc("/ws", s.HandleWS)
		clu.RegisterRoutes(mux)
		api.RegisterRoutes(mux, svc, apiCfg, log)
		ts := httptest.NewServer(mux)
		t.Cleanup(ts.Close)
		return apiNode{srv: s, clu: clu, ts: ts, wsURL: wsURLOf(ts.URL), client: ts.Client()}
	}
	return mount(sA, cluA), mount(sB, cluB)
}

// TestAPIMultiNodeContract is the end-to-end: create a room via
// the API, mint a token, connect a participant to its owner node, verify the
// other node reports ROOM_ON_OTHER_NODE, then simulate an ownership
// recovery to node B and verify the participant can rejoin there and that the
// API on node B reflects the new owner and the bumped room generation.
func TestAPIMultiNodeContract(t *testing.T) {
	a, b := buildAPICluster(t)

	// 1. create room + token (issued on node B — the API is stateless for this)
	if st, _ := b.post(t, "/v1/rooms", `{"roomId":"room-mn"}`); st != 201 {
		t.Fatalf("create room: %d", st)
	}
	st, tok := b.post(t, "/v1/rooms/room-mn/tokens", `{"identity":"alice"}`)
	if st != 201 {
		t.Fatalf("mint token: %d", st)
	}
	token := tok["token"].(string)

	// 2. alice joins node A -> node A claims the room
	ca := dialWithToken(t, a.wsURL, token)
	_ = welcomeID(t, ca)
	join(t, ca, "room-mn")
	rj := expectType(t, ca, "room_joined")
	roomGen0, _ := rj["roomGeneration"].(float64)

	// 3. node A: ACTIVE with the participant
	waitFor(t, func() bool {
		code, r := a.get(t, "/v1/rooms/room-mn")
		return code == 200 && r["status"] == "ACTIVE" && r["participants"] == float64(1)
	}, "node A reports room ACTIVE")

	// 4. node B: the room is owned elsewhere
	code, r := b.get(t, "/v1/rooms/room-mn")
	if code != 409 || r["ownerNodeId"] != "node-A" {
		t.Fatalf("node B should report ROOM_ON_OTHER_NODE(node-A), got %d %v", code, r)
	}

	// 5. simulate recovery: ownership node-A -> node-B, generation bumps.
	rr, ok := a.clu.Rooms().(cluster.RecoverableRooms)
	if !ok {
		t.Skip("room locator has no recovery support")
	}
	if out := rr.RecoverOwner("room-mn", "node-A", "node-B"); out.Outcome != cluster.RecoverOK {
		t.Fatalf("RecoverOwner outcome = %+v", out)
	}
	_, genAfter, _ := rr.Ownership("room-mn")
	if int64(genAfter) <= int64(roomGen0) {
		t.Fatalf("room generation did not advance: %d -> %d", int64(roomGen0), genAfter)
	}

	// alice's old socket is dead to her; she reconnects to node B with a fresh
	// join (the logical session lived on the lost node —).
	_ = ca.Close()
	cb := dialWithToken(t, b.wsURL, token)
	_ = welcomeID(t, cb)
	join(t, cb, "room-mn")
	rj2 := expectType(t, cb, "room_joined")
	if g, _ := rj2["roomGeneration"].(float64); int64(g) != int64(genAfter) {
		t.Fatalf("rejoin roomGeneration = %v, want %d", rj2["roomGeneration"], genAfter)
	}

	// 6. node B now reports the room ACTIVE with alice, at the new generation.
	waitFor(t, func() bool {
		code, r := b.get(t, "/v1/rooms/room-mn")
		return code == 200 && r["status"] == "ACTIVE" &&
			r["participants"] == float64(1) && r["generation"] == float64(genAfter)
	}, "node B reports the recovered room ACTIVE")

	code, pl := b.get(t, "/v1/rooms/room-mn/participants")
	parts, _ := pl["participants"].([]any)
	if code != 200 || len(parts) != 1 {
		t.Fatalf("node B participants after recovery: %d %v", code, pl)
	}
}
