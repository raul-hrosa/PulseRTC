package signaling

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/raulhrosa/pulsertc/internal/auth"
	"github.com/raulhrosa/pulsertc/internal/cluster"
)

// twoSignalingNodes builds nodes A and B sharing one in-memory cluster state and
// an in-process message transport (without HTTP wiring).
func twoSignalingNodes(t *testing.T) (sA *Server, wsA string, cluA, cluB *cluster.Cluster) {
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
	cluA, cluB = mk("node-A"), mk("node-B")
	nodes := map[string]*cluster.Cluster{"node-A": cluA, "node-B": cluB}
	cluA.UseInMemoryMessageTransport(nodes)
	cluB.UseInMemoryMessageTransport(nodes)

	authn, err := auth.Build(testAuthConfig(), log)
	if err != nil {
		t.Fatalf("auth.Build: %v", err)
	}
	sA, err = NewServerWithDeps(log, authn, cluA)
	if err != nil {
		t.Fatalf("NewServerWithDeps A: %v", err)
	}
	if _, err = NewServerWithDeps(log, authn, cluB); err != nil {
		t.Fatalf("NewServerWithDeps B: %v", err)
	}
	cluA.Start()
	cluB.Start()
	t.Cleanup(func() { cluA.Shutdown(); cluB.Shutdown() })

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", sA.HandleWS)
	cluA.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return sA, wsURLOf(ts.URL), cluA, cluB
}

// A participant on node-B sends a signaling message to a participant connected
// to node-A; it must arrive over the cluster transport.
func TestCrossNodeSignalingDeliversToLocalParticipant(t *testing.T) {
	_, wsA, _, cluB := twoSignalingNodes(t)

	ca := dialWithToken(t, wsA, mintToken(t, "alice", "room-x", nil))
	aliceID := welcomeID(t, ca)
	join(t, ca, "room-x")
	expectType(t, ca, "room_joined")

	// Bob lives on node-B; craft the frame node-B's handleForward would build.
	frame, _ := json.Marshal(ForwardedMessage{
		Type:    "signal",
		From:    "bob.1234abcd",
		Target:  aliceID,
		Payload: json.RawMessage(`{"text":"hi from node B"}`),
	})
	msg := cluB.NewClusterMessage(cluster.MsgParticipantMessage, "room-x", aliceID, frame)
	if err := cluB.RouteMessage(context.Background(), aliceID, msg); err != nil {
		t.Fatalf("route B->A: %v", err)
	}

	got := expectType(t, ca, "signal")
	if got["from"] != "bob.1234abcd" {
		t.Fatalf("unexpected forwarded message: %v", got)
	}
}

// A message for a participant nobody has must fail with PARTICIPANT_NOT_FOUND,
// not be broadcast blindly.
func TestCrossNodeSignalingUnknownParticipant(t *testing.T) {
	_, _, cluA, _ := twoSignalingNodes(t)
	msg := cluA.NewClusterMessage(cluster.MsgParticipantMessage, "room-x", "ghost", json.RawMessage(`{}`))
	err := cluA.RouteMessage(context.Background(), "ghost", msg)
	ce, _ := err.(*cluster.Error)
	if ce == nil || ce.Code != cluster.CodeParticipantNotFound {
		t.Fatalf("want PARTICIPANT_NOT_FOUND, got %v", err)
	}
}

// The disconnect of a participant is delivered cross-node and closes the socket.
func TestCrossNodeParticipantDisconnect(t *testing.T) {
	_, wsA, _, cluB := twoSignalingNodes(t)
	ca := dialWithToken(t, wsA, mintToken(t, "carol", "room-y", nil))
	carolID := welcomeID(t, ca)
	join(t, ca, "room-y")
	expectType(t, ca, "room_joined")

	msg := cluB.NewClusterMessage(cluster.MsgParticipantDisconnect, "room-y", carolID, nil)
	if err := cluB.RouteMessage(context.Background(), carolID, msg); err != nil {
		t.Fatalf("route disconnect: %v", err)
	}

	_ = ca.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		if _, _, err := ca.ReadMessage(); err != nil {
			return // socket closed as expected
		}
	}
}
