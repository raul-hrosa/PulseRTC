package signaling

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// waitType reads up to 6 messages looking for one of type want, skipping
// participant_joined / participant_left churn that can interleave with a
// concurrent reconnect.
func waitType(t *testing.T, c *websocket.Conn, want string) map[string]any {
	t.Helper()
	for i := 0; i < 6; i++ {
		m := readMsg(t, c)
		switch m["type"] {
		case want:
			return m
		case "participant_joined", "participant_left":
			continue
		default:
			t.Fatalf("waiting for %q, got %v", want, m)
		}
	}
	t.Fatalf("did not see %q within 6 messages", want)
	return nil
}

// joinResume sends a join carrying session-resume info.
func joinResume(t *testing.T, c *websocket.Conn, roomID, sessionID string, gen float64) {
	t.Helper()
	msg := map[string]any{"type": "join", "roomId": roomID}
	if sessionID != "" {
		msg["resume"] = map[string]any{"sessionId": sessionID, "generation": gen}
	}
	if err := c.WriteJSON(msg); err != nil {
		t.Fatalf("write join(resume): %v", err)
	}
}

func TestSessionJoinCarriesRecoveryMetadata(t *testing.T) {
	_, wsURL := newTestServer(t)
	c := dial(t, wsURL)
	_ = welcomeID(t, c)
	join(t, c, "room-1")

	rj := expectType(t, c, "room_joined")
	if rj["sessionId"] == nil || rj["sessionId"].(string) == "" {
		t.Fatalf("room_joined missing sessionId: %v", rj)
	}
	if rj["generation"].(float64) != 1 {
		t.Fatalf("generation = %v, want 1", rj["generation"])
	}
	if _, ok := rj["snapshot"].(map[string]any); !ok {
		t.Fatalf("room_joined missing snapshot: %v", rj)
	}
	rc, ok := rj["reconnect"].(map[string]any)
	if !ok || rc["initialDelayMs"].(float64) <= 0 {
		t.Fatalf("room_joined missing reconnect hints: %v", rj)
	}
}

func TestSessionResumeReconnectsWithNewGeneration(t *testing.T) {
	_, wsURL := newTestServer(t)
	tok := mintToken(t, "user-resume", "", nil)

	c1 := dialWithToken(t, wsURL, tok)
	_ = welcomeID(t, c1)
	join(t, c1, "room-1")
	rj := expectType(t, c1, "room_joined")
	sid := rj["sessionId"].(string)
	_ = c1.Close() // simulate a connection drop

	time.Sleep(50 * time.Millisecond)

	c2 := dialWithToken(t, wsURL, tok)
	_ = welcomeID(t, c2)
	joinResume(t, c2, "room-1", sid, 1)

	rj2 := expectType(t, c2, "room_joined")
	if rj2["reconnected"] != true {
		t.Fatalf("expected reconnected=true, got %v", rj2)
	}
	if rj2["generation"].(float64) != 2 {
		t.Fatalf("generation = %v, want 2", rj2["generation"])
	}
	if rj2["sessionId"].(string) != sid {
		t.Fatalf("session id changed on resume")
	}
	expectType(t, c2, "session.reconnected")
}

func TestSessionResumeStaleGenerationRejectedOverWS(t *testing.T) {
	_, wsURL := newTestServer(t)
	tok := mintToken(t, "user-stale", "", nil)

	c1 := dialWithToken(t, wsURL, tok)
	_ = welcomeID(t, c1)
	join(t, c1, "room-1")
	sid := expectType(t, c1, "room_joined")["sessionId"].(string)
	_ = c1.Close()

	// first resume → generation 2
	c2 := dialWithToken(t, wsURL, tok)
	_ = welcomeID(t, c2)
	joinResume(t, c2, "room-1", sid, 1)
	expectType(t, c2, "room_joined")
	expectType(t, c2, "session.reconnected")
	_ = c2.Close()

	// straggler still claiming generation 1
	c3 := dialWithToken(t, wsURL, tok)
	_ = welcomeID(t, c3)
	joinResume(t, c3, "room-1", sid, 1)
	ev := expectType(t, c3, "session.stale")
	if ev["reason"] != CodeStaleSession {
		t.Fatalf("session.stale reason = %v", ev["reason"])
	}
}

func TestSessionResumeReplacesLiveDuplicate(t *testing.T) {
	_, wsURL := newTestServer(t)
	tok := mintToken(t, "user-dup", "", nil)

	c1 := dialWithToken(t, wsURL, tok)
	_ = welcomeID(t, c1)
	join(t, c1, "room-1")
	sid := expectType(t, c1, "room_joined")["sessionId"].(string)

	// c1 is still connected; c2 resumes the same session.
	c2 := dialWithToken(t, wsURL, tok)
	_ = welcomeID(t, c2)
	joinResume(t, c2, "room-1", sid, 1)
	waitType(t, c2, "room_joined")

	// c1 must be told it was replaced, then its socket closes.
	ev := waitType(t, c1, "session.replaced")
	if ev["reason"] != CodeSessionReplaced {
		t.Fatalf("session.replaced reason = %v", ev["reason"])
	}
	_ = c1.SetReadDeadline(time.Now().Add(2 * time.Second))
	closed := false
	for i := 0; i < 6; i++ {
		if _, _, err := c1.ReadMessage(); err != nil {
			closed = true
			break
		}
	}
	if !closed {
		t.Fatal("replaced connection should have been closed")
	}
}
