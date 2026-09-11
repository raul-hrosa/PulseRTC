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

	"github.com/gorilla/websocket"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

const testJWTSecret = "test-secret-do-not-use-in-production"

// testAuthConfig is auth enabled with a known secret and generous limits so the
// existing signaling suite exercises the real token path without flaking on
// rate limits.
func testAuthConfig() auth.Config {
	cfg := auth.FromEnv()
	cfg.Enabled = true
	cfg.Secret = testJWTSecret
	cfg.Issuer = "pulsertc"
	cfg.Audience = "pulsertc"
	cfg.Algs = []string{auth.AlgHS256}
	cfg.Leeway = time.Minute
	cfg.MaxTokenAge = time.Hour
	cfg.MaxMessageSize = 64 * 1024
	cfg.ConnRatePerMin = 100000
	cfg.MsgRatePerSec = 100000
	return cfg
}

// mintToken builds a signed HS256 token. An empty room means "any room";
// perms defaults to all-true when nil.
func mintToken(t *testing.T, sub, room string, perms *auth.Permissions) string {
	t.Helper()
	p := auth.Permissions{Join: true, Publish: true, Subscribe: true, Control: true}
	if perms != nil {
		p = *perms
	}
	now := time.Now()
	tok, err := auth.Sign(auth.Claims{
		Subject:     sub,
		Room:        room,
		Permissions: p,
		IssuedAt:    now.Unix(),
		ExpiresAt:   now.Add(time.Hour).Unix(),
		Issuer:      "pulsertc",
		Audience:    "pulsertc",
	}, auth.AlgHS256, testJWTSecret)
	if err != nil {
		t.Fatalf("mintToken: %v", err)
	}
	return tok
}

func newTestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	authn, err := auth.Build(testAuthConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("auth.Build: %v", err)
	}
	s, err := NewServerWithAuthenticator(slog.New(slog.NewTextHandler(io.Discard, nil)), authn)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.HandleHealth)
	mux.HandleFunc("/ws", s.HandleWS)
	mux.HandleFunc("/metrics", s.Protected(s.HandleMetrics))
	mux.HandleFunc("/metrics.json", s.Protected(s.HandleMetricsJSON))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	return ts, wsURL
}

// dial connects with a default all-permissions token not pinned to any room.
func dial(t *testing.T, wsURL string) *websocket.Conn {
	t.Helper()
	return dialWithToken(t, wsURL, mintToken(t, "user-"+t.Name(), "", nil))
}

// dialWithToken connects offering the token as the "pulsertc.token.*"
// subprotocol, exactly like the browser client.
func dialWithToken(t *testing.T, wsURL, token string) *websocket.Conn {
	t.Helper()
	hdr := http.Header{}
	if token != "" {
		hdr.Set("Sec-WebSocket-Protocol", auth.WSProtocol+", pulsertc.token."+token)
	}
	c, _, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// dialExpectStatus asserts the WS handshake is rejected with wantStatus.
func dialExpectStatus(t *testing.T, wsURL, token string, wantStatus int) {
	t.Helper()
	hdr := http.Header{}
	if token != "" {
		hdr.Set("Sec-WebSocket-Protocol", auth.WSProtocol+", pulsertc.token."+token)
	}
	c, resp, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	if err == nil {
		_ = c.Close()
		t.Fatalf("expected handshake failure, got success")
	}
	if resp == nil || resp.StatusCode != wantStatus {
		t.Fatalf("expected status %d, got %v (%v)", wantStatus, resp, err)
	}
}

func readMsg(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, data, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
	return m
}

func expectType(t *testing.T, c *websocket.Conn, want string) map[string]any {
	t.Helper()
	m := readMsg(t, c)
	if m["type"] != want {
		t.Fatalf("expected type %q, got %v", want, m)
	}
	return m
}

func welcomeID(t *testing.T, c *websocket.Conn) string {
	t.Helper()
	m := expectType(t, c, "welcome")
	return m["participantId"].(string)
}

func join(t *testing.T, c *websocket.Conn, roomID string) {
	t.Helper()
	if err := c.WriteJSON(map[string]string{"type": "join", "roomId": roomID}); err != nil {
		t.Fatalf("write join: %v", err)
	}
}

func TestHealth(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Fatalf("body %v", body)
	}
}

func TestConnectAssignsUniqueID(t *testing.T) {
	_, wsURL := newTestServer(t)
	a := welcomeID(t, dial(t, wsURL))
	b := welcomeID(t, dial(t, wsURL))
	if a == "" || b == "" || a == b {
		t.Fatalf("expected two distinct ids, got %q and %q", a, b)
	}
}

func TestJoinReturnsRoomJoinedWithParticipants(t *testing.T) {
	_, wsURL := newTestServer(t)

	c1 := dial(t, wsURL)
	id1 := welcomeID(t, c1)
	join(t, c1, "room-1")
	rj := expectType(t, c1, "room_joined")
	if got := rj["participants"].([]any); len(got) != 0 {
		t.Fatalf("first joiner should see no participants, got %v", got)
	}

	c2 := dial(t, wsURL)
	_ = welcomeID(t, c2)
	join(t, c2, "room-1")
	rj2 := expectType(t, c2, "room_joined")
	parts := rj2["participants"].([]any)
	if len(parts) != 1 || parts[0].(map[string]any)["participantId"] != id1 {
		t.Fatalf("second joiner should see participant %s, got %v", id1, parts)
	}
}

func TestParticipantJoinedEvent(t *testing.T) {
	_, wsURL := newTestServer(t)

	c1 := dial(t, wsURL)
	_ = welcomeID(t, c1)
	join(t, c1, "room-1")
	expectType(t, c1, "room_joined")

	c2 := dial(t, wsURL)
	id2 := welcomeID(t, c2)
	join(t, c2, "room-1")

	ev := expectType(t, c1, "participant_joined")
	if ev["participantId"] != id2 {
		t.Fatalf("expected participant_joined for %s, got %v", id2, ev)
	}
}

func TestParticipantLeftEvent(t *testing.T) {
	_, wsURL := newTestServer(t)

	c1 := dial(t, wsURL)
	_ = welcomeID(t, c1)
	join(t, c1, "room-1")
	expectType(t, c1, "room_joined")

	c2 := dial(t, wsURL)
	id2 := welcomeID(t, c2)
	join(t, c2, "room-1")
	expectType(t, c1, "participant_joined")

	_ = c2.Close()

	ev := expectType(t, c1, "participant_left")
	if ev["participantId"] != id2 {
		t.Fatalf("expected participant_left for %s, got %v", id2, ev)
	}
}

func TestSignalForwardedWithinRoom(t *testing.T) {
	_, wsURL := newTestServer(t)

	c1 := dial(t, wsURL)
	id1 := welcomeID(t, c1)
	join(t, c1, "room-1")
	expectType(t, c1, "room_joined")

	c2 := dial(t, wsURL)
	id2 := welcomeID(t, c2)
	join(t, c2, "room-1")
	expectType(t, c2, "room_joined")
	expectType(t, c1, "participant_joined")

	if err := c1.WriteJSON(map[string]any{
		"type":    "signal",
		"target":  id2,
		"payload": map[string]string{"hello": "world"},
	}); err != nil {
		t.Fatal(err)
	}

	msg := expectType(t, c2, "signal")
	if msg["from"] != id1 {
		t.Fatalf("expected signal from %s, got %v", id1, msg)
	}
	payload := msg["payload"].(map[string]any)
	if payload["hello"] != "world" {
		t.Fatalf("payload not forwarded: %v", payload)
	}
}

func TestSignalToOtherRoomRejected(t *testing.T) {
	_, wsURL := newTestServer(t)

	c1 := dial(t, wsURL)
	_ = welcomeID(t, c1)
	join(t, c1, "room-1")
	expectType(t, c1, "room_joined")

	c2 := dial(t, wsURL)
	id2 := welcomeID(t, c2)
	join(t, c2, "room-2")
	expectType(t, c2, "room_joined")

	if err := c1.WriteJSON(map[string]any{"type": "signal", "target": id2, "payload": map[string]string{}}); err != nil {
		t.Fatal(err)
	}
	expectType(t, c1, "error")
}

func TestInvalidMessages(t *testing.T) {
	_, wsURL := newTestServer(t)
	c := dial(t, wsURL)
	_ = welcomeID(t, c)

	if err := c.WriteMessage(websocket.TextMessage, []byte("not json")); err != nil {
		t.Fatal(err)
	}
	expectType(t, c, "error")

	if err := c.WriteJSON(map[string]string{"type": "bogus"}); err != nil {
		t.Fatal(err)
	}
	expectType(t, c, "error")

	if err := c.WriteJSON(map[string]string{"type": "join"}); err != nil {
		t.Fatal(err)
	}
	expectType(t, c, "error")

	if err := c.WriteJSON(map[string]string{"type": "signal", "target": "x"}); err != nil {
		t.Fatal(err)
	}
	expectType(t, c, "error")
}

func TestConcurrentConnections(t *testing.T) {
	_, wsURL := newTestServer(t)
	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			c := dial(t, wsURL)
			_ = welcomeID(t, c)
			join(t, c, "shared")
			for {
				_ = c.SetReadDeadline(time.Now().Add(time.Second))
				if _, _, err := c.ReadMessage(); err != nil {
					return
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}
}
