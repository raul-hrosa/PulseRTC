package signaling

import (
	"net/http"
	"testing"
	"time"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

// Scenario 1 — a valid token joins its room.
func TestAuthJoinAuthorized(t *testing.T) {
	_, wsURL := newTestServer(t)
	c := dialWithToken(t, wsURL, mintToken(t, "user-1", "room-a", nil))
	welcomeID(t, c)
	join(t, c, "room-a")
	expectType(t, c, "room_joined")
}

// Scenario 2 — a token with a bad signature never establishes the socket.
func TestAuthInvalidTokenRejected(t *testing.T) {
	_, wsURL := newTestServer(t)
	tok, _ := auth.Sign(auth.Claims{
		Subject: "user-x", Permissions: auth.Permissions{Join: true},
		IssuedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Hour).Unix(),
		Issuer: "pulsertc", Audience: "pulsertc",
	}, auth.AlgHS256, "the-wrong-secret")
	dialExpectStatus(t, wsURL, tok, http.StatusUnauthorized)
}

func TestAuthNoTokenRejected(t *testing.T) {
	_, wsURL := newTestServer(t)
	dialExpectStatus(t, wsURL, "", http.StatusUnauthorized)
}

// Scenario 3 — an expired token is rejected at the handshake.
func TestAuthExpiredTokenRejected(t *testing.T) {
	_, wsURL := newTestServer(t)
	now := time.Now()
	tok, _ := auth.Sign(auth.Claims{
		Subject: "user-x", Permissions: auth.Permissions{Join: true},
		IssuedAt: now.Add(-2 * time.Hour).Unix(), ExpiresAt: now.Add(-time.Hour).Unix(),
		Issuer: "pulsertc", Audience: "pulsertc",
	}, auth.AlgHS256, testJWTSecret)
	dialExpectStatus(t, wsURL, tok, http.StatusUnauthorized)
}

// Scenario 4 — a token pinned to room-a cannot join room-b.
func TestAuthWrongRoomDenied(t *testing.T) {
	_, wsURL := newTestServer(t)
	c := dialWithToken(t, wsURL, mintToken(t, "user-1", "room-a", nil))
	welcomeID(t, c)
	join(t, c, "room-b")
	e := expectType(t, c, "error")
	if e["code"] != auth.CodeRoomAccessDenied {
		t.Fatalf("want ROOM_ACCESS_DENIED, got %v", e)
	}
}

// Scenario 5 — JOIN succeeds, JOIN-only cannot even ask to subscribe.
func TestAuthSubscribeDenied(t *testing.T) {
	_, wsURL := newTestServer(t)
	c := dialWithToken(t, wsURL, mintToken(t, "user-1", "room-a",
		&auth.Permissions{Join: true, Publish: true}))
	welcomeID(t, c)
	join(t, c, "room-a")
	expectType(t, c, "room_joined")

	if err := c.WriteJSON(map[string]any{"type": "subscribe", "publicationId": "pub-xyz"}); err != nil {
		t.Fatal(err)
	}
	e := expectType(t, c, "error")
	if e["code"] != auth.CodeSubscribeDenied {
		t.Fatalf("want SUBSCRIBE_NOT_ALLOWED, got %v", e)
	}
}

// Scenario 7 — CONTROL defaults to false.
func TestAuthControlDenied(t *testing.T) {
	_, wsURL := newTestServer(t)
	c := dialWithToken(t, wsURL, mintToken(t, "user-1", "room-a",
		&auth.Permissions{Join: true, Publish: true, Subscribe: true}))
	welcomeID(t, c)
	join(t, c, "room-a")
	expectType(t, c, "room_joined")

	if err := c.WriteJSON(map[string]any{"type": "control", "target": "someone"}); err != nil {
		t.Fatal(err)
	}
	e := expectType(t, c, "error")
	if e["code"] != auth.CodeControlDenied {
		t.Fatalf("want CONTROL_NOT_ALLOWED, got %v", e)
	}
}

func TestAuthControlAllowedButNotImplemented(t *testing.T) {
	_, wsURL := newTestServer(t)
	c := dialWithToken(t, wsURL, mintToken(t, "admin", "room-a", nil)) // control:true
	welcomeID(t, c)
	join(t, c, "room-a")
	expectType(t, c, "room_joined")

	if err := c.WriteJSON(map[string]any{"type": "control", "target": "someone"}); err != nil {
		t.Fatal(err)
	}
	e := expectType(t, c, "error")
	if e["code"] != nil {
		t.Fatalf("authorized control should not be a security error: %v", e)
	}
}

// The participant id is derived from the token subject; a client cannot pick it.
func TestAuthIdentityDerivedFromToken(t *testing.T) {
	_, wsURL := newTestServer(t)
	c := dialWithToken(t, wsURL, mintToken(t, "user-777", "room-a", nil))
	m := expectType(t, c, "welcome")
	if m["userId"] != "user-777" {
		t.Fatalf("userId should be the token subject, got %v", m["userId"])
	}
	pid, _ := m["participantId"].(string)
	if len(pid) < 9 || pid[:9] != "user-777." {
		t.Fatalf("participantId should be derived from the subject, got %v", pid)
	}
}

// The metrics endpoint is protected once auth is enabled.
func TestAuthMetricsProtected(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /metrics should be 401, got %d", resp.StatusCode)
	}

	req, _ := http.NewRequest("GET", ts.URL+"/metrics", nil)
	req.Header.Set("Authorization", "Bearer "+mintToken(t, "admin", "", nil))
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("authenticated /metrics should be 200, got %d", resp2.StatusCode)
	}
}
