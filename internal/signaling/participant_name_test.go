package signaling

import (
	"testing"
	"time"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

// mintNamedToken is mintToken plus the "name" claim (display name).
func mintNamedToken(t *testing.T, sub, name string) string {
	t.Helper()
	now := time.Now()
	tok, err := auth.Sign(auth.Claims{
		Subject:     sub,
		Name:        name,
		Permissions: auth.Permissions{Join: true, Publish: true, Subscribe: true, Control: true},
		IssuedAt:    now.Unix(),
		ExpiresAt:   now.Add(time.Hour).Unix(),
		Issuer:      "pulsertc",
		Audience:    "pulsertc",
	}, auth.AlgHS256, testJWTSecret)
	if err != nil {
		t.Fatalf("mintNamedToken: %v", err)
	}
	return tok
}

// TestParticipantNamePropagation checks that a participant's display name shows
// up on welcome, room_joined.participants[] and participant_joined.
func TestParticipantNamePropagation(t *testing.T) {
	_, wsURL := newTestServer(t)

	alice := dialWithToken(t, wsURL, mintNamedToken(t, "user-alice", "Alice"))
	if w := expectType(t, alice, "welcome"); w["name"] != "Alice" {
		t.Fatalf("welcome name: want Alice, got %v", w["name"])
	}
	join(t, alice, "room-names")
	expectType(t, alice, "room_joined")

	bob := dialWithToken(t, wsURL, mintNamedToken(t, "user-bob", "Bob"))
	expectType(t, bob, "welcome")
	join(t, bob, "room-names")

	rj := expectType(t, bob, "room_joined")
	parts := rj["participants"].([]any)
	if len(parts) != 1 {
		t.Fatalf("want 1 existing participant, got %v", parts)
	}
	if p0 := parts[0].(map[string]any); p0["name"] != "Alice" {
		t.Fatalf("room_joined participant name: want Alice, got %v", p0["name"])
	}

	// Alice sees bob's participant_joined with his name.
	pj := expectType(t, alice, "participant_joined")
	if pj["name"] != "Bob" {
		t.Fatalf("participant_joined name: want Bob, got %v", pj["name"])
	}
}

// TestParticipantNameOmittedWhenUnset keeps the field absent for nameless tokens.
func TestParticipantNameOmittedWhenUnset(t *testing.T) {
	_, wsURL := newTestServer(t)

	a := dialWithToken(t, wsURL, mintToken(t, "user-a", "", nil))
	w := expectType(t, a, "welcome")
	if _, ok := w["name"]; ok && w["name"] != "" {
		t.Fatalf("welcome name should be empty/absent, got %v", w["name"])
	}
	join(t, a, "room-anon")
	expectType(t, a, "room_joined")

	b := dialWithToken(t, wsURL, mintToken(t, "user-b", "", nil))
	expectType(t, b, "welcome")
	join(t, b, "room-anon")
	expectType(t, b, "room_joined")

	pj := expectType(t, a, "participant_joined")
	if _, ok := pj["name"]; ok {
		t.Fatalf("participant_joined should omit name, got %v", pj["name"])
	}
}
