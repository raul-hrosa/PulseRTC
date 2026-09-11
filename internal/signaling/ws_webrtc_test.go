package signaling

import (
	"testing"

	"github.com/gorilla/websocket"
)

// joinedPair connects two clients into the same room and drains the join
// handshake, returning the two connections and their ids.
func joinedPair(t *testing.T, wsURL, roomID string) (a, b *websocket.Conn, idA, idB string) {
	t.Helper()

	a = dial(t, wsURL)
	idA = welcomeID(t, a)
	join(t, a, roomID)
	expectType(t, a, "room_joined")

	b = dial(t, wsURL)
	idB = welcomeID(t, b)
	join(t, b, roomID)
	expectType(t, b, "room_joined")
	expectType(t, a, "participant_joined")

	return a, b, idA, idB
}

func webrtcCases() []struct {
	name    string
	msgType string
	payload map[string]any
} {
	return []struct {
		name    string
		msgType string
		payload map[string]any
	}{
		{"offer", TypeWebRTCOffer, map[string]any{"sdp": "v=0..."}},
		{"answer", TypeWebRTCAnswer, map[string]any{"sdp": "v=0..."}},
		{"ice", TypeWebRTCICECandidate, map[string]any{"candidate": "candidate:1 1 udp", "sdpMid": "0", "sdpMLineIndex": 0}},
	}
}

func TestWebRTCForwardedWithinRoom(t *testing.T) {
	for _, tc := range webrtcCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, wsURL := newTestServer(t)
			a, b, idA, idB := joinedPair(t, wsURL, "room-1")

			if err := a.WriteJSON(map[string]any{"type": tc.msgType, "target": idB, "payload": tc.payload}); err != nil {
				t.Fatal(err)
			}

			msg := expectType(t, b, tc.msgType)
			if msg["from"] != idA {
				t.Fatalf("expected from %s, got %v", idA, msg["from"])
			}
			if _, ok := msg["payload"].(map[string]any); !ok {
				t.Fatalf("payload not forwarded: %v", msg)
			}
		})
	}
}

func TestWebRTCRejectedAcrossRooms(t *testing.T) {
	for _, tc := range webrtcCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, wsURL := newTestServer(t)

			a := dial(t, wsURL)
			_ = welcomeID(t, a)
			join(t, a, "room-a")
			expectType(t, a, "room_joined")

			b := dial(t, wsURL)
			idB := welcomeID(t, b)
			join(t, b, "room-b")
			expectType(t, b, "room_joined")

			if err := a.WriteJSON(map[string]any{"type": tc.msgType, "target": idB, "payload": tc.payload}); err != nil {
				t.Fatal(err)
			}
			expectType(t, a, "error")
		})
	}
}

func TestWebRTCRejectedUnknownTarget(t *testing.T) {
	for _, tc := range webrtcCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, wsURL := newTestServer(t)
			a, _, _, _ := joinedPair(t, wsURL, "room-1")

			if err := a.WriteJSON(map[string]any{"type": tc.msgType, "target": "does-not-exist", "payload": tc.payload}); err != nil {
				t.Fatal(err)
			}
			expectType(t, a, "error")
		})
	}
}

func TestWebRTCRejectedInvalidPayload(t *testing.T) {
	for _, tc := range webrtcCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, wsURL := newTestServer(t)
			a, _, _, idB := joinedPair(t, wsURL, "room-1")

			// Missing payload entirely.
			if err := a.WriteJSON(map[string]any{"type": tc.msgType, "target": idB}); err != nil {
				t.Fatal(err)
			}
			expectType(t, a, "error")
		})
	}
}

func TestWebRTCRejectedWithoutJoin(t *testing.T) {
	_, wsURL := newTestServer(t)
	a := dial(t, wsURL)
	_ = welcomeID(t, a)

	if err := a.WriteJSON(map[string]any{"type": TypeWebRTCOffer, "target": "x", "payload": map[string]any{"sdp": "x"}}); err != nil {
		t.Fatal(err)
	}
	expectType(t, a, "error")
}

func TestWebRTCFullNegotiationRelay(t *testing.T) {
	_, wsURL := newTestServer(t)
	a, b, idA, idB := joinedPair(t, wsURL, "room-demo")

	// A -> offer -> B
	if err := a.WriteJSON(map[string]any{"type": TypeWebRTCOffer, "target": idB, "payload": map[string]any{"sdp": "OFFER"}}); err != nil {
		t.Fatal(err)
	}
	if got := expectType(t, b, TypeWebRTCOffer); got["from"] != idA {
		t.Fatalf("offer from %v", got["from"])
	}

	// B -> answer -> A
	if err := b.WriteJSON(map[string]any{"type": TypeWebRTCAnswer, "target": idA, "payload": map[string]any{"sdp": "ANSWER"}}); err != nil {
		t.Fatal(err)
	}
	if got := expectType(t, a, TypeWebRTCAnswer); got["from"] != idB {
		t.Fatalf("answer from %v", got["from"])
	}

	// both -> ICE
	if err := a.WriteJSON(map[string]any{"type": TypeWebRTCICECandidate, "target": idB, "payload": map[string]any{"candidate": "A-CAND"}}); err != nil {
		t.Fatal(err)
	}
	expectType(t, b, TypeWebRTCICECandidate)
	if err := b.WriteJSON(map[string]any{"type": TypeWebRTCICECandidate, "target": idA, "payload": map[string]any{"candidate": "B-CAND"}}); err != nil {
		t.Fatal(err)
	}
	expectType(t, a, TypeWebRTCICECandidate)
}
