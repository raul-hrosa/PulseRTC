package signaling

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var qualityEventTypes = map[string]bool{
	"quality_degraded": true, "quality_changed": true, "quality_recovered": true,
}

// reportConnFailed pushes n failing connection samples, enough for the engine to
// commit a stable POOR verdict for the connection leg.
func reportConnFailed(t *testing.T, c *websocket.Conn, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := c.WriteJSON(map[string]any{
			"type": "quality_report",
			"samples": []map[string]any{{
				"kind":      "connection",
				"tMs":       (i + 1) * 1000,
				"connState": "failed",
				"iceState":  "failed",
				"rttMs":     50,
			}},
		}); err != nil {
			t.Fatalf("write quality_report: %v", err)
		}
	}
}

// waitForQualityEvent reads up to `tries` frames looking for a quality_* event.
func waitForQualityEvent(t *testing.T, c *websocket.Conn, tries int) map[string]any {
	t.Helper()
	for i := 0; i < tries; i++ {
		m := readMsg(t, c)
		typ, _ := m["type"].(string)
		if qualityEventTypes[typ] {
			return m
		}
	}
	t.Fatal("no quality_* event received")
	return nil
}

// expectNoQualityEvent asserts no quality_* event arrives within a short window.
func expectNoQualityEvent(t *testing.T, c *websocket.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	for {
		_, data, err := c.ReadMessage()
		if err != nil {
			return // read timeout: nothing leaked
		}
		var m map[string]any
		if json.Unmarshal(data, &m) != nil {
			continue
		}
		if typ, _ := m["type"].(string); qualityEventTypes[typ] {
			t.Fatalf("unexpected quality event: %v", m)
		}
	}
}

// A failed transport is a decisive POOR verdict; after DegradeSamples reports
// the engine should push a quality_* event to the reporting client.
func TestQualityReportEmitsEventOnConnectionFailure(t *testing.T) {
	_, wsURL := newTestServer(t)
	c := dial(t, wsURL)
	_ = welcomeID(t, c)
	join(t, c, "room-q")
	expectType(t, c, "room_joined")

	reportConnFailed(t, c, 5)

	ev := waitForQualityEvent(t, c, 8)
	if ev["status"] != "POOR" {
		t.Fatalf("expected POOR status, got %v", ev)
	}
}

func TestQualityReportRequiresRoom(t *testing.T) {
	_, wsURL := newTestServer(t)
	c := dial(t, wsURL)
	_ = welcomeID(t, c)

	if err := c.WriteJSON(map[string]any{"type": "quality_report", "samples": []any{}}); err != nil {
		t.Fatal(err)
	}
	expectType(t, c, "error")
}

// A quality change of B is delivered to every participant of the room,
// carrying B's id as the observed participant.
func TestQualityEventBroadcastToWholeRoom(t *testing.T) {
	_, wsURL := newTestServer(t)

	a := dial(t, wsURL)
	_ = welcomeID(t, a)
	join(t, a, "room-share")
	expectType(t, a, "room_joined")

	b := dial(t, wsURL)
	bID := welcomeID(t, b)
	join(t, b, "room-share")
	expectType(t, b, "room_joined")
	expectType(t, a, "participant_joined")

	reportConnFailed(t, b, 5)

	for _, peer := range []*websocket.Conn{a, b} {
		ev := waitForQualityEvent(t, peer, 8)
		if ev["participantId"] != bID {
			t.Fatalf("event should identify B (%s), got %v", bID, ev)
		}
		if ev["status"] != "POOR" {
			t.Fatalf("expected POOR, got %v", ev)
		}
	}
}

// Participants of another room never see the event.
func TestQualityEventNotLeakedToOtherRooms(t *testing.T) {
	_, wsURL := newTestServer(t)

	a1 := dial(t, wsURL)
	_ = welcomeID(t, a1)
	join(t, a1, "room-a")
	expectType(t, a1, "room_joined")

	b1 := dial(t, wsURL)
	_ = welcomeID(t, b1)
	join(t, b1, "room-b")
	expectType(t, b1, "room_joined")

	reportConnFailed(t, a1, 5)

	// a1 gets its own event; b1 (other room) gets nothing.
	waitForQualityEvent(t, a1, 8)
	expectNoQualityEvent(t, b1)
}

// A stream that stays POOR does not keep emitting events.
func TestQualityNoSpamWhenStable(t *testing.T) {
	_, wsURL := newTestServer(t)
	c := dial(t, wsURL)
	_ = welcomeID(t, c)
	join(t, c, "room-stable")
	expectType(t, c, "room_joined")

	reportConnFailed(t, c, 5)
	first := waitForQualityEvent(t, c, 8)
	if first["status"] != "POOR" {
		t.Fatalf("expected POOR, got %v", first)
	}

	// Keep reporting the same failed state: no further transition -> no event.
	reportConnFailed(t, c, 10)
	expectNoQualityEvent(t, c)
}

// A participant joining later learns the current quality from room_joined.
func TestQualityInitialStateInRoomJoined(t *testing.T) {
	_, wsURL := newTestServer(t)

	b := dial(t, wsURL)
	bID := welcomeID(t, b)
	join(t, b, "room-late")
	expectType(t, b, "room_joined")

	reportConnFailed(t, b, 5)
	waitForQualityEvent(t, b, 8)

	c := dial(t, wsURL)
	_ = welcomeID(t, c)
	join(t, c, "room-late")
	rj := expectType(t, c, "room_joined")

	q, ok := rj["quality"].([]any)
	if !ok || len(q) == 0 {
		t.Fatalf("room_joined should carry quality state, got %v", rj["quality"])
	}
	entry := q[0].(map[string]any)
	if entry["participantId"] != bID || entry["status"] != "POOR" {
		t.Fatalf("quality entry wrong: %v", entry)
	}
}

// A quality event published on node B reaches a participant of the same
// room connected to node A over the existing cross-node room-event transport.
func TestQualityEventCrossNode(t *testing.T) {
	_, wsA, _, cluB := twoSignalingNodes(t)

	ca := dialWithToken(t, wsA, mintToken(t, "alice", "room-mn", nil))
	_ = welcomeID(t, ca)
	join(t, ca, "room-mn")
	expectType(t, ca, "room_joined")

	// Frame identical to what node B's handleQualityReport would fan out.
	ev, _ := json.Marshal(map[string]any{
		"type": "quality_changed", "participantId": "bob.remote",
		"mediaType": "connection", "from": "GOOD", "status": "POOR",
		"reason": "CONNECTION_UNSTABLE",
	})
	cluB.BroadcastRoomEvent(context.Background(), "room-mn", ev)

	got := waitForQualityEvent(t, ca, 8)
	if got["participantId"] != "bob.remote" || got["status"] != "POOR" {
		t.Fatalf("cross-node quality event wrong: %v", got)
	}
}
