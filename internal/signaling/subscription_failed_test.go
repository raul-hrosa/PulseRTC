package signaling

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

func TestSubscriptionFailedReachesSubscriber(t *testing.T) {
	authn, err := auth.Build(testAuthConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("auth.Build: %v", err)
	}
	s, err := NewServerWithAuthenticator(slog.New(slog.NewTextHandler(io.Discard, nil)), authn)
	if err != nil {
		t.Fatalf("NewServerWithAuthenticator: %v", err)
	}

	c := &Client{id: "sub-1", send: make(chan []byte, 4)}
	s.trackClient(c)

	s.onSubscriptionFailed("room-1", "sub-1", "pub-9")

	var raw []byte
	select {
	case raw = <-c.send:
	case <-time.After(time.Second):
		t.Fatal("no message delivered to subscriber")
	}

	var msg map[string]any
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg["type"] != "subscription_failed" {
		t.Fatalf("type = %v, want subscription_failed", msg["type"])
	}
	if msg["publicationId"] != "pub-9" {
		t.Fatalf("publicationId = %v, want pub-9", msg["publicationId"])
	}

	// Unknown subscriber must be a no-op, not a panic.
	s.onSubscriptionFailed("room-1", "ghost", "pub-9")
}

// TestNewServerWithDepsWiresSubscriptionFailedHook proves the SFU→signaling hook
// is actually registered by the server constructor: deleting the
// srv.sfu.SetSubscriptionFailedHook(srv.onSubscriptionFailed) line makes this
// fail (the hook comes back unregistered, or the message never reaches the
// client).
func TestNewServerWithDepsWiresSubscriptionFailedHook(t *testing.T) {
	authn, err := auth.Build(testAuthConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("auth.Build: %v", err)
	}
	s, err := NewServerWithAuthenticator(slog.New(slog.NewTextHandler(io.Discard, nil)), authn)
	if err != nil {
		t.Fatalf("NewServerWithAuthenticator: %v", err)
	}

	c := &Client{id: "sub-7", send: make(chan []byte, 4)}
	s.trackClient(c)

	if !s.sfu.FireSubscriptionFailedHook("room-2", "sub-7", "pub-3") {
		t.Fatal("SFU subscription-failed hook not registered by NewServerWithDeps")
	}

	var raw []byte
	select {
	case raw = <-c.send:
	case <-time.After(time.Second):
		t.Fatal("hook fired but no message reached the subscriber — wiring points elsewhere")
	}
	var msg map[string]any
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg["type"] != "subscription_failed" || msg["publicationId"] != "pub-3" {
		t.Fatalf("unexpected message: %v", msg)
	}
}
