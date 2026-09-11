package cluster

import (
	"encoding/json"
	"testing"
	"time"
)

func validMsg() ClusterMessage {
	return ClusterMessage{
		Type:         MsgParticipantMessage,
		RequestID:    "req-1",
		SourceNodeID: "node-a",
		TargetNodeID: "node-b",
		Timestamp:    time.Now().UnixMilli(),
		Payload:      json.RawMessage(`{"x":1}`),
	}
}

func TestClusterMessageValidate(t *testing.T) {
	now := time.Now()
	maxAge := 30 * time.Second

	if e := validMsg().Validate(now, maxAge, 1024); e != nil {
		t.Fatalf("valid message rejected: %v", e)
	}

	cases := []struct {
		name string
		mut  func(*ClusterMessage)
		code string
	}{
		{"missing requestId", func(m *ClusterMessage) { m.RequestID = "" }, CodeClusterMessageInvalid},
		{"missing sourceNodeId", func(m *ClusterMessage) { m.SourceNodeID = "" }, CodeClusterMessageInvalid},
		{"missing targetNodeId", func(m *ClusterMessage) { m.TargetNodeID = "" }, CodeClusterMessageInvalid},
		{"unknown type", func(m *ClusterMessage) { m.Type = "participant.telepathy" }, CodeClusterMessageInvalid},
		{"missing timestamp", func(m *ClusterMessage) { m.Timestamp = 0 }, CodeClusterMessageInvalid},
		{"expired timestamp", func(m *ClusterMessage) { m.Timestamp = now.Add(-time.Hour).UnixMilli() }, CodeClusterMessageExpired},
		{"oversized payload", func(m *ClusterMessage) { m.Payload = make([]byte, 2048) }, CodeClusterMessageTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := validMsg()
			tc.mut(&m)
			e := m.Validate(now, maxAge, 1024)
			if e == nil || e.Code != tc.code {
				t.Fatalf("want %s, got %v", tc.code, e)
			}
		})
	}
}

func TestClusterMessageValidateClockSkewTolerance(t *testing.T) {
	now := time.Now()
	// A message 10s in the future is accepted (clocks are not perfectly synced).
	m := validMsg()
	m.Timestamp = now.Add(10 * time.Second).UnixMilli()
	if e := m.Validate(now, 30*time.Second, 1024); e != nil {
		t.Fatalf("small forward skew must be tolerated: %v", e)
	}
}

func TestDedupCache(t *testing.T) {
	d := newDedupCache(time.Minute)
	if d.markSeen("req-123") {
		t.Fatal("first sight must not be a duplicate")
	}
	if !d.markSeen("req-123") {
		t.Fatal("second sight must be a duplicate")
	}
	if d.markSeen("req-456") {
		t.Fatal("a different id is not a duplicate")
	}
}

func TestDedupCacheExpiry(t *testing.T) {
	d := newDedupCache(50 * time.Millisecond)
	base := time.Now()
	d.now = func() time.Time { return base }
	d.markSeen("r")
	d.now = func() time.Time { return base.Add(100 * time.Millisecond) }
	d.sweep()
	if d.markSeen("r") {
		t.Fatal("entry should have expired and swept")
	}
}
