package cluster

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ClusterMessage is the node-to-node signaling envelope. It is
// deliberately NOT the public WebSocket protocol — it carries routing and
// anti-replay metadata a client never sees. It never carries media: RTP, RTCP,
// SRTP, audio and video stay on the local SFU.
type ClusterMessage struct {
	Type          string          `json:"type"`
	RequestID     string          `json:"requestId"`
	SourceNodeID  string          `json:"sourceNodeId"`
	TargetNodeID  string          `json:"targetNodeId"`
	Timestamp     int64           `json:"timestamp"` // unix millis
	RoomID        string          `json:"roomId,omitempty"`
	ParticipantID string          `json:"participantId,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

// Message types. The first four are implemented now; the rest are
// reserved so the transport/routing mechanism does not need to change to carry
// them later.
const (
	MsgParticipantMessage    = "participant.message"
	MsgParticipantDisconnect = "participant.disconnect"
	MsgParticipantControl    = "participant.control"
	MsgRoomEvent             = "room.event"

	// Reserved — not handled yet.
	MsgParticipantPublish     = "participant.publish"
	MsgParticipantUnpublish   = "participant.unpublish"
	MsgParticipantMute        = "participant.mute"
	MsgParticipantUnmute      = "participant.unmute"
	MsgParticipantSubscribe   = "participant.subscribe"
	MsgParticipantUnsubscribe = "participant.unsubscribe"
)

var knownMessageTypes = map[string]bool{
	MsgParticipantMessage:    true,
	MsgParticipantDisconnect: true,
	MsgParticipantControl:    true,
	MsgRoomEvent:             true,
}

// participantScoped reports whether a type is delivered to one participant
// (vs. fanned out to a room).
func participantScoped(typ string) bool {
	return typ == MsgParticipantMessage || typ == MsgParticipantDisconnect || typ == MsgParticipantControl
}

// NewClusterMessage builds an envelope originating from this node. RequestID is
// a fresh UUID and Timestamp is now.
func (c *Cluster) NewClusterMessage(typ, roomID, participantID string, payload json.RawMessage) ClusterMessage {
	return ClusterMessage{
		Type:          typ,
		RequestID:     uuid.NewString(),
		SourceNodeID:  c.cfg.NodeID,
		Timestamp:     time.Now().UnixMilli(),
		RoomID:        roomID,
		ParticipantID: participantID,
		Payload:       payload,
	}
}

// Validate checks the envelope against the node's limits. It does
// NOT check TargetNodeID against the local node — that is the loop-prevention
// check the receiver does separately.
func (m ClusterMessage) Validate(now time.Time, maxAge time.Duration, maxSize int) *Error {
	if strings.TrimSpace(m.Type) == "" || !knownMessageTypes[m.Type] {
		return &Error{Code: CodeClusterMessageInvalid, Message: "unknown message type"}
	}
	if strings.TrimSpace(m.RequestID) == "" {
		return &Error{Code: CodeClusterMessageInvalid, Message: "requestId is required"}
	}
	if strings.TrimSpace(m.SourceNodeID) == "" {
		return &Error{Code: CodeClusterMessageInvalid, Message: "sourceNodeId is required"}
	}
	if strings.TrimSpace(m.TargetNodeID) == "" {
		return &Error{Code: CodeClusterMessageInvalid, Message: "targetNodeId is required"}
	}
	if maxSize > 0 && len(m.Payload) > maxSize {
		return &Error{Code: CodeClusterMessageTooLarge, Message: "payload exceeds limit"}
	}
	if m.Timestamp <= 0 {
		return &Error{Code: CodeClusterMessageInvalid, Message: "timestamp is required"}
	}
	if maxAge > 0 {
		ts := time.UnixMilli(m.Timestamp)
		// Reject too-old messages (replay / stuck). Allow a generous forward
		// skew — clocks between hosts are not perfectly synced.
		if now.Sub(ts) > maxAge || ts.Sub(now) > maxAge+30*time.Second {
			return &Error{Code: CodeClusterMessageExpired, Message: "message timestamp outside the accepted window"}
		}
	}
	return nil
}
