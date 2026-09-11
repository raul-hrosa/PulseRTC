package signaling

import (
	"encoding/json"

	"github.com/raulhrosa/pulsertc/internal/auth"
	"github.com/raulhrosa/pulsertc/internal/cluster"
	"github.com/raulhrosa/pulsertc/internal/sfu"
)

// Client -> Server message types.
const (
	TypeJoin   = "join"
	TypeSignal = "signal"

	// WebRTC negotiation messages. They are forwarded to a single
	// target within the same room, exactly like TypeSignal, with the sender id
	// added as "from". The server never inspects the SDP/ICE payload.
	TypeWebRTCOffer        = "webrtc_offer"
	TypeWebRTCAnswer       = "webrtc_answer"
	TypeWebRTCICECandidate = "webrtc_ice_candidate"

	// SFU negotiation messages. Unlike the webrtc_* messages above,
	// these are NOT forwarded to another participant: the server terminates the
	// PeerConnection itself, so it consumes and produces them via the sfu
	// package. Perfect negotiation: both the browser and the SFU may
	// send sfu_offer; each answers the other with sfu_answer.
	TypeSFUOffer        = sfu.MsgOffer        // "sfu_offer"
	TypeSFUAnswer       = sfu.MsgAnswer       // "sfu_answer"
	TypeSFUICECandidate = sfu.MsgICECandidate // "sfu_ice_candidate"

	// Pub/sub control messages, client -> server. Handled by the
	// participant's SFU peer, never forwarded.
	TypeSubscribe   = "subscribe"
	TypeUnsubscribe = "unsubscribe"
	TypeUnpublish   = "unpublish"
	TypeSetMute     = "set_mute"
	// TypePublish declares the source ("screen") of a track the browser is
	// about to add. It carries trackId + source at the top level and produces
	// no reply; the SFU stamps the resulting Publication.
	TypePublish = "publish"

	// Control plane: administrative commands over other
	// participants (mute/remove/disable). Gated by the CONTROL permission. No
	// concrete command is defined yet — an authorized request currently returns
	// "not implemented", an unauthorized one returns CONTROL_NOT_ALLOWED.
	TypeControl = "control"

	// QoE. The browser periodically reports normalised WebRTC
	// stats; the server feeds them to the quality engine and pushes back a
	// quality_* event only when a stable status transition occurs.
	TypeQualityReport = "quality_report"

	// Session recovery. The client sends session.resume (usually as
	// part of a join after a detected disconnect) to declare it is recovering a
	// prior session; the server validates identity + room + generation.
	TypeSessionResume = "session.resume"
)

// Server -> Client session-recovery events.
const (
	TypeSessionRecovery       = "session.recovery"
	TypeSessionReconnected    = "session.reconnected"
	TypeSessionRecoveryFailed = "session.recovery_failed"
	TypeSessionStale          = "session.stale"
	TypeRoomSnapshot          = "room.snapshot"
	TypeSessionReplacedNotice = "session.replaced"
)

// sfuTypes are the client->server messages handled by the SFU peer directly
// (negotiation + pub/sub control).
var sfuTypes = map[string]bool{
	TypeSFUOffer:        true,
	TypeSFUAnswer:       true,
	TypeSFUICECandidate: true,
	TypeSubscribe:       true,
	TypeUnsubscribe:     true,
	TypeUnpublish:       true,
	TypeSetMute:         true,
	TypePublish:         true,
}

// forwardableTypes are the client->server messages that the server relays
// verbatim to another participant in the same room.
var forwardableTypes = map[string]bool{
	TypeSignal:             true,
	TypeWebRTCOffer:        true,
	TypeWebRTCAnswer:       true,
	TypeWebRTCICECandidate: true,
}

// Server -> Client message types.
const (
	TypeRoomJoined        = "room_joined"
	TypeParticipantJoined = "participant_joined"
	TypeParticipantLeft   = "participant_left"
	TypeError             = "error"
	// TypeSignal is reused for the forwarded signal message.

	// TypeSubscriptionFailed tells a subscriber the SFU gave up delivering one
	// publication to them after a persistent media-transport failure (D3).
	TypeSubscriptionFailed = "subscription_failed"
)

// SubscriptionFailedMsg tells a subscriber the SFU gave up delivering one
// publication to them after a persistent media-transport failure; the
// subscription has been removed server-side and the client may re-subscribe.
type SubscriptionFailedMsg struct {
	Type          string `json:"type"`
	PublicationID string `json:"publicationId"`
	Reason        string `json:"reason"`
}

// Inbound is the envelope for every message received from a client.
// Only the fields relevant to the given type are populated.
type Inbound struct {
	Type          string          `json:"type"`
	RoomID        string          `json:"roomId"`
	Target        string          `json:"target"`
	Payload       json.RawMessage `json:"payload"`
	PublicationID string          `json:"publicationId"` // subscribe / unsubscribe / unpublish / set_mute
	Muted         bool            `json:"muted"`         // set_mute
	// Session recovery.
	Resume  *ResumeInfo `json:"resume,omitempty"`  // join / session.resume
	TrackID string      `json:"trackId,omitempty"` // logical track id for publish/subscribe intent
	Kind    string      `json:"kind,omitempty"`    // "audio" / "video" for publish intent
	Source  string      `json:"source,omitempty"`  // "screen" for publish intent
	Enabled *bool       `json:"enabled,omitempty"` // subscribe intent toggle
}

// ParticipantInfo describes a participant in room listings.
type ParticipantInfo struct {
	ParticipantID string `json:"participantId"`
	// Name is the participant's display name (from the token "name" claim).
	// Omitted when unknown or empty.
	Name string `json:"name,omitempty"`
}

// RoomJoined is sent to a participant right after it joins a room.
type RoomJoined struct {
	Type         string            `json:"type"`
	RoomID       string            `json:"roomId"`
	Participants []ParticipantInfo `json:"participants"`
	// Session recovery. Omitted when recovery is disabled.
	SessionID      string         `json:"sessionId,omitempty"`
	Generation     uint64         `json:"generation,omitempty"`
	RoomGeneration int64          `json:"roomGeneration,omitempty"`
	OwnerNodeID    string         `json:"ownerNodeId,omitempty"`
	Reconnected    bool           `json:"reconnected,omitempty"`
	Reconnect      *ReconnectWire `json:"reconnect,omitempty"`
	Snapshot       *RoomSnapshot  `json:"snapshot,omitempty"`
	// Quality carries the current consolidated QoE status of the participants
	// that were already in the room, so a newcomer knows their state without
	// waiting for the next stable transition (QoE propagation). Omitted when
	// no participant has a verdict yet.
	Quality []ParticipantQualityState `json:"quality,omitempty"`
}

// ParticipantQualityState is the consolidated QoE verdict for one participant,
// as carried in room_joined. It mirrors the top-level fields of a quality_*
// event (participantId + status + reason) — never the raw metrics.
type ParticipantQualityState struct {
	ParticipantID string `json:"participantId"`
	Status        string `json:"status"`
	Reason        string `json:"reason,omitempty"`
}

// ReconnectWire is the client-facing shape of ReconnectHints (milliseconds).
type ReconnectWire struct {
	InitialDelayMs int64   `json:"initialDelayMs"`
	MaxDelayMs     int64   `json:"maxDelayMs"`
	Jitter         float64 `json:"jitter"`
}

func (h ReconnectHints) wire() *ReconnectWire {
	return &ReconnectWire{
		InitialDelayMs: h.InitialDelay.Milliseconds(),
		MaxDelayMs:     h.MaxDelay.Milliseconds(),
		Jitter:         h.JitterFrac,
	}
}

// SnapshotTrack is one logical track available in a room right now.
type SnapshotTrack struct {
	PublicationID string `json:"publicationId"`
	Kind          string `json:"kind"`
	Source        string `json:"source,omitempty"`
	Muted         bool   `json:"muted"`
}

// SnapshotParticipant is one participant's current publications.
type SnapshotParticipant struct {
	ParticipantID string          `json:"participantId"`
	Name          string          `json:"name,omitempty"`
	Tracks        []SnapshotTrack `json:"tracks"`
}

// RoomSnapshot is the current LOGICAL room state a reconnecting client
// reconciles against. It carries no PeerConnection / ICE / DTLS /
// SRTP / Pion state and is built from the live nodes, never from the dead
// node's memory.
type RoomSnapshot struct {
	Type           string                `json:"type"`
	RoomID         string                `json:"roomId"`
	RoomGeneration int64                 `json:"roomGeneration"`
	Participants   []SnapshotParticipant `json:"participants"`
}

// SessionEvent is a server->client session.* recovery notification.
type SessionEvent struct {
	Type      string `json:"type"`
	Reason    string `json:"reason,omitempty"`
	RoomID    string `json:"roomId,omitempty"`
	SessionID string `json:"sessionId,omitempty"`
}

// ParticipantEvent is used for participant_joined and participant_left.
type ParticipantEvent struct {
	Type          string `json:"type"`
	ParticipantID string `json:"participantId"`
	// Name is the participant's display name (from the token "name" claim).
	// Omitted when unknown or empty. Filled in by the origin node, so it also
	// travels with the event when it is fanned out to other cluster nodes.
	Name string `json:"name,omitempty"`
}

// ForwardedMessage is a message relayed to a participant of the same room. It
// is used for "signal" and every "webrtc_*" type; Type is preserved from the
// inbound message and From is filled in by the server.
type ForwardedMessage struct {
	Type    string          `json:"type"`
	From    string          `json:"from"`
	Target  string          `json:"target"`
	Payload json.RawMessage `json:"payload"`
}

// ErrorMessage reports a problem back to the sending client. Code is populated
// for security failures with a stable, non-sensitive value;
// generic protocol errors leave it empty.
type ErrorMessage struct {
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message"`
	// NodeID / RoomID are set for cluster errors: a
	// ROOM_ON_OTHER_NODE tells the client which node actually owns the room.
	NodeID string `json:"nodeId,omitempty"`
	RoomID string `json:"roomId,omitempty"`
}

func newError(msg string) ErrorMessage {
	return ErrorMessage{Type: TypeError, Message: msg}
}

// newSecError builds an error message from an *auth.Error: the client sees the
// stable code and the generic message, never crypto internals.
func newSecError(e *auth.Error) ErrorMessage {
	return ErrorMessage{Type: TypeError, Code: e.Code, Message: e.Message}
}

// newClusterError builds an error message from a *cluster.Error, carrying the
// owning node id so the client can reconnect to the right place.
func newClusterError(e *cluster.Error) ErrorMessage {
	return ErrorMessage{Type: TypeError, Code: e.Code, Message: e.Message, NodeID: e.NodeID, RoomID: e.RoomID}
}
