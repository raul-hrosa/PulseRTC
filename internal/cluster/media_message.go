package cluster

import "encoding/json"

// Media handshake message types. They travel over the MessageTransport
// (authenticated, idempotent) — only session control, never media.
const (
	MsgMediaSessionOpen   = "media.session.open"
	MsgMediaSessionAccept = "media.session.accept"
	MsgMediaSessionReject = "media.session.reject"
	MsgMediaSessionClose  = "media.session.close"
	MsgMediaSubscribe     = "media.subscribe"
	MsgMediaSubscribeAck  = "media.subscribe.ack"
	MsgMediaUnsubscribe   = "media.unsubscribe"
	MsgMediaTrackEnded    = "media.track.ended"
)

func init() {
	for _, t := range []string{
		MsgMediaSessionOpen, MsgMediaSessionAccept, MsgMediaSessionReject,
		MsgMediaSessionClose, MsgMediaSubscribe, MsgMediaSubscribeAck,
		MsgMediaUnsubscribe, MsgMediaTrackEnded,
	} {
		knownMessageTypes[t] = true
	}
}

// mediaSessionOpen asks a peer to accept a media session. sessionId is chosen by
// the opener; mediaAddr is the opener's UDP media address.
type mediaSessionOpen struct {
	SessionID string `json:"sessionId"`
	RoomID    string `json:"roomId"`
	MediaAddr string `json:"mediaAddr"`
}

type mediaSessionAccept struct {
	SessionID string `json:"sessionId"`
	MediaAddr string `json:"mediaAddr"`
}

type mediaSessionReject struct {
	SessionID string `json:"sessionId"`
	Reason    string `json:"reason"`
}

type mediaSessionClose struct {
	SessionID string `json:"sessionId"`
}

// mediaSubscribe: the subscriber node asks the publisher node to start
// forwarding a publication over the session.
type mediaSubscribe struct {
	SessionID     string `json:"sessionId"`
	RoomID        string `json:"roomId"`
	PublicationID string `json:"publicationId"`
	SubscriberID  string `json:"subscriberId"`
}

// mediaSubscribeAck carries the track parameters the subscriber needs to build a
// local synthetic publication.
type mediaSubscribeAck struct {
	SessionID     string `json:"sessionId"`
	PublicationID string `json:"publicationId"`
	ParticipantID string `json:"participantId"`
	Kind          string `json:"kind"`
	MimeType      string `json:"mimeType"`
	ClockRate     uint32 `json:"clockRate"`
	Channels      uint16 `json:"channels"`
	PayloadType   uint8  `json:"payloadType"`
	SSRC          uint32 `json:"ssrc"`
	Accepted      bool   `json:"accepted"`
	Reason        string `json:"reason,omitempty"`
}

type mediaUnsubscribe struct {
	SessionID     string `json:"sessionId"`
	PublicationID string `json:"publicationId"`
	SubscriberID  string `json:"subscriberId"`
}

type mediaTrackEnded struct {
	SessionID     string `json:"sessionId"`
	PublicationID string `json:"publicationId"`
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
