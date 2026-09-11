// Package api is the PulseRTC public integration layer.
//
// It exposes a small REST surface under /v1 so an external application (the
// business-logic backend) can create rooms, mint participant tokens and query
// room / participant / QoE / session state WITHOUT knowing anything about the
// SFU, the cluster, Redis or Pion.
//
// Layering:
//
//	handler (handlers.go) -> Service (service.go) -> Core (this file) + RoomRegistry + TokenIssuer
//
// The join, the WebRTC negotiation and the Session Recovery handshake stay
// entirely on the existing WebSocket endpoint (/ws); this package never touches
// media, RTP or the signaling protocol. `internal/api` does not import
// `internal/signaling` — the signaling layer supplies a Core adapter.
package api

import "errors"

// RoomStatus is the public lifecycle state of a room (design).
type RoomStatus string

const (
	RoomActive     RoomStatus = "ACTIVE"     // at least one participant connected
	RoomEmpty      RoomStatus = "EMPTY"      // known to the API, nobody connected
	RoomRecovering RoomStatus = "RECOVERING" // a session in this room is mid recovery
	RoomClosed     RoomStatus = "CLOSED"     // closed via DELETE, still within the retention TTL
)

// CoreRoom is the live core view of a room, distinct from the API-owned
// metadata held by RoomRegistry.
type CoreRoom struct {
	Exists       bool   // the core (cluster/SFU) knows this room right now
	Generation   int64  // ownership generation (0 when the backend has no recovery)
	Participants int    // participants currently attached on this node
	OwnerNodeID  string // node that owns the room ("" single-node)
	Recovering   bool   // a session in this room is in RECOVERING
}

// CoreTrack is one published track, stripped of every Pion / RTP detail.
type CoreTrack struct {
	PublicationID string `json:"publicationId"`
	Kind          string `json:"kind"`
	Muted         bool   `json:"muted"`
}

// CoreParticipant is one participant's public view.
type CoreParticipant struct {
	Identity string // participant id assigned by the core ("<subject>.<8hex>")
	Subject  string // token subject (the stable account id)
	Name     string // display name from the token "name" claim ("" when unset)
	State    string // CONNECTED / CONNECTING / DISCONNECTED
	Role     string // publisher / subscriber / publisher+subscriber / idle
	Tracks   []CoreTrack
}

// CoreSession is the minimal technical view of a logical session (design).
type CoreSession struct {
	Exists      bool
	SessionID   string
	State       string // CONNECTING / CONNECTED / RECOVERING / RECONNECTED / CLOSED
	Generation  uint64
	Recoverable bool
}

// CoreQualityLeg is one media/connection leg's verdict, derived 1:1 from the
// quality engine — never recomputed here (design).
type CoreQualityLeg struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// CoreQuality is a participant's aggregated QoE view.
type CoreQuality struct {
	Identity   string
	Status     string // GOOD / WARNING / POOR / UNKNOWN (Overall)
	Score      int
	Reason     string
	Audio      *CoreQualityLeg
	Video      *CoreQualityLeg
	Connection *CoreQualityLeg
}

// ErrRoomRemote is returned by a Core method when the room is owned by another
// cluster node. The API answers 409 ROOM_ON_OTHER_NODE and echoes the owner so
// the caller can retry against it (same semantics as the WebSocket path).
type ErrRoomRemote struct{ OwnerNodeID string }

func (e *ErrRoomRemote) Error() string { return "room owned by node " + e.OwnerNodeID }

// ErrClusterStateUnavailable is returned when room ownership cannot be resolved
// because the shared cluster state backend is down (fail closed).
var ErrClusterStateUnavailable = errors.New("cluster state unavailable")

// Core is the read/close surface the signaling layer implements for the API.
// Every method is safe for concurrent use. A method returns *ErrRoomRemote or
// ErrClusterStateUnavailable instead of a result when appropriate.
type Core interface {
	// Room returns the live view. CoreRoom.Exists reports whether the core
	// currently knows the room; the error is non-nil only for remote / degraded.
	Room(roomID string) (CoreRoom, error)

	// Participants lists the room's participants attached on this node.
	Participants(roomID string) ([]CoreParticipant, error)

	// Quality returns the QoE snapshot for every participant of the room.
	Quality(roomID string) ([]CoreQuality, error)

	// Session returns the logical session for (subject, roomID).
	Session(roomID, subject string) (CoreSession, error)

	// CloseRoom notifies every local participant of the room with a room_closed
	// event and closes their sockets. Returns how many were notified.
	CloseRoom(roomID, reason string) (int, error)
}
