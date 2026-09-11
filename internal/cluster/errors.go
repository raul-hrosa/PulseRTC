package cluster

// Error is a cluster failure with a stable code. Codes surface to clients (for
// ROOM_ON_OTHER_NODE) and to peer nodes over the /internal API.
type Error struct {
	Code    string
	Message string
	// NodeID / RoomID / ParticipantID give the caller enough to act (redirect,
	// retry, report).
	NodeID        string `json:",omitempty"`
	RoomID        string `json:",omitempty"`
	ParticipantID string `json:",omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

const (
	// CodeRoomOnOtherNode: the room is owned by a different node. The client (or
	// an upstream router) should reconnect to Error.NodeID.
	CodeRoomOnOtherNode = "ROOM_ON_OTHER_NODE"
	// CodeNodeShuttingDown: this node is draining and will not take new rooms.
	CodeNodeShuttingDown = "NODE_SHUTTING_DOWN"
	// CodeClusterStateUnavailable: the shared-state backend (Redis) is
	// unreachable, so ownership cannot be determined — the join fails closed
	// rather than risk a duplicate room. Maps to HTTP 503.
	CodeClusterStateUnavailable = "CLUSTER_STATE_UNAVAILABLE"
	// CodeClusterUnauthorized: an /internal call without a valid cluster token.
	CodeClusterUnauthorized = "CLUSTER_UNAUTHORIZED"
	CodeBadRequest          = "BAD_REQUEST"
	CodeNotFound            = "NOT_FOUND"

	// Cross-node signaling.
	CodeClusterNodeNotFound    = "CLUSTER_NODE_NOT_FOUND"
	CodeClusterNodeUnavailable = "CLUSTER_NODE_UNAVAILABLE"
	CodeClusterMessageInvalid  = "CLUSTER_MESSAGE_INVALID"
	CodeClusterMessageTooLarge = "CLUSTER_MESSAGE_TOO_LARGE"
	CodeClusterMessageExpired  = "CLUSTER_MESSAGE_EXPIRED"
	CodeClusterMessageDup      = "CLUSTER_MESSAGE_DUPLICATE"
	CodeClusterTargetMismatch  = "CLUSTER_TARGET_MISMATCH"
	CodeParticipantNotFound    = "PARTICIPANT_NOT_FOUND"

	// Cross-node media.
	CodeMediaSessionNotFound  = "MEDIA_SESSION_NOT_FOUND"
	CodeMediaSessionInvalid   = "MEDIA_SESSION_INVALID"
	CodeMediaSessionExpired   = "MEDIA_SESSION_EXPIRED"
	CodeMediaNodeUnavailable  = "MEDIA_NODE_UNAVAILABLE"
	CodeMediaAuthFailed       = "MEDIA_AUTH_FAILED"
	CodeMediaPacketInvalid    = "MEDIA_PACKET_INVALID"
	CodeMediaPacketTooLarge   = "MEDIA_PACKET_TOO_LARGE"
	CodeMediaTransportTimeout = "MEDIA_TRANSPORT_TIMEOUT"
	CodeMediaTransportClosed  = "MEDIA_TRANSPORT_CLOSED"
	CodeRemoteTrackNotFound   = "REMOTE_TRACK_NOT_FOUND"

	// Routing.
	CodeNoCapacity = "NO_CAPACITY"

	// Failure detection & recovery.
	// CodeNodeStale: the target node missed its heartbeat window and is treated
	// as failed.
	CodeNodeStale = "NODE_STALE"
	// CodeNodeOffline: the target node is in the OFFLINE lifecycle state.
	CodeNodeOffline = "NODE_OFFLINE"
	// CodeParticipantStale: the participant's last known node is stale, so the
	// message is not routed to a dead node.
	CodeParticipantStale = "PARTICIPANT_STALE"
	// CodeOwnershipConflict: an atomic recovery lost the race to another node.
	CodeOwnershipConflict = "OWNERSHIP_CONFLICT"
	// CodeAlreadyRecovered: the room is already owned by the recovering node —
	// idempotent no-op.
	CodeAlreadyRecovered = "ALREADY_RECOVERED"
	// CodeRecoveryDisabled: PULSERTC_RECOVERY_ENABLED=false.
	CodeRecoveryDisabled = "RECOVERY_DISABLED"
)

// HTTPStatus maps a cluster error code to the status the /internal/cluster
// endpoint returns. Unknown codes are 500.
func (e *Error) HTTPStatus() int {
	switch e.Code {
	case CodeClusterMessageInvalid, CodeBadRequest:
		return 400
	case CodeClusterUnauthorized:
		return 401
	case CodeParticipantNotFound, CodeClusterNodeNotFound, CodeNotFound:
		return 404
	case CodeClusterTargetMismatch, CodeOwnershipConflict:
		return 409
	case CodeClusterMessageTooLarge:
		return 413
	case CodeClusterMessageExpired:
		return 400
	case CodeClusterNodeUnavailable, CodeClusterStateUnavailable,
		CodeNodeStale, CodeNodeOffline, CodeParticipantStale:
		return 503
	default:
		return 500
	}
}

// RoomRemoteError builds the ROOM_ON_OTHER_NODE error for a room owned by owner.
func RoomRemoteError(roomID string, owner NodeInfo) *Error {
	return &Error{
		Code:    CodeRoomOnOtherNode,
		Message: "room is owned by another node",
		NodeID:  owner.ID,
		RoomID:  roomID,
	}
}
