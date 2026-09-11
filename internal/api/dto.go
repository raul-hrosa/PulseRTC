package api

import "time"

// ---- requests -------------------------------------------------------------

type createRoomRequest struct {
	RoomID   string            `json:"roomId"`
	Metadata map[string]string `json:"metadata"`
}

type permissionsRequest struct {
	Join      *bool `json:"join"`
	Publish   *bool `json:"publish"`
	Subscribe *bool `json:"subscribe"`
	Control   *bool `json:"control"`
}

type createTokenRequest struct {
	Identity    string              `json:"identity"`
	Name        string              `json:"name"`
	Permissions *permissionsRequest `json:"permissions"`
	TTLSeconds  int                 `json:"ttlSeconds"`
	Metadata    map[string]string   `json:"metadata"` // app-side only; not embedded in the JWT
}

// ---- responses ----------------------------------------------------------

type connectionInfo struct {
	ServerURL string `json:"serverUrl"`
	RoomID    string `json:"roomId"`
}

type roomResponse struct {
	RoomID       string            `json:"roomId"`
	Status       RoomStatus        `json:"status"`
	Generation   int64             `json:"generation"`
	Participants int               `json:"participants"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	CreatedAt    time.Time         `json:"createdAt"`
	OwnerNodeID  string            `json:"ownerNodeId,omitempty"`
	Connection   *connectionInfo   `json:"connection,omitempty"`
}

type closeRoomResponse struct {
	RoomID               string     `json:"roomId"`
	Status               RoomStatus `json:"status"`
	ParticipantsNotified int        `json:"participantsNotified"`
}

type tokenResponse struct {
	Token     string    `json:"token"`
	Identity  string    `json:"identity"`
	RoomID    string    `json:"roomId"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type participantSummary struct {
	Identity string `json:"identity"`
	Name     string `json:"name,omitempty"`
	State    string `json:"state"`
	Role     string `json:"role"`
	Tracks   int    `json:"tracks"`
}

type participantListResponse struct {
	Participants []participantSummary `json:"participants"`
}

type participantDetailResponse struct {
	Identity  string      `json:"identity"`
	Name      string      `json:"name,omitempty"`
	State     string      `json:"state"`
	Role      string      `json:"role"`
	Tracks    []CoreTrack `json:"tracks"`
	Ambiguous bool        `json:"ambiguous,omitempty"`
}

type sessionResponse struct {
	State       string `json:"state"`
	Generation  uint64 `json:"generation"`
	Recoverable bool   `json:"recoverable"`
	SessionID   string `json:"sessionId,omitempty"`
}

type roomQualityEntry struct {
	Identity string `json:"identity"`
	Status   string `json:"status"`
	Score    int    `json:"score"`
	Reason   string `json:"reason,omitempty"`
}

type roomQualityResponse struct {
	Participants []roomQualityEntry `json:"participants"`
}

type participantQualityResponse struct {
	Identity   string          `json:"identity"`
	Status     string          `json:"status"`
	Score      int             `json:"score"`
	Reason     string          `json:"reason,omitempty"`
	Audio      *CoreQualityLeg `json:"audio,omitempty"`
	Video      *CoreQualityLeg `json:"video,omitempty"`
	Connection *CoreQualityLeg `json:"connection,omitempty"`
}
