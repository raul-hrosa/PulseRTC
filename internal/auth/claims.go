// Package auth is the PulseRTC trust boundary.
//
// It answers two questions and keeps them separate:
//
//	Authentication -> "who are you?"   Validator.Validate turns a signed token
//	                                   into an Identity, or rejects it.
//	Authorization  -> "what may you    Identity.Permissions + the Authorize*
//	                  do?"             helpers gate join / publish / subscribe /
//	                                   control.
//
// Nothing else in the codebase parses JWTs: the signaling and SFU layers only
// ever see an *Identity and a Permissions value.
package auth

import "strings"

// Permissions is the explicit authorization model carried by a token. Every
// capability is deny-by-default: an absent "permissions" claim yields the zero
// value, which grants nothing.
type Permissions struct {
	Join      bool `json:"join"`
	Publish   bool `json:"publish"`
	Subscribe bool `json:"subscribe"`
	Control   bool `json:"control"`
}

// Claims is the JWT payload PulseRTC understands. Times are UNIX seconds.
type Claims struct {
	Subject     string      `json:"sub"`
	Name        string      `json:"name,omitempty"`
	Room        string      `json:"room,omitempty"`
	Permissions Permissions `json:"permissions"`
	IssuedAt    int64       `json:"iat"`
	ExpiresAt   int64       `json:"exp"`
	Issuer      string      `json:"iss,omitempty"`
	Audience    string      `json:"aud,omitempty"`
	// JTI is optional. PulseRTC does not maintain a distributed revocation list
	// (single instance); it is accepted, logged for correlation and
	// reserved for a future per-node replay cache. See docs/security/authentication.md.
	JTI string `json:"jti,omitempty"`
}

// Identity is the authenticated principal derived from a validated token. It is
// what the rest of the server trusts; a client can never set these fields.
type Identity struct {
	Subject     string
	Name        string
	Room        string
	Permissions Permissions
	JTI         string
	ExpiresAt   int64
}

// UserID is the stable account identifier (the token subject). Several
// concurrent connections may share one UserID; each gets its own participant id
// (see ParticipantID) so the room map stays collision-free.
func (i Identity) UserID() string { return i.Subject }

// RoomAllowed reports whether this identity may join the given room. An empty
// room claim is a wildcard: the token is not pinned to a single room (service
// tokens, load tests). A non-empty claim must match exactly.
func (i Identity) RoomAllowed(roomID string) bool {
	if strings.TrimSpace(i.Room) == "" {
		return true
	}
	return i.Room == roomID
}
