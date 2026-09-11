package loadtest

import (
	"fmt"
	"time"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

// AuthOptions controls how the load-test client authenticates.
// When Enabled is false the client connects with no token, matching a server
// started with PULSERTC_AUTH_ENABLED=false.
type AuthOptions struct {
	Enabled      bool    `json:"enabled"`
	Secret       string  `json:"-"` // never serialised into result files
	Issuer       string  `json:"issuer"`
	Audience     string  `json:"audience"`
	InvalidRatio float64 `json:"invalidRatio"` // fraction of clients given a bad token
	AdminToken   string  `json:"-"`            // bearer for the /metrics poll
}

// tokenForClient mints a per-participant token. Every `1/InvalidRatio`-th client
// (roughly) gets a token signed with the wrong secret so the run also measures
// rejection handling.
func (o AuthOptions) tokenForClient(index int, room string, publishing bool) (string, bool) {
	if !o.Enabled {
		return "", true
	}
	valid := true
	secret := o.Secret
	if o.InvalidRatio > 0 {
		every := int(1.0 / o.InvalidRatio)
		if every > 0 && index%every == 0 {
			valid = false
			secret = o.Secret + "-tampered"
		}
	}
	now := time.Now()
	tok, err := auth.Sign(auth.Claims{
		Subject: fmt.Sprintf("loadtest-%d", index),
		Room:    room,
		Permissions: auth.Permissions{
			Join: true, Publish: publishing, Subscribe: true,
		},
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(time.Hour).Unix(),
		Issuer:    o.Issuer,
		Audience:  o.Audience,
	}, auth.AlgHS256, secret)
	if err != nil {
		return "", false
	}
	return tok, valid
}
