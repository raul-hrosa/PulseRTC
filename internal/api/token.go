package api

import (
	"errors"
	"strings"
	"time"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

// TokenIssuer mints participant JWTs for a room. It is self-contained: it signs
// with the same HS256 secret / issuer / audience the signaling layer validates
// (auth.FromEnv), so a token minted here is accepted verbatim by /ws.
type TokenIssuer struct {
	secret   string
	issuer   string
	audience string
	ttl      time.Duration
	ttlMax   time.Duration
	now      func() time.Time
}

// NewTokenIssuer builds an issuer from the auth config and the API config. It
// returns an error when signing is impossible (no secret) — the caller turns
// that into a NOT_CONFIGURED at request time rather than failing startup, so a
// deployment that only uses the read endpoints still works.
func NewTokenIssuer(authCfg auth.Config, apiCfg Config) *TokenIssuer {
	return &TokenIssuer{
		secret:   authCfg.Secret,
		issuer:   authCfg.Issuer,
		audience: authCfg.Audience,
		ttl:      apiCfg.TokenTTL,
		ttlMax:   apiCfg.TokenTTLMax,
		now:      time.Now,
	}
}

// TokenParams is a validated token request.
type TokenParams struct {
	RoomID      string
	Identity    string
	Name        string
	Permissions auth.Permissions
	TTL         time.Duration
}

// MintedToken is what the endpoint returns — the token and nothing that could
// be used to forge one.
type MintedToken struct {
	Token     string
	Identity  string
	RoomID    string
	ExpiresAt time.Time
}

var errNoSigningSecret = errors.New("no signing secret configured")

// Mint signs a token for p. It clamps the TTL to [1s, ttlMax] and pins the
// token to p.RoomID via the "room" claim.
func (ti *TokenIssuer) Mint(p TokenParams) (MintedToken, error) {
	if strings.TrimSpace(ti.secret) == "" {
		return MintedToken{}, errNoSigningSecret
	}
	ttl := p.TTL
	if ttl <= 0 {
		ttl = ti.ttl
	}
	if ttl > ti.ttlMax {
		ttl = ti.ttlMax
	}
	if ttl < time.Second {
		ttl = time.Second
	}
	now := ti.now()
	exp := now.Add(ttl)

	tok, err := auth.Sign(auth.Claims{
		Subject:     p.Identity,
		Name:        p.Name,
		Room:        p.RoomID,
		Permissions: p.Permissions,
		IssuedAt:    now.Unix(),
		ExpiresAt:   exp.Unix(),
		Issuer:      ti.issuer,
		Audience:    ti.audience,
	}, auth.AlgHS256, ti.secret)
	if err != nil {
		return MintedToken{}, err
	}
	return MintedToken{
		Token:     tok,
		Identity:  p.Identity,
		RoomID:    p.RoomID,
		ExpiresAt: exp,
	}, nil
}
