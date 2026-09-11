package auth

import (
	"strings"
	"time"
)

// Validator turns a raw token string into an Identity, applying signature then
// policy (exp, iat, replay window, issuer, audience, subject). It is safe for
// concurrent use.
type Validator struct {
	secret      string
	issuer      string
	audience    string
	algs        map[string]bool
	leeway      time.Duration
	maxTokenAge time.Duration
	now         func() time.Time // injectable for tests
}

// NewValidator builds a Validator from cfg. It does not check cfg.Enabled — the
// caller decides whether to invoke it.
func NewValidator(cfg Config) *Validator {
	algs := make(map[string]bool, len(cfg.Algs))
	for _, a := range cfg.Algs {
		algs[strings.ToUpper(strings.TrimSpace(a))] = true
	}
	if len(algs) == 0 {
		algs[AlgHS256] = true
	}
	return &Validator{
		secret:      cfg.Secret,
		issuer:      cfg.Issuer,
		audience:    cfg.Audience,
		algs:        algs,
		leeway:      cfg.Leeway,
		maxTokenAge: cfg.MaxTokenAge,
		now:         time.Now,
	}
}

// Validate is the single entry point. Every failure is an *Error with a stable
// Code and no sensitive detail.
func (v *Validator) Validate(token string) (*Identity, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errMissing
	}

	claims, err := parse(token, v.secret, v.algs)
	if err != nil {
		return nil, err
	}

	now := v.now()

	if claims.ExpiresAt == 0 {
		return nil, errExpired // a token with no exp is treated as already expired
	}
	if now.After(time.Unix(claims.ExpiresAt, 0).Add(v.leeway)) {
		return nil, errExpired
	}
	if claims.IssuedAt == 0 {
		return nil, errNoIat
	}
	iat := time.Unix(claims.IssuedAt, 0)
	if iat.After(now.Add(v.leeway)) {
		return nil, errNotYetValid
	}
	if v.maxTokenAge > 0 && now.Sub(iat) > v.maxTokenAge+v.leeway {
		return nil, errStale
	}
	if strings.TrimSpace(claims.Subject) == "" {
		return nil, errNoSub
	}
	if v.issuer != "" && claims.Issuer != v.issuer {
		return nil, errIssuer
	}
	if v.audience != "" && claims.Audience != v.audience {
		return nil, errAudience
	}

	return &Identity{
		Subject:     claims.Subject,
		Name:        claims.Name,
		Room:        claims.Room,
		Permissions: claims.Permissions,
		JTI:         claims.JTI,
		ExpiresAt:   claims.ExpiresAt,
	}, nil
}

// ExpiresIn reports how long until this identity's token expires (may be
// negative). Used to schedule a graceful disconnect mid-session.
func (i Identity) ExpiresIn(now time.Time) time.Duration {
	return time.Unix(i.ExpiresAt, 0).Sub(now)
}
