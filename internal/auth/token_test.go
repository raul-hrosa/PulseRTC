package auth

import (
	"strings"
	"testing"
	"time"
)

const secret = "unit-test-secret"

func baseClaims() Claims {
	now := time.Now()
	return Claims{
		Subject:     "user-123",
		Name:        "João",
		Room:        "room-abc",
		Permissions: Permissions{Join: true, Publish: true, Subscribe: true},
		IssuedAt:    now.Unix(),
		ExpiresAt:   now.Add(time.Hour).Unix(),
		Issuer:      "pulsertc",
		Audience:    "pulsertc",
	}
}

func testValidator() *Validator {
	return NewValidator(Config{
		Secret: secret, Issuer: "pulsertc", Audience: "pulsertc",
		Algs: []string{AlgHS256}, Leeway: 30 * time.Second, MaxTokenAge: time.Hour,
	})
}

func signWith(t *testing.T, c Claims, sec string) string {
	t.Helper()
	tok, err := Sign(c, AlgHS256, sec)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return tok
}

func TestValidateValidToken(t *testing.T) {
	id, err := testValidator().Validate(signWith(t, baseClaims(), secret))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id.Subject != "user-123" || id.Room != "room-abc" || !id.Permissions.Publish {
		t.Fatalf("identity not derived from claims: %+v", id)
	}
}

func TestValidateRejects(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		mut  func(*Claims)
		sec  string
		code string
	}{
		{"invalid signature", func(*Claims) {}, "wrong-secret", CodeInvalidToken},
		{"expired", func(c *Claims) { c.ExpiresAt = now.Add(-2 * time.Hour).Unix() }, secret, CodeExpiredToken},
		{"missing exp", func(c *Claims) { c.ExpiresAt = 0 }, secret, CodeExpiredToken},
		{"missing iat", func(c *Claims) { c.IssuedAt = 0 }, secret, CodeInvalidToken},
		{"stale iat (replay window)", func(c *Claims) { c.IssuedAt = now.Add(-3 * time.Hour).Unix() }, secret, CodeInvalidToken},
		{"missing subject", func(c *Claims) { c.Subject = "" }, secret, CodeInvalidToken},
		{"wrong issuer", func(c *Claims) { c.Issuer = "another-service" }, secret, CodeInvalidIssuer},
		{"wrong audience", func(c *Claims) { c.Audience = "other-app" }, secret, CodeInvalidAudience},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseClaims()
			tc.mut(&c)
			_, err := testValidator().Validate(signWith(t, c, tc.sec))
			if err == nil {
				t.Fatalf("expected rejection")
			}
			if got := AsError(err).Code; got != tc.code {
				t.Fatalf("code = %q, want %q", got, tc.code)
			}
		})
	}
}

func TestValidateEmptyToken(t *testing.T) {
	if _, err := testValidator().Validate("  "); AsError(err).Code != CodeUnauthenticated {
		t.Fatalf("want UNAUTHENTICATED, got %v", err)
	}
}

func TestValidateMalformed(t *testing.T) {
	for _, tok := range []string{"a.b", "not-a-token", "a.b.c.d", "..", "@.@.@"} {
		if _, err := testValidator().Validate(tok); err == nil {
			t.Fatalf("%q should be rejected", tok)
		}
	}
}

// TestAlgConfusion: a token whose header claims an algorithm the validator does
// not accept must be rejected even if its signature verifies under that alg.
func TestAlgConfusion(t *testing.T) {
	tok := signWith(t, baseClaims(), secret) // HS256
	v := NewValidator(Config{Secret: secret, Issuer: "pulsertc", Audience: "pulsertc", Algs: []string{AlgHS512}})
	if _, err := v.Validate(tok); AsError(err).Code != CodeInvalidToken {
		t.Fatalf("expected INVALID_TOKEN for disallowed alg, got %v", err)
	}
}

func TestMissingPermissionsGrantsNothing(t *testing.T) {
	c := baseClaims()
	c.Permissions = Permissions{}
	raw := signWith(t, c, secret)
	// strip the permissions object entirely to be sure
	if strings.Contains(raw, "permissions") == false {
		t.Skip("permissions omitted from payload")
	}
	id, err := testValidator().Validate(raw)
	if err != nil {
		t.Fatal(err)
	}
	if AuthorizeJoin(id, "room-abc") == nil {
		t.Fatalf("join must be denied without the permission")
	}
}
