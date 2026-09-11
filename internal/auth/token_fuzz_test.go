package auth

import "testing"

func FuzzParseToken(f *testing.F) {
	valid, _ := Sign(Claims{Subject: "u", Room: "r", IssuedAt: 1, ExpiresAt: 1 << 31, Issuer: "pulsertc", Audience: "pulsertc"}, AlgHS256, "secret")
	f.Add(valid, "secret")
	f.Add("not.a.jwt", "secret")
	f.Add("", "")
	f.Add("a.b.c", "secret")
	f.Add("eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1In0.", "secret") // alg:none must never verify

	allowed := map[string]bool{AlgHS256: true, AlgHS384: true, AlgHS512: true}
	f.Fuzz(func(t *testing.T, token, secret string) {
		// Must never panic, never hang, never return a non-nil claims with a non-nil error.
		claims, err := parse(token, secret, allowed)
		if err == nil && claims == nil {
			t.Fatal("nil claims with nil error")
		}
		if err != nil && claims != nil {
			t.Fatal("non-nil claims with an error")
		}
	})
}
