package auth

import (
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"
)

func enabledAuth(t *testing.T) *Authenticator {
	t.Helper()
	cfg := Config{
		Enabled: true, Secret: secret, Issuer: "pulsertc", Audience: "pulsertc",
		Algs: []string{AlgHS256}, Leeway: time.Minute, MaxTokenAge: time.Hour,
		ConnRatePerMin: 1000, MsgRatePerSec: 1000, MaxMessageSize: 4096,
	}
	a, err := Build(cfg, slog.Default())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return a
}

func TestBuildFailsClosed(t *testing.T) {
	if _, err := Build(Config{Enabled: true}, slog.Default()); err == nil {
		t.Fatalf("enabled auth without a secret must fail to build")
	}
	if _, err := Build(Config{Enabled: false}, slog.Default()); err != nil {
		t.Fatalf("disabled auth should build: %v", err)
	}
}

func TestTokenFromRequest(t *testing.T) {
	r := httptest.NewRequest("GET", "/ws", nil)
	r.Header.Set("Authorization", "Bearer abc.def.ghi")
	if TokenFromRequest(r) != "abc.def.ghi" {
		t.Fatalf("bearer header not read")
	}

	r = httptest.NewRequest("GET", "/ws", nil)
	r.Header.Set("Sec-WebSocket-Protocol", "pulsertc, pulsertc.token.xxx.yyy.zzz")
	if TokenFromRequest(r) != "xxx.yyy.zzz" {
		t.Fatalf("subprotocol token not read")
	}

	r = httptest.NewRequest("GET", "/ws?access_token=q.r.s", nil)
	if TokenFromRequest(r) != "q.r.s" {
		t.Fatalf("query token not read")
	}
}

func TestAuthenticateRequestValid(t *testing.T) {
	a := enabledAuth(t)
	tok := signWith(t, baseClaims(), secret)
	r := httptest.NewRequest("GET", "/ws", nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	id, secErr := a.AuthenticateRequest(r)
	if secErr != nil {
		t.Fatalf("unexpected: %v", secErr)
	}
	if id.Subject != "user-123" {
		t.Fatalf("identity: %+v", id)
	}
	if a.MetricsSnapshot().Success != 1 {
		t.Fatalf("success metric not bumped")
	}
}

func TestAuthenticateRequestInvalid(t *testing.T) {
	a := enabledAuth(t)
	r := httptest.NewRequest("GET", "/ws", nil)
	r.Header.Set("Authorization", "Bearer bogus")
	if _, secErr := a.AuthenticateRequest(r); secErr == nil {
		t.Fatalf("expected rejection")
	}
	if a.MetricsSnapshot().Failed != 1 {
		t.Fatalf("failed metric not bumped")
	}
}

func TestAuthenticateRequestRateLimited(t *testing.T) {
	cfg := Config{
		Enabled: true, Secret: secret, Issuer: "pulsertc", Audience: "pulsertc",
		Algs: []string{AlgHS256}, ConnRatePerMin: 2,
	}
	a, _ := Build(cfg, slog.Default())
	tok := signWith(t, baseClaims(), secret)
	mk := func() *Error {
		r := httptest.NewRequest("GET", "/ws", nil)
		r.RemoteAddr = "10.0.0.9:1234"
		r.Header.Set("Authorization", "Bearer "+tok)
		_, e := a.AuthenticateRequest(r)
		return e
	}
	mk()
	mk()
	if e := mk(); e == nil || e.Code != CodeRateLimited {
		t.Fatalf("3rd attempt should be RATE_LIMITED, got %v", e)
	}
}

func TestAuthDisabledGrantsSyntheticIdentity(t *testing.T) {
	a, _ := Build(Config{Enabled: false, ConnRatePerMin: 100}, slog.Default())
	r := httptest.NewRequest("GET", "/ws", nil)
	id, secErr := a.AuthenticateRequest(r)
	if secErr != nil {
		t.Fatalf("disabled auth should accept: %v", secErr)
	}
	if !id.Permissions.Join || !id.Permissions.Publish {
		t.Fatalf("synthetic identity should grant everything")
	}
}

func TestParticipantIDDerivedFromSubject(t *testing.T) {
	a := ParticipantID(&Identity{Subject: "user-123"})
	b := ParticipantID(&Identity{Subject: "user-123"})
	if a == b {
		t.Fatalf("two sessions of one user must get distinct participant ids")
	}
	if len(a) < len("user-123.") || a[:9] != "user-123." {
		t.Fatalf("participant id should be prefixed with the subject: %s", a)
	}
}
