package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

// ---- fake Core -----------------------------------------------------------

type fakeCore struct {
	room         func(string) (CoreRoom, error)
	participants func(string) ([]CoreParticipant, error)
	quality      func(string) ([]CoreQuality, error)
	session      func(string, string) (CoreSession, error)
	closeRoom    func(string, string) (int, error)
}

func (f fakeCore) Room(id string) (CoreRoom, error) {
	if f.room != nil {
		return f.room(id)
	}
	return CoreRoom{}, nil
}
func (f fakeCore) Participants(id string) ([]CoreParticipant, error) {
	if f.participants != nil {
		return f.participants(id)
	}
	return nil, nil
}
func (f fakeCore) Quality(id string) ([]CoreQuality, error) {
	if f.quality != nil {
		return f.quality(id)
	}
	return nil, nil
}
func (f fakeCore) Session(id, sub string) (CoreSession, error) {
	if f.session != nil {
		return f.session(id, sub)
	}
	return CoreSession{}, nil
}
func (f fakeCore) CloseRoom(id, reason string) (int, error) {
	if f.closeRoom != nil {
		return f.closeRoom(id, reason)
	}
	return 0, nil
}

func testConfig() Config {
	return Config{
		Enabled:         true,
		Keys:            ParseAPIKeys("k-admin:*;k-read:rooms:read,participants:read"),
		RateLimitPerMin: 1000,
		PublicWSURL:     "wss://pulsertc.test/ws",
		TokenTTL:        time.Hour,
		TokenTTLMax:     12 * time.Hour,
		RoomRetention:   time.Minute,
	}
}

func newTestServer(t *testing.T, cfg Config, core Core) *httptest.Server {
	t.Helper()
	issuer := NewTokenIssuer(auth.Config{Secret: "test-secret", Issuer: "pulsertc", Audience: "pulsertc"}, cfg)
	svc := NewService(cfg, core, issuer, nil, nil)
	mux := http.NewServeMux()
	RegisterRoutes(mux, svc, cfg, nil)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

func do(t *testing.T, ts *httptest.Server, method, path, key string, body any) (*http.Response, []byte) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, ts.URL+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, data
}

// ---- config ------------------------------------------------------------

func TestParseAPIKeys(t *testing.T) {
	keys := ParseAPIKeys("a:rooms:read,tokens:create ; b:* ;;  c:bogus:scope ")
	if len(keys) != 3 {
		t.Fatalf("want 3 keys, got %d (%+v)", len(keys), keys)
	}
	byKey := map[string]APIKey{}
	for _, k := range keys {
		byKey[k.Key] = k
	}
	if !byKey["a"].Has(ScopeRoomsRead) || !byKey["a"].Has(ScopeTokensCreate) || byKey["a"].Has(ScopeRoomsClose) {
		t.Errorf("key a scopes wrong: %v", byKey["a"].scopeList())
	}
	if len(byKey["b"].Scopes) != len(AllScopes) {
		t.Errorf("wildcard should grant all scopes, got %v", byKey["b"].scopeList())
	}
	if len(byKey["c"].Scopes) != 0 {
		t.Errorf("unknown scopes must be ignored, got %v", byKey["c"].scopeList())
	}
}

// ---- token ------------------------------------------------------------

func TestTokenIssuerMintAndValidate(t *testing.T) {
	cfg := testConfig()
	iss := NewTokenIssuer(auth.Config{Secret: "s3cr3t", Issuer: "pulsertc", Audience: "pulsertc"}, cfg)

	mt, err := iss.Mint(TokenParams{RoomID: "room-1", Identity: "user-9",
		Permissions: auth.Permissions{Join: true, Subscribe: true}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(mt.Token, "s3cr3t") {
		t.Fatal("token must not contain the secret")
	}

	v := auth.NewValidator(auth.Config{
		Secret: "s3cr3t", Issuer: "pulsertc", Audience: "pulsertc",
		Algs: []string{auth.AlgHS256}, Leeway: time.Minute, MaxTokenAge: 2 * time.Hour,
	})
	id, verr := v.Validate(mt.Token)
	if verr != nil {
		t.Fatalf("minted token rejected by validator: %v", verr)
	}
	if id.Subject != "user-9" || id.Room != "room-1" || !id.Permissions.Join || id.Permissions.Publish {
		t.Fatalf("claims wrong: %+v", id)
	}
}

func TestTokenIssuerTTLCap(t *testing.T) {
	cfg := testConfig()
	cfg.TokenTTLMax = time.Hour
	iss := NewTokenIssuer(auth.Config{Secret: "x"}, cfg)
	mt, err := iss.Mint(TokenParams{RoomID: "r", Identity: "u", TTL: 48 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Until(mt.ExpiresAt); d > time.Hour+time.Minute {
		t.Fatalf("ttl not capped: %v", d)
	}
}

func TestTokenIssuerNoSecret(t *testing.T) {
	iss := NewTokenIssuer(auth.Config{}, testConfig())
	if _, err := iss.Mint(TokenParams{RoomID: "r", Identity: "u"}); err == nil {
		t.Fatal("want error without signing secret")
	}
}

// ---- registry --------------------------------------------------------

func TestRoomRegistryIdempotency(t *testing.T) {
	r := NewRoomRegistry(time.Minute)
	body := []byte(`{"roomId":"x"}`)
	if _, _, hit, conflict := r.idempotencyLookup("k1", body); hit || conflict {
		t.Fatal("fresh key should miss")
	}
	r.idempotencyStore("k1", body, []byte(`{"ok":true}`), 201)
	if resp, st, hit, _ := r.idempotencyLookup("k1", body); !hit || st != 201 || string(resp) != `{"ok":true}` {
		t.Fatalf("expected cached hit, got hit=%v st=%d", hit, st)
	}
	if _, _, _, conflict := r.idempotencyLookup("k1", []byte(`{"roomId":"y"}`)); !conflict {
		t.Fatal("different body under same key must conflict")
	}
}

func TestValidateMetadata(t *testing.T) {
	if err := ValidateMetadata(map[string]string{"token": "x"}); err == nil {
		t.Fatal("reserved key must be rejected")
	}
	big := strings.Repeat("a", 5000)
	if err := ValidateMetadata(map[string]string{"note": big}); err == nil {
		t.Fatal("oversize metadata must be rejected")
	}
	if err := ValidateMetadata(map[string]string{"externalId": "42"}); err != nil {
		t.Fatalf("normal metadata rejected: %v", err)
	}
}

// ---- auth middleware -----------------------------------------------

func TestAPIAuth(t *testing.T) {
	ts := newTestServer(t, testConfig(), fakeCore{})

	if resp, _ := do(t, ts, "POST", "/v1/rooms", "", map[string]any{}); resp.StatusCode != 401 {
		t.Fatalf("no key: want 401, got %d", resp.StatusCode)
	}
	if resp, _ := do(t, ts, "POST", "/v1/rooms", "wrong", map[string]any{}); resp.StatusCode != 401 {
		t.Fatalf("bad key: want 401, got %d", resp.StatusCode)
	}
	// k-read lacks rooms:create
	if resp, body := do(t, ts, "POST", "/v1/rooms", "k-read", map[string]any{}); resp.StatusCode != 403 {
		t.Fatalf("wrong scope: want 403, got %d (%s)", resp.StatusCode, body)
	}
	if resp, _ := do(t, ts, "POST", "/v1/rooms", "k-admin", map[string]any{"roomId": "r1"}); resp.StatusCode != 201 {
		t.Fatalf("admin create: want 201, got %d", resp.StatusCode)
	}
}

func TestAPIAuthDisabled(t *testing.T) {
	cfg := testConfig()
	cfg.Enabled = false
	ts := newTestServer(t, cfg, fakeCore{})
	if resp, _ := do(t, ts, "POST", "/v1/rooms", "", map[string]any{"roomId": "r1"}); resp.StatusCode != 201 {
		t.Fatalf("disabled auth should allow: got %d", resp.StatusCode)
	}
}

func TestRateLimit(t *testing.T) {
	cfg := testConfig()
	cfg.RateLimitPerMin = 3
	ts := newTestServer(t, cfg, fakeCore{})
	codes := make([]int, 0, 5)
	for i := 0; i < 5; i++ {
		resp, _ := do(t, ts, "GET", "/v1/rooms/r1/connection", "k-admin", nil)
		codes = append(codes, resp.StatusCode)
	}
	got429 := false
	for _, c := range codes {
		if c == 429 {
			got429 = true
		}
	}
	if !got429 {
		t.Fatalf("expected a 429 within 5 calls at limit 3, got %v", codes)
	}
}

// ---- handlers ------------------------------------------------------

func TestErrorContract(t *testing.T) {
	ts := newTestServer(t, testConfig(), fakeCore{
		room: func(string) (CoreRoom, error) { return CoreRoom{Exists: false}, nil },
	})
	resp, body := do(t, ts, "GET", "/v1/rooms/missing", "k-admin", nil)
	if resp.StatusCode != 404 {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Request-Id") == "" {
		t.Fatal("missing X-Request-Id header")
	}
	var eb errorBody
	if err := json.Unmarshal(body, &eb); err != nil {
		t.Fatal(err)
	}
	if eb.Error.Code != CodeRoomNotFound || eb.Error.RequestID == "" || eb.Error.Message == "" {
		t.Fatalf("bad error body: %s", body)
	}
	if strings.Contains(strings.ToLower(string(body)), "goroutine") {
		t.Fatal("error body leaked a stack trace")
	}
}

func TestRoomLifecycle(t *testing.T) {
	live := 0
	ts := newTestServer(t, testConfig(), fakeCore{
		room: func(string) (CoreRoom, error) {
			return CoreRoom{Exists: live > 0, Participants: live, Generation: 3}, nil
		},
		closeRoom: func(string, string) (int, error) { return live, nil },
	})

	resp, body := do(t, ts, "POST", "/v1/rooms", "k-admin",
		map[string]any{"roomId": "teleconsulta-1", "metadata": map[string]string{"externalId": "42"}})
	if resp.StatusCode != 201 {
		t.Fatalf("create want 201, got %d (%s)", resp.StatusCode, body)
	}
	var rr roomResponse
	_ = json.Unmarshal(body, &rr)
	if rr.Status != RoomEmpty || rr.Connection == nil || rr.Connection.ServerURL == "" {
		t.Fatalf("unexpected create response: %s", body)
	}

	// idempotent re-create -> 200
	if resp, _ := do(t, ts, "POST", "/v1/rooms", "k-admin", map[string]any{"roomId": "teleconsulta-1"}); resp.StatusCode != 200 {
		t.Fatalf("re-create want 200, got %d", resp.StatusCode)
	}

	live = 2
	resp, body = do(t, ts, "GET", "/v1/rooms/teleconsulta-1", "k-admin", nil)
	_ = json.Unmarshal(body, &rr)
	if resp.StatusCode != 200 || rr.Status != RoomActive || rr.Participants != 2 || rr.Generation != 3 {
		t.Fatalf("get room wrong: %d %s", resp.StatusCode, body)
	}

	resp, body = do(t, ts, "DELETE", "/v1/rooms/teleconsulta-1", "k-admin", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("close want 200, got %d (%s)", resp.StatusCode, body)
	}
	var cr closeRoomResponse
	_ = json.Unmarshal(body, &cr)
	if cr.Status != RoomClosed || cr.ParticipantsNotified != 2 {
		t.Fatalf("close response wrong: %s", body)
	}
}

func TestTokenEndpoint(t *testing.T) {
	ts := newTestServer(t, testConfig(), fakeCore{})
	resp, body := do(t, ts, "POST", "/v1/rooms/room-x/tokens", "k-admin",
		map[string]any{"identity": "user-1", "permissions": map[string]any{"publish": false}})
	if resp.StatusCode != 201 {
		t.Fatalf("want 201, got %d (%s)", resp.StatusCode, body)
	}
	var tr tokenResponse
	_ = json.Unmarshal(body, &tr)
	if tr.Token == "" || tr.RoomID != "room-x" || tr.Identity != "user-1" {
		t.Fatalf("bad token response: %s", body)
	}
	if strings.Contains(string(body), "test-secret") {
		t.Fatal("token response leaked the signing secret")
	}
	// room now queryable
	if resp, _ := do(t, ts, "GET", "/v1/rooms/room-x", "k-admin", nil); resp.StatusCode != 200 {
		t.Fatalf("room should exist after token mint, got %d", resp.StatusCode)
	}
}

func TestTokenControlBlocked(t *testing.T) {
	ts := newTestServer(t, testConfig(), fakeCore{})
	resp, _ := do(t, ts, "POST", "/v1/rooms/r/tokens", "k-admin",
		map[string]any{"identity": "u", "permissions": map[string]any{"control": true}})
	if resp.StatusCode != 403 {
		t.Fatalf("control token without flag: want 403, got %d", resp.StatusCode)
	}
}

func TestParticipantsAndQuality(t *testing.T) {
	core := fakeCore{
		room: func(string) (CoreRoom, error) { return CoreRoom{Exists: true, Participants: 2}, nil },
		participants: func(string) ([]CoreParticipant, error) {
			return []CoreParticipant{
				{Identity: "user-1.aaaa1111", Subject: "user-1", State: "CONNECTED", Role: "publisher",
					Tracks: []CoreTrack{{PublicationID: "p1", Kind: "video", Muted: false}}},
				{Identity: "user-2.bbbb2222", Subject: "user-2", State: "CONNECTED", Role: "subscriber"},
			}, nil
		},
		quality: func(string) ([]CoreQuality, error) {
			return []CoreQuality{
				{Identity: "user-1.aaaa1111", Status: "GOOD", Score: 95},
				{Identity: "user-2.bbbb2222", Status: "POOR", Score: 40, Reason: "HIGH_PACKET_LOSS",
					Video: &CoreQualityLeg{Status: "POOR", Reason: "HIGH_PACKET_LOSS"}},
			}, nil
		},
	}
	ts := newTestServer(t, testConfig(), core)

	resp, body := do(t, ts, "GET", "/v1/rooms/r/participants", "k-admin", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("participants %d %s", resp.StatusCode, body)
	}
	var pl participantListResponse
	_ = json.Unmarshal(body, &pl)
	if len(pl.Participants) != 2 || pl.Participants[0].Tracks != 1 {
		t.Fatalf("participant list wrong: %s", body)
	}
	if strings.Contains(string(body), "ice") || strings.Contains(string(body), "dtls") {
		t.Fatal("participant list leaked transport internals")
	}

	// prefix resolution
	resp, body = do(t, ts, "GET", "/v1/rooms/r/participants/user-1", "k-admin", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("participant by subject prefix: %d %s", resp.StatusCode, body)
	}

	resp, body = do(t, ts, "GET", "/v1/rooms/r/participants/user-2/quality", "k-admin", nil)
	var pq participantQualityResponse
	_ = json.Unmarshal(body, &pq)
	if resp.StatusCode != 200 || pq.Status != "POOR" || pq.Video == nil || pq.Video.Reason != "HIGH_PACKET_LOSS" {
		t.Fatalf("participant quality wrong: %d %s", resp.StatusCode, body)
	}

	resp, _ = do(t, ts, "GET", "/v1/rooms/r/participants/nobody", "k-admin", nil)
	if resp.StatusCode != 404 {
		t.Fatalf("unknown participant: want 404, got %d", resp.StatusCode)
	}
}

func TestRoomOnOtherNode(t *testing.T) {
	ts := newTestServer(t, testConfig(), fakeCore{
		room: func(string) (CoreRoom, error) { return CoreRoom{}, &ErrRoomRemote{OwnerNodeID: "node-b"} },
	})
	resp, body := do(t, ts, "GET", "/v1/rooms/r", "k-admin", nil)
	if resp.StatusCode != 409 {
		t.Fatalf("want 409, got %d", resp.StatusCode)
	}
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	if m["ownerNodeId"] != "node-b" {
		t.Fatalf("expected ownerNodeId hint, got %s", body)
	}
}

func TestSessionEndpoint(t *testing.T) {
	ts := newTestServer(t, testConfig(), fakeCore{
		participants: func(string) ([]CoreParticipant, error) {
			return []CoreParticipant{{Identity: "u.1234abcd", Subject: "u", State: "CONNECTED"}}, nil
		},
		session: func(_, sub string) (CoreSession, error) {
			if sub != "u" {
				return CoreSession{}, nil
			}
			return CoreSession{Exists: true, SessionID: "sess-1", State: "CONNECTED", Generation: 2, Recoverable: true}, nil
		},
	})
	resp, body := do(t, ts, "GET", "/v1/rooms/r/participants/u/session", "k-admin", nil)
	var sr sessionResponse
	_ = json.Unmarshal(body, &sr)
	if resp.StatusCode != 200 || sr.Generation != 2 || sr.State != "CONNECTED" || !sr.Recoverable {
		t.Fatalf("session response wrong: %d %s", resp.StatusCode, body)
	}
}

func TestConnectionNotConfigured(t *testing.T) {
	cfg := testConfig()
	cfg.PublicWSURL = ""
	ts := newTestServer(t, cfg, fakeCore{})
	resp, _ := do(t, ts, "GET", "/v1/rooms/r/connection", "k-admin", nil)
	if resp.StatusCode != 503 {
		t.Fatalf("want 503 NOT_CONFIGURED, got %d", resp.StatusCode)
	}
}
