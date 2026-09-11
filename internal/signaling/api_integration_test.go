package signaling

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/raulhrosa/pulsertc/internal/api"
	"github.com/raulhrosa/pulsertc/internal/auth"
)

// buildAPIServer wires a real signaling server + the /v1 integration API on one
// mux, so a token minted by the API is exercised against the real /ws path.
func buildAPIServer(t *testing.T) (wsURL, baseURL, apiKey string, client *http.Client) {
	t.Helper()
	authn, err := auth.Build(testAuthConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("auth.Build: %v", err)
	}
	s, err := NewServerWithAuthenticator(slog.New(slog.NewTextHandler(io.Discard, nil)), authn)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	apiCfg := api.Config{
		Enabled:         true,
		Keys:            api.ParseAPIKeys("itest:*"),
		RateLimitPerMin: 100000,
		PublicWSURL:     "wss://pulsertc.test/ws",
		TokenTTL:        time.Hour,
		TokenTTLMax:     2 * time.Hour,
		RoomRetention:   time.Minute,
	}
	issuer := api.NewTokenIssuer(testAuthConfig(), apiCfg)
	svc := api.NewService(apiCfg, NewAPICore(s), issuer, nil, nil)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.HandleWS)
	api.RegisterRoutes(mux, svc, apiCfg, nil)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws", ts.URL, "itest", ts.Client()
}

func apiGET(t *testing.T, client *http.Client, url, key string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m
}

func apiPOST(t *testing.T, client *http.Client, url, key, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest("POST", url, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m
}

func TestIntegrationAPIFullFlow(t *testing.T) {
	wsURL, base, key, client := buildAPIServer(t)

	// 1. create room
	st, room := apiPOST(t, client, base+"/v1/rooms", key, `{"roomId":"teleconsulta-42","metadata":{"externalId":"42"}}`)
	if st != 201 {
		t.Fatalf("create room: %d %v", st, room)
	}

	// 2. mint a participant token for the room
	st, tok := apiPOST(t, client, base+"/v1/rooms/teleconsulta-42/tokens", key, `{"identity":"dr-house"}`)
	if st != 201 {
		t.Fatalf("mint token: %d %v", st, tok)
	}
	token, _ := tok["token"].(string)
	if token == "" {
		t.Fatal("no token in response")
	}

	// 3. the API-minted token is accepted by the real /ws path
	c := dialWithToken(t, wsURL, token)
	welcome := readMsg(t, c)
	if welcome["type"] != "welcome" {
		t.Fatalf("want welcome, got %v", welcome)
	}
	if err := c.WriteJSON(map[string]any{"type": "join", "roomId": "teleconsulta-42"}); err != nil {
		t.Fatal(err)
	}
	joined := readMsg(t, c)
	if joined["type"] != "room_joined" {
		t.Fatalf("want room_joined, got %v", joined)
	}

	waitFor(t, func() bool {
		st, r := apiGET(t, client, base+"/v1/rooms/teleconsulta-42", key)
		return st == 200 && r["status"] == "ACTIVE" && r["participants"] == float64(1)
	}, "room ACTIVE with 1 participant")

	// 4. participants list
	st, pl := apiGET(t, client, base+"/v1/rooms/teleconsulta-42/participants", key)
	parts, _ := pl["participants"].([]any)
	if st != 200 || len(parts) != 1 {
		t.Fatalf("participants: %d %v", st, pl)
	}
	p0 := parts[0].(map[string]any)
	identity, _ := p0["identity"].(string)
	if !strings.HasPrefix(identity, "dr-house.") {
		t.Fatalf("unexpected participant identity %q", identity)
	}

	// 5. session metadata (recovery enabled by default)
	st, sess := apiGET(t, client, base+"/v1/rooms/teleconsulta-42/participants/dr-house/session", key)
	if st != 200 || sess["state"] != "CONNECTED" {
		t.Fatalf("session: %d %v", st, sess)
	}
	if g, _ := sess["generation"].(float64); g < 1 {
		t.Fatalf("session generation should be >= 1, got %v", sess["generation"])
	}

	// 6. quality endpoint responds (verdict may be UNKNOWN without stats)
	if st, _ := apiGET(t, client, base+"/v1/rooms/teleconsulta-42/quality", key); st != 200 {
		t.Fatalf("quality: %d", st)
	}

	// 7. close room -> participant receives room_closed
	st, closed := apiPOST(t, client, base+"/v1/rooms/teleconsulta-42/tokens", key, `{"identity":"x"}`) // keep room record alive
	_ = st
	_ = closed
	req, _ := http.NewRequest("DELETE", base+"/v1/rooms/teleconsulta-42", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("close room: %d", resp.StatusCode)
	}

	sawClosed := false
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		var m map[string]any
		_, data, rerr := c.ReadMessage()
		if rerr != nil {
			break
		}
		_ = json.Unmarshal(data, &m)
		if m["type"] == "room_closed" {
			sawClosed = true
			break
		}
	}
	if !sawClosed {
		t.Fatal("participant never received room_closed")
	}
}

func TestIntegrationAPIRequiresKey(t *testing.T) {
	_, base, _, client := buildAPIServer(t)
	req, _ := http.NewRequest("GET", base+"/v1/rooms/whatever", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("want 401 without API key, got %d", resp.StatusCode)
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}
