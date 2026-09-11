package signaling

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

func FuzzInbound(f *testing.F) {
	// A real server so the fuzzer reaches the message router, not just the
	// unmarshal. The client below is unjoined and carries a permissionless
	// identity, so every handler either rejects the frame during validation
	// (join → AuthorizeJoin, others → "join a room first") or bails on the nil
	// SFU peer — none of them mutate room / SFU / cluster state. Driving a fully
	// joined session would need a live WebSocket, which a fuzz target can't hold.
	authn, err := auth.Build(testAuthConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		f.Fatalf("auth.Build: %v", err)
	}
	srv, err := NewServerWithAuthenticator(slog.New(slog.NewTextHandler(io.Discard, nil)), authn)
	if err != nil {
		f.Fatalf("NewServerWithAuthenticator: %v", err)
	}

	for _, s := range []string{
		`{"type":"join","roomId":"r"}`,
		`{"type":"sfu_offer","payload":{"sdp":"v=0"}}`,
		`{"type":"set_mute","publicationId":"p","muted":true,"kind":"audio"}`,
		`{"type":"join","resume":{"sessionId":"s","generation":2}}`,
		`{"type":"quality_report","samples":[]}`,
		`{"type":"control"}`, `{"type":"signal","target":"x"}`,
		`{`, `null`, `[]`, `{"type":123}`, `{"payload":"not-an-object"}`,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var in Inbound
		_ = json.Unmarshal(data, &in) // must never panic
		// Payload is json.RawMessage — accessing it must be safe even when garbage.
		_ = len(in.Payload)
		if in.Resume != nil {
			_ = in.Resume.SessionID
		}

		// Real router entry point. A panic here is the finding.
		c := &Client{
			id:       "fuzz",
			send:     make(chan []byte, 64),
			identity: &auth.Identity{Subject: "fuzz"},
		}
		srv.handleMessage(c, data)
	})
}
