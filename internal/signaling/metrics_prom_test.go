package signaling

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

func newPromTestServer(t *testing.T) *Server {
	t.Helper()
	authn, err := auth.Build(testAuthConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("auth.Build: %v", err)
	}
	s, err := NewServerWithAuthenticator(slog.New(slog.NewTextHandler(io.Discard, nil)), authn)
	if err != nil {
		t.Fatalf("NewServerWithAuthenticator: %v", err)
	}
	return s
}

func TestHandleMetricsPrometheus(t *testing.T) {
	s := newPromTestServer(t)
	rec := httptest.NewRecorder()
	s.HandleMetrics(rec, httptest.NewRequest("GET", "/metrics", nil))

	body := rec.Body.String()
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content-type = %q", ct)
	}
	for _, want := range []string{
		"# TYPE pulsertc_sfu_rooms gauge",
		"pulsertc_sfu_rooms ",
		"# TYPE pulsertc_sfu_rtp_packets_received_total counter",
		"# TYPE pulsertc_build_info gauge",
		"pulsertc_process_goroutines ",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in metrics output:\n%s", want, body)
		}
	}
}

func TestHandleMetricsJSONStillWorks(t *testing.T) {
	s := newPromTestServer(t)
	rec := httptest.NewRecorder()
	s.HandleMetricsJSON(rec, httptest.NewRequest("GET", "/metrics.json", nil))
	if !strings.Contains(rec.Body.String(), `"sfu"`) {
		t.Fatalf("json body missing sfu block: %s", rec.Body.String())
	}
}
