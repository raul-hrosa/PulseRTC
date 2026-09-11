package signaling

import (
	"encoding/json"
	"io"
	"runtime"
	"strings"
	"time"

	"github.com/raulhrosa/pulsertc/internal/metrics"
)

func writePrometheus(w io.Writer, s *Server) {
	e := metrics.NewEncoder(w)

	e.Gauge("pulsertc_build_info", "Build/runtime info; always 1.", 1,
		"go", runtime.Version(), "goos", runtime.GOOS, "goarch", runtime.GOARCH)
	e.Gauge("pulsertc_process_uptime_seconds", "Seconds since start.", time.Since(s.started).Seconds())

	rt := readRuntimeMetrics()
	e.Gauge("pulsertc_process_goroutines", "Live goroutines.", float64(rt.Goroutines))
	e.Gauge("pulsertc_go_heap_alloc_bytes", "Heap bytes in use.", float64(rt.HeapAllocBytes))
	e.Gauge("pulsertc_go_heap_sys_bytes", "Heap bytes from the OS.", float64(rt.HeapSysBytes))
	e.Counter("pulsertc_go_gc_total", "Completed GC cycles.", float64(rt.NumGC))
	e.Counter("pulsertc_go_cpu_seconds_total", "Go-attributed CPU seconds.", rt.GoCPUSeconds)

	m := s.sfu.Metrics()
	e.Gauge("pulsertc_sfu_rooms", "Active SFU rooms.", float64(m.Rooms.Active))
	e.Counter("pulsertc_sfu_rooms_created_total", "Rooms created.", float64(m.Rooms.Created))
	e.Gauge("pulsertc_sfu_peer_connections", "Active PeerConnections.", float64(m.PeerConns.Active))
	e.Gauge("pulsertc_sfu_publications", "Active publications.", float64(m.Publications.Active))
	e.Gauge("pulsertc_sfu_subscriptions", "Active subscriptions.", float64(m.Subscriptions.Active))
	e.Counter("pulsertc_sfu_subscriptions_removed_total", "Subscriptions removed.", float64(m.Subscriptions.Removed))
	e.Counter("pulsertc_sfu_rtp_packets_received_total", "RTP packets received from publishers.", float64(m.RTP.PacketsReceived))
	e.Counter("pulsertc_sfu_rtp_packets_sent_total", "RTP packets sent to subscribers.", float64(m.RTP.PacketsSent))
	e.Counter("pulsertc_sfu_rtp_bytes_received_total", "RTP bytes received.", float64(m.RTP.BytesReceived))
	e.Counter("pulsertc_sfu_rtp_bytes_sent_total", "RTP bytes sent.", float64(m.RTP.BytesSent))
	e.Counter("pulsertc_sfu_screen_publications_total", "Screen-share publications created (source=screen).", float64(m.ScreenPublicationsTotal))
	e.Counter("pulsertc_sfu_negotiations_total", "Negotiations started.", float64(m.Negotiations.Started))
	e.Counter("pulsertc_sfu_negotiations_failed_total", "Negotiations failed.", float64(m.Negotiations.Failed))
	e.Counter("pulsertc_sfu_negotiations_coalesced_total", "Negotiations coalesced by debounce.", float64(m.Negotiations.Coalesced))
	e.Counter("pulsertc_sfu_subscriber_queue_dropped_audio_total", "Audio packets dropped by a full subscriber queue.", float64(m.SubQueue.DroppedAudioPackets))
	e.Counter("pulsertc_sfu_subscriber_queue_dropped_video_total", "Video backlog packets dropped by a full subscriber queue.", float64(m.SubQueue.DroppedVideoPackets))
	e.Counter("pulsertc_sfu_keyframe_resyncs_total", "Keyframe requests triggered by a video queue overflow.", float64(m.SubQueue.KeyframeResyncs))
	e.Counter("pulsertc_sfu_subscriptions_failed_total", "Subscriptions torn down after a persistent media-transport failure.", float64(m.SubQueue.Failed))

	b, c, sum, total := m.NegotiationDuration.Bounds, m.NegotiationDuration.Counts, m.NegotiationDuration.SumSeconds, m.NegotiationDuration.Count
	e.Histogram("pulsertc_sfu_negotiation_seconds", "SFU-side renegotiation duration.", b, c, sum, total)

	// auth / cluster / session: emit the numeric leaves of their snapshots.
	writeGenericSnapshot(e, "pulsertc_auth", s.auth.MetricsSnapshot())
	writeGenericSnapshot(e, "pulsertc_cluster", s.cluster.MetricsSnapshot())
	writeGenericSnapshot(e, "pulsertc_session", s.sessionMetricsSnapshot())
	writeGenericSnapshot(e, "pulsertc_history", s.history.MetricsSnapshot())
}

// writeGenericSnapshot marshals v to JSON and emits every numeric (and bool)
// leaf as a gauge named prefix + "_" + snake(key), recursing into nested
// objects. Non-scalar leaves are skipped.
func writeGenericSnapshot(e *metrics.Encoder, prefix string, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		return
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return
	}
	walkSnapshot(e, prefix, m)
}

func walkSnapshot(e *metrics.Encoder, prefix string, m map[string]any) {
	for key, val := range m {
		name := prefix + "_" + snake(key)
		switch t := val.(type) {
		case float64:
			e.Gauge(name, key, t)
		case bool:
			// N2: expose bool leaves (e.g. cluster "enabled") as 0/1 gauges.
			v := 0.0
			if t {
				v = 1.0
			}
			e.Gauge(name, key, v)
		case map[string]any:
			walkSnapshot(e, name, t)
		default:
			// Non-scalar leaves (strings, []any / nested-unhandled, nil) have no
			// numeric meaning — skip them rather than risk a bad series.
		}
	}
}

// snake converts camelCase / PascalCase to snake_case.
func snake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r - 'A' + 'a')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
