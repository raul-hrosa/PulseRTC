package metrics

import (
	"strings"
	"testing"
)

func TestEncoderGaugeAndCounter(t *testing.T) {
	var b strings.Builder
	e := NewEncoder(&b)
	e.Gauge("pulsertc_sfu_rooms", "Active rooms.", 3)
	e.Counter("pulsertc_sfu_rtp_packets_total", "RTP packets forwarded.", 42, "kind", "video")
	out := b.String()

	for _, want := range []string{
		"# HELP pulsertc_sfu_rooms Active rooms.",
		"# TYPE pulsertc_sfu_rooms gauge",
		"pulsertc_sfu_rooms 3",
		"# TYPE pulsertc_sfu_rtp_packets_total counter",
		`pulsertc_sfu_rtp_packets_total{kind="video"} 42`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing line %q in:\n%s", want, out)
		}
	}
}

func TestEncoderHistogram(t *testing.T) {
	var b strings.Builder
	e := NewEncoder(&b)
	e.Histogram("pulsertc_sfu_negotiation_seconds", "Negotiation duration.",
		[]float64{0.01, 0.1, 1}, []uint64{5, 8, 10}, 1.23, 10)
	out := b.String()
	for _, want := range []string{
		`pulsertc_sfu_negotiation_seconds_bucket{le="0.01"} 5`,
		`pulsertc_sfu_negotiation_seconds_bucket{le="+Inf"} 10`,
		"pulsertc_sfu_negotiation_seconds_sum 1.23",
		"pulsertc_sfu_negotiation_seconds_count 10",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestEncoderEscapesLabelValues(t *testing.T) {
	var b strings.Builder
	NewEncoder(&b).Gauge("x", "h", 1, "reason", `a"b\c`)
	if !strings.Contains(b.String(), `reason="a\"b\\c"`) {
		t.Fatalf("label not escaped: %s", b.String())
	}
}
