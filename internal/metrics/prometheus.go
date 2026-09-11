// Package metrics writes the Prometheus text exposition format by hand — no
// client library, to keep the server dependency-free (see internal/signaling/metrics.go).
package metrics

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Encoder struct {
	w    io.Writer
	seen map[string]bool
}

func NewEncoder(w io.Writer) *Encoder { return &Encoder{w: w, seen: map[string]bool{}} }

// sanitizeName coerces s into a valid Prometheus metric name
// ([a-zA-Z_:][a-zA-Z0-9_:]*): any other rune becomes '_', and a leading digit
// is prefixed with '_'. Callers sanitize the base name, then append fixed
// suffixes like "_bucket"/"_sum"/"_count" themselves.
func sanitizeName(s string) string {
	if s == "" {
		return "_"
	}
	var b strings.Builder
	b.Grow(len(s))
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_', r == ':':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			if i == 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func (e *Encoder) header(name, help, typ string) {
	name = sanitizeName(name)
	if e.seen[name] {
		return
	}
	e.seen[name] = true
	fmt.Fprintf(e.w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func (e *Encoder) sample(name string, value float64, labels []string) {
	e.w.Write([]byte(sanitizeName(name)))
	if len(labels) >= 2 {
		e.w.Write([]byte{'{'})
		for i := 0; i+1 < len(labels); i += 2 {
			if i > 0 {
				e.w.Write([]byte{','})
			}
			fmt.Fprintf(e.w, `%s="%s"`, labels[i], escape(labels[i+1]))
		}
		e.w.Write([]byte{'}'})
	}
	fmt.Fprintf(e.w, " %s\n", strconv.FormatFloat(value, 'g', -1, 64))
}

func (e *Encoder) Gauge(name, help string, value float64, labels ...string) {
	e.header(name, help, "gauge")
	e.sample(name, value, labels)
}

func (e *Encoder) Counter(name, help string, value float64, labels ...string) {
	e.header(name, help, "counter")
	e.sample(name, value, labels)
}

func (e *Encoder) Histogram(name, help string, buckets []float64, counts []uint64, sum float64, count uint64, labels ...string) {
	e.header(name, help, "histogram")
	name = sanitizeName(name)
	// Guard against a malformed snapshot: without a per-bucket count for every
	// bound we cannot emit cumulative buckets, so fall back to _sum/_count only.
	if len(counts) == len(buckets) {
		cumulative := uint64(0)
		for i, b := range buckets {
			cumulative += counts[i]
			e.sample(name+"_bucket", float64(cumulative), append(append([]string{}, labels...), "le", strconv.FormatFloat(b, 'g', -1, 64)))
		}
		e.sample(name+"_bucket", float64(count), append(append([]string{}, labels...), "le", "+Inf"))
	}
	e.sample(name+"_sum", sum, labels)
	e.sample(name+"_count", float64(count), labels)
}

func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}
