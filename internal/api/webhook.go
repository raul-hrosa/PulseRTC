package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Event is an outbound integration event (design). version is fixed at 1.
type Event struct {
	Type   string
	RoomID string
	Data   map[string]any
}

type eventEnvelope struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Version   int            `json:"version"`
	Timestamp time.Time      `json:"timestamp"`
	RoomID    string         `json:"roomId"`
	Data      map[string]any `json:"data,omitempty"`
}

// WebhookMetrics is the /metrics "webhooks" block.
type WebhookMetrics struct {
	Enabled   bool  `json:"enabled"`
	Sent      int64 `json:"sent"`
	Delivered int64 `json:"delivered"`
	Failed    int64 `json:"failed"`
	Dropped   int64 `json:"dropped"`
}

// WebhookDispatcher POSTs events to a single configured URL, HMAC-signed, with a
// bounded retry. No queue, no persistence (design): if the buffer is full or
// all retries fail the event is dropped and a metric is bumped.
type WebhookDispatcher struct {
	url    string
	secret string
	client *http.Client
	logger *slog.Logger
	now    func() time.Time

	ch     chan eventEnvelope
	wg     sync.WaitGroup
	closed chan struct{}
	once   sync.Once

	sent, delivered, failed, dropped atomic.Int64
}

// NewWebhookDispatcher returns nil when url is empty (the no-op case: callers
// guard on nil, so there is zero overhead).
func NewWebhookDispatcher(url, secret string, logger *slog.Logger) *WebhookDispatcher {
	if url == "" {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	d := &WebhookDispatcher{
		url:    url,
		secret: secret,
		client: &http.Client{Timeout: 5 * time.Second},
		logger: logger,
		now:    time.Now,
		ch:     make(chan eventEnvelope, 256),
		closed: make(chan struct{}),
	}
	d.wg.Add(1)
	go d.run()
	return d
}

// Emit queues an event. Non-blocking: a full buffer drops the event.
func (d *WebhookDispatcher) Emit(ev Event) {
	if d == nil {
		return
	}
	env := eventEnvelope{
		ID:        "evt_" + randHex(12),
		Type:      ev.Type,
		Version:   1,
		Timestamp: d.now().UTC(),
		RoomID:    ev.RoomID,
		Data:      ev.Data,
	}
	select {
	case d.ch <- env:
		d.sent.Add(1)
	default:
		d.dropped.Add(1)
		d.logger.Warn("webhook_dropped", "reason", "buffer full", "type", ev.Type)
	}
}

// Close stops the dispatcher and waits for the in-flight event to finish.
func (d *WebhookDispatcher) Close() {
	if d == nil {
		return
	}
	d.once.Do(func() { close(d.closed) })
	d.wg.Wait()
}

// Metrics returns a snapshot.
func (d *WebhookDispatcher) Metrics() WebhookMetrics {
	if d == nil {
		return WebhookMetrics{Enabled: false}
	}
	return WebhookMetrics{
		Enabled:   true,
		Sent:      d.sent.Load(),
		Delivered: d.delivered.Load(),
		Failed:    d.failed.Load(),
		Dropped:   d.dropped.Load(),
	}
}

func (d *WebhookDispatcher) run() {
	defer d.wg.Done()
	backoff := []time.Duration{0, time.Second, 4 * time.Second}
	for {
		select {
		case <-d.closed:
			return
		case env := <-d.ch:
			body, _ := json.Marshal(env)
			ok := false
			for attempt, wait := range backoff {
				if wait > 0 {
					select {
					case <-d.closed:
						return
					case <-time.After(wait):
					}
				}
				if d.deliver(body) {
					ok = true
					d.delivered.Add(1)
					break
				}
				d.logger.Warn("webhook_attempt_failed", "type", env.Type, "attempt", attempt+1)
			}
			if !ok {
				d.failed.Add(1)
				d.dropped.Add(1)
				d.logger.Warn("webhook_dropped", "reason", "retries exhausted", "type", env.Type)
			}
		}
	}
}

func (d *WebhookDispatcher) deliver(body []byte) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "PulseRTC-Webhook/1")
	ts := d.now().UTC().Unix()
	req.Header.Set("X-PulseRTC-Signature", signPayload(d.secret, ts, body))

	resp, err := d.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// signPayload builds "t=<unix>,v1=<hex>" over "<t>.<body>" (design).
func signPayload(secret string, ts int64, body []byte) string {
	t := strconv.FormatInt(ts, 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(t))
	mac.Write([]byte("."))
	mac.Write(body)
	return "t=" + t + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
