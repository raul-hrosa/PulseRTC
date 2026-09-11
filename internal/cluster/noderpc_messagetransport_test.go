package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRegistry(t *testing.T, entries map[string]NodeInfo) *InMemoryNodeRegistry {
	t.Helper()
	r := NewInMemoryNodeRegistry(time.Minute)
	for _, n := range entries {
		if err := r.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func nodeInfoForURL(t *testing.T, id, rawURL string, state NodeState) NodeInfo {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(u.Port())
	return NodeInfo{ID: id, Host: u.Hostname(), Port: port, State: state}
}

func httpTransport(t *testing.T, reg NodeRegistry) *HTTPMessageTransport {
	t.Helper()
	m := &Metrics{}
	return newHTTPMessageTransport(transportDeps{
		self: "n1", secret: "s", timeout: 200 * time.Millisecond,
		maxRetries: 2, maxSize: 64 * 1024, registry: reg, metrics: m,
	})
}

func TestHTTPTransport_SendSuccess(t *testing.T) {
	var got atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer s" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		got.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	reg := newTestRegistry(t, map[string]NodeInfo{"n2": nodeInfoForURL(t, "n2", ts.URL, NodeReady)})
	tr := httpTransport(t, reg)
	if err := tr.Send(context.Background(), "n2", validMsg()); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got.Load() != 1 {
		t.Fatalf("server saw %d requests", got.Load())
	}
}

func TestHTTPTransport_UnknownNode(t *testing.T) {
	tr := httpTransport(t, NewInMemoryNodeRegistry(time.Minute))
	err := tr.Send(context.Background(), "ghost", validMsg())
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeClusterNodeNotFound {
		t.Fatalf("want CLUSTER_NODE_NOT_FOUND, got %v", err)
	}
}

func TestHTTPTransport_DrainingNode(t *testing.T) {
	reg := newTestRegistry(t, map[string]NodeInfo{
		"n2": {ID: "n2", Host: "localhost", Port: 9, State: NodeShuttingDown},
	})
	tr := httpTransport(t, reg)
	err := tr.Send(context.Background(), "n2", validMsg())
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeClusterNodeUnavailable {
		t.Fatalf("want CLUSTER_NODE_UNAVAILABLE, got %v", err)
	}
}

func TestHTTPTransport_PayloadTooLarge(t *testing.T) {
	reg := newTestRegistry(t, map[string]NodeInfo{
		"n2": {ID: "n2", Host: "localhost", Port: 9, State: NodeReady},
	})
	tr := httpTransport(t, reg)
	tr.deps.maxSize = 128
	msg := validMsg()
	big := make([]byte, 4096)
	for i := range big {
		big[i] = 'x'
	}
	msg.Payload = json.RawMessage(`"` + string(big) + `"`)
	err := tr.Send(context.Background(), "n2", msg)
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeClusterMessageTooLarge {
		t.Fatalf("want CLUSTER_MESSAGE_TOO_LARGE, got %v", err)
	}
}

func TestHTTPTransport_RetryThenFail(t *testing.T) {
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	reg := newTestRegistry(t, map[string]NodeInfo{"n2": nodeInfoForURL(t, "n2", ts.URL, NodeReady)})
	tr := httpTransport(t, reg)
	err := tr.Send(context.Background(), "n2", validMsg())
	if err == nil {
		t.Fatal("expected failure after retries")
	}
	if hits.Load() != 3 { // 1 + 2 retries
		t.Fatalf("want 3 attempts, got %d", hits.Load())
	}
	if tr.deps.metrics.msgRetriedC.Load() != 2 {
		t.Fatalf("want 2 retries counted, got %d", tr.deps.metrics.msgRetriedC.Load())
	}
}

func TestHTTPTransport_Timeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	reg := newTestRegistry(t, map[string]NodeInfo{"n2": nodeInfoForURL(t, "n2", ts.URL, NodeReady)})
	tr := httpTransport(t, reg)
	tr.deps.timeout = 80 * time.Millisecond
	tr.client.Timeout = 80 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- tr.Send(context.Background(), "n2", validMsg()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a timeout error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Send blocked past the timeout")
	}
	if tr.deps.metrics.msgTimeoutC.Load() == 0 {
		t.Fatal("timeout metric not incremented")
	}
}

// TestCrossNodeOverHTTP is the full path: two real Cluster nodes
// sharing Redis (miniredis), talking over the /internal/cluster/messages HTTP
// endpoint with cluster-secret auth.
func TestCrossNodeOverHTTP(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := RedisConfig{
		Enabled: true, Required: true, Addr: mr.Addr(), Prefix: "pulsertc",
		Timeout: time.Second, NodeTTL: 15 * time.Second, ParticipTTL: 30 * time.Second,
	}

	build := func(id string, port int) (*Cluster, *fakeDelivery) {
		st := newRedisClusterStateWith(
			newRedisClientFrom(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "pulsertc"),
			rc, testLogger())
		c, err := NewWithState(Config{
			Enabled: true, NodeID: id, Secret: "shared-secret", Host: "127.0.0.1", Port: port,
			HeartbeatInterval: time.Hour, StaleAfter: time.Minute,
			RequestTimeout: time.Second, MessageMaxAge: 30 * time.Second, MaxMessageSize: 64 * 1024,
			Redis: rc,
		}, testLogger(), st)
		if err != nil {
			t.Fatal(err)
		}
		d := newFakeDelivery()
		c.SetLocalDelivery(d)
		return c, d
	}

	n1, _ := build("n1", 0)
	n2, d2 := build("n2", 0)
	t.Cleanup(func() { n1.Shutdown(); n2.Shutdown() })

	mux2 := http.NewServeMux()
	n2.RegisterRoutes(mux2)
	ts2 := httptest.NewServer(mux2)
	defer ts2.Close()

	n1.Start()
	n2.Start()
	// Point n1 at n2's real address.
	n1.registry.Register(nodeInfoForURL(t, "n2", ts2.URL, NodeReady))

	// B connects on n2.
	n2.RegisterParticipant("B", "room-1")
	d2.addLocal("B")

	msg := n1.NewClusterMessage(MsgParticipantMessage, "room-1", "B", json.RawMessage(`{"hello":"http"}`))
	if err := n1.RouteMessage(context.Background(), "B", msg); err != nil {
		t.Fatalf("route over HTTP: %v", err)
	}
	if d2.count() != 1 {
		t.Fatalf("B should have received 1 message over HTTP, got %d", d2.count())
	}

	// Unauthenticated call is refused.
	req, _ := http.NewRequest(http.MethodPost, ts2.URL+"/internal/cluster/messages", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated cluster message must be 401, got %d", resp.StatusCode)
	}
}
