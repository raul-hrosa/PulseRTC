package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// MessageTransport carries ClusterMessages between nodes. It is
// separate from the NodeTransport (peer state queries) and, crucially,
// from Redis: the destination is explicit, the call is authenticated node→node,
// and its latency is measurable. It never carries media.
type MessageTransport interface {
	// Send delivers message to nodeID. The implementation resolves the node's
	// address from the NodeRegistry, applies a timeout and a bounded retry,
	// and returns a *Error on failure.
	Send(ctx context.Context, nodeID string, message ClusterMessage) error
	Close() error
}

// transportDeps is what a transport needs from the cluster: node discovery, the
// shared secret, limits and metric hooks.
type transportDeps struct {
	self       string
	secret     string
	timeout    time.Duration
	maxRetries int
	maxSize    int
	registry   NodeRegistry
	metrics    *Metrics
}

func (d transportDeps) retries() int {
	if d.maxRetries <= 0 {
		return 2
	}
	return d.maxRetries
}

func (d transportDeps) reqTimeout() time.Duration {
	if d.timeout <= 0 {
		return 2 * time.Second
	}
	return d.timeout
}

// resolveReadyNode returns a node's base URL only if it is known and READY/ACTIVE:
// STARTING / DRAINING / OFFLINE nodes never receive new operations.
func (d transportDeps) resolveReadyNode(nodeID string) (string, *Error) {
	info, ok := d.registry.Get(nodeID)
	if !ok {
		return "", &Error{Code: CodeClusterNodeNotFound, Message: "target node is not registered", NodeID: nodeID}
	}
	if !info.State.AcceptsSessions() {
		return "", &Error{Code: CodeClusterNodeUnavailable, Message: "target node is not accepting operations", NodeID: nodeID}
	}
	return info.BaseURL(), nil
}

// --------------------------------------------------------------------------
// HTTP transport
// --------------------------------------------------------------------------

// HTTPMessageTransport POSTs the envelope to the peer's
// /internal/cluster/messages endpoint with the cluster bearer credential.
type HTTPMessageTransport struct {
	deps   transportDeps
	client *http.Client
}

func newHTTPMessageTransport(deps transportDeps) *HTTPMessageTransport {
	return &HTTPMessageTransport{
		deps:   deps,
		client: &http.Client{Timeout: deps.reqTimeout()},
	}
}

func (t *HTTPMessageTransport) Close() error { return nil }

func (t *HTTPMessageTransport) Send(ctx context.Context, nodeID string, msg ClusterMessage) error {
	base, cerr := t.deps.resolveReadyNode(nodeID)
	if cerr != nil {
		return cerr
	}
	msg.TargetNodeID = nodeID
	body, err := json.Marshal(msg)
	if err != nil {
		return &Error{Code: CodeClusterMessageInvalid, Message: "cannot encode message"}
	}
	if t.deps.maxSize > 0 && len(body) > t.deps.maxSize {
		return &Error{Code: CodeClusterMessageTooLarge, Message: "message exceeds cluster max size"}
	}

	url := base + "/internal/cluster/messages"
	attempts := t.deps.retries() + 1
	backoff := 50 * time.Millisecond
	var last error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			t.deps.metrics.msgRetried()
			select {
			case <-ctx.Done():
				return &Error{Code: CodeClusterNodeUnavailable, Message: "context cancelled during retry", NodeID: nodeID}
			case <-time.After(backoff):
			}
			backoff *= 2
		}
		start := time.Now()
		err := t.post(ctx, url, body)
		t.deps.metrics.observeTransportLatency(time.Since(start))
		if err == nil {
			t.deps.metrics.msgSent()
			return nil
		}
		last = err
		t.deps.metrics.transportError()
		if ce, ok := err.(*Error); ok {
			// 4xx-class problems will not be fixed by retrying.
			switch ce.Code {
			case CodeClusterMessageInvalid, CodeClusterMessageTooLarge,
				CodeClusterMessageExpired, CodeClusterTargetMismatch,
				CodeParticipantNotFound, CodeClusterUnauthorized:
				t.deps.metrics.msgFailed()
				return ce
			}
		}
	}
	t.deps.metrics.msgFailed()
	if _, ok := last.(*Error); ok {
		return last
	}
	return &Error{Code: CodeClusterNodeUnavailable, Message: "remote node unreachable", NodeID: nodeID}
}

func (t *HTTPMessageTransport) post(ctx context.Context, url string, body []byte) error {
	reqCtx, cancel := context.WithTimeout(ctx, t.deps.reqTimeout())
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", internalToken(t.deps.secret))
	resp, err := t.client.Do(req)
	if err != nil {
		if reqCtx.Err() == context.DeadlineExceeded {
			t.deps.metrics.msgTimeout()
			return &Error{Code: CodeClusterNodeUnavailable, Message: "request timed out"}
		}
		return err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	var payload struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&payload)
	code := payload.Error
	if code == "" {
		code = CodeClusterNodeUnavailable
	}
	return &Error{Code: code, Message: fmt.Sprintf("peer returned %d", resp.StatusCode)}
}

// --------------------------------------------------------------------------
// In-memory transport
// --------------------------------------------------------------------------

// InMemoryMessageTransport routes messages to other *Cluster instances in the
// same process (tests, multi-node simulation). It goes through the exact
// same handler path a real node would.
type InMemoryMessageTransport struct {
	deps  transportDeps
	nodes map[string]*Cluster
}

// NewInMemoryMessageTransport builds a transport over an explicit nodeID→Cluster
// table shared by every simulated node.
func NewInMemoryMessageTransport(nodes map[string]*Cluster) *InMemoryMessageTransport {
	return &InMemoryMessageTransport{nodes: nodes}
}

func (t *InMemoryMessageTransport) Close() error { return nil }

func (t *InMemoryMessageTransport) Send(ctx context.Context, nodeID string, msg ClusterMessage) error {
	if t.deps.registry != nil {
		if _, cerr := t.deps.resolveReadyNode(nodeID); cerr != nil {
			t.metricsFailed(cerr)
			return cerr
		}
	}
	peer, ok := t.nodes[nodeID]
	if !ok {
		err := &Error{Code: CodeClusterNodeNotFound, Message: "unknown node", NodeID: nodeID}
		t.metricsFailed(err)
		return err
	}
	msg.TargetNodeID = nodeID
	start := time.Now()
	err := peer.handleInboundClusterMessage(ctx, msg)
	if t.deps.metrics != nil {
		t.deps.metrics.observeTransportLatency(time.Since(start))
	}
	if err != nil {
		t.metricsFailed(err)
		return err
	}
	if t.deps.metrics != nil {
		t.deps.metrics.msgSent()
	}
	return nil
}

func (t *InMemoryMessageTransport) metricsFailed(err error) {
	if t.deps.metrics == nil {
		return
	}
	t.deps.metrics.transportError()
	t.deps.metrics.msgFailed()
	_ = err
}
