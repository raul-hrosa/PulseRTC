package cluster

import (
	"context"
	"sync"
	"time"
)

// ParticipantRouter decides whether a message is delivered to a local
// participant or forwarded to the node that holds it.
type ParticipantRouter interface {
	Route(ctx context.Context, participantID string, message ClusterMessage) error
}

// LocalDelivery is implemented by the signaling layer: it hands a routed
// message to a locally-connected participant or fans a room event out to local
// room members. It must return a *Error{Code: PARTICIPANT_NOT_FOUND} when the
// participant is not on this node.
type LocalDelivery interface {
	DeliverToParticipant(ctx context.Context, message ClusterMessage) error
	DeliverRoomEvent(ctx context.Context, message ClusterMessage) error
}

// locationCache is a small, non-authoritative cache in front of the
// ParticipantLocator, which remains the source of truth; the cache only saves
// repeat lookups within a short TTL and is invalidated on any miss or send
// failure.
type locationCache struct {
	ttl time.Duration
	now func() time.Time
	mu  sync.Mutex
	m   map[string]cachedLoc
}

type cachedLoc struct {
	nodeID string
	exp    time.Time
}

func newLocationCache(ttl time.Duration) *locationCache {
	if ttl <= 0 {
		ttl = 3 * time.Second
	}
	return &locationCache{ttl: ttl, now: time.Now, m: make(map[string]cachedLoc)}
}

func (c *locationCache) get(id string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[id]
	if !ok || c.now().After(v.exp) {
		delete(c.m, id)
		return "", false
	}
	return v.nodeID, true
}

func (c *locationCache) put(id, nodeID string) {
	c.mu.Lock()
	c.m[id] = cachedLoc{nodeID: nodeID, exp: c.now().Add(c.ttl)}
	c.mu.Unlock()
}

func (c *locationCache) invalidate(id string) {
	c.mu.Lock()
	delete(c.m, id)
	c.mu.Unlock()
}

// participantRouter is the concrete router. It reads location from the cache,
// then the ParticipantLocator, decides local vs remote, and either
// calls LocalDelivery or MessageTransport.
type participantRouter struct {
	self      string
	cache     *locationCache
	locator   ParticipantLocator
	transport MessageTransport
	delivery  func() LocalDelivery
	metrics   *Metrics
	// stateHealthy reports whether the distributed state backend is usable; when
	// it is not, routing that needs a lookup fails closed.
	stateHealthy func() bool
	// nodeStale reports whether a node is failed. A message
	// is never forwarded to a stale node — it returns PARTICIPANT_STALE instead
	// of hanging on an HTTP timeout.
	nodeStale func(nodeID string) bool
}

func (r *participantRouter) Route(ctx context.Context, participantID string, msg ClusterMessage) error {
	nodeID, err := r.resolve(participantID)
	if err != nil {
		r.metrics.routeNotFound()
		return err
	}

	if nodeID == r.self {
		r.metrics.routeLocal()
		msg.TargetNodeID = r.self
		if d := r.delivery(); d != nil {
			derr := d.DeliverToParticipant(ctx, msg)
			if isNotFound(derr) {
				// Cache said local but the participant is gone — drop it and try
				// once more via a fresh lookup (stale location).
				r.cache.invalidate(participantID)
				return r.routeAfterRefresh(ctx, participantID, msg)
			}
			return derr
		}
		return &Error{Code: CodeParticipantNotFound, Message: "no local delivery configured"}
	}

	r.metrics.routeRemote()
	sendErr := r.transport.Send(ctx, nodeID, msg)
	if isNotFound(sendErr) {
		// The remote node no longer has this participant. Invalidate and retry
		// via a fresh lookup exactly once.
		r.cache.invalidate(participantID)
		return r.routeAfterRefresh(ctx, participantID, msg)
	}
	return sendErr
}

// routeAfterRefresh re-resolves from the authoritative locator (bypassing the
// cache) and routes once more. It never recurses further, so a genuinely
// missing participant returns PARTICIPANT_NOT_FOUND rather than looping.
func (r *participantRouter) routeAfterRefresh(ctx context.Context, participantID string, msg ClusterMessage) error {
	loc, ok := r.locator.Lookup(participantID)
	if !ok {
		r.metrics.routeNotFound()
		return &Error{Code: CodeParticipantNotFound, Message: "participant not found", ParticipantID: participantID}
	}
	if r.isStale(loc.NodeID) {
		if r.metrics != nil {
			r.metrics.routingStaleNode()
		}
		return &Error{Code: CodeParticipantStale, Message: "participant's node is stale", ParticipantID: participantID, NodeID: loc.NodeID}
	}
	r.cache.put(participantID, loc.NodeID)
	if loc.NodeID == r.self {
		r.metrics.routeLocal()
		msg.TargetNodeID = r.self
		if d := r.delivery(); d != nil {
			return d.DeliverToParticipant(ctx, msg)
		}
		return &Error{Code: CodeParticipantNotFound, Message: "no local delivery configured"}
	}
	r.metrics.routeRemote()
	return r.transport.Send(ctx, loc.NodeID, msg)
}

func (r *participantRouter) resolve(participantID string) (string, *Error) {
	// 1. cache. 2. if it points at a stale node, drop it.
	if n, ok := r.cache.get(participantID); ok {
		if !r.isStale(n) {
			return n, nil
		}
		r.cache.invalidate(participantID)
	}
	if r.stateHealthy != nil && !r.stateHealthy() {
		return "", &Error{Code: CodeClusterStateUnavailable, Message: "cannot resolve participant location"}
	}
	// 4. authoritative lookup.
	loc, ok := r.locator.Lookup(participantID)
	if !ok {
		return "", &Error{Code: CodeParticipantNotFound, Message: "participant not found"}
	}
	// 5. still stale → refuse, never send to a dead node.
	if r.isStale(loc.NodeID) {
		if r.metrics != nil {
			r.metrics.routingStaleNode()
		}
		return "", &Error{Code: CodeParticipantStale, Message: "participant's node is stale", ParticipantID: participantID, NodeID: loc.NodeID}
	}
	r.cache.put(participantID, loc.NodeID)
	return loc.NodeID, nil
}

func (r *participantRouter) isStale(nodeID string) bool {
	return nodeID != r.self && r.nodeStale != nil && r.nodeStale(nodeID)
}

func isNotFound(err error) bool {
	ce, ok := err.(*Error)
	return ok && ce.Code == CodeParticipantNotFound
}
