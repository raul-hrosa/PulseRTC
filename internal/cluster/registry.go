package cluster

import (
	"sort"
	"sync"
	"time"
)

// NodeRegistry tracks the nodes known to the cluster.
// All operations are idempotent: registering the same node twice yields
// one entry.
type NodeRegistry interface {
	// Register inserts or updates a node. Registering an id that already exists
	// updates its metadata and refreshes LastSeen.
	Register(node NodeInfo) error
	// Unregister removes a node. Removing an unknown id is a no-op.
	Unregister(nodeID string)
	// Heartbeat refreshes a node's LastSeen. Returns false if the node is
	// unknown (it should Register first).
	Heartbeat(nodeID string) bool
	// Get returns a node by id.
	Get(nodeID string) (NodeInfo, bool)
	// List returns every known node, ordered by id.
	List() []NodeInfo
	// IsStale reports whether node missed its heartbeat window. Detection
	// only — no failover.
	IsStale(node NodeInfo) bool
	// Counts returns (total, active, stale) for /metrics.
	Counts() (total, active, stale int)
}

// InMemoryNodeRegistry is the registry: a mutex-guarded map. A Redis
// implementation with TTL keys replaces it.
type InMemoryNodeRegistry struct {
	staleAfter time.Duration
	now        func() time.Time

	mu    sync.RWMutex
	nodes map[string]NodeInfo
}

// NewInMemoryNodeRegistry builds a registry. A node whose LastSeen is older
// than staleAfter is reported as stale by Stale(); 0 disables staleness.
func NewInMemoryNodeRegistry(staleAfter time.Duration) *InMemoryNodeRegistry {
	return &InMemoryNodeRegistry{
		staleAfter: staleAfter,
		now:        time.Now,
		nodes:      make(map[string]NodeInfo),
	}
}

func (r *InMemoryNodeRegistry) Register(node NodeInfo) error {
	if node.ID == "" {
		return &Error{Code: CodeBadRequest, Message: "node id is required"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, ok := r.nodes[node.ID]
	if ok && node.StartedAt.IsZero() {
		node.StartedAt = existing.StartedAt
	}
	node.LastSeen = r.now()
	r.nodes[node.ID] = node
	return nil
}

func (r *InMemoryNodeRegistry) Unregister(nodeID string) {
	r.mu.Lock()
	delete(r.nodes, nodeID)
	r.mu.Unlock()
}

func (r *InMemoryNodeRegistry) Heartbeat(nodeID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.nodes[nodeID]
	if !ok {
		return false
	}
	node.LastSeen = r.now()
	r.nodes[nodeID] = node
	return true
}

func (r *InMemoryNodeRegistry) Get(nodeID string) (NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[nodeID]
	return n, ok
}

func (r *InMemoryNodeRegistry) List() []NodeInfo {
	r.mu.RLock()
	out := make([]NodeInfo, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, n)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// IsStale reports whether a node has not sent a heartbeat within staleAfter.
// only detects staleness — it does NOT trigger failover.
func (r *InMemoryNodeRegistry) IsStale(node NodeInfo) bool {
	if r.staleAfter <= 0 {
		return false
	}
	return r.now().Sub(node.LastSeen) > r.staleAfter
}

// Counts returns (total, active, stale) for metrics.
func (r *InMemoryNodeRegistry) Counts() (total, active, stale int) {
	for _, n := range r.List() {
		total++
		if r.IsStale(n) {
			stale++
		} else if n.State.AcceptsSessions() {
			active++
		}
	}
	return
}
