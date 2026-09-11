package cluster

import (
	"context"
	"time"
)

// ClusterState is the coordination store the rest of the server talks to. It
// bundles the three locators plus a Backend health probe.
// ships two implementations behind this one contract:
//
//	InMemoryClusterState - development, tests, benchmarks, single node
//	RedisClusterState    - shared state across many nodes
//
// RTP, PeerConnections, tracks and SDP never touch a ClusterState — only
// coordination metadata does.
type ClusterState interface {
	Nodes() NodeRegistry
	Rooms() RoomLocator
	Participants() ParticipantLocator
	// Load is the node-load store (periodic reports with TTL).
	Load() LoadStore
	Backend
}

// Backend is the connection-health half of a ClusterState.
type Backend interface {
	// Kind is a short label for /metrics and logs ("inmemory", "redis").
	Kind() string
	// Ping checks the backend is reachable right now.
	Ping(ctx context.Context) error
	// Healthy reports the last known reachability without doing I/O. It is what
	// /ready and the fail-closed join path consult.
	Healthy() bool
	// Close releases the backend (connection pool, goroutines).
	Close() error
}

// InMemoryClusterState is the process-local implementation. It is always
// healthy and Ping never fails — single-node behavior is unchanged.
type InMemoryClusterState struct {
	nodes        *InMemoryNodeRegistry
	rooms        *InMemoryRoomLocator
	participants *InMemoryParticipantLocator
	load         *InMemoryLoadStore
}

// NewInMemoryClusterState builds the in-memory store. staleAfter is the node
// TTL used for staleness detection.
func NewInMemoryClusterState(staleAfter time.Duration) *InMemoryClusterState {
	return &InMemoryClusterState{
		nodes:        NewInMemoryNodeRegistry(staleAfter),
		rooms:        NewInMemoryRoomLocator(),
		participants: NewInMemoryParticipantLocator(),
		load:         NewInMemoryLoadStore(),
	}
}

func (s *InMemoryClusterState) Nodes() NodeRegistry              { return s.nodes }
func (s *InMemoryClusterState) Rooms() RoomLocator               { return s.rooms }
func (s *InMemoryClusterState) Participants() ParticipantLocator { return s.participants }
func (s *InMemoryClusterState) Load() LoadStore                  { return s.load }
func (s *InMemoryClusterState) Kind() string                     { return "inmemory" }
func (s *InMemoryClusterState) Ping(context.Context) error       { return nil }
func (s *InMemoryClusterState) Healthy() bool                    { return true }
func (s *InMemoryClusterState) Close() error                     { return nil }
