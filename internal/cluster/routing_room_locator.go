package cluster

import "sync"

// RoomLocator answers "where does this room live?". The rest
// of the system asks it and never needs to know whether the answer comes from
// memory, Redis or a database.
type RoomLocator interface {
	// GetOwner returns the node id that owns roomID, if any.
	GetOwner(roomID string) (string, bool)
	// ClaimOwner atomically assigns roomID to nodeID unless it is already
	// owned. It returns the effective owner and whether THIS call was the one
	// that claimed it. Claiming a room already owned by nodeID is idempotent
	// (claimed=false, owner=nodeID). Claiming a room owned by someone
	// else returns that owner (claimed=false) — the caller must not proceed.
	ClaimOwner(roomID, nodeID string) (owner string, claimed bool)
	// SetOwner force-assigns ownership (used when reconciling from a peer).
	SetOwner(roomID, nodeID string)
	// RemoveOwner unconditionally releases a room (force; reconciliation only).
	RemoveOwner(roomID string)
	// ReleaseOwner atomically releases roomID only if nodeID is still its owner
	// (compare-and-delete). Returns true when this call did the release.
	// This is the safe path the join/leave lifecycle uses.
	ReleaseOwner(roomID, nodeID string) (released bool)
	// OwnedBy returns every room owned by nodeID.
	OwnedBy(nodeID string) []string
	// All returns the full roomID→nodeID map (snapshot).
	All() map[string]string
}

// RecoverOutcome is the result of a RecoverableRooms.RecoverOwner call.
type RecoverOutcome int

const (
	// RecoverFailed: the backend was unavailable or the room does not exist —
	// ownership was NOT changed (fail-closed).
	RecoverFailed RecoverOutcome = iota
	// RecoverConflict: the room's current owner is neither oldOwner nor newOwner,
	// so someone else won the race — ownership was NOT changed.
	RecoverConflict
	// RecoverAlready: newOwner already owns the room. Idempotent no-op; the
	// generation is left untouched.
	RecoverAlready
	// RecoverOK: ownership moved oldOwner → newOwner and the generation was
	// incremented.
	RecoverOK
)

func (o RecoverOutcome) String() string {
	switch o {
	case RecoverOK:
		return "recovered"
	case RecoverAlready:
		return "already_recovered"
	case RecoverConflict:
		return "conflict"
	default:
		return "failed"
	}
}

// RecoverResult carries the outcome plus the room's owner and generation after
// the attempt.
type RecoverResult struct {
	Outcome    RecoverOutcome
	Owner      string
	Generation int64
}

// RecoverableRooms is the extension a RoomLocator may also implement:
// an ownership generation and an atomic compare-and-recover.
// The RecoveryManager type-asserts for it; a locator without it cannot host
// failure recovery.
type RecoverableRooms interface {
	// Ownership returns the current owner and its generation (0 when the room has
	// never been recovered).
	Ownership(roomID string) (owner string, generation int64, ok bool)
	// RecoverOwner atomically moves ownership of roomID from oldOwner to newOwner
	// and bumps the generation — but only if the room is still owned by oldOwner.
	// It never does a separate GET then SET.
	RecoverOwner(roomID, oldOwner, newOwner string) RecoverResult
}

// InMemoryRoomLocator is the locator. ClaimOwner is atomic under a
// single mutex, which is what makes the "100 goroutines claim the same room →
// exactly one owner" guarantee hold WITHIN a node. Across nodes the
// guarantee is best-effort (peer query then claim); a truly atomic
// cross-node claim needs the Redis backend.
type InMemoryRoomLocator struct {
	mu     sync.RWMutex
	owners map[string]string // roomID -> nodeID
	gens   map[string]int64  // roomID -> ownership generation

	// onClaim / onConflict are optional metric hooks.
	onClaim    func()
	onConflict func()
}

// NewInMemoryRoomLocator builds an empty locator.
func NewInMemoryRoomLocator() *InMemoryRoomLocator {
	return &InMemoryRoomLocator{owners: make(map[string]string), gens: make(map[string]int64)}
}

// Ownership implements RecoverableRooms.
func (l *InMemoryRoomLocator) Ownership(roomID string) (string, int64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	owner, ok := l.owners[roomID]
	if !ok {
		return "", 0, false
	}
	return owner, l.gens[roomID], true
}

// RecoverOwner implements RecoverableRooms. The whole check-and-swap runs under
// the single write lock, so concurrent recoveries of the same room converge on
// exactly one winner and the generation moves at most once.
func (l *InMemoryRoomLocator) RecoverOwner(roomID, oldOwner, newOwner string) RecoverResult {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur, ok := l.owners[roomID]
	if !ok {
		return RecoverResult{Outcome: RecoverFailed}
	}
	if cur == newOwner {
		return RecoverResult{Outcome: RecoverAlready, Owner: cur, Generation: l.gens[roomID]}
	}
	if cur != oldOwner {
		if l.onConflict != nil {
			l.onConflict()
		}
		return RecoverResult{Outcome: RecoverConflict, Owner: cur, Generation: l.gens[roomID]}
	}
	l.owners[roomID] = newOwner
	l.gens[roomID]++
	return RecoverResult{Outcome: RecoverOK, Owner: newOwner, Generation: l.gens[roomID]}
}

func (l *InMemoryRoomLocator) GetOwner(roomID string) (string, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	n, ok := l.owners[roomID]
	return n, ok
}

func (l *InMemoryRoomLocator) ClaimOwner(roomID, nodeID string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cur, ok := l.owners[roomID]; ok {
		if cur != nodeID && l.onConflict != nil {
			l.onConflict()
		}
		return cur, false
	}
	l.owners[roomID] = nodeID
	if l.onClaim != nil {
		l.onClaim()
	}
	return nodeID, true
}

func (l *InMemoryRoomLocator) SetOwner(roomID, nodeID string) {
	l.mu.Lock()
	l.owners[roomID] = nodeID
	l.mu.Unlock()
}

func (l *InMemoryRoomLocator) RemoveOwner(roomID string) {
	l.mu.Lock()
	delete(l.owners, roomID)
	delete(l.gens, roomID)
	l.mu.Unlock()
}

func (l *InMemoryRoomLocator) ReleaseOwner(roomID, nodeID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cur, ok := l.owners[roomID]; ok && cur == nodeID {
		delete(l.owners, roomID)
		delete(l.gens, roomID)
		return true
	}
	return false
}

func (l *InMemoryRoomLocator) OwnedBy(nodeID string) []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var out []string
	for room, owner := range l.owners {
		if owner == nodeID {
			out = append(out, room)
		}
	}
	return out
}

func (l *InMemoryRoomLocator) All() map[string]string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make(map[string]string, len(l.owners))
	for k, v := range l.owners {
		out[k] = v
	}
	return out
}
