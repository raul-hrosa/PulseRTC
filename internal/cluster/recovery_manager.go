package cluster

import (
	"context"
	"sync"
	"sync/atomic"
)

// RecoveryManager takes over rooms whose owning node has failed.
// It never invents a balancing policy of its own — it asks the RoomRouter which
// node should host each orphaned room — and it never mutates ownership
// except through the locator's atomic compare-and-recover.
//
// Every entry point is fail-closed: with the authoritative state backend down,
// nothing is recovered.
type RecoveryManager interface {
	// RecoverNode recovers every room currently owned by nodeID, provided nodeID
	// is genuinely stale. Bounded by RecoveryConcurrency.
	RecoverNode(ctx context.Context, nodeID string) error
	// RecoverRoom recovers a single room. Idempotent: calling it again after a
	// successful recovery returns ALREADY_RECOVERED and does not bump the
	// generation.
	RecoverRoom(ctx context.Context, roomID string) error
}

type recoveryManager struct {
	self        func() NodeInfo
	rooms       RecoverableRooms
	roomsAll    func() map[string]string
	router      RoomRouter
	detector    FailureDetector
	participant ParticipantLocator
	backendOK   func() bool
	load        LoadStore
	enabled     bool
	concurrency int
	metrics     *Metrics
	logger      logger

	inProgress atomic.Int64
	mu         sync.Mutex
	active     map[string]bool // rooms being recovered right now (dedupe)
}

func newRecoveryManager(self func() NodeInfo, rooms RecoverableRooms, roomsAll func() map[string]string,
	router RoomRouter, detector FailureDetector, participant ParticipantLocator, load LoadStore,
	backendOK func() bool, enabled bool, concurrency int, m *Metrics, lg logger,
) *recoveryManager {
	if concurrency <= 0 {
		concurrency = 4
	}
	return &recoveryManager{
		self: self, rooms: rooms, roomsAll: roomsAll, router: router, detector: detector,
		participant: participant, load: load, backendOK: backendOK, enabled: enabled,
		concurrency: concurrency, metrics: m, logger: lg, active: map[string]bool{},
	}
}

func (r *recoveryManager) stateUsable() bool {
	if r.backendOK == nil {
		return true
	}
	return r.backendOK()
}

// RecoverNode fans out over the stale node's rooms with a bounded worker pool.
func (r *recoveryManager) RecoverNode(ctx context.Context, nodeID string) error {
	if !r.enabled {
		return &Error{Code: CodeRecoveryDisabled, Message: "room recovery is disabled"}
	}
	if !r.stateUsable() {
		return &Error{Code: CodeClusterStateUnavailable, Message: "cannot recover: state backend unavailable", NodeID: nodeID}
	}
	if !r.detector.IsStale(nodeID) {
		// The node looks alive — nothing to do (no silent takeover).
		return nil
	}
	self := r.self().ID
	if nodeID == self {
		return nil
	}

	var orphaned []string
	for room, owner := range r.roomsAll() {
		if owner == nodeID {
			orphaned = append(orphaned, room)
		}
	}

	r.metrics.recoveryBegan()
	r.metrics.nodeDetectedStale()
	if r.logger != nil {
		r.logger.Info("node recovery started", "nodeId", nodeID, "rooms", len(orphaned), "newOwnerCandidate", self)
	}

	sem := make(chan struct{}, r.concurrency)
	var wg sync.WaitGroup
	var failed atomic.Int64
	for _, room := range orphaned {
		room := room
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := r.RecoverRoom(ctx, room); err != nil {
				if ce, ok := err.(*Error); !ok || (ce.Code != CodeAlreadyRecovered && ce.Code != CodeOwnershipConflict) {
					failed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if failed.Load() > 0 {
		r.metrics.recoveryFailed()
		r.metrics.nodeRecoveryFailed()
		if r.logger != nil {
			r.logger.Warn("node recovery incomplete", "nodeId", nodeID, "failed", failed.Load())
		}
		return &Error{Code: CodeClusterStateUnavailable, Message: "some rooms could not be recovered", NodeID: nodeID}
	}
	r.metrics.recoveryDone()
	r.metrics.nodeRecovered()
	// The failed node no longer participates in the scheduler.
	if r.load != nil {
		_ = r.load.DeleteLoad(nodeID)
	}
	if r.logger != nil {
		r.logger.Info("node ownership recovered", "nodeId", nodeID, "rooms", len(orphaned))
	}
	return nil
}

func (r *recoveryManager) RecoverRoom(ctx context.Context, roomID string) error {
	if !r.enabled {
		return &Error{Code: CodeRecoveryDisabled, Message: "room recovery is disabled"}
	}
	if !r.stateUsable() {
		return &Error{Code: CodeClusterStateUnavailable, Message: "cannot recover: state backend unavailable", RoomID: roomID}
	}

	// One recovery per room at a time on this node.
	r.mu.Lock()
	if r.active[roomID] {
		r.mu.Unlock()
		return &Error{Code: CodeOwnershipConflict, Message: "recovery already in progress", RoomID: roomID}
	}
	r.active[roomID] = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.active, roomID)
		r.mu.Unlock()
	}()

	r.inProgress.Add(1)
	defer r.inProgress.Add(-1)
	r.metrics.recoveryRoom()

	oldOwner, gen, ok := r.rooms.Ownership(roomID)
	if !ok {
		r.metrics.recoveryRoomSkipped()
		return nil // room no longer exists
	}
	self := r.self()
	if oldOwner == self.ID {
		r.metrics.recoveryRoomSkipped()
		return &Error{Code: CodeAlreadyRecovered, Message: "room already owned locally", RoomID: roomID}
	}
	if !r.detector.IsStale(oldOwner) {
		// Owner is healthy — never take a room from a live node.
		r.metrics.recoveryRoomSkipped()
		return nil
	}

	// Ask the shared routing policy which node should host it. A
	// deterministic policy means every node agrees, so only the chosen one
	// proceeds; the others defer.
	target, reason, rerr := r.router.SelectNode(ctx, roomID)
	if rerr != nil {
		r.metrics.recoveryFailed()
		if ce, ok := rerr.(*Error); ok {
			ce.RoomID = roomID
			return ce
		}
		return &Error{Code: CodeNoCapacity, Message: rerr.Error(), RoomID: roomID}
	}
	if target.ID != "" && target.ID != self.ID {
		// Another node is the elected new owner; it will run its own recovery.
		r.metrics.recoveryRoomSkipped()
		if r.logger != nil {
			r.logger.Info("room recovery deferred", "roomId", roomID, "electedNode", target.ID, "reason", reason)
		}
		return nil
	}

	if r.logger != nil {
		r.logger.Info("room recovery started",
			"roomId", roomID, "oldOwner", oldOwner, "newOwner", self.ID, "generation", gen)
	}

	res := r.rooms.RecoverOwner(roomID, oldOwner, self.ID)
	switch res.Outcome {
	case RecoverOK:
		r.metrics.recoveryRoomRecov()
		r.cleanupParticipants(roomID, oldOwner)
		if r.logger != nil {
			r.logger.Info("room ownership recovered",
				"roomId", roomID, "oldOwner", oldOwner, "newOwner", self.ID, "generation", res.Generation)
		}
		return nil
	case RecoverAlready:
		r.metrics.recoveryRoomSkipped()
		return &Error{Code: CodeAlreadyRecovered, Message: "room already recovered", RoomID: roomID, NodeID: self.ID}
	case RecoverConflict:
		r.metrics.recoveryConflict()
		if r.logger != nil {
			r.logger.Warn("room recovery conflict",
				"roomId", roomID, "oldOwner", oldOwner, "currentOwner", res.Owner)
		}
		return &Error{Code: CodeOwnershipConflict, Message: "another node won recovery", RoomID: roomID, NodeID: res.Owner}
	default:
		r.metrics.recoveryFailed()
		return &Error{Code: CodeClusterStateUnavailable, Message: "recovery compare-and-swap failed", RoomID: roomID}
	}
}

// cleanupParticipants expires the location records that pointed at the dead node.
// The WebRTC sessions are gone; a reconnecting client creates a fresh
// participant. Best-effort — never blocks recovery.
func (r *recoveryManager) cleanupParticipants(roomID, deadNode string) {
	if r.participant == nil {
		return
	}
	for _, loc := range r.participant.InRoom(roomID) {
		if loc.NodeID == deadNode {
			r.participant.Remove(loc.ParticipantID)
		}
	}
}

// InProgress is the current number of rooms being recovered (for the
// /internal/cluster/recovery endpoint).
func (r *recoveryManager) InProgress() int64 { return r.inProgress.Load() }

var _ RecoveryManager = (*recoveryManager)(nil)
