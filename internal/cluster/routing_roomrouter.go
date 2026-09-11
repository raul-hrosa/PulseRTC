package cluster

import (
	"context"
	"sort"
	"sync"
	"time"
)

// RoomRouter decides which node a NEW room should be created on.
// It never touches an existing room — that is RoomLocator's job — and
// it is never on the RTP path.
type RoomRouter interface {
	SelectNode(ctx context.Context, roomID string) (NodeInfo, string, error)
}

// RoomSelectionPolicy is the pure scoring algorithm, split from the router so
// LeastLoaded / Weighted / CapacityAware / LatencyAware can be swapped without
// touching discovery or claiming.
type RoomSelectionPolicy interface {
	// Select picks one candidate. candidates is already filtered to healthy,
	// non-draining, within-capacity nodes. It must be deterministic.
	Select(candidates []NodeLoad) (NodeLoad, string, error)
}

// LoadWeights turns raw counts into a single score. Kept simple on purpose.
type LoadWeights struct {
	Participant  float64
	Publication  float64
	Subscription float64
	Room         float64
}

func defaultLoadWeights() LoadWeights {
	return LoadWeights{Participant: 1, Publication: 0.5, Subscription: 0.25, Room: 2}
}

func (w LoadWeights) score(l NodeLoad) float64 {
	return float64(l.Participants)*w.Participant +
		float64(l.Publications)*w.Publication +
		float64(l.Subscriptions)*w.Subscription +
		float64(l.Rooms)*w.Room
}

// LeastLoadedPolicy picks the candidate with the lowest weighted score,
// breaking ties by node id so the choice is reproducible across nodes.
type LeastLoadedPolicy struct {
	Weights LoadWeights
	// PreferLocalMargin: if the local node's score is within this many points of
	// the best score, keep the room local. This preserves single-node / small
	// cluster behavior and avoids needless redirects.
	PreferLocalMargin float64
	LocalNodeID       string
}

func (p LeastLoadedPolicy) Select(candidates []NodeLoad) (NodeLoad, string, error) {
	if len(candidates) == 0 {
		return NodeLoad{}, "", &Error{Code: CodeNoCapacity, Message: "no node is available to host a new room"}
	}
	w := p.Weights
	if (w == LoadWeights{}) {
		w = defaultLoadWeights()
	}
	sorted := append([]NodeLoad(nil), candidates...)
	sort.Slice(sorted, func(i, j int) bool {
		si, sj := w.score(sorted[i]), w.score(sorted[j])
		if si != sj {
			return si < sj
		}
		return sorted[i].NodeID < sorted[j].NodeID
	})
	best := sorted[0]

	if p.LocalNodeID != "" {
		for _, c := range sorted {
			if c.NodeID == p.LocalNodeID {
				if w.score(c) <= w.score(best)+p.PreferLocalMargin {
					return c, "local", nil
				}
				break
			}
		}
	}
	return best, "least_loaded", nil
}

// --------------------------------------------------------------------------
// roomRouter
// --------------------------------------------------------------------------

type roomRouter struct {
	self      func() NodeInfo
	registry  NodeRegistry
	load      LoadStore
	rooms     RoomLocator
	policy    RoomSelectionPolicy
	cap       CapacityConfig
	metrics   *Metrics
	logger    logger
	stateOK   func() bool
	loadStale time.Duration

	// pending counts rooms this node has selected since the last observed load
	// snapshot for a node, so a burst of joins in one report interval still
	// spreads (join storm). Decays as snapshots refresh.
	mu      sync.Mutex
	pending map[string]pendingCount
}

type pendingCount struct {
	n    int
	seen time.Time
}

func newRoomRouter(self func() NodeInfo, reg NodeRegistry, load LoadStore, rooms RoomLocator,
	policy RoomSelectionPolicy, capCfg CapacityConfig, m *Metrics, lg logger, stateOK func() bool,
	loadStale time.Duration,
) *roomRouter {
	if loadStale <= 0 {
		loadStale = 10 * time.Second
	}
	return &roomRouter{
		self: self, registry: reg, load: load, rooms: rooms, policy: policy,
		cap: capCfg, metrics: m, logger: lg, stateOK: stateOK, loadStale: loadStale,
		pending: map[string]pendingCount{},
	}
}

func (r *roomRouter) SelectNode(_ context.Context, roomID string) (NodeInfo, string, error) {
	if r.stateOK != nil && !r.stateOK() {
		r.metrics.roomSelectionFailed()
		return NodeInfo{}, "", &Error{Code: CodeClusterStateUnavailable, Message: "cannot route: cluster state unavailable", RoomID: roomID}
	}

	nodes := r.registry.List()
	snap, err := r.load.LoadSnapshot()
	if err != nil {
		r.metrics.roomSelectionFailed()
		return NodeInfo{}, "", &Error{Code: CodeClusterStateUnavailable, Message: "cannot route: load snapshot failed", RoomID: roomID}
	}
	byID := make(map[string]NodeLoad, len(snap))
	now := time.Now()
	for _, l := range snap {
		if now.Sub(l.ReportedAt) <= r.loadStale {
			byID[l.NodeID] = l
		} else {
			r.metrics.loadStale()
		}
	}

	self := r.self()

	candidates := make([]NodeLoad, 0, len(nodes))
	for _, n := range nodes {
		if !n.State.AcceptsSessions() { // filters STARTING / DRAINING / OFFLINE
			continue
		}
		if r.registry.IsStale(n) {
			continue
		}
		l, ok := byID[n.ID]
		if !ok {
			// No fresh load report yet — assume empty but still eligible.
			l = NodeLoad{NodeID: n.ID, State: n.State}
		}
		l = r.applyPending(l)
		if r.overHardLimit(l) { // hard limit / capacity
			continue
		}
		candidates = append(candidates, l)
	}

	if len(candidates) == 0 {
		r.metrics.roomSelectionFailed()
		return NodeInfo{}, "", &Error{Code: CodeNoCapacity, Message: "all nodes are at capacity or unavailable", RoomID: roomID}
	}

	// Honour an existing routing decision so a redirect never bounces between
	// nodes — but only if the assigned node is still a valid
	// candidate (READY, not stale, within capacity).
	if assigned, ok := r.load.RoomAssignment(roomID); ok {
		for _, c := range candidates {
			if c.NodeID == assigned {
				r.metrics.roomSelected("assigned")
				info, _ := r.registry.Get(assigned)
				if info.ID == "" {
					info = NodeInfo{ID: assigned}
				}
				return info, "assigned", nil
			}
		}
	}

	pol := r.policy
	if llp, ok := pol.(LeastLoadedPolicy); ok {
		llp.LocalNodeID = self.ID
		pol = llp
	}
	chosen, reason, perr := pol.Select(candidates)
	if perr != nil {
		r.metrics.roomSelectionFailed()
		return NodeInfo{}, "", perr
	}

	info, ok := r.registry.Get(chosen.NodeID)
	if !ok {
		info = NodeInfo{ID: chosen.NodeID}
	}
	r.notePending(chosen.NodeID)
	_ = r.load.PutRoomAssignment(roomID, chosen.NodeID, 15*time.Second)
	r.metrics.roomSelected(reason)
	r.logf("room_selected", "room", roomID, "selectedNode", chosen.NodeID, "reason", reason)
	return info, reason, nil
}

// overHardLimit / softLimited implement the capacity checks. A limit of 0
// means "no limit".
func (r *roomRouter) overHardLimit(l NodeLoad) bool {
	hard := r.cap.HardLimitFraction
	if hard <= 0 {
		hard = 1.0
	}
	return exceeds(l.Rooms, r.cap.MaxRooms, hard) ||
		exceeds(l.Participants, r.cap.MaxParticipants, hard) ||
		exceeds(l.Publications, r.cap.MaxPublications, hard)
}

func exceeds(current, max int, frac float64) bool {
	if max <= 0 {
		return false
	}
	return float64(current) >= float64(max)*frac
}

func (r *roomRouter) applyPending(l NodeLoad) NodeLoad {
	r.mu.Lock()
	pc, ok := r.pending[l.NodeID]
	r.mu.Unlock()
	if ok {
		l.Rooms += pc.n
		l.Participants += pc.n // rough: assume ~1 participant seeds a new room
	}
	return l
}

func (r *roomRouter) notePending(nodeID string) {
	r.mu.Lock()
	pc := r.pending[nodeID]
	// Reset the window if the last note is old (a fresh snapshot has surely
	// absorbed the earlier picks).
	if time.Since(pc.seen) > 5*time.Second {
		pc.n = 0
	}
	pc.n++
	pc.seen = time.Now()
	r.pending[nodeID] = pc
	r.mu.Unlock()
}

// clearPending is called after a successful local claim / observed snapshot so
// the local correction does not drift.
func (r *roomRouter) clearPending(nodeID string) {
	r.mu.Lock()
	delete(r.pending, nodeID)
	r.mu.Unlock()
}

func (r *roomRouter) logf(msg string, kv ...any) {
	if r.logger != nil {
		r.logger.Info(msg, kv...)
	}
}
