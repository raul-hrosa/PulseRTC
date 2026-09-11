package cluster

import (
	"sync"
	"time"
)

// NodeLoad is a node's self-reported load. It is written to
// the shared state at a low fixed cadence — never per RTP packet —
// and read as a best-effort snapshot by the RoomRouter.
type NodeLoad struct {
	NodeID        string    `json:"nodeId"`
	State         NodeState `json:"state"`
	Rooms         int       `json:"rooms"`
	Participants  int       `json:"participants"`
	Publications  int       `json:"publications"`
	Subscriptions int       `json:"subscriptions"`
	CPUPercent    float64   `json:"cpuPercent,omitempty"`
	MemoryBytes   uint64    `json:"memoryBytes,omitempty"`
	Goroutines    int       `json:"goroutines,omitempty"`
	ReportedAt    time.Time `json:"reportedAt"`
}

// LoadStore persists NodeLoad with a TTL and reads the cluster-wide snapshot.
// The in-memory implementation is process-local (single node); the Redis one is
// shared.
type LoadStore interface {
	PublishLoad(load NodeLoad, ttl time.Duration) error
	// LoadSnapshot returns every non-expired NodeLoad. A stale report (past its
	// TTL) must not appear.
	LoadSnapshot() ([]NodeLoad, error)
	DeleteLoad(nodeID string) error

	// PutRoomAssignment records "the router decided room R goes to node N" with a
	// short TTL, so a redirect does not bounce: every node's router honours an
	// existing assignment instead of re-deciding. ClaimOwner is still
	// the authority.
	PutRoomAssignment(roomID, nodeID string, ttl time.Duration) error
	RoomAssignment(roomID string) (string, bool)
}

// InMemoryLoadStore is the single-node LoadStore: a map with per-entry expiry.
type InMemoryLoadStore struct {
	mu     sync.RWMutex
	now    func() time.Time
	m      map[string]loadEntry
	assign map[string]assignEntry
}

type assignEntry struct {
	nodeID string
	exp    time.Time
}

type loadEntry struct {
	load NodeLoad
	exp  time.Time
}

func NewInMemoryLoadStore() *InMemoryLoadStore {
	return &InMemoryLoadStore{now: time.Now, m: map[string]loadEntry{}, assign: map[string]assignEntry{}}
}

func (s *InMemoryLoadStore) PutRoomAssignment(roomID, nodeID string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	s.mu.Lock()
	s.assign[roomID] = assignEntry{nodeID: nodeID, exp: s.now().Add(ttl)}
	s.mu.Unlock()
	return nil
}

func (s *InMemoryLoadStore) RoomAssignment(roomID string) (string, bool) {
	s.mu.RLock()
	e, ok := s.assign[roomID]
	s.mu.RUnlock()
	if !ok || s.now().After(e.exp) {
		return "", false
	}
	return e.nodeID, true
}

func (s *InMemoryLoadStore) PublishLoad(load NodeLoad, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 6 * time.Second
	}
	s.mu.Lock()
	s.m[load.NodeID] = loadEntry{load: load, exp: s.now().Add(ttl)}
	s.mu.Unlock()
	return nil
}

func (s *InMemoryLoadStore) LoadSnapshot() ([]NodeLoad, error) {
	now := s.now()
	s.mu.RLock()
	out := make([]NodeLoad, 0, len(s.m))
	for _, e := range s.m {
		if now.Before(e.exp) {
			out = append(out, e.load)
		}
	}
	s.mu.RUnlock()
	return out, nil
}

func (s *InMemoryLoadStore) DeleteLoad(nodeID string) error {
	s.mu.Lock()
	delete(s.m, nodeID)
	s.mu.Unlock()
	return nil
}

// --------------------------------------------------------------------------
// loadReporter
// --------------------------------------------------------------------------

// LoadProvider returns this node's current live counts. The signaling layer
// supplies it (it knows the rooms / SFU).
type LoadProvider func() NodeLoad

// loadReporter aggregates load locally and pushes it to the LoadStore on a
// fixed interval.
type loadReporter struct {
	store    LoadStore
	provider func() LoadProvider
	self     func() NodeInfo
	interval time.Duration
	ttl      time.Duration
	metrics  *Metrics
	logger   logger

	stop chan struct{}
	once sync.Once
}

func newLoadReporter(store LoadStore, interval time.Duration, m *Metrics, lg logger,
	provider func() LoadProvider, self func() NodeInfo,
) *loadReporter {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &loadReporter{
		store: store, provider: provider, self: self,
		interval: interval, ttl: interval * 3, metrics: m, logger: lg,
		stop: make(chan struct{}),
	}
}

func (r *loadReporter) run() {
	r.report() // publish immediately so the router has data at once
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			r.report()
		}
	}
}

func (r *loadReporter) report() {
	self := r.self()
	load := NodeLoad{NodeID: self.ID, State: self.State}
	if p := r.provider(); p != nil {
		load = p()
		load.NodeID = self.ID
		load.State = self.State
	}
	load.ReportedAt = time.Now()
	if err := r.store.PublishLoad(load, r.ttl); err != nil {
		if r.logger != nil {
			r.logger.Warn("load_report_failed", "node", self.ID, "err", err.Error())
		}
		return
	}
	r.metrics.loadReported()
}

func (r *loadReporter) Close() { r.once.Do(func() { close(r.stop) }) }
