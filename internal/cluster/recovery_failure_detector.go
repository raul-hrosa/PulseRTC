package cluster

import (
	"context"
	"sync"
	"time"
)

// FailureDetector watches the nodes known to the cluster and decides which ones
// have failed. It only detects — it never claims a room. That is
// the RecoveryManager's job.
//
// Detection is fail-closed: while the authoritative state backend (Redis) is
// unreachable the detector reports UNKNOWN and marks nothing stale, so a Redis
// blip can never trigger a takeover.
type FailureDetector interface {
	Start(ctx context.Context) error
	Stop() error

	// IsHealthy reports whether nodeID is known and sending heartbeats.
	IsHealthy(nodeID string) bool
	// IsStale reports whether nodeID missed its heartbeat window past the grace
	// period, or has vanished from the registry entirely.
	IsStale(nodeID string) bool
	// Failures is the current set of stale nodes (for /internal/cluster/failures).
	Failures() []NodeFailure
	// BackendKnown reports whether the last scan could trust the state backend.
	BackendKnown() bool
}

// NodeFailure describes one node the detector currently considers failed.
type NodeFailure struct {
	NodeID        string    `json:"nodeId"`
	State         string    `json:"state"`
	LastHeartbeat time.Time `json:"lastHeartbeat"`
	AgeSeconds    float64   `json:"ageSeconds"`
}

type failureDetector struct {
	registry      NodeRegistry
	self          func() string
	backendOK     func() bool
	checkInterval time.Duration
	grace         time.Duration
	metrics       *Metrics
	logger        logger
	onStale       func(nodeID string) // fired once per transition healthy→stale

	now func() time.Time

	mu          sync.RWMutex
	firstMissed map[string]time.Time // node -> when we first saw it miss
	stale       map[string]NodeFailure
	backendKnwn bool

	stop     chan struct{}
	stopOnce sync.Once
	started  bool
}

func newFailureDetector(reg NodeRegistry, self func() string, backendOK func() bool,
	checkInterval, grace time.Duration, m *Metrics, lg logger, onStale func(string),
) *failureDetector {
	if checkInterval <= 0 {
		checkInterval = 5 * time.Second
	}
	if grace < 0 {
		grace = 0
	}
	return &failureDetector{
		registry: reg, self: self, backendOK: backendOK,
		checkInterval: checkInterval, grace: grace, metrics: m, logger: lg, onStale: onStale,
		now:         time.Now,
		firstMissed: map[string]time.Time{},
		stale:       map[string]NodeFailure{},
		backendKnwn: true,
		stop:        make(chan struct{}),
	}
}

func (d *failureDetector) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return nil
	}
	d.started = true
	d.mu.Unlock()

	go func() {
		t := time.NewTicker(d.checkInterval)
		defer t.Stop()
		d.scan()
		for {
			select {
			case <-ctx.Done():
				return
			case <-d.stop:
				return
			case <-t.C:
				d.scan()
			}
		}
	}()
	return nil
}

func (d *failureDetector) Stop() error {
	d.stopOnce.Do(func() { close(d.stop) })
	return nil
}

// scan is one detection pass. It is O(nodes) and runs on the check interval —
// never per participant or per packet.
func (d *failureDetector) scan() {
	backendOK := d.backendOK == nil || d.backendOK()

	d.mu.Lock()
	d.backendKnwn = backendOK
	d.mu.Unlock()

	if !backendOK {
		// UNKNOWN: we cannot trust the registry, so we do not change our verdict
		// and we do not emit new failures. Existing stale marks are kept
		// but recovery is gated separately on the backend too.
		if d.logger != nil {
			d.logger.Warn("failure_detector_backend_unknown", "reason", "state backend unavailable")
		}
		return
	}

	nodes := d.registry.List()
	live := make(map[string]NodeInfo, len(nodes))
	for _, n := range nodes {
		live[n.ID] = n
	}

	self := ""
	if d.self != nil {
		self = d.self()
	}

	now := d.now()
	var newlyStale []NodeFailure

	d.mu.Lock()
	for _, n := range nodes {
		if n.ID == self {
			delete(d.firstMissed, n.ID)
			delete(d.stale, n.ID)
			continue
		}
		missing := d.registry.IsStale(n) || n.State == NodeOffline
		if !missing {
			delete(d.firstMissed, n.ID)
			if _, was := d.stale[n.ID]; was {
				delete(d.stale, n.ID)
				if d.logger != nil {
					d.logger.Info("node_recovered_heartbeat", "nodeId", n.ID)
				}
			}
			continue
		}
		if _, seen := d.firstMissed[n.ID]; !seen {
			d.firstMissed[n.ID] = now
		}
		if now.Sub(d.firstMissed[n.ID]) < d.grace {
			continue // still inside the grace window
		}
		if _, already := d.stale[n.ID]; already {
			continue
		}
		f := NodeFailure{
			NodeID:        n.ID,
			State:         string(n.State),
			LastHeartbeat: n.LastSeen,
			AgeSeconds:    now.Sub(n.LastSeen).Seconds(),
		}
		d.stale[n.ID] = f
		newlyStale = append(newlyStale, f)
	}
	d.mu.Unlock()

	for _, f := range newlyStale {
		d.metrics.nodeDetectedStale()
		if d.logger != nil {
			d.logger.Warn("node became stale",
				"nodeId", f.NodeID, "lastHeartbeat", f.LastHeartbeat, "age", f.AgeSeconds)
		}
		if d.onStale != nil {
			d.onStale(f.NodeID)
		}
	}
}

func (d *failureDetector) IsHealthy(nodeID string) bool {
	n, ok := d.registry.Get(nodeID)
	if !ok {
		return false
	}
	if d.registry.IsStale(n) || n.State == NodeOffline {
		return false
	}
	d.mu.RLock()
	_, stale := d.stale[nodeID]
	d.mu.RUnlock()
	return !stale
}

func (d *failureDetector) IsStale(nodeID string) bool {
	d.mu.RLock()
	_, stale := d.stale[nodeID]
	d.mu.RUnlock()
	if stale {
		return true
	}
	// A node that vanished from the registry entirely, or explicitly went
	// OFFLINE, is stale even though a scan never got to "mark" it. A node
	// that is merely late is left to the grace-period logic in scan.
	n, ok := d.registry.Get(nodeID)
	if !ok {
		return true
	}
	return n.State == NodeOffline
}

func (d *failureDetector) Failures() []NodeFailure {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]NodeFailure, 0, len(d.stale))
	for _, f := range d.stale {
		out = append(out, f)
	}
	return out
}

func (d *failureDetector) BackendKnown() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.backendKnwn
}

var _ FailureDetector = (*failureDetector)(nil)
