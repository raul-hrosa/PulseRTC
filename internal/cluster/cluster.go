package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Cluster is the façade the rest of the server uses. When Config.Enabled is
// false it still works — every room resolves locally, no peers are queried, no
// /internal server is needed — so single-node behavior is byte-for-byte the
// earlier one.
type Cluster struct {
	cfg    Config
	logger *slog.Logger

	self         NodeInfo
	state        ClusterState
	registry     NodeRegistry
	rooms        RoomLocator
	participants ParticipantLocator
	transport    NodeTransport
	metrics      Metrics

	// Cross-node signaling.
	msgTransport MessageTransport
	router       *participantRouter
	msgHandler   *clusterMessageHandler
	dedup        *dedupCache
	locCache     *locationCache
	media        MediaBridge

	// Load balancing / room routing.
	roomRtr  *roomRouter
	loadRptr *loadReporter

	// Failure detection & recovery.
	failureDet *failureDetector
	recovery   *recoveryManager
	fdCtx      context.Context
	fdCancel   context.CancelFunc
	fdOnce     sync.Once

	mu           sync.RWMutex
	nodeState    NodeState
	delivery     LocalDelivery
	loadProvider LoadProvider
	stopHB       chan struct{}
	hbOnce       sync.Once
	stopOnce     sync.Once
	ddOnce       sync.Once
	lrOnce       sync.Once
}

func (c *Cluster) dedupOnce() {
	c.ddOnce.Do(func() { go c.dedup.run() })
}

func staleFor(cfg Config) time.Duration {
	iv := cfg.LoadReportInterval
	if iv <= 0 {
		iv = 2 * time.Second
	}
	return iv * 5
}

// SetLoadProvider registers the callback the load reporter uses to read this
// node's live counts (the signaling layer supplies it). Safe to call before or
// after Start.
func (c *Cluster) SetLoadProvider(p LoadProvider) {
	c.mu.Lock()
	c.loadProvider = p
	c.mu.Unlock()
}

// RoomRouter exposes the scheduler (tests / introspection).
func (c *Cluster) RoomRouter() RoomRouter { return c.roomRtr }

// LoadSnapshot returns the current cluster-wide load view.
func (c *Cluster) LoadSnapshot() ([]NodeLoad, error) { return c.state.Load().LoadSnapshot() }

// New builds a Cluster from cfg, choosing the coordination backend: a
// RedisClusterState when cfg.Redis.Enabled, otherwise the in-memory one.
// It does not start the heartbeat loop or register the node — call Start for
// that (main wires it; tests usually don't).
func New(cfg Config, logger *slog.Logger) (*Cluster, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	// cfg.Validate already rejects an enabled cluster with an empty secret; the
	// standalone secret check lives in NewWithState for its direct callers.
	var state ClusterState
	if cfg.Redis.Enabled {
		rs, err := newRedisClusterState(cfg, logger)
		if err != nil {
			return nil, err
		}
		state = rs
	}
	return NewWithState(cfg, logger, state)
}

// NewWithState builds a Cluster on a caller-supplied ClusterState. A nil state
// means "use the in-memory backend". Tests use this to inject miniredis.
func NewWithState(cfg Config, logger *slog.Logger, state ClusterState) (*Cluster, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Enabled && cfg.Secret == "" {
		return nil, errors.New("cluster: PULSERTC_CLUSTER_ENABLED=true but PULSERTC_CLUSTER_SECRET is empty")
	}
	if strings.TrimSpace(cfg.NodeID) == "" {
		cfg.NodeID = "node-" + uuid.NewString()[:8]
	}
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = 15 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 2 * time.Second
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 64 * 1024
	}
	if cfg.MessageMaxAge <= 0 {
		cfg.MessageMaxAge = 30 * time.Second
	}

	self := NodeInfo{
		ID:        cfg.NodeID,
		Host:      cfg.Host,
		Port:      cfg.Port,
		Version:   cfg.Version,
		State:     NodeStarting,
		StartedAt: time.Now(),
	}

	if state == nil {
		state = NewInMemoryClusterState(cfg.StaleAfter)
	}

	c := &Cluster{
		cfg:          cfg,
		logger:       logger,
		self:         self,
		state:        state,
		registry:     state.Nodes(),
		rooms:        state.Rooms(),
		participants: state.Participants(),
		transport:    NewHTTPTransport(cfg.Secret, 3*time.Second),
		nodeState:    NodeStarting,
		dedup:        newDedupCache(cfg.MessageMaxAge),
		locCache:     newLocationCache(2 * time.Second),
		stopHB:       make(chan struct{}),
	}
	if ht, ok := c.transport.(*HTTPTransport); ok {
		ht.onReq = c.metrics.internalReq
		ht.onFail = c.metrics.internalReqFail
	}
	if rs, ok := state.(*RedisClusterState); ok {
		rs.setErrorHook(c.metrics.redisError)
	}

	// Cross-node signaling wiring. The default transport is
	// HTTP; tests swap in an in-process one via UseInMemoryMessageTransport.
	c.msgTransport = newHTTPMessageTransport(c.transportDeps())
	c.router = &participantRouter{
		self:         cfg.NodeID,
		cache:        c.locCache,
		locator:      c.participants,
		transport:    c.msgTransport,
		delivery:     c.currentDelivery,
		metrics:      &c.metrics,
		stateHealthy: c.routingStateHealthy,
	}
	c.msgHandler = &clusterMessageHandler{
		self:     cfg.NodeID,
		maxAge:   cfg.MessageMaxAge,
		maxSize:  cfg.MaxMessageSize,
		dedup:    c.dedup,
		delivery: c.currentDelivery,
		metrics:  &c.metrics,
	}

	// Room routing + load reporting.
	policy := LeastLoadedPolicy{Weights: defaultLoadWeights(), PreferLocalMargin: 4}
	capCfg := cfg.Capacity
	if capCfg.HardLimitFraction <= 0 {
		capCfg.HardLimitFraction = 1.0
	}
	if capCfg.SoftLimitFraction <= 0 {
		capCfg.SoftLimitFraction = 0.8
	}
	c.roomRtr = newRoomRouter(c.currentSelf, c.registry, state.Load(), c.rooms, policy,
		capCfg, &c.metrics, logger, c.routingStateHealthy, staleFor(cfg))
	c.loadRptr = newLoadReporter(state.Load(), cfg.LoadReportInterval, &c.metrics, logger,
		func() LoadProvider { c.mu.RLock(); defer c.mu.RUnlock(); return c.loadProvider },
		c.currentSelf)

	// Failure detection + recovery. Constructed always (cheap); the
	// detection loop only runs for an enabled cluster (Start).
	c.failureDet = newFailureDetector(c.registry, c.NodeID, c.routingStateHealthy,
		cfg.FailureCheckInterval, cfg.FailureGracePeriod, &c.metrics, logger, c.onNodeStale)
	if rr, ok := c.rooms.(RecoverableRooms); ok {
		c.recovery = newRecoveryManager(c.currentSelf, rr, c.rooms.All, c.roomRtr, c.failureDet,
			c.participants, state.Load(), c.routingStateHealthy,
			!cfg.RecoveryDisabled, cfg.RecoveryConcurrency, &c.metrics, logger)
	}
	c.router.nodeStale = c.failureDet.IsStale
	c.fdCtx, c.fdCancel = context.WithCancel(context.Background())

	// Inter-SFU media transport.
	c.media = noopMediaBridge{}
	if cfg.Media.Enabled {
		mb, err := newMediaBridge(cfg, &c.metrics, c.registry, logger,
			func(ctx context.Context, target string, msg ClusterMessage) error {
				return c.msgTransport.Send(ctx, target, msg)
			},
			func(typ string, payload json.RawMessage) ClusterMessage {
				return c.NewClusterMessage(typ, "", "", payload)
			},
		)
		if err != nil {
			return nil, err
		}
		c.media = mb
		self.MediaAddr = mb.LocalMediaAddr()
		c.self.MediaAddr = mb.LocalMediaAddr()
		c.msgHandler.media = mb.handleControl
	}

	_ = c.registry.Register(self)
	return c, nil
}

// Media returns the cross-node media bridge (a no-op when disabled).
func (c *Cluster) Media() MediaBridge { return c.media }

// FailureDetector exposes the detector (tests / introspection).
func (c *Cluster) FailureDetector() FailureDetector { return c.failureDet }

// RecoveryManager exposes the recovery manager (nil when the room
// locator does not support recovery).
func (c *Cluster) RecoveryManager() RecoveryManager {
	if c.recovery == nil {
		return nil
	}
	return c.recovery
}

// onNodeStale is the FailureDetector→RecoveryManager bridge (trigger A).
func (c *Cluster) onNodeStale(nodeID string) {
	if c.recovery == nil || c.cfg.RecoveryDisabled {
		return
	}
	go func() {
		if err := c.recovery.RecoverNode(c.fdCtx, nodeID); err != nil {
			c.logger.Warn("node_recovery_failed", "nodeId", nodeID, "err", err.Error())
		}
	}()
}

// runStartupRecovery is trigger B: on becoming READY, sweep
// every room whose owner is already stale so recovery does not depend on this
// node having witnessed the failure live.
func (c *Cluster) runStartupRecovery() {
	if c.recovery == nil || c.cfg.RecoveryDisabled || !c.routingStateHealthy() {
		return
	}
	seen := map[string]bool{c.cfg.NodeID: true}
	for _, owner := range c.rooms.All() {
		if seen[owner] {
			continue
		}
		seen[owner] = true
		if c.failureDet.IsStale(owner) {
			if err := c.recovery.RecoverNode(c.fdCtx, owner); err != nil {
				c.logger.Warn("startup_recovery_failed", "nodeId", owner, "err", err.Error())
			}
		}
	}
}

func (c *Cluster) transportDeps() transportDeps {
	return transportDeps{
		self:       c.cfg.NodeID,
		secret:     c.cfg.Secret,
		timeout:    c.cfg.RequestTimeout,
		maxRetries: c.cfg.MaxRetries,
		maxSize:    c.cfg.MaxMessageSize,
		registry:   c.registry,
		metrics:    &c.metrics,
	}
}

// routingStateHealthy reports whether participant location can be trusted: with
// Redis it must be reachable; otherwise the in-memory state is always ok.
func (c *Cluster) routingStateHealthy() bool {
	if c.cfg.Redis.Enabled {
		return c.state.Healthy()
	}
	return true
}

func (c *Cluster) currentDelivery() LocalDelivery {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.delivery
}

// SetLocalDelivery registers the signaling layer as the sink for messages
// routed to this node. Must be called before Start in a clustered setup.
func (c *Cluster) SetLocalDelivery(d LocalDelivery) {
	c.mu.Lock()
	c.delivery = d
	c.mu.Unlock()
}

// Router exposes the participant router (signaling calls it to send cross-node).
func (c *Cluster) Router() ParticipantRouter { return c.router }

// UseInMemoryMessageTransport swaps the HTTP transport for an in-process one
// backed by a shared nodeID→*Cluster table (tests / same-process multi-node).
func (c *Cluster) UseInMemoryMessageTransport(nodes map[string]*Cluster) {
	mt := NewInMemoryMessageTransport(nodes)
	mt.deps = c.transportDeps()
	c.msgTransport = mt
	c.router.transport = mt
}

// handleInboundClusterMessage is the entry point the transport (and the HTTP
// endpoint) call on the receiving node.
func (c *Cluster) handleInboundClusterMessage(ctx context.Context, msg ClusterMessage) error {
	return c.msgHandler.Handle(ctx, msg)
}

// RouteMessage sends a ClusterMessage toward participantID — locally if it is
// here, otherwise across the transport to its node.
func (c *Cluster) RouteMessage(ctx context.Context, participantID string, msg ClusterMessage) error {
	if c.router == nil {
		return &Error{Code: CodeParticipantNotFound, Message: "router not initialised"}
	}
	return c.router.Route(ctx, participantID, msg)
}

// BroadcastRoomEvent fans a room.event out to every OTHER node that currently
// holds a participant of roomID. Best-effort: a failed peer is logged,
// not retried here — room membership will re-sync on the next join. The
// participant lookup is on the signaling (join/leave) path, never on RTP.
func (c *Cluster) BroadcastRoomEvent(ctx context.Context, roomID string, payload []byte) {
	if !c.cfg.Enabled || c.msgTransport == nil {
		return
	}
	seen := map[string]bool{c.cfg.NodeID: true}
	for _, loc := range c.participants.InRoom(roomID) {
		if seen[loc.NodeID] {
			continue
		}
		seen[loc.NodeID] = true
		msg := c.NewClusterMessage(MsgRoomEvent, roomID, "", payload)
		if err := c.msgTransport.Send(ctx, loc.NodeID, msg); err != nil {
			c.logger.Warn("cluster_room_event_failed", "room", roomID, "targetNodeId", loc.NodeID, "err", err.Error())
		}
	}
}

// Enabled reports whether multi-node coordination is active.
func (c *Cluster) Enabled() bool { return c.cfg.Enabled }

// NodeID is this node's identity.
func (c *Cluster) NodeID() string { return c.cfg.NodeID }

// Self returns this node's current NodeInfo (with live State).
func (c *Cluster) Self() NodeInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := c.self
	n.State = c.nodeState
	return n
}

// State returns the node lifecycle phase.
func (c *Cluster) State() NodeState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodeState
}

func (c *Cluster) setState(s NodeState) {
	c.mu.Lock()
	c.nodeState = s
	c.self.State = s
	c.mu.Unlock()
	_ = c.registry.Register(c.currentSelf())
	c.logger.Info("node_state", "node", c.cfg.NodeID, "state", string(s))
}

func (c *Cluster) currentSelf() NodeInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := c.self
	n.State = c.nodeState
	return n
}

// Start moves the node toward READY and starts the heartbeat loop. When the
// cluster state backend is a hard dependency the node connects and PINGs
// first; if that fails it stays in STARTING and Ready() is false until a later
// heartbeat tick succeeds — a node never announces READY without the shared
// state it needs.
func (c *Cluster) Start() {
	if !c.cfg.Enabled {
		c.setState(NodeReady)
		return
	}
	c.dedupOnce()
	c.lrOnce.Do(func() { go c.loadRptr.run() })

	ctx, cancel := context.WithTimeout(context.Background(), c.pingTimeout())
	err := c.state.Ping(ctx)
	cancel()
	if err != nil {
		c.metrics.redisUnavailable()
		c.logger.Warn("redis_disconnected", "node", c.cfg.NodeID, "backend", c.state.Kind(), "err", err)
		if c.cfg.Redis.Enabled && c.cfg.Redis.Required {
			c.logger.Warn("node_not_ready", "reason", "cluster state unavailable at startup", "node", c.cfg.NodeID)
			// Stay STARTING; the heartbeat loop retries and promotes to READY.
			c.hbOnce.Do(func() { go c.heartbeatLoop() })
			return
		}
	} else if c.state.Kind() == "redis" {
		c.logger.Info("redis_connected", "node", c.cfg.NodeID, "addr", c.cfg.Redis.Addr)
	}

	c.setState(NodeReady)
	c.hbOnce.Do(func() { go c.heartbeatLoop() })
	c.fdOnce.Do(func() {
		_ = c.failureDet.Start(c.fdCtx)
		go c.runStartupRecovery()
	})
	c.logger.Info("cluster_started",
		"node", c.cfg.NodeID, "backend", c.state.Kind(), "peers", c.cfg.Peers,
		"heartbeat", c.cfg.HeartbeatInterval.String())
}

func (c *Cluster) pingTimeout() time.Duration {
	if c.cfg.Redis.Timeout > 0 {
		return c.cfg.Redis.Timeout
	}
	return 3 * time.Second
}

// Ready reports whether the node accepts new sessions (for /ready). A
// disabled cluster is always ready — single-node behavior is unchanged.
// When Redis is a hard dependency, an unhealthy backend forces NOT READY.
func (c *Cluster) Ready() bool {
	if !c.cfg.Enabled {
		return true
	}
	if !c.State().AcceptsSessions() {
		return false
	}
	if c.cfg.Redis.Enabled && c.cfg.Redis.Required && !c.state.Healthy() {
		return false
	}
	return true
}

func (c *Cluster) heartbeatLoop() {
	t := time.NewTicker(c.cfg.HeartbeatInterval)
	defer t.Stop()
	wasHealthy := c.state.Healthy()
	for {
		select {
		case <-c.stopHB:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), c.pingTimeout())
			pingErr := c.state.Ping(ctx)
			cancel()
			healthy := pingErr == nil

			if healthy && !wasHealthy {
				c.metrics.redisReconnect()
				c.logger.Info("redis_reconnected", "node", c.cfg.NodeID)
				// Re-register: our TTL key may have expired while disconnected.
				_ = c.registry.Register(c.currentSelf())
				if c.State() == NodeStarting {
					c.setState(NodeReady)
					c.logger.Info("node_ready", "node", c.cfg.NodeID, "reason", "cluster state recovered")
				}
			}
			if !healthy && wasHealthy {
				c.metrics.redisUnavailable()
				c.logger.Warn("redis_disconnected", "node", c.cfg.NodeID, "err", pingErr)
			}
			wasHealthy = healthy

			if healthy {
				// Refresh our node key TTL.
				_ = c.registry.Register(c.currentSelf())
				c.registry.Heartbeat(c.cfg.NodeID)
			}
			// Refresh peer NodeInfo so List() shows the cluster (detection
			// only — no failover).
			c.reconcilePeers()
		}
	}
}

func (c *Cluster) reconcilePeers() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, peer := range c.cfg.Peers {
		info, err := c.transport.NodeInfo(ctx, peer)
		if err != nil {
			c.logger.Debug("peer_unreachable", "peer", peer, "err", err)
			continue
		}
		_ = c.registry.Register(info)
	}
}

// ---------------------------------------------------------------------------
// Room ownership (the core of)
// ---------------------------------------------------------------------------

// RoomResolution is the outcome of asking where a room lives.
type RoomResolution struct {
	RoomID  string
	OwnerID string
	Local   bool // true when this node owns the room
}

// ResolveRoom returns where roomID currently lives, WITHOUT claiming it.
// Checks the local locator first, then peers.
func (c *Cluster) ResolveRoom(ctx context.Context, roomID string) (RoomResolution, bool) {
	if owner, ok := c.rooms.GetOwner(roomID); ok {
		return RoomResolution{RoomID: roomID, OwnerID: owner, Local: owner == c.cfg.NodeID}, true
	}
	if !c.cfg.Enabled {
		return RoomResolution{}, false
	}
	for _, peer := range c.cfg.Peers {
		owner, ok, err := c.transport.RoomOwner(ctx, peer, roomID)
		if err != nil {
			c.logger.Debug("peer_room_lookup_failed", "peer", peer, "room", roomID, "err", err)
			continue
		}
		if ok {
			return RoomResolution{RoomID: roomID, OwnerID: owner, Local: owner == c.cfg.NodeID}, true
		}
	}
	return RoomResolution{}, false
}

// ClaimRoom is called from the join path. It returns:
//   - (resolution, nil)      when this node owns the room (freshly claimed or
//     already ours) and the join may proceed
//   - (_, *Error ROOM_ON_OTHER_NODE) when another node owns it — the
//     caller must reject the join and tell the client where to go
//   - (_, *Error NODE_SHUTTING_DOWN) when this node is draining
func (c *Cluster) ClaimRoom(ctx context.Context, roomID string) (RoomResolution, error) {
	// Fail closed: with a hard cluster-state dependency down we cannot know
	// whether another node owns this room, so we must not create a possibly
	// duplicate one.
	if c.cfg.Enabled && c.cfg.Redis.Enabled && !c.state.Healthy() {
		c.metrics.roomClaimFailed()
		return RoomResolution{}, &Error{Code: CodeClusterStateUnavailable, Message: "cluster state backend unavailable", NodeID: c.cfg.NodeID, RoomID: roomID}
	}

	// A draining node never takes a room it does not already own.
	if c.cfg.Enabled && !c.State().AcceptsSessions() {
		if owner, ok := c.rooms.GetOwner(roomID); ok && owner == c.cfg.NodeID {
			return RoomResolution{RoomID: roomID, OwnerID: owner, Local: true}, nil
		}
		return RoomResolution{}, &Error{Code: CodeNodeShuttingDown, Message: "node is not accepting new rooms", NodeID: c.cfg.NodeID, RoomID: roomID}
	}

	// Is it already somewhere?
	if res, ok := c.ResolveRoom(ctx, roomID); ok {
		if res.Local {
			return res, nil
		}
		if owner, hit := c.registry.Get(res.OwnerID); hit {
			c.metrics.redirect()
			c.logger.Info("room_remote", "room", roomID, "owner", res.OwnerID, "local_node", c.cfg.NodeID)
			return RoomResolution{}, RoomRemoteError(roomID, owner)
		}
		c.metrics.redirect()
		return RoomResolution{}, RoomRemoteError(roomID, NodeInfo{ID: res.OwnerID})
	}

	// Nobody owns it yet — this is a NEW room, so the RoomRouter picks
	// which node should host it. The router is advisory: the winner still has to
	// win ClaimOwner atomically. When the router points at another
	// node we redirect the client there; the deterministic policy means that
	// node's own router will agree and claim.
	if c.cfg.Enabled && c.roomRtr != nil {
		target, reason, rerr := c.roomRtr.SelectNode(ctx, roomID)
		if rerr != nil {
			c.metrics.roomClaimFailed()
			if ce, ok := rerr.(*Error); ok {
				ce.RoomID = roomID
				return RoomResolution{}, ce
			}
			return RoomResolution{}, &Error{Code: CodeClusterStateUnavailable, Message: rerr.Error(), RoomID: roomID}
		}
		if target.ID != "" && target.ID != c.cfg.NodeID {
			c.metrics.redirect()
			c.logger.Info("room_routed", "room", roomID, "target", target.ID, "reason", reason, "local_node", c.cfg.NodeID)
			return RoomResolution{}, RoomRemoteError(roomID, target)
		}
		c.metrics.roomClaimRouted()
	}

	// ClaimOwner is atomic across the whole cluster (Redis SET NX).
	owner, claimed := c.rooms.ClaimOwner(roomID, c.cfg.NodeID)
	if owner == "" {
		// The backend failed the claim (e.g. Redis dropped mid-call). Fail
		// closed — never assume the room is free.
		c.metrics.roomClaimFailed()
		return RoomResolution{}, &Error{Code: CodeClusterStateUnavailable, Message: "room claim failed", NodeID: c.cfg.NodeID, RoomID: roomID}
	}
	if owner != c.cfg.NodeID {
		// Someone claimed it in the race window.
		c.metrics.conflict()
		c.logger.Info("room_claim_conflict", "room", roomID, "winner", owner, "local_node", c.cfg.NodeID)
		if info, ok := c.registry.Get(owner); ok {
			return RoomResolution{}, RoomRemoteError(roomID, info)
		}
		return RoomResolution{}, RoomRemoteError(roomID, NodeInfo{ID: owner})
	}
	if claimed {
		c.metrics.claimed()
		c.logger.Info("room_claimed", "room", roomID, "node", c.cfg.NodeID)
	}
	return RoomResolution{RoomID: roomID, OwnerID: c.cfg.NodeID, Local: true}, nil
}

// ReleaseRoom drops this node's ownership of an emptied room via an atomic
// compare-and-delete — it only ever deletes ownership this node still holds, so
// it can never stomp a room another node claimed after a TTL expiry.
func (c *Cluster) ReleaseRoom(roomID string) {
	if c.rooms.ReleaseOwner(roomID, c.cfg.NodeID) {
		c.metrics.roomReleased()
		c.logger.Info("room_released", "room", roomID, "node", c.cfg.NodeID)
	}
}

// ---------------------------------------------------------------------------
// Participant location
// ---------------------------------------------------------------------------

// RegisterParticipant records that participantID's session lives on this node.
func (c *Cluster) RegisterParticipant(participantID, roomID string) {
	c.participants.Register(ParticipantLocation{
		ParticipantID: participantID, RoomID: roomID, NodeID: c.cfg.NodeID,
	})
	c.metrics.participantRegistered()
	c.logger.Info("participant_registered", "participant", participantID, "room", roomID, "node", c.cfg.NodeID)
}

// ForgetParticipant removes a participant's location record. Idempotent.
func (c *Cluster) ForgetParticipant(participantID string) {
	c.participants.Remove(participantID)
	c.metrics.participantRemoved()
	c.logger.Info("participant_removed", "participant", participantID, "node", c.cfg.NodeID)
}

// LocateParticipant finds a participant anywhere in the cluster (local first).
func (c *Cluster) LocateParticipant(ctx context.Context, participantID string) (ParticipantLocation, bool) {
	if loc, ok := c.participants.Lookup(participantID); ok {
		return loc, true
	}
	if !c.cfg.Enabled {
		return ParticipantLocation{}, false
	}
	for _, peer := range c.cfg.Peers {
		if loc, ok, err := c.transport.Participant(ctx, peer, participantID); err == nil && ok {
			return loc, true
		}
	}
	return ParticipantLocation{}, false
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

// BeginShutdown moves the node to SHUTTING_DOWN so it stops being assigned new
// rooms. Existing sessions are untouched. Rooms are NOT migrated.
func (c *Cluster) BeginShutdown() {
	c.setState(NodeShuttingDown)
}

// Shutdown finishes teardown: marks the node OFFLINE, stops the heartbeat,
// deletes its registry key and closes the state backend.
func (c *Cluster) Shutdown() {
	c.setState(NodeOffline)
	c.stopOnce.Do(func() { close(c.stopHB) })
	if c.fdCancel != nil {
		c.fdCancel()
	}
	if c.failureDet != nil {
		_ = c.failureDet.Stop()
	}
	if c.dedup != nil {
		c.dedup.Close()
	}
	if c.loadRptr != nil {
		c.loadRptr.Close()
		_ = c.state.Load().DeleteLoad(c.cfg.NodeID)
	}
	if c.msgTransport != nil {
		_ = c.msgTransport.Close()
	}
	if mb, ok := c.media.(*mediaBridge); ok {
		_ = mb.Close()
	}
	c.registry.Unregister(c.cfg.NodeID)
	if err := c.state.Close(); err != nil {
		c.logger.Warn("cluster_state_close", "err", err)
	}
	c.logger.Info("node_shutdown", "node", c.cfg.NodeID)
}

// State exposes the coordination backend (tests, introspection).
func (c *Cluster) ClusterState() ClusterState { return c.state }

// ---------------------------------------------------------------------------
// Introspection
// ---------------------------------------------------------------------------

// Registry exposes the node registry (for /internal/nodes and tests).
func (c *Cluster) Registry() NodeRegistry { return c.registry }

// Rooms exposes the room locator.
func (c *Cluster) Rooms() RoomLocator { return c.rooms }

// Participants exposes the participant locator.
func (c *Cluster) Participants() ParticipantLocator { return c.participants }

// MetricsSnapshot builds the /metrics "cluster" block.
func (c *Cluster) MetricsSnapshot() Snapshot {
	total, active, stale := c.registry.Counts()
	rooms := c.rooms.All()
	local, remote := 0, 0
	for _, owner := range rooms {
		if owner == c.cfg.NodeID {
			local++
		} else {
			remote++
		}
	}
	return Snapshot{
		Enabled:                c.cfg.Enabled,
		NodeID:                 c.cfg.NodeID,
		State:                  string(c.State()),
		Nodes:                  total,
		NodesActive:            active,
		NodesStale:             stale,
		Rooms:                  len(rooms),
		RoomsLocal:             local,
		RoomsRemote:            remote,
		OwnershipClaimed:       c.metrics.ownershipClaimed.Load(),
		OwnershipConflict:      c.metrics.ownershipConflict.Load(),
		RoomRedirects:          c.metrics.roomRedirects.Load(),
		InternalRequests:       c.metrics.internalRequests.Load(),
		InternalRequestsFailed: c.metrics.internalFailed.Load(),
		Participants:           c.participants.CountByNode(c.cfg.NodeID),

		Backend:            c.state.Kind(),
		RedisConnected:     c.cfg.Enabled && c.cfg.Redis.Enabled && c.state.Healthy(),
		RedisErrors:        c.metrics.redisErrors.Load(),
		RedisReconnects:    c.metrics.redisReconnects.Load(),
		RedisUnavailable:   c.metrics.redisUnavail.Load(),
		RoomClaimFailed:    c.metrics.roomClaimFail.Load(),
		RoomReleased:       c.metrics.roomRelease.Load(),
		ParticipantRegs:    c.metrics.participantReg.Load(),
		ParticipantRemoves: c.metrics.participantRem.Load(),

		Messages: MessageMetrics{
			Sent:       c.metrics.msgSentC.Load(),
			Received:   c.metrics.msgReceivedC.Load(),
			Failed:     c.metrics.msgFailedC.Load(),
			Retried:    c.metrics.msgRetriedC.Load(),
			Duplicated: c.metrics.msgDuplicatedC.Load(),
			Rejected:   c.metrics.msgRejectedC.Load(),
			Invalid:    c.metrics.msgInvalidC.Load(),
			Timeout:    c.metrics.msgTimeoutC.Load(),
		},
		Routing: RoutingMetrics{
			Local:    c.metrics.routeLocalC.Load(),
			Remote:   c.metrics.routeRemoteC.Load(),
			NotFound: c.metrics.routeNotFoundC.Load(),
		},
		TransportErrs:  c.metrics.transportErrC.Load(),
		TransportLatMs: c.metrics.transportLatencyAvgMs(),

		Scheduler: RoutingSchedMetrics{
			RoomSelections:        c.metrics.roomSelections.Load(),
			RoomSelectionFailures: c.metrics.roomSelectionFails.Load(),
			RoomClaimsRouted:      c.metrics.roomClaimsRouted.Load(),
			RoomClaimRetries:      c.metrics.roomClaimRetries.Load(),
			SelectLocal:           c.metrics.selLocal.Load(),
			SelectLeastLoaded:     c.metrics.selLeastLoaded.Load(),
			SelectCapacity:        c.metrics.selCapacity.Load(),
			SelectFallback:        c.metrics.selFallback.Load(),
		},
		Load: LoadMetrics{
			Reports: c.metrics.loadReports.Load(),
			Stale:   c.metrics.loadStaleC.Load(),
		},
		Recovery: RecoveryMetrics{
			NodesDetectedStale:  c.metrics.nodesDetectedStale.Load(),
			NodesRecovered:      c.metrics.nodesRecovered.Load(),
			NodesRecoveryFailed: c.metrics.nodesRecoveryFailed.Load(),
			Started:             c.metrics.recoveryStarted.Load(),
			Completed:           c.metrics.recoveryCompleted.Load(),
			Failed:              c.metrics.recoveryFailedC.Load(),
			Rooms:               c.metrics.recoveryRoomsC.Load(),
			RoomsRecovered:      c.metrics.recoveryRoomsRecov.Load(),
			RoomsSkipped:        c.metrics.recoveryRoomsSkip.Load(),
			Conflicts:           c.metrics.recoveryConflictsC.Load(),
			RoutingStaleNode:    c.metrics.routingStaleNodeC.Load(),
			RoutingUnavailNode:  c.metrics.routingUnavailNodeC.Load(),
		},

		Media: MediaMetrics{
			Enabled:          c.cfg.Media.Enabled,
			LocalAddr:        c.media.LocalMediaAddr(),
			SessionsActive:   c.metrics.mediaSessActive.Load(),
			SessionsCreated:  c.metrics.mediaSessCreated.Load(),
			SessionsClosed:   c.metrics.mediaSessClosed.Load(),
			SessionsFailed:   c.metrics.mediaSessFailed.Load(),
			RemoteTracks:     c.metrics.mediaRemoteTracks.Load(),
			RTPSent:          c.metrics.mediaRTPSentC.Load(),
			RTPReceived:      c.metrics.mediaRTPRecvC.Load(),
			RTPDropped:       c.metrics.mediaRTPDropC.Load(),
			RTPInvalid:       c.metrics.mediaRTPInvalidC.Load(),
			RTCPSent:         c.metrics.mediaRTCPSentC.Load(),
			RTCPReceived:     c.metrics.mediaRTCPRecvC.Load(),
			RTCPDropped:      c.metrics.mediaRTCPDropC.Load(),
			OversizedPackets: c.metrics.mediaOversizedC.Load(),
			AuthFailures:     c.metrics.mediaAuthFailedC.Load(),
		},
	}
}
