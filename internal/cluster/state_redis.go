package cluster

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClusterState is the shared-state backend. It
// implements the exact same NodeRegistry / RoomLocator / ParticipantLocator
// contract as the in-memory one, so every node sees one consistent view of
// "room → owner", "participant → node" and "node → status".
//
// Key layout:
//
//	<prefix>:nodes:<nodeId>                     JSON NodeInfo,   TTL = NodeTTL
//	<prefix>:rooms:<roomId>                     nodeId string,   TTL = RoomTTL (0 = none)
//	<prefix>:participants:<participantId>       JSON location,   TTL = ParticipTTL
type RedisClusterState struct {
	client  *redisClient
	cfg     RedisConfig
	logger  *slog.Logger
	healthy atomic.Bool

	nodes        *redisNodeRegistry
	rooms        *redisRoomLocator
	participants *redisParticipantLocator
}

func newRedisClusterState(cfg Config, logger *slog.Logger) (*RedisClusterState, error) {
	if logger == nil {
		logger = slog.Default()
	}
	rc := cfg.Redis
	return newRedisClusterStateWith(newRedisClient(rc), rc, logger), nil
}

// newRedisClusterStateWith builds the state on an existing client (tests inject
// a miniredis-backed one).
func newRedisClusterStateWith(client *redisClient, rc RedisConfig, logger *slog.Logger) *RedisClusterState {
	if rc.NodeTTL <= 0 {
		rc.NodeTTL = 15 * time.Second
	}
	if rc.ParticipTTL <= 0 {
		rc.ParticipTTL = 60 * time.Second
	}
	s := &RedisClusterState{client: client, cfg: rc, logger: logger}
	s.nodes = &redisNodeRegistry{s: s}
	s.rooms = &redisRoomLocator{s: s}
	s.participants = &redisParticipantLocator{s: s}
	return s
}

func (s *RedisClusterState) setErrorHook(fn func()) { s.client.onError = fn }

func (s *RedisClusterState) Nodes() NodeRegistry              { return s.nodes }
func (s *RedisClusterState) Rooms() RoomLocator               { return s.rooms }
func (s *RedisClusterState) Participants() ParticipantLocator { return s.participants }
func (s *RedisClusterState) Load() LoadStore                  { return &redisLoadStore{s: s} }
func (s *RedisClusterState) Kind() string                     { return "redis" }
func (s *RedisClusterState) Healthy() bool                    { return s.healthy.Load() }
func (s *RedisClusterState) Close() error                     { return s.client.Close() }

func (s *RedisClusterState) Ping(ctx context.Context) error {
	err := s.client.Ping(ctx)
	s.healthy.Store(err == nil)
	return err
}

func (s *RedisClusterState) ctx() (context.Context, context.CancelFunc) {
	t := s.cfg.Timeout
	if t <= 0 {
		t = 3 * time.Second
	}
	return context.WithTimeout(context.Background(), t)
}

// --------------------------------------------------------------------------
// Node registry
// --------------------------------------------------------------------------

type redisNodeRegistry struct{ s *RedisClusterState }

func (r *redisNodeRegistry) Register(node NodeInfo) error {
	if strings.TrimSpace(node.ID) == "" {
		return &Error{Code: CodeBadRequest, Message: "node id is required"}
	}
	node.LastSeen = time.Now()
	if node.StartedAt.IsZero() {
		if cur, ok := r.get(node.ID); ok {
			node.StartedAt = cur.StartedAt
		}
	}
	body, _ := json.Marshal(node)
	ctx, cancel := r.s.ctx()
	defer cancel()
	return r.s.client.Set(ctx, r.s.client.key("nodes", node.ID), string(body), r.s.cfg.NodeTTL)
}

func (r *redisNodeRegistry) Unregister(nodeID string) {
	ctx, cancel := r.s.ctx()
	defer cancel()
	_ = r.s.client.Del(ctx, r.s.client.key("nodes", nodeID))
}

func (r *redisNodeRegistry) Heartbeat(nodeID string) bool {
	ctx, cancel := r.s.ctx()
	defer cancel()
	// Refresh the TTL; if the key is gone the caller must Register again.
	if err := r.s.client.Expire(ctx, r.s.client.key("nodes", nodeID), r.s.cfg.NodeTTL); err != nil {
		return false
	}
	_, ok := r.get(nodeID)
	return ok
}

func (r *redisNodeRegistry) get(nodeID string) (NodeInfo, bool) {
	ctx, cancel := r.s.ctx()
	defer cancel()
	v, ok, err := r.s.client.Get(ctx, r.s.client.key("nodes", nodeID))
	if err != nil || !ok {
		return NodeInfo{}, false
	}
	var n NodeInfo
	if json.Unmarshal([]byte(v), &n) != nil {
		return NodeInfo{}, false
	}
	return n, true
}

func (r *redisNodeRegistry) Get(nodeID string) (NodeInfo, bool) { return r.get(nodeID) }

func (r *redisNodeRegistry) List() []NodeInfo {
	ctx, cancel := r.s.ctx()
	defer cancel()
	keys, err := r.s.client.ScanKeys(ctx, r.s.client.key("nodes", "*"))
	if err != nil || len(keys) == 0 {
		return nil
	}
	vals, err := r.s.client.MGet(ctx, keys...)
	if err != nil {
		return nil
	}
	out := make([]NodeInfo, 0, len(vals))
	for _, v := range vals {
		str, ok := v.(string)
		if !ok {
			continue
		}
		var n NodeInfo
		if json.Unmarshal([]byte(str), &n) == nil {
			out = append(out, n)
		}
	}
	sortNodes(out)
	return out
}

func (r *redisNodeRegistry) IsStale(node NodeInfo) bool {
	if r.s.cfg.NodeTTL <= 0 {
		return false
	}
	return time.Since(node.LastSeen) > r.s.cfg.NodeTTL
}

func (r *redisNodeRegistry) Counts() (total, active, stale int) {
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

func sortNodes(ns []NodeInfo) {
	for i := 1; i < len(ns); i++ {
		for j := i; j > 0 && ns[j-1].ID > ns[j].ID; j-- {
			ns[j-1], ns[j] = ns[j], ns[j-1]
		}
	}
}

// --------------------------------------------------------------------------
// Room ownership
// --------------------------------------------------------------------------

type redisRoomLocator struct{ s *RedisClusterState }

func (l *redisRoomLocator) key(roomID string) string    { return l.s.client.key("rooms", roomID) }
func (l *redisRoomLocator) genKey(roomID string) string { return l.s.client.key("roomgen", roomID) }

// Ownership implements RecoverableRooms.
func (l *redisRoomLocator) Ownership(roomID string) (string, int64, bool) {
	owner, ok := l.GetOwner(roomID)
	if !ok {
		return "", 0, false
	}
	ctx, cancel := l.s.ctx()
	defer cancel()
	var gen int64
	if v, found, err := l.s.client.Get(ctx, l.genKey(roomID)); err == nil && found {
		if n, perr := strconv.ParseInt(strings.TrimSpace(v), 10, 64); perr == nil {
			gen = n
		}
	}
	return owner, gen, true
}

// RecoverOwner implements RecoverableRooms via the server-side Lua CAS.
func (l *redisRoomLocator) RecoverOwner(roomID, oldOwner, newOwner string) RecoverResult {
	ctx, cancel := l.s.ctx()
	defer cancel()
	code, gen, err := l.s.client.RecoverOwner(ctx, l.key(roomID), l.genKey(roomID), oldOwner, newOwner)
	if err != nil {
		return RecoverResult{Outcome: RecoverFailed}
	}
	switch code {
	case 1:
		return RecoverResult{Outcome: RecoverOK, Owner: newOwner, Generation: gen}
	case 2:
		return RecoverResult{Outcome: RecoverAlready, Owner: newOwner, Generation: gen}
	case 3:
		cur, _ := l.GetOwner(roomID)
		return RecoverResult{Outcome: RecoverConflict, Owner: cur, Generation: gen}
	default:
		return RecoverResult{Outcome: RecoverFailed}
	}
}

func (l *redisRoomLocator) GetOwner(roomID string) (string, bool) {
	ctx, cancel := l.s.ctx()
	defer cancel()
	v, ok, err := l.s.client.Get(ctx, l.key(roomID))
	if err != nil {
		return "", false
	}
	return v, ok
}

// ClaimOwner is atomic across the whole cluster: SET key nodeId NX. Exactly one
// concurrent caller — on any node — gets claimed=true. On a Redis
// error it returns ("", false) so the caller fails the join closed.
func (l *redisRoomLocator) ClaimOwner(roomID, nodeID string) (string, bool) {
	ctx, cancel := l.s.ctx()
	defer cancel()
	ok, err := l.s.client.SetNX(ctx, l.key(roomID), nodeID, l.s.cfg.RoomTTL)
	if err != nil {
		return "", false
	}
	if ok {
		return nodeID, true
	}
	cur, found, err := l.s.client.Get(ctx, l.key(roomID))
	if err != nil {
		return "", false
	}
	if !found {
		// Expired between SETNX and GET — retry once.
		if ok, err := l.s.client.SetNX(ctx, l.key(roomID), nodeID, l.s.cfg.RoomTTL); err == nil && ok {
			return nodeID, true
		}
		return "", false
	}
	return cur, false // already owned (by us => idempotent, or by another node)
}

func (l *redisRoomLocator) SetOwner(roomID, nodeID string) {
	ctx, cancel := l.s.ctx()
	defer cancel()
	_ = l.s.client.Set(ctx, l.key(roomID), nodeID, l.s.cfg.RoomTTL)
}

func (l *redisRoomLocator) RemoveOwner(roomID string) {
	ctx, cancel := l.s.ctx()
	defer cancel()
	_ = l.s.client.Del(ctx, l.key(roomID))
}

func (l *redisRoomLocator) ReleaseOwner(roomID, nodeID string) bool {
	ctx, cancel := l.s.ctx()
	defer cancel()
	ok, _ := l.s.client.CompareDelete(ctx, l.key(roomID), nodeID)
	return ok
}

func (l *redisRoomLocator) OwnedBy(nodeID string) []string {
	var out []string
	for room, owner := range l.All() {
		if owner == nodeID {
			out = append(out, room)
		}
	}
	return out
}

func (l *redisRoomLocator) All() map[string]string {
	ctx, cancel := l.s.ctx()
	defer cancel()
	prefix := l.s.client.key("rooms", "")
	keys, err := l.s.client.ScanKeys(ctx, prefix+"*")
	if err != nil || len(keys) == 0 {
		return map[string]string{}
	}
	vals, err := l.s.client.MGet(ctx, keys...)
	if err != nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(keys))
	for i, k := range keys {
		if i >= len(vals) {
			break
		}
		if str, ok := vals[i].(string); ok {
			out[strings.TrimPrefix(k, prefix)] = str
		}
	}
	return out
}

// --------------------------------------------------------------------------
// Participant location
// --------------------------------------------------------------------------

type redisParticipantLocator struct{ s *RedisClusterState }

func (p *redisParticipantLocator) key(id string) string { return p.s.client.key("participants", id) }

func (p *redisParticipantLocator) Register(loc ParticipantLocation) {
	body, _ := json.Marshal(loc)
	ctx, cancel := p.s.ctx()
	defer cancel()
	_ = p.s.client.Set(ctx, p.key(loc.ParticipantID), string(body), p.s.cfg.ParticipTTL)
}

func (p *redisParticipantLocator) Remove(participantID string) {
	ctx, cancel := p.s.ctx()
	defer cancel()
	_ = p.s.client.Del(ctx, p.key(participantID))
}

func (p *redisParticipantLocator) Lookup(participantID string) (ParticipantLocation, bool) {
	ctx, cancel := p.s.ctx()
	defer cancel()
	v, ok, err := p.s.client.Get(ctx, p.key(participantID))
	if err != nil || !ok {
		return ParticipantLocation{}, false
	}
	var loc ParticipantLocation
	if json.Unmarshal([]byte(v), &loc) != nil {
		return ParticipantLocation{}, false
	}
	return loc, true
}

func (p *redisParticipantLocator) all() []ParticipantLocation {
	ctx, cancel := p.s.ctx()
	defer cancel()
	keys, err := p.s.client.ScanKeys(ctx, p.s.client.key("participants", "*"))
	if err != nil || len(keys) == 0 {
		return nil
	}
	vals, err := p.s.client.MGet(ctx, keys...)
	if err != nil {
		return nil
	}
	out := make([]ParticipantLocation, 0, len(vals))
	for _, v := range vals {
		if str, ok := v.(string); ok {
			var loc ParticipantLocation
			if json.Unmarshal([]byte(str), &loc) == nil {
				out = append(out, loc)
			}
		}
	}
	return out
}

func (p *redisParticipantLocator) InRoom(roomID string) []ParticipantLocation {
	var out []ParticipantLocation
	for _, loc := range p.all() {
		if loc.RoomID == roomID {
			out = append(out, loc)
		}
	}
	return out
}

func (p *redisParticipantLocator) CountByNode(nodeID string) int {
	n := 0
	for _, loc := range p.all() {
		if loc.NodeID == nodeID {
			n++
		}
	}
	return n
}

// --------------------------------------------------------------------------
// Load store
// --------------------------------------------------------------------------

type redisLoadStore struct{ s *RedisClusterState }

func (l *redisLoadStore) key(nodeID string) string { return l.s.client.key("load", nodeID) }

func (l *redisLoadStore) PublishLoad(load NodeLoad, ttl time.Duration) error {
	body, _ := json.Marshal(load)
	ctx, cancel := l.s.ctx()
	defer cancel()
	return l.s.client.Set(ctx, l.key(load.NodeID), string(body), ttl)
}

func (l *redisLoadStore) LoadSnapshot() ([]NodeLoad, error) {
	ctx, cancel := l.s.ctx()
	defer cancel()
	keys, err := l.s.client.ScanKeys(ctx, l.s.client.key("load", "*"))
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	vals, err := l.s.client.MGet(ctx, keys...)
	if err != nil {
		return nil, err
	}
	out := make([]NodeLoad, 0, len(vals))
	for _, v := range vals {
		if str, ok := v.(string); ok {
			var nl NodeLoad
			if json.Unmarshal([]byte(str), &nl) == nil {
				out = append(out, nl)
			}
		}
	}
	return out, nil
}

func (l *redisLoadStore) DeleteLoad(nodeID string) error {
	ctx, cancel := l.s.ctx()
	defer cancel()
	return l.s.client.Del(ctx, l.key(nodeID))
}

func (l *redisLoadStore) PutRoomAssignment(roomID, nodeID string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	ctx, cancel := l.s.ctx()
	defer cancel()
	return l.s.client.Set(ctx, l.s.client.key("assign", roomID), nodeID, ttl)
}

func (l *redisLoadStore) RoomAssignment(roomID string) (string, bool) {
	ctx, cancel := l.s.ctx()
	defer cancel()
	v, ok, err := l.s.client.Get(ctx, l.s.client.key("assign", roomID))
	if err != nil {
		return "", false
	}
	return v, ok
}

// compile-time interface checks.
var (
	_ ClusterState       = (*RedisClusterState)(nil)
	_ LoadStore          = (*redisLoadStore)(nil)
	_ LoadStore          = (*InMemoryLoadStore)(nil)
	_ NodeRegistry       = (*redisNodeRegistry)(nil)
	_ RoomLocator        = (*redisRoomLocator)(nil)
	_ RecoverableRooms   = (*redisRoomLocator)(nil)
	_ RecoverableRooms   = (*InMemoryRoomLocator)(nil)
	_ ParticipantLocator = (*redisParticipantLocator)(nil)
	_                    = redis.Nil
)
