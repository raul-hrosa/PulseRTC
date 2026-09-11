package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"
)

// reservedMetadataKeys must never appear in room metadata — the app must not
// stash credentials there (design).
var reservedMetadataKeys = map[string]bool{
	"token": true, "secret": true, "jwt": true, "password": true,
	"apikey": true, "api_key": true, "authorization": true,
}

const (
	maxMetadataBytes = 4 * 1024
	maxRoomIDLen     = 128
)

// RoomRecord is the API-owned metadata for a room. It is deliberately NOT the
// source of truth for liveness (that is Core) — only for the app's metadata and
// the explicit lifecycle markers the core does not model.
type RoomRecord struct {
	RoomID    string
	Metadata  map[string]string
	CreatedAt time.Time
	Closed    bool
	closedAt  time.Time
	lastSeen  time.Time
}

// idemEntry caches one Idempotency-Key result.
type idemEntry struct {
	bodyHash string
	response []byte
	status   int
	at       time.Time
}

// RoomRegistry is the node-local metadata store. In a multi-node deployment the
// records are per-node and best-effort (design / ADR 010): the durable truth is
// the core. Safe for concurrent use.
type RoomRegistry struct {
	retention time.Duration
	now       func() time.Time

	mu      sync.Mutex
	rooms   map[string]*RoomRecord
	idem    map[string]idemEntry
	idemTTL time.Duration
}

// NewRoomRegistry builds a registry. retention <= 0 falls back to 5 minutes.
func NewRoomRegistry(retention time.Duration) *RoomRegistry {
	if retention <= 0 {
		retention = defaultRoomRetention
	}
	return &RoomRegistry{
		retention: retention,
		now:       time.Now,
		rooms:     make(map[string]*RoomRecord),
		idem:      make(map[string]idemEntry),
		idemTTL:   10 * time.Minute,
	}
}

// GenerateRoomID returns a fresh, URL-safe room id.
func GenerateRoomID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "room_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]))
}

// ValidateRoomID enforces a conservative, path-safe id.
func ValidateRoomID(id string) error {
	if id == "" {
		return apiErr(CodeInvalidRequest, "roomId is required")
	}
	if len(id) > maxRoomIDLen {
		return apiErr(CodeInvalidRequest, "roomId is too long")
	}
	for _, r := range id {
		ok := r == '-' || r == '_' || r == '.' || r == ':' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !ok {
			return apiErr(CodeInvalidRequest, "roomId contains an unsupported character")
		}
	}
	return nil
}

// ValidateMetadata rejects reserved keys and oversize payloads.
func ValidateMetadata(md map[string]string) error {
	if md == nil {
		return nil
	}
	total := 0
	for k, v := range md {
		if reservedMetadataKeys[strings.ToLower(strings.TrimSpace(k))] {
			return apiErr(CodeInvalidRequest, "metadata key '"+k+"' is reserved")
		}
		total += len(k) + len(v)
	}
	if total > maxMetadataBytes {
		return apiErr(CodeInvalidRequest, "metadata exceeds 4KB")
	}
	return nil
}

// GetOrCreate returns the record for roomID, creating it if absent. created
// reports whether a new record was made. It also refreshes lastSeen.
func (r *RoomRegistry) GetOrCreate(roomID string, metadata map[string]string) (*RoomRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if rec, ok := r.rooms[roomID]; ok {
		rec.lastSeen = now
		return rec.clone(), false
	}
	rec := &RoomRecord{
		RoomID:    roomID,
		Metadata:  cloneMeta(metadata),
		CreatedAt: now,
		lastSeen:  now,
	}
	r.rooms[roomID] = rec
	return rec.clone(), true
}

// EnsureExists creates a bare record if none exists (used by the token endpoint
// so a GET works before the first join). No-op if present.
func (r *RoomRegistry) EnsureExists(roomID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rooms[roomID]; !ok {
		now := r.now()
		r.rooms[roomID] = &RoomRecord{RoomID: roomID, CreatedAt: now, lastSeen: now}
	}
}

// Get returns a copy of the record, if any.
func (r *RoomRegistry) Get(roomID string) (*RoomRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.rooms[roomID]
	if !ok {
		return nil, false
	}
	rec.lastSeen = r.now()
	return rec.clone(), true
}

// MarkClosed flags the record CLOSED. Returns false if there is no record.
func (r *RoomRegistry) MarkClosed(roomID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.rooms[roomID]
	if !ok {
		return false
	}
	rec.Closed = true
	rec.closedAt = r.now()
	rec.lastSeen = rec.closedAt
	return true
}

// Sweep drops CLOSED records past the retention window and idle bare records
// (never seen live) older than retention. Call periodically.
func (r *RoomRegistry) Sweep() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for id, rec := range r.rooms {
		if rec.Closed && now.Sub(rec.closedAt) > r.retention {
			delete(r.rooms, id)
			continue
		}
		if !rec.Closed && now.Sub(rec.lastSeen) > r.retention {
			delete(r.rooms, id)
		}
	}
	for k, e := range r.idem {
		if now.Sub(e.at) > r.idemTTL {
			delete(r.idem, k)
		}
	}
}

// idempotencyLookup returns a cached response for (key, body) or reports a
// conflict when key was used with a different body.
func (r *RoomRegistry) idempotencyLookup(key string, body []byte) (resp []byte, status int, hit bool, conflict bool) {
	if key == "" {
		return nil, 0, false, false
	}
	h := hashBody(body)
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.idem[key]
	if !ok {
		return nil, 0, false, false
	}
	if e.bodyHash != h {
		return nil, 0, false, true
	}
	return e.response, e.status, true, false
}

// idempotencyStore records a response under key.
func (r *RoomRegistry) idempotencyStore(key string, body, resp []byte, status int) {
	if key == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.idem[key] = idemEntry{bodyHash: hashBody(body), response: resp, status: status, at: r.now()}
}

func hashBody(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func (rec *RoomRecord) clone() *RoomRecord {
	cp := *rec
	cp.Metadata = cloneMeta(rec.Metadata)
	return &cp
}

func cloneMeta(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// sortedKeys is a tiny helper for stable metadata rendering in tests.
func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
