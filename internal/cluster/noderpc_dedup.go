package cluster

import (
	"sync"
	"time"
)

// dedupCache detects duplicate cluster messages by RequestID within a TTL
// window. It is intentionally node-local — Redis is not used
// per-message. A duplicate `Send` (retry that actually arrived, or a
// genuine resend) is recognised and its side effects are skipped.
type dedupCache struct {
	ttl  time.Duration
	now  func() time.Time
	mu   sync.Mutex
	seen map[string]time.Time
	stop chan struct{}
	once sync.Once
}

func newDedupCache(ttl time.Duration) *dedupCache {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &dedupCache{
		ttl:  ttl,
		now:  time.Now,
		seen: make(map[string]time.Time),
		stop: make(chan struct{}),
	}
}

// markSeen records requestID and reports whether it was already present (a
// duplicate). The first caller for an id gets false; every later caller within
// the TTL gets true.
func (d *dedupCache) markSeen(requestID string) (duplicate bool) {
	now := d.now()
	d.mu.Lock()
	defer d.mu.Unlock()
	if exp, ok := d.seen[requestID]; ok && now.Before(exp) {
		return true
	}
	d.seen[requestID] = now.Add(d.ttl)
	return false
}

func (d *dedupCache) sweep() {
	now := d.now()
	d.mu.Lock()
	for id, exp := range d.seen {
		if now.After(exp) {
			delete(d.seen, id)
		}
	}
	d.mu.Unlock()
}

// run sweeps expired entries until Close. Started from Cluster.Start.
func (d *dedupCache) run() {
	t := time.NewTicker(d.ttl)
	defer t.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-t.C:
			d.sweep()
		}
	}
}

func (d *dedupCache) Close() { d.once.Do(func() { close(d.stop) }) }
