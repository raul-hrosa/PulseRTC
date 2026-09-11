package cluster

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestRoomLocatorClaimAndGet(t *testing.T) {
	l := NewInMemoryRoomLocator()

	owner, claimed := l.ClaimOwner("room-a", "node-1")
	if owner != "node-1" || !claimed {
		t.Fatalf("first claim: owner=%s claimed=%v", owner, claimed)
	}
	if o, ok := l.GetOwner("room-a"); !ok || o != "node-1" {
		t.Fatalf("GetOwner = %s,%v", o, ok)
	}

	// Idempotent re-claim by the same node.
	owner, claimed = l.ClaimOwner("room-a", "node-1")
	if owner != "node-1" || claimed {
		t.Fatalf("re-claim by owner: owner=%s claimed=%v", owner, claimed)
	}

	// Claim by a different node returns the incumbent, does not steal.
	owner, claimed = l.ClaimOwner("room-a", "node-2")
	if owner != "node-1" || claimed {
		t.Fatalf("claim by other node: owner=%s claimed=%v", owner, claimed)
	}
}

func TestRoomLocatorRemoveAndOwnedBy(t *testing.T) {
	l := NewInMemoryRoomLocator()
	l.ClaimOwner("r1", "n1")
	l.ClaimOwner("r2", "n1")
	l.ClaimOwner("r3", "n2")

	got := l.OwnedBy("n1")
	if len(got) != 2 {
		t.Fatalf("OwnedBy(n1) = %v", got)
	}
	l.RemoveOwner("r1")
	if _, ok := l.GetOwner("r1"); ok {
		t.Fatal("r1 still owned after RemoveOwner")
	}
	if len(l.All()) != 2 {
		t.Fatalf("All() = %v", l.All())
	}
}

// The headline test: 100 goroutines race to claim the same room;
// exactly one wins.
func TestRoomLocatorConcurrentClaimSingleOwner(t *testing.T) {
	l := NewInMemoryRoomLocator()
	var claims atomic.Int64
	owners := &sync.Map{}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		nodeID := "node-" + string(rune('A'+i%7))
		go func(n string) {
			defer wg.Done()
			owner, claimed := l.ClaimOwner("hot-room", n)
			owners.Store(owner, true)
			if claimed {
				claims.Add(1)
			}
		}(nodeID)
	}
	wg.Wait()

	if claims.Load() != 1 {
		t.Fatalf("expected exactly 1 successful claim, got %d", claims.Load())
	}
	distinct := 0
	owners.Range(func(_, _ any) bool { distinct++; return true })
	if distinct != 1 {
		t.Fatalf("expected all goroutines to agree on 1 owner, saw %d", distinct)
	}
}

func TestRoomLocatorConflictHook(t *testing.T) {
	l := NewInMemoryRoomLocator()
	var conflicts, claims int
	l.onClaim = func() { claims++ }
	l.onConflict = func() { conflicts++ }

	l.ClaimOwner("r", "n1") // claim
	l.ClaimOwner("r", "n1") // idempotent, no hook
	l.ClaimOwner("r", "n2") // conflict
	if claims != 1 || conflicts != 1 {
		t.Fatalf("claims=%d conflicts=%d", claims, conflicts)
	}
}
