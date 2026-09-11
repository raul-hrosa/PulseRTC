package cluster

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// These benchmarks guard: failure detection and recovery are
// O(nodes) / O(rooms) periodic work, never on the media path and never doing a
// Redis call per packet.

func BenchmarkFailureCheck(b *testing.B) {
	reg := NewInMemoryNodeRegistry(time.Second)
	for i := 0; i < 50; i++ {
		_ = reg.Register(NodeInfo{ID: "node-" + strconv.Itoa(i), State: NodeReady})
	}
	d := newFailureDetector(reg, func() string { return "node-0" }, func() bool { return true },
		time.Hour, 0, &Metrics{}, testLogger(), nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.scan()
	}
}

func BenchmarkOwnershipRecovery(b *testing.B) {
	l := NewInMemoryRoomLocator()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		room := "room-" + strconv.Itoa(i)
		l.ClaimOwner(room, "node-dead")
		l.RecoverOwner(room, "node-dead", "node-live")
	}
}

func BenchmarkRoomRecovery(b *testing.B) {
	rooms := NewInMemoryRoomLocator()
	det := newFakeDetector("node-dead")
	rm, _, _ := newTestRecovery(&testing.T{}, "node-live", rooms, det, "node-live")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		room := "room-" + strconv.Itoa(i)
		rooms.ClaimOwner(room, "node-dead")
		_ = rm.RecoverRoom(ctx, room)
	}
}
