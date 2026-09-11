package cluster

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func disabledCluster(t *testing.T) *Cluster {
	t.Helper()
	c, err := New(Config{Enabled: false}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	return c
}

func enabledCluster(t *testing.T, id string, peers ...string) *Cluster {
	t.Helper()
	c, err := New(Config{
		Enabled: true, NodeID: id, Secret: "cluster-secret", Peers: peers,
		Host: "localhost", Port: 8090,
		HeartbeatInterval: time.Hour, StaleAfter: 2 * time.Hour,
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNewFailsClosedWhenEnabledWithoutSecret(t *testing.T) {
	if _, err := New(Config{Enabled: true}, testLogger()); err == nil {
		t.Fatal("enabled cluster without a secret must fail to build")
	}
}

func TestDisabledClusterClaimsEverythingLocally(t *testing.T) {
	c := disabledCluster(t)
	res, err := c.ClaimRoom(context.Background(), "room-1")
	if err != nil || !res.Local {
		t.Fatalf("disabled cluster should claim locally: res=%+v err=%v", res, err)
	}
	// Idempotent.
	res, err = c.ClaimRoom(context.Background(), "room-1")
	if err != nil || !res.Local {
		t.Fatalf("re-claim: res=%+v err=%v", res, err)
	}
	if !c.Ready() {
		t.Fatal("disabled cluster is always ready")
	}
}

func TestClaimRoomLocalThenRelease(t *testing.T) {
	c := enabledCluster(t, "node-1")
	c.Start()

	res, err := c.ClaimRoom(context.Background(), "room-a")
	if err != nil || res.OwnerID != "node-1" || !res.Local {
		t.Fatalf("claim: %+v %v", res, err)
	}
	if got, _ := c.rooms.GetOwner("room-a"); got != "node-1" {
		t.Fatalf("owner not recorded: %s", got)
	}
	c.ReleaseRoom("room-a")
	if _, ok := c.rooms.GetOwner("room-a"); ok {
		t.Fatal("room still owned after release")
	}
}

func TestClaimRoomRejectedWhileDraining(t *testing.T) {
	c := enabledCluster(t, "node-1")
	c.Start()
	c.BeginShutdown()

	_, err := c.ClaimRoom(context.Background(), "new-room")
	ce, ok := err.(*Error)
	if !ok || ce.Code != CodeNodeShuttingDown {
		t.Fatalf("draining node should reject new rooms, got %v", err)
	}

	// A room it already owns still resolves.
	c.rooms.SetOwner("mine", "node-1")
	res, err := c.ClaimRoom(context.Background(), "mine")
	if err != nil || !res.Local {
		t.Fatalf("draining node should still serve its own rooms: %+v %v", res, err)
	}
}

func TestParticipantRegisterForget(t *testing.T) {
	c := disabledCluster(t)
	c.RegisterParticipant("u1", "r1")
	if loc, ok := c.LocateParticipant(context.Background(), "u1"); !ok || loc.RoomID != "r1" {
		t.Fatalf("locate: %+v %v", loc, ok)
	}
	c.ForgetParticipant("u1")
	if _, ok := c.LocateParticipant(context.Background(), "u1"); ok {
		t.Fatal("u1 still located after forget")
	}
}

func TestMetricsSnapshot(t *testing.T) {
	c := enabledCluster(t, "node-1")
	c.Start()
	c.ClaimRoom(context.Background(), "r1")
	c.ClaimRoom(context.Background(), "r2")
	c.RegisterParticipant("u1", "r1")

	s := c.MetricsSnapshot()
	if !s.Enabled || s.NodeID != "node-1" {
		t.Fatalf("snapshot header: %+v", s)
	}
	if s.Rooms != 2 || s.RoomsLocal != 2 || s.RoomsRemote != 0 {
		t.Fatalf("room counts: %+v", s)
	}
	if s.OwnershipClaimed != 2 {
		t.Fatalf("ownershipClaimed = %d", s.OwnershipClaimed)
	}
	if s.Participants != 1 {
		t.Fatalf("participants = %d", s.Participants)
	}
}
