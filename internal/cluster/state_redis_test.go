package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func redisCluster(t *testing.T, id string) (*Cluster, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rc := RedisConfig{
		Enabled: true, Required: true, Addr: mr.Addr(), Prefix: "pulsertc",
		Timeout: time.Second, NodeTTL: 15 * time.Second, ParticipTTL: 30 * time.Second,
	}
	st := newRedisClusterStateWith(
		newRedisClientFrom(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "pulsertc"),
		rc, testLogger())
	cfg := Config{
		Enabled: true, NodeID: id, Secret: "s", Host: "localhost", Port: 8090,
		HeartbeatInterval: time.Hour, StaleAfter: 15 * time.Second, Redis: rc,
	}
	c, err := NewWithState(cfg, testLogger(), st)
	if err != nil {
		t.Fatal(err)
	}
	return c, mr
}

func TestRedisClusterClaimAndResolveShared(t *testing.T) {
	a, mr := redisCluster(t, "node-a")
	a.Start()
	if !a.Ready() {
		t.Fatal("node-a not ready with redis up")
	}
	if _, err := a.ClaimRoom(context.Background(), "room-1"); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// A second node sharing the same Redis sees the ownership.
	b := clusterOnSameRedis(t, mr, "node-b")
	b.Start()
	res, ok := b.ResolveRoom(context.Background(), "room-1")
	if !ok || res.OwnerID != "node-a" || res.Local {
		t.Fatalf("node-b resolve: %+v ok=%v", res, ok)
	}
	// node-b joining room-1 must be redirected, not create a duplicate.
	_, err := b.ClaimRoom(context.Background(), "room-1")
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeRoomOnOtherNode {
		t.Fatalf("want ROOM_ON_OTHER_NODE, got %v", err)
	}
}

func clusterOnSameRedis(t *testing.T, mr *miniredis.Miniredis, id string) *Cluster {
	t.Helper()
	rc := RedisConfig{
		Enabled: true, Required: true, Addr: mr.Addr(), Prefix: "pulsertc",
		Timeout: time.Second, NodeTTL: 15 * time.Second, ParticipTTL: 30 * time.Second,
	}
	st := newRedisClusterStateWith(
		newRedisClientFrom(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "pulsertc"),
		rc, testLogger())
	c, err := NewWithState(Config{
		Enabled: true, NodeID: id, Secret: "s", Host: "localhost", Port: 8092,
		HeartbeatInterval: time.Hour, StaleAfter: 15 * time.Second, Redis: rc,
	}, testLogger(), st)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRedisDownFailsClosed(t *testing.T) {
	c, mr := redisCluster(t, "node-a")
	c.Start()
	mr.Close() // Redis goes away

	// Health check via the heartbeat path equivalent.
	_ = c.state.Ping(context.Background())
	if c.Ready() {
		t.Fatal("node must be NOT READY while a required Redis is down")
	}
	_, err := c.ClaimRoom(context.Background(), "brand-new-room")
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeClusterStateUnavailable {
		t.Fatalf("join must fail closed with CLUSTER_STATE_UNAVAILABLE, got %v", err)
	}
}

func TestRedisReconnectRecoversReadiness(t *testing.T) {
	c, mr := redisCluster(t, "node-a")
	c.Start()
	addr := mr.Addr()
	mr.Close()
	_ = c.state.Ping(context.Background())
	if c.Ready() {
		t.Fatal("not ready expected while down")
	}
	mr2 := miniredis.NewMiniRedis()
	if err := mr2.StartAddr(addr); err != nil {
		t.Skipf("cannot rebind miniredis addr: %v", err)
	}
	defer mr2.Close()
	if err := c.state.Ping(context.Background()); err != nil {
		t.Fatalf("ping after reconnect: %v", err)
	}
	_ = c.registry.Register(c.currentSelf())
	if !c.Ready() {
		t.Fatal("node should be READY again after Redis reconnect")
	}
}
