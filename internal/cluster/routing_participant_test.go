package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestRouter_FailsClosedWhenStateUnavailable is: with Redis down the router
// cannot determine a participant's location, so it must error — never guess a
// node.
func TestRouter_FailsClosedWhenStateUnavailable(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := RedisConfig{
		Enabled: true, Required: true, Addr: mr.Addr(), Prefix: "pulsertc",
		Timeout: 200 * time.Millisecond, NodeTTL: 15 * time.Second, ParticipTTL: 30 * time.Second,
	}
	st := newRedisClusterStateWith(
		newRedisClientFrom(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "pulsertc"), rc, testLogger())
	c, err := NewWithState(Config{
		Enabled: true, NodeID: "n1", Secret: "s", Host: "localhost", Port: 8090,
		HeartbeatInterval: time.Hour, StaleAfter: time.Minute,
		RequestTimeout: 200 * time.Millisecond, MessageMaxAge: 30 * time.Second, MaxMessageSize: 64 * 1024,
		Redis: rc,
	}, testLogger(), st)
	if err != nil {
		t.Fatal(err)
	}
	c.SetLocalDelivery(newFakeDelivery())
	c.Start()
	t.Cleanup(c.Shutdown)

	mr.Close() // Redis goes away
	_ = st.Ping(context.Background())

	msg := c.NewClusterMessage(MsgParticipantMessage, "room-1", "somebody", json.RawMessage(`{}`))
	rerr := c.RouteMessage(context.Background(), "somebody", msg)
	ce, _ := rerr.(*Error)
	if ce == nil || ce.Code != CodeClusterStateUnavailable {
		t.Fatalf("router must fail closed with CLUSTER_STATE_UNAVAILABLE, got %v", rerr)
	}
}

// TestRouter_RecoversAfterStateReconnect is: routing works again once Redis
// is back — the transport is not permanently broken.
func TestRouter_RecoversAfterStateReconnect(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := RedisConfig{
		Enabled: true, Required: true, Addr: mr.Addr(), Prefix: "pulsertc",
		Timeout: 200 * time.Millisecond, NodeTTL: 15 * time.Second, ParticipTTL: 30 * time.Second,
	}
	st := newRedisClusterStateWith(
		newRedisClientFrom(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "pulsertc"), rc, testLogger())
	c, err := NewWithState(Config{
		Enabled: true, NodeID: "n1", Secret: "s", Host: "localhost", Port: 8090,
		HeartbeatInterval: time.Hour, StaleAfter: time.Minute,
		RequestTimeout: 200 * time.Millisecond, MessageMaxAge: 30 * time.Second, MaxMessageSize: 64 * 1024,
		Redis: rc,
	}, testLogger(), st)
	if err != nil {
		t.Fatal(err)
	}
	d := newFakeDelivery()
	d.addLocal("A")
	c.SetLocalDelivery(d)
	c.Start()
	t.Cleanup(c.Shutdown)
	c.RegisterParticipant("A", "room-1")

	addr := mr.Addr()
	mr.Close()
	_ = st.Ping(context.Background())
	if err := c.RouteMessage(context.Background(), "A",
		c.NewClusterMessage(MsgParticipantMessage, "room-1", "A", json.RawMessage(`{}`))); err == nil {
		t.Fatal("expected failure while Redis is down")
	}

	mr2 := miniredis.NewMiniRedis()
	if err := mr2.StartAddr(addr); err != nil {
		t.Skipf("cannot rebind miniredis: %v", err)
	}
	defer mr2.Close()
	if err := st.Ping(context.Background()); err != nil {
		t.Fatalf("ping after reconnect: %v", err)
	}
	c.RegisterParticipant("A", "room-1") // state rebuilt after reconnect

	if err := c.RouteMessage(context.Background(), "A",
		c.NewClusterMessage(MsgParticipantMessage, "room-1", "A", json.RawMessage(`{}`))); err != nil {
		t.Fatalf("routing should work again after Redis recovers: %v", err)
	}
	if d.count() != 1 {
		t.Fatalf("A should have received the message, got %d", d.count())
	}
}
