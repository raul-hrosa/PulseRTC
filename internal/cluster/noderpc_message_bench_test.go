package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// Run:
//
//	go test ./internal/cluster/ -run=^$ -bench='Route|ClusterMessage|Dedup' -benchmem
//
// Goal: the local route path allocates as little as possible — it must not be a
// tax on same-node signaling.

func benchMiniCluster(b *testing.B, ids ...string) *miniCluster {
	b.Helper()
	st := NewInMemoryClusterState(time.Minute)
	mc := &miniCluster{nodes: map[string]*Cluster{}, del: map[string]*fakeDelivery{}}
	for _, id := range ids {
		c, _ := NewWithState(Config{
			Enabled: true, NodeID: id, Secret: "s", Host: "localhost", Port: 8090,
			HeartbeatInterval: time.Hour, StaleAfter: time.Minute,
			RequestTimeout: time.Second, MessageMaxAge: 30 * time.Second, MaxMessageSize: 64 * 1024,
		}, testLogger(), st)
		d := newFakeDelivery()
		c.SetLocalDelivery(d)
		mc.nodes[id] = c
		mc.del[id] = d
	}
	for _, c := range mc.nodes {
		c.UseInMemoryMessageTransport(mc.nodes)
		c.Start()
	}
	b.Cleanup(func() {
		for _, c := range mc.nodes {
			c.Shutdown()
		}
	})
	return mc
}

func BenchmarkLocalRoute(b *testing.B) {
	mc := benchMiniCluster(b, "n1")
	mc.connect("n1", "A", "room-1")
	c := mc.nodes["n1"]
	payload := json.RawMessage(`{"k":"v"}`)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := c.NewClusterMessage(MsgParticipantMessage, "room-1", "A", payload)
		if err := c.RouteMessage(ctx, "A", msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRemoteRoute(b *testing.B) {
	mc := benchMiniCluster(b, "n1", "n2")
	mc.connect("n2", "B", "room-1")
	c := mc.nodes["n1"]
	payload := json.RawMessage(`{"k":"v"}`)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := c.NewClusterMessage(MsgParticipantMessage, "room-1", "B", payload)
		if err := c.RouteMessage(ctx, "B", msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterMessageValidation(b *testing.B) {
	m := validMsg()
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if e := m.Validate(now, 30*time.Second, 64*1024); e != nil {
			b.Fatal(e)
		}
	}
}

func BenchmarkMessageDeduplication(b *testing.B) {
	d := newDedupCache(time.Minute)
	ids := make([]string, 1024)
	for i := range ids {
		ids[i] = fmt.Sprintf("req-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.markSeen(ids[i&1023])
	}
}
