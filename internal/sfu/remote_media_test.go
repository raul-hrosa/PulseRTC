package sfu

import (
	"context"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/raulhrosa/pulsertc/internal/cluster"
)

// crossNodeMedia wires two SFUs to two clusters that share an in-process
// signaling transport and talk media over real loopback UDP.
type crossNodeMedia struct {
	sfuA, sfuB *SFU
	cluA, cluB *cluster.Cluster
}

func newCrossNodeMedia(t *testing.T) *crossNodeMedia {
	t.Helper()
	st := cluster.NewInMemoryClusterState(time.Minute)
	mk := func(id string, s *SFU) *cluster.Cluster {
		c, err := cluster.NewWithState(cluster.Config{
			Enabled: true, NodeID: id, Secret: "media-secret", Host: "127.0.0.1", Port: 8090,
			HeartbeatInterval: time.Hour, StaleAfter: time.Minute,
			RequestTimeout: time.Second, MessageMaxAge: 30 * time.Second, MaxMessageSize: 64 * 1024,
			Media: cluster.MediaConfig{
				Enabled: true, BindAddr: "127.0.0.1", Port: 0, AdvertiseHost: "127.0.0.1",
				RequestTimeout: time.Second, MaxPacketSize: 1500,
				SessionTimeout: 3 * time.Second, HeartbeatInterval: 200 * time.Millisecond,
				SendQueue: 512,
			},
		}, testLogger(), st)
		if err != nil {
			t.Fatal(err)
		}
		c.Media().SetSource(s)
		c.Media().SetSink(s)
		s.SetMediaPublicationEndedHook(c.Media().PublicationEnded)
		return c
	}
	sfuA, sfuB := newTestSFU(t), newTestSFU(t)
	cluA, cluB := mk("node-a", sfuA), mk("node-b", sfuB)
	nodes := map[string]*cluster.Cluster{"node-a": cluA, "node-b": cluB}
	cluA.UseInMemoryMessageTransport(nodes)
	cluB.UseInMemoryMessageTransport(nodes)
	cluA.Start()
	cluB.Start()
	t.Cleanup(func() { cluA.Shutdown(); cluB.Shutdown() })
	return &crossNodeMedia{sfuA: sfuA, sfuB: sfuB, cluA: cluA, cluB: cluB}
}

// mirrorInto is what the signaling layer does: node `into` mirrors a publication
// that physically lives on `from`.
func mirrorInto(t *testing.T, into *cluster.Cluster, fromNodeID, roomID, pubID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := into.Media().SubscribeRemote(ctx, fromNodeID, roomID, pubID, "__node__"); err != nil {
		t.Fatalf("SubscribeRemote %s: %v", pubID, err)
	}
}

func TestCrossNodeMedia_AToB(t *testing.T) {
	cn := newCrossNodeMedia(t)
	rA := cn.sfuA.Room("demo")
	rB := cn.sfuB.Room("demo")
	stop := make(chan struct{})
	defer close(stop)

	var aVideo *webrtc.TrackLocalStaticSample
	join(t, rA, "alice", func(pc *webrtc.PeerConnection) {
		aVideo = addSample(t, pc, webrtc.MimeTypeVP8, "vidA", "alice")
	})
	go pump(aVideo, stop)

	waitFor(t, "alice publication on node-a", 5*time.Second, func() bool {
		p, ok := rA.participant("alice")
		return ok && len(p.publicationIDs()) == 1
	})
	pubID := rA.mustParticipant(t, "alice").publicationIDs()[0]

	// bob joins node-b first, then node-b mirrors alice's publication.
	bob := join(t, rB, "bob", recvOnly(webrtc.RTPCodecTypeVideo))
	mirrorInto(t, cn.cluB, "node-a", "demo", pubID)

	waitFor(t, "bob receives alice's video across nodes", 20*time.Second, func() bool {
		return bob.trackIDs()[pubID]
	})

	// The media crossed the UDP transport, not signaling / Redis.
	m := cn.cluB.MetricsSnapshot().Media
	if m.RTPReceived == 0 {
		t.Fatal("no inter-node RTP recorded on node-b")
	}
	if cn.cluA.MetricsSnapshot().Media.RTPSent == 0 {
		t.Fatal("no inter-node RTP recorded as sent on node-a")
	}
}

func TestCrossNodeMedia_Bidirectional(t *testing.T) {
	cn := newCrossNodeMedia(t)
	rA := cn.sfuA.Room("demo")
	rB := cn.sfuB.Room("demo")
	stop := make(chan struct{})
	defer close(stop)

	var aAudio, bVideo *webrtc.TrackLocalStaticSample
	alice := join(t, rA, "alice", func(pc *webrtc.PeerConnection) {
		aAudio = addSample(t, pc, webrtc.MimeTypeOpus, "audA", "alice")
		recvOnly(webrtc.RTPCodecTypeVideo)(pc)
	})
	bob := join(t, rB, "bob", func(pc *webrtc.PeerConnection) {
		bVideo = addSample(t, pc, webrtc.MimeTypeVP8, "vidB", "bob")
		recvOnly(webrtc.RTPCodecTypeAudio)(pc)
	})
	go pump(aAudio, stop)
	go pump(bVideo, stop)

	waitFor(t, "both publications exist", 5*time.Second, func() bool {
		pa, oka := rA.participant("alice")
		pb, okb := rB.participant("bob")
		return oka && okb && len(pa.publicationIDs()) == 1 && len(pb.publicationIDs()) == 1
	})
	aPub := rA.mustParticipant(t, "alice").publicationIDs()[0]
	bPub := rB.mustParticipant(t, "bob").publicationIDs()[0]

	mirrorInto(t, cn.cluB, "node-a", "demo", aPub) // alice audio -> bob
	mirrorInto(t, cn.cluA, "node-b", "demo", bPub) // bob video -> alice

	waitFor(t, "bob hears alice", 20*time.Second, func() bool { return bob.trackIDs()[aPub] })
	waitFor(t, "alice sees bob", 20*time.Second, func() bool { return alice.trackIDs()[bPub] })
}

func TestCrossNodeMedia_UnpublishEndsRemoteTrack(t *testing.T) {
	cn := newCrossNodeMedia(t)
	rA := cn.sfuA.Room("demo")
	rB := cn.sfuB.Room("demo")
	stop := make(chan struct{})
	defer close(stop)

	var aVideo *webrtc.TrackLocalStaticSample
	alice := join(t, rA, "alice", func(pc *webrtc.PeerConnection) {
		aVideo = addSample(t, pc, webrtc.MimeTypeVP8, "vidA", "alice")
	})
	go pump(aVideo, stop)
	waitFor(t, "alice publication", 5*time.Second, func() bool {
		p, ok := rA.participant("alice")
		return ok && len(p.publicationIDs()) == 1
	})
	pubID := rA.mustParticipant(t, "alice").publicationIDs()[0]

	bob := join(t, rB, "bob", recvOnly(webrtc.RTPCodecTypeVideo))
	mirrorInto(t, cn.cluB, "node-a", "demo", pubID)
	waitFor(t, "bob receives video", 20*time.Second, func() bool { return bob.trackIDs()[pubID] })

	// alice unpublishes -> node-b's synthetic publication must disappear.
	alice.peer.Unpublish(pubID)
	waitFor(t, "node-b drops the mirrored publication", 5*time.Second, func() bool {
		return !cn.sfuB.RoomHasPublication("demo", pubID)
	})
}
