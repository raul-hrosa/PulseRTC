package sfu

import (
	"runtime"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// TestCrossNodePublisherNodeDeath kills the node that OWNS a publication which
// is mirrored onto another node, mid-call. The surviving node must retire the
// mirrored (synthetic) publication instead of keeping a dead track forever, and
// must keep working: a fresh local publisher on the survivor still reaches the
// subscriber that was watching the dead node's participant (Part F, F1).
func TestCrossNodePublisherNodeDeath(t *testing.T) {
	cn := newCrossNodeMedia(t)
	rA := cn.sfuA.Room("demo")
	rB := cn.sfuB.Room("demo")

	aliceStop := make(chan struct{})
	carolStop := make(chan struct{})
	defer close(carolStop)

	// --- node-a publishes, node-b mirrors, bob (on node-b) receives ----------
	var aVideo *webrtc.TrackLocalStaticSample
	join(t, rA, "alice", func(pc *webrtc.PeerConnection) {
		aVideo = addSample(t, pc, webrtc.MimeTypeVP8, "vidA", "alice")
	})
	go pump(aVideo, aliceStop)

	waitFor(t, "alice publication on node-a", 5*time.Second, func() bool {
		p, ok := rA.participant("alice")
		return ok && len(p.publicationIDs()) == 1
	})
	pubID := rA.mustParticipant(t, "alice").publicationIDs()[0]

	bob := join(t, rB, "bob", recvOnly(webrtc.RTPCodecTypeVideo))
	mirrorInto(t, cn.cluB, "node-a", "demo", pubID)

	waitFor(t, "bob receives alice's video across nodes", 20*time.Second, func() bool {
		return bob.trackIDs()[pubID]
	})
	if cn.cluB.MetricsSnapshot().Media.RTPReceived == 0 {
		t.Fatal("no inter-node RTP recorded on node-b before the kill")
	}
	if !cn.sfuB.RoomHasPublication("demo", pubID) {
		t.Fatal("node-b should hold the mirrored publication before the kill")
	}

	// --- kill node-a --------------------------------------------------------
	// Shutdown stops its heartbeat, closes the message transport and the media
	// bridge (and with it the UDP media transport). node-b is now talking to a
	// corpse. The harness's t.Cleanup shuts cluA down again; Cluster.Shutdown is
	// idempotent (stopOnce + mediaBridge.closed), so the double call is safe.
	close(aliceStop) // no more RTP from the dead node
	preKillGoroutines := runtime.NumGoroutine()
	cn.cluA.Shutdown()

	// --- the mirror must tear down on node-b --------------------------------
	// Either via the BYE / session-close path or, if the node vanished silently,
	// via the media SessionTimeout (3s here) in udpMediaConn.heartbeatLoop,
	// which calls notifyLost -> mediaBridge.onSessionLost -> MediaSession.Close
	// -> RemoteTrack.end -> RemotePublication.Close, the same teardown path
	// TestCrossNodeMedia_UnpublishEndsRemoteTrack relies on.
	waitFor(t, "node-b drops the mirrored publication", 10*time.Second, func() bool {
		return !cn.sfuB.RoomHasPublication("demo", pubID)
	})

	// No goroutine pile-up from the dead session. The package has no goleak
	// dependency, so this is a coarse before/after check with a settle window.
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	if now := runtime.NumGoroutine(); now > preKillGoroutines+10 {
		t.Fatalf("goroutine leak after node death: %d before kill, %d after teardown", preKillGoroutines, now)
	}

	// --- media resumes on the surviving node --------------------------------
	var cVideo *webrtc.TrackLocalStaticSample
	join(t, rB, "carol", func(pc *webrtc.PeerConnection) {
		cVideo = addSample(t, pc, webrtc.MimeTypeVP8, "vidC", "carol")
	})
	go pump(cVideo, carolStop)

	waitFor(t, "carol publication on node-b", 5*time.Second, func() bool {
		p, ok := rB.participant("carol")
		return ok && len(p.publicationIDs()) == 1
	})
	carolPub := rB.mustParticipant(t, "carol").publicationIDs()[0]

	waitFor(t, "bob receives carol's video locally after the node death", 15*time.Second, func() bool {
		return bob.trackIDs()[carolPub]
	})
}
