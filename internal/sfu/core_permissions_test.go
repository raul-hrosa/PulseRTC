package sfu

import (
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// joinWithPerms is join with an explicit permission set.
func joinWithPerms(t *testing.T, room *Room, id string, perms Permissions, setup func(pc *webrtc.PeerConnection)) *testPeer {
	t.Helper()
	tp := newTestPeer(t)
	if setup != nil {
		setup(tp.pc)
	}
	peer, err := room.JoinWithPermissions(id, tp, perms)
	if err != nil {
		t.Fatalf("JoinWithPermissions(%s): %v", id, err)
	}
	tp.peer = peer
	peer.SendExistingPublications()
	tp.offer()
	return tp
}

// A participant without PUBLISH must not create a Publication even though its
// browser sends a media track.
func TestPublishDenied(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")
	stop := make(chan struct{})
	defer close(stop)

	var track *webrtc.TrackLocalStaticSample
	tp := joinWithPerms(t, r, "A", Permissions{Publish: false, Subscribe: true}, func(pc *webrtc.PeerConnection) {
		track = addSample(t, pc, webrtc.MimeTypeVP8, "vidA", "A")
	})
	go pump(track, stop)

	waitFor(t, "publish_denied delivered", 5*time.Second, func() bool {
		return tp.sawType(MsgPublishDenied)
	})

	time.Sleep(300 * time.Millisecond)
	if p, ok := r.participant("A"); ok && len(p.publicationIDs()) != 0 {
		t.Fatalf("publication should not have been created without PUBLISH")
	}
}

// A participant without SUBSCRIBE cannot subscribe, explicitly or automatically.
func TestSubscribeDenied(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")
	stop := make(chan struct{})
	defer close(stop)

	var aVideo *webrtc.TrackLocalStaticSample
	join(t, r, "A", func(pc *webrtc.PeerConnection) {
		aVideo = addSample(t, pc, webrtc.MimeTypeVP8, "vidA", "A")
	})
	go pump(aVideo, stop)
	waitFor(t, "A publication", 5*time.Second, func() bool {
		p, ok := r.participant("A")
		return ok && len(p.publicationIDs()) == 1
	})
	pubID := r.mustParticipant(t, "A").publicationIDs()[0]

	b := joinWithPerms(t, r, "B", Permissions{Publish: true, Subscribe: false}, recvOnly(webrtc.RTPCodecTypeVideo))

	if err := b.peer.Subscribe(pubID); err != errSubscribeDenied {
		t.Fatalf("explicit Subscribe should be denied, got %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if len(b.peer.subscriptionIDs()) != 0 {
		t.Fatalf("auto-subscribe must not happen without SUBSCRIBE")
	}
	if !b.sawType(MsgSubscribeDenied) {
		t.Fatalf("expected subscribe_denied event")
	}
}
