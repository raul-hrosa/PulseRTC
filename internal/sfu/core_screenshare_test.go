package sfu

import (
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// Publication.Source falls back to a kind-derived default when the publisher
// never declared one, and honours an explicit "screen".
func TestPublicationDefaultSource(t *testing.T) {
	cases := []struct {
		name string
		pub  *Publication
		want string
	}{
		{"video default", &Publication{kind: webrtc.RTPCodecTypeVideo}, SourceCamera},
		{"audio default", &Publication{kind: webrtc.RTPCodecTypeAudio}, SourceMicrophone},
		{"explicit screen", &Publication{kind: webrtc.RTPCodecTypeVideo, source: SourceScreen}, SourceScreen},
	}
	for _, c := range cases {
		if got := c.pub.Source(); got != c.want {
			t.Errorf("%s: Source() = %q, want %q", c.name, got, c.want)
		}
	}
}

// A track declared as a screen share becomes an ordinary video Publication
// stamped source=screen; the label reaches subscribers and the metric counts it.
func TestScreenShareIsAnOrdinaryPublicationWithSource(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")
	stop := make(chan struct{})
	defer close(stop)

	tp := newTestPeer(t)
	screen := addSample(t, tp.pc, webrtc.MimeTypeVP8, "screen0", "A")
	peer, err := r.JoinWithPermissions("A", tp, FullPermissions())
	if err != nil {
		t.Fatalf("JoinWithPermissions: %v", err)
	}
	tp.peer = peer
	// The browser declares the source just before adding the track.
	peer.DeclareSource("screen0", SourceScreen)
	peer.SendExistingPublications()
	tp.offer()
	go pump(screen, stop)

	waitFor(t, "screen publication created", 5*time.Second, func() bool {
		p, ok := r.participant("A")
		return ok && len(p.publicationIDs()) == 1
	})
	pubID := r.mustParticipant(t, "A").publicationIDs()[0]
	pub, ok := r.findPublication(pubID)
	if !ok {
		t.Fatal("findPublication: not found")
	}
	if pub.Source() != SourceScreen {
		t.Fatalf("pub.Source() = %q, want %q", pub.Source(), SourceScreen)
	}
	if pub.Kind() != "video" {
		t.Fatalf("pub.Kind() = %q, want video", pub.Kind())
	}

	waitFor(t, "publication_added carries source=screen", 2*time.Second, func() bool {
		tp.mu.Lock()
		defer tp.mu.Unlock()
		return tp.pubs[pubID].Source == SourceScreen
	})

	if n := s.Metrics().ScreenPublicationsTotal; n != 1 {
		t.Fatalf("ScreenPublicationsTotal = %d, want 1", n)
	}

	// The diagnostics snapshot exposes it too.
	st := s.Stats("demo")
	var found bool
	for _, p := range st.Participants {
		for _, ps := range p.Publications {
			if ps.ID == pubID {
				found = true
				if ps.Source != SourceScreen {
					t.Fatalf("stats source = %q, want screen", ps.Source)
				}
			}
		}
	}
	if !found {
		t.Fatal("publication missing from /sfu/stats")
	}
}

// A normal camera track (no DeclareSource) keeps the default source, so screen
// and camera coexist as two distinct video publications.
func TestCameraAndScreenCoexist(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")
	stop := make(chan struct{})
	defer close(stop)

	tp := newTestPeer(t)
	cam := addSample(t, tp.pc, webrtc.MimeTypeVP8, "cam0", "A")
	screen := addSample(t, tp.pc, webrtc.MimeTypeVP8, "screen0", "A")
	peer, err := r.JoinWithPermissions("A", tp, FullPermissions())
	if err != nil {
		t.Fatalf("JoinWithPermissions: %v", err)
	}
	tp.peer = peer
	peer.DeclareSource("screen0", SourceScreen)
	peer.SendExistingPublications()
	tp.offer()
	go pump(cam, stop)
	go pump(screen, stop)

	waitFor(t, "both publications created", 5*time.Second, func() bool {
		p, ok := r.participant("A")
		return ok && len(p.publicationIDs()) == 2
	})

	sources := map[string]int{}
	for _, pub := range r.mustParticipant(t, "A").listPublications() {
		sources[pub.Source()]++
	}
	if sources[SourceCamera] != 1 || sources[SourceScreen] != 1 {
		t.Fatalf("sources = %v, want one camera + one screen", sources)
	}
}
