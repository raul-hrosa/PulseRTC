package sfu

import (
	"sync"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// browserAPI builds a webrtc.API that stands in for a real browser in tests.
func browserAPI(t *testing.T) *webrtc.API {
	t.Helper()
	me := &webrtc.MediaEngine{}
	if err := me.RegisterDefaultCodecs(); err != nil {
		t.Fatalf("RegisterDefaultCodecs: %v", err)
	}
	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(me, ir); err != nil {
		t.Fatalf("RegisterDefaultInterceptors: %v", err)
	}
	return webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir))
}

// testPeer is a fake browser wired to one SFU Participant through the same
// Transport interface the real WebSocket client implements. It plays the
// "polite" role of perfect negotiation.
type testPeer struct {
	t    *testing.T
	pc   *webrtc.PeerConnection
	peer *Participant

	mu          sync.Mutex
	tracks      map[string]*webrtc.TrackRemote // key: track id == publication id
	pubs        map[string]publicationEvent    // publication_added/removed/muted seen
	seen        map[string]bool                // signalEnvelope types received
	makingOffer bool
	remoteOK    bool
	pendICE     []webrtc.ICECandidateInit
}

func newTestPeer(t *testing.T) *testPeer {
	pc, err := browserAPI(t).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("browser NewPeerConnection: %v", err)
	}
	tp := &testPeer{t: t, pc: pc, tracks: map[string]*webrtc.TrackRemote{}, pubs: map[string]publicationEvent{}, seen: map[string]bool{}}
	pc.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		tp.mu.Lock()
		tp.tracks[tr.ID()] = tr
		tp.mu.Unlock()
	})
	return tp
}

func (tp *testPeer) flushICE() {
	tp.mu.Lock()
	tp.remoteOK = true
	pend := tp.pendICE
	tp.pendICE = nil
	tp.mu.Unlock()
	for _, c := range pend {
		_ = tp.pc.AddICECandidate(c)
	}
}

func (tp *testPeer) SendSFU(msg any) {
	switch m := msg.(type) {
	case signalEnvelope:
		tp.mu.Lock()
		tp.seen[m.Type] = true
		tp.mu.Unlock()
		tp.handleSignal(m)
	case publicationEvent:
		tp.mu.Lock()
		if m.Type == MsgPublicationRemoved {
			delete(tp.pubs, m.PublicationID)
		} else {
			tp.pubs[m.PublicationID] = m
		}
		tp.mu.Unlock()
	case subscriptionEvent:
		// not asserted directly
	}
}

func (tp *testPeer) handleSignal(m signalEnvelope) {
	switch m.Type {
	case MsgAnswer:
		if err := tp.pc.SetRemoteDescription(webrtc.SessionDescription{
			Type: webrtc.SDPTypeAnswer, SDP: m.Payload.(sdpPayload).SDP,
		}); err != nil {
			tp.t.Errorf("browser SetRemoteDescription(answer): %v", err)
			return
		}
		tp.flushICE()

	case MsgOffer:
		sdp := m.Payload.(sdpPayload).SDP
		tp.mu.Lock()
		collision := tp.makingOffer || tp.pc.SignalingState() != webrtc.SignalingStateStable
		tp.mu.Unlock()
		if collision {
			_ = tp.pc.SetLocalDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeRollback})
		}
		if err := tp.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}); err != nil {
			tp.t.Errorf("browser SetRemoteDescription(offer): %v", err)
			return
		}
		tp.flushICE()
		ans, err := tp.pc.CreateAnswer(nil)
		if err != nil {
			tp.t.Errorf("browser CreateAnswer: %v", err)
			return
		}
		gather := webrtc.GatheringCompletePromise(tp.pc)
		if err := tp.pc.SetLocalDescription(ans); err != nil {
			tp.t.Errorf("browser SetLocalDescription(answer): %v", err)
			return
		}
		<-gather
		go func() {
			if err := tp.peer.HandleAnswer(tp.pc.LocalDescription().SDP); err != nil {
				tp.t.Errorf("SFU HandleAnswer: %v", err)
			}
		}()

	case MsgICECandidate:
		init := m.Payload.(webrtc.ICECandidateInit)
		tp.mu.Lock()
		if !tp.remoteOK {
			tp.pendICE = append(tp.pendICE, init)
			tp.mu.Unlock()
			return
		}
		tp.mu.Unlock()
		_ = tp.pc.AddICECandidate(init)
	}
}

// offer performs a browser-initiated negotiation (initial join, dynamic publish).
func (tp *testPeer) offer() {
	tp.t.Helper()
	tp.mu.Lock()
	tp.makingOffer = true
	tp.mu.Unlock()
	defer func() {
		tp.mu.Lock()
		tp.makingOffer = false
		tp.mu.Unlock()
	}()

	offer, err := tp.pc.CreateOffer(nil)
	if err != nil {
		tp.t.Fatalf("browser CreateOffer: %v", err)
	}
	gather := webrtc.GatheringCompletePromise(tp.pc)
	if err := tp.pc.SetLocalDescription(offer); err != nil {
		tp.t.Fatalf("browser SetLocalDescription(offer): %v", err)
	}
	<-gather
	if err := tp.peer.HandleOffer(tp.pc.LocalDescription().SDP); err != nil {
		tp.t.Fatalf("SFU HandleOffer: %v", err)
	}
}

func (tp *testPeer) sawType(typ string) bool {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	return tp.seen[typ]
}

func (tp *testPeer) trackIDs() map[string]bool {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	out := map[string]bool{}
	for id := range tp.tracks {
		out[id] = true
	}
	return out
}

func (tp *testPeer) publicationIDs() map[string]publicationEvent {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	out := map[string]publicationEvent{}
	for k, v := range tp.pubs {
		out[k] = v
	}
	return out
}

// join connects the fake browser to the room and completes the first handshake.
func join(t *testing.T, room *Room, id string, setup func(pc *webrtc.PeerConnection)) *testPeer {
	t.Helper()
	tp := newTestPeer(t)
	if setup != nil {
		setup(tp.pc)
	}
	peer, err := room.Join(id, tp)
	if err != nil {
		t.Fatalf("room.Join(%s): %v", id, err)
	}
	tp.peer = peer
	peer.SendExistingPublications() // the signaling layer does this on join
	tp.offer()
	return tp
}

func addSample(t *testing.T, pc *webrtc.PeerConnection, mime, id, stream string) *webrtc.TrackLocalStaticSample {
	t.Helper()
	tr, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: mime}, id, stream)
	if err != nil {
		t.Fatalf("NewTrackLocalStaticSample: %v", err)
	}
	if _, err := pc.AddTrack(tr); err != nil {
		t.Fatalf("browser AddTrack: %v", err)
	}
	return tr
}

func recvOnly(kinds ...webrtc.RTPCodecType) func(pc *webrtc.PeerConnection) {
	return func(pc *webrtc.PeerConnection) {
		for _, k := range kinds {
			if _, err := pc.AddTransceiverFromKind(k, webrtc.RTPTransceiverInit{
				Direction: webrtc.RTPTransceiverDirectionRecvonly,
			}); err != nil {
				panic(err)
			}
		}
	}
}

func pump(track *webrtc.TrackLocalStaticSample, stop <-chan struct{}) {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			_ = track.WriteSample(media.Sample{Data: []byte{0, 1, 2, 3}, Duration: 20 * time.Millisecond})
		}
	}
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// --- Scenario 1 & 2: A publishes video, B and C subscribe --------------------

func TestScenarioPublisherToTwoSubscribers(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")
	stop := make(chan struct{})
	defer close(stop)

	var aVideo *webrtc.TrackLocalStaticSample
	join(t, r, "A", func(pc *webrtc.PeerConnection) {
		aVideo = addSample(t, pc, webrtc.MimeTypeVP8, "vidA", "A")
	})
	go pump(aVideo, stop)

	waitFor(t, "A publication registered", 5*time.Second, func() bool {
		p, ok := r.participant("A")
		return ok && len(p.publicationIDs()) == 1
	})
	aVideoPub := r.mustParticipant(t, "A").publicationIDs()[0]

	b := join(t, r, "B", recvOnly(webrtc.RTPCodecTypeVideo))
	waitFor(t, "B receives A video", 15*time.Second, func() bool { return b.trackIDs()[aVideoPub] })

	c := join(t, r, "C", recvOnly(webrtc.RTPCodecTypeVideo))
	waitFor(t, "C receives existing A video", 15*time.Second, func() bool { return c.trackIDs()[aVideoPub] })

	st := s.Stats("demo")
	roles := map[string]string{}
	for _, p := range st.Participants {
		roles[p.ID] = p.Role
	}
	if roles["A"] != "publisher" || roles["B"] != "subscriber" || roles["C"] != "subscriber" {
		t.Fatalf("unexpected roles: %v", roles)
	}

	// B leaves: A keeps publishing, C keeps receiving.
	r.Leave("B")
	waitFor(t, "B gone", 5*time.Second, func() bool { _, ok := r.participant("B"); return !ok })
	if _, ok := r.participant("A"); !ok {
		t.Fatal("A disappeared when B left")
	}
	if _, ok := r.participant("C"); !ok {
		t.Fatal("C disappeared when B left")
	}
}

// --- Scenario 3: selective subscription (B gets video only) ------------------

func TestScenarioSelectiveSubscription(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")
	stop := make(chan struct{})
	defer close(stop)

	var aAudio, aVideo *webrtc.TrackLocalStaticSample
	join(t, r, "A", func(pc *webrtc.PeerConnection) {
		aAudio = addSample(t, pc, webrtc.MimeTypeOpus, "audA", "A")
		aVideo = addSample(t, pc, webrtc.MimeTypeVP8, "vidA", "A")
	})
	go pump(aAudio, stop)
	go pump(aVideo, stop)

	waitFor(t, "A has 2 publications", 5*time.Second, func() bool {
		p, ok := r.participant("A")
		return ok && len(p.publicationIDs()) == 2
	})

	b := join(t, r, "B", recvOnly(webrtc.RTPCodecTypeAudio, webrtc.RTPCodecTypeVideo))

	// B learns both publications, then opts out of audio BEFORE connecting.
	waitFor(t, "B sees A's publications", 5*time.Second, func() bool { return len(b.publicationIDs()) == 2 })
	var audioPub, videoPub string
	for id, ev := range b.publicationIDs() {
		if ev.Kind == "audio" {
			audioPub = id
		} else {
			videoPub = id
		}
	}
	b.peer.Unsubscribe(audioPub)

	waitFor(t, "B receives A video", 15*time.Second, func() bool { return b.trackIDs()[videoPub] })

	// Give the SFU time to (not) deliver audio.
	time.Sleep(1 * time.Second)
	if b.trackIDs()[audioPub] {
		t.Fatal("B received audio despite unsubscribing from it")
	}

	// B can subscribe to audio later.
	if err := b.peer.Subscribe(audioPub); err != nil {
		t.Fatalf("Subscribe(audio): %v", err)
	}
	waitFor(t, "B now receives A audio", 15*time.Second, func() bool { return b.trackIDs()[audioPub] })
}

// --- Scenario 4: dynamic unpublish notifies subscribers ---------------------

func TestScenarioDynamicUnpublish(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")
	stop := make(chan struct{})
	defer close(stop)

	var aVideo *webrtc.TrackLocalStaticSample
	a := join(t, r, "A", func(pc *webrtc.PeerConnection) {
		aVideo = addSample(t, pc, webrtc.MimeTypeVP8, "vidA", "A")
	})
	go pump(aVideo, stop)

	waitFor(t, "A publication", 5*time.Second, func() bool {
		p, ok := r.participant("A")
		return ok && len(p.publicationIDs()) == 1
	})
	videoPub := a.peer.publicationIDs()[0]

	b := join(t, r, "B", recvOnly(webrtc.RTPCodecTypeVideo))
	waitFor(t, "B receives A video", 15*time.Second, func() bool { return b.trackIDs()[videoPub] })

	// A unpublishes video.
	a.peer.Unpublish(videoPub)

	waitFor(t, "B notified of publication_removed", 5*time.Second, func() bool {
		_, still := b.publicationIDs()[videoPub]
		return !still
	})
	waitFor(t, "B subscription cleaned", 5*time.Second, func() bool {
		return len(b.peer.subscriptionIDs()) == 0
	})
}

// --- Multi-publisher: A and B publish, C receives everyone ------------------

func TestMultiPublisher(t *testing.T) {
	s := newTestSFU(t)
	r := s.Room("demo")
	stop := make(chan struct{})
	defer close(stop)

	var aV, bV *webrtc.TrackLocalStaticSample
	join(t, r, "A", func(pc *webrtc.PeerConnection) { aV = addSample(t, pc, webrtc.MimeTypeVP8, "vA", "A") })
	join(t, r, "B", func(pc *webrtc.PeerConnection) { bV = addSample(t, pc, webrtc.MimeTypeVP8, "vB", "B") })
	go pump(aV, stop)
	go pump(bV, stop)

	waitFor(t, "A and B each have a publication", 5*time.Second, func() bool {
		pa, oka := r.participant("A")
		pb, okb := r.participant("B")
		return oka && okb && len(pa.publicationIDs()) == 1 && len(pb.publicationIDs()) == 1
	})
	aPub := r.mustParticipant(t, "A").publicationIDs()[0]
	bPub := r.mustParticipant(t, "B").publicationIDs()[0]
	if aPub == bPub {
		t.Fatal("publication ids collided across publishers")
	}

	c := join(t, r, "C", recvOnly(webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeVideo))
	waitFor(t, "C receives A and B", 20*time.Second, func() bool {
		ids := c.trackIDs()
		return ids[aPub] && ids[bPub]
	})
}

// --- Cross-room subscription is rejected -----------------------------------

func TestCrossRoomSubscriptionRejected(t *testing.T) {
	s := newTestSFU(t)
	ra := s.Room("room-a")
	rb := s.Room("room-b")
	stop := make(chan struct{})
	defer close(stop)

	var cV *webrtc.TrackLocalStaticSample
	join(t, rb, "C", func(pc *webrtc.PeerConnection) { cV = addSample(t, pc, webrtc.MimeTypeVP8, "vC", "C") })
	go pump(cV, stop)
	waitFor(t, "C publication", 5*time.Second, func() bool {
		p, ok := rb.participant("C")
		return ok && len(p.publicationIDs()) == 1
	})
	cPub := rb.mustParticipant(t, "C").publicationIDs()[0]

	a := join(t, ra, "A", recvOnly(webrtc.RTPCodecTypeVideo))
	if err := a.peer.Subscribe(cPub); err != errNotFound {
		t.Fatalf("cross-room Subscribe should fail with errNotFound, got %v", err)
	}
}

// test helper
func (r *Room) mustParticipant(t *testing.T, id string) *Participant {
	t.Helper()
	p, ok := r.participant(id)
	if !ok {
		t.Fatalf("participant %s not found", id)
	}
	return p
}
