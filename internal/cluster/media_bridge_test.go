package cluster

import (
	"context"
	"sync"
	"testing"
	"time"
)

// --- fake SFU (both MediaSource and MediaSink) --------------------------------

type fakeRemotePub struct {
	mu     sync.Mutex
	rtp    [][]byte
	closed bool
}

func (f *fakeRemotePub) WriteRTP(pkt []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(pkt))
	copy(cp, pkt)
	f.rtp = append(f.rtp, cp)
	return nil
}
func (f *fakeRemotePub) WriteRTCP([]byte) error { return nil }
func (f *fakeRemotePub) Close()                 { f.mu.Lock(); f.closed = true; f.mu.Unlock() }
func (f *fakeRemotePub) count() int             { f.mu.Lock(); defer f.mu.Unlock(); return len(f.rtp) }
func (f *fakeRemotePub) isClosed() bool         { f.mu.Lock(); defer f.mu.Unlock(); return f.closed }

type fakeMediaSFU struct {
	mu           sync.Mutex
	pubs         map[string]RemotePublicationInfo // "room|pub"
	sinks        map[string]func([]byte)          // "room|pub" -> onRTP forwarder
	remotePubs   map[string]*fakeRemotePub        // "room|pub"
	rtcpToOrigin map[string]func([]byte)
	rtcpFromSub  map[string][][]byte // publisher side: feedback received
}

func newFakeMediaSFU() *fakeMediaSFU {
	return &fakeMediaSFU{
		pubs: map[string]RemotePublicationInfo{}, sinks: map[string]func([]byte){},
		remotePubs: map[string]*fakeRemotePub{}, rtcpToOrigin: map[string]func([]byte){},
		rtcpFromSub: map[string][][]byte{},
	}
}

func (f *fakeMediaSFU) addLocalPub(info RemotePublicationInfo) {
	f.mu.Lock()
	f.pubs[info.RoomID+"|"+info.PublicationID] = info
	f.mu.Unlock()
}

func (f *fakeMediaSFU) AttachForwarder(roomID, pubID string, onRTP func(pkt []byte)) (RemotePublicationInfo, func(), bool) {
	key := roomID + "|" + pubID
	f.mu.Lock()
	info, ok := f.pubs[key]
	if ok {
		f.sinks[key] = onRTP
	}
	f.mu.Unlock()
	if !ok {
		return RemotePublicationInfo{}, nil, false
	}
	return info, func() { f.mu.Lock(); delete(f.sinks, key); f.mu.Unlock() }, true
}

func (f *fakeMediaSFU) DeliverRTCPToPublisher(roomID, pubID string, pkt []byte) {
	f.mu.Lock()
	f.rtcpFromSub[roomID+"|"+pubID] = append(f.rtcpFromSub[roomID+"|"+pubID], pkt)
	f.mu.Unlock()
}

func (f *fakeMediaSFU) AddRemotePublication(info RemotePublicationInfo, rtcpToOrigin func(pkt []byte)) (RemotePublication, error) {
	key := info.RoomID + "|" + info.PublicationID
	rp := &fakeRemotePub{}
	f.mu.Lock()
	f.remotePubs[key] = rp
	f.rtcpToOrigin[key] = rtcpToOrigin
	f.mu.Unlock()
	return rp, nil
}

// --- 2-node media harness ---------------------------------------------------

type mediaPair struct {
	a, b *Cluster
	sfuA *fakeMediaSFU
	sfuB *fakeMediaSFU
}

func newMediaPair(t *testing.T) *mediaPair {
	t.Helper()
	st := NewInMemoryClusterState(time.Minute)
	mk := func(id string) *Cluster {
		mc := testMediaCfg()
		c, err := NewWithState(Config{
			Enabled: true, NodeID: id, Secret: "media-secret", Host: "127.0.0.1", Port: 8090,
			HeartbeatInterval: time.Hour, StaleAfter: time.Minute,
			RequestTimeout: time.Second, MessageMaxAge: 30 * time.Second, MaxMessageSize: 64 * 1024,
			Media: mc,
		}, testLogger(), st)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a, b := mk("node-a"), mk("node-b")
	nodes := map[string]*Cluster{"node-a": a, "node-b": b}
	a.UseInMemoryMessageTransport(nodes)
	b.UseInMemoryMessageTransport(nodes)
	a.Start()
	b.Start()

	p := &mediaPair{a: a, b: b, sfuA: newFakeMediaSFU(), sfuB: newFakeMediaSFU()}
	a.Media().SetSource(p.sfuA)
	a.Media().SetSink(p.sfuA)
	b.Media().SetSource(p.sfuB)
	b.Media().SetSink(p.sfuB)
	t.Cleanup(func() { a.Shutdown(); b.Shutdown() })
	return p
}

func TestMediaBridge_SubscribeRemoteRTPFlows(t *testing.T) {
	p := newMediaPair(t)
	info := RemotePublicationInfo{
		RoomID: "r1", PublicationID: "pub-1", ParticipantID: "alice", OriginNodeID: "node-a",
		Kind: "video", MimeType: "video/VP8", ClockRate: 90000, PayloadType: 96, SSRC: 111,
	}
	p.sfuA.addLocalPub(info)

	rt, err := p.b.Media().SubscribeRemote(context.Background(), "node-a", "r1", "pub-1", "bob")
	if err != nil {
		t.Fatalf("SubscribeRemote: %v", err)
	}
	if rt.State() != RemoteTrackActive {
		t.Fatalf("remote track state = %s", rt.State())
	}

	// node-a "publishes": drive the forwarder like ingestLoop would.
	p.sfuA.mu.Lock()
	fwd := p.sfuA.sinks["r1|pub-1"]
	p.sfuA.mu.Unlock()
	if fwd == nil {
		t.Fatal("forwarder not attached on node-a")
	}
	pkt := []byte{0x80, 0x60, 0, 1, 0, 0, 0, 0, 0, 0, 0, 111, 9, 9, 9, 9}
	for i := 0; i < 5; i++ {
		fwd(pkt)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.sfuB.mu.Lock()
		rp := p.sfuB.remotePubs["r1|pub-1"]
		p.sfuB.mu.Unlock()
		if rp != nil && rp.count() >= 5 {
			// RTCP reverse path.
			p.sfuB.mu.Lock()
			back := p.sfuB.rtcpToOrigin["r1|pub-1"]
			p.sfuB.mu.Unlock()
			back([]byte{0x81, 0xce, 0, 2, 1, 2, 3, 4})
			time.Sleep(200 * time.Millisecond)
			p.sfuA.mu.Lock()
			n := len(p.sfuA.rtcpFromSub["r1|pub-1"])
			p.sfuA.mu.Unlock()
			if n == 0 {
				t.Fatal("RTCP feedback did not reach the origin node")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("RTP did not cross to node-b")
}

func TestMediaBridge_SubscribeUnknownPublication(t *testing.T) {
	p := newMediaPair(t)
	_, err := p.b.Media().SubscribeRemote(context.Background(), "node-a", "r1", "ghost", "bob")
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeRemoteTrackNotFound {
		t.Fatalf("want REMOTE_TRACK_NOT_FOUND, got %v", err)
	}
}

func TestMediaBridge_NodeOfflineEndsRemoteTrack(t *testing.T) {
	p := newMediaPair(t)
	info := RemotePublicationInfo{
		RoomID: "r1", PublicationID: "pub-1", ParticipantID: "alice", OriginNodeID: "node-a",
		Kind: "audio", MimeType: "audio/opus", ClockRate: 48000, Channels: 2, PayloadType: 111, SSRC: 5,
	}
	p.sfuA.addLocalPub(info)
	rt, err := p.b.Media().SubscribeRemote(context.Background(), "node-a", "r1", "pub-1", "bob")
	if err != nil {
		t.Fatal(err)
	}

	p.a.Shutdown() // node-a vanishes

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p.sfuB.mu.Lock()
		rp := p.sfuB.remotePubs["r1|pub-1"]
		p.sfuB.mu.Unlock()
		if rt.State() == RemoteTrackEnded && rp != nil && rp.isClosed() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("remote track not cleaned up after node offline (state=%s)", rt.State())
}

// TestMediaSessionSilentDeathEndsRemoteTrack is the ungraceful twin of
// TestMediaBridge_NodeOfflineEndsRemoteTrack: node-a does NOT get to say
// goodbye. Its UDP socket is hard-closed underneath the bridge, so no BYE and
// no heartbeat ever reach node-b again. node-b must notice on its own via the
// SessionTimeout path (udpMediaConn.heartbeatLoop -> notifyLost ->
// mediaBridge.onSessionLost -> MediaSession.Close -> RemoteTrack.end) and,
// crucially, that must reach the SINK so the mirrored publication is retired —
// the end-to-end half TestMediaTransport_HeartbeatTimeoutClosesSession does not
// cover (it stops at a bare lost-handler channel).
func TestMediaSessionSilentDeathEndsRemoteTrack(t *testing.T) {
	p := newMediaPair(t)
	info := RemotePublicationInfo{
		RoomID: "r1", PublicationID: "pub-1", ParticipantID: "alice", OriginNodeID: "node-a",
		Kind: "video", MimeType: "video/VP8", ClockRate: 90000, PayloadType: 96, SSRC: 111,
	}
	p.sfuA.addLocalPub(info)

	rt, err := p.b.Media().SubscribeRemote(context.Background(), "node-a", "r1", "pub-1", "bob")
	if err != nil {
		t.Fatalf("SubscribeRemote: %v", err)
	}
	if rt.State() != RemoteTrackActive {
		t.Fatalf("remote track state = %s", rt.State())
	}

	// Prove media is really flowing before we kill anything.
	p.sfuA.mu.Lock()
	fwd := p.sfuA.sinks["r1|pub-1"]
	p.sfuA.mu.Unlock()
	if fwd == nil {
		t.Fatal("forwarder not attached on node-a")
	}
	pkt := []byte{0x80, 0x60, 0, 1, 0, 0, 0, 0, 0, 0, 0, 111, 9, 9, 9, 9}
	for i := 0; i < 5; i++ {
		fwd(pkt)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.sfuB.mu.Lock()
		rp := p.sfuB.remotePubs["r1|pub-1"]
		p.sfuB.mu.Unlock()
		if rp != nil && rp.count() >= 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	p.sfuB.mu.Lock()
	rpB := p.sfuB.remotePubs["r1|pub-1"]
	p.sfuB.mu.Unlock()
	if rpB == nil || rpB.count() == 0 {
		t.Fatal("RTP did not cross to node-b before the kill")
	}
	if rpB.isClosed() {
		t.Fatal("mirrored publication already closed before the kill")
	}

	// Hard-kill node-a: close the raw UDP socket out from under the bridge. Every
	// subsequent writeTo (including the best-effort BYE in udpMediaConn.Close)
	// fails, and the read loop dies — node-a simply goes silent. This is NOT
	// a.Shutdown(), which would deliver a BYE and short-circuit the timeout.
	killedAt := time.Now()
	if err := p.a.Media().(*mediaBridge).transport.conn.Close(); err != nil {
		t.Fatalf("hard-close node-a media socket: %v", err)
	}

	timeout := testMediaCfg().SessionTimeout
	deadline = killedAt.Add(timeout + 6*time.Second)
	for time.Now().Before(deadline) {
		if rt.State() == RemoteTrackEnded && rpB.isClosed() {
			// It must have taken roughly the session timeout: an instant teardown
			// would mean a BYE slipped through and the timeout path went untested.
			if elapsed := time.Since(killedAt); elapsed < timeout/2 {
				t.Fatalf("mirror ended after only %v — that is the BYE path, not the %v SessionTimeout path", elapsed, timeout)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("mirrored publication survived a silent origin death (track=%s, sinkClosed=%v)",
		rt.State(), rpB.isClosed())
}

func TestMediaBridge_SessionReuse(t *testing.T) {
	p := newMediaPair(t)
	p.sfuA.addLocalPub(RemotePublicationInfo{RoomID: "r1", PublicationID: "a", Kind: "audio", MimeType: "audio/opus", ClockRate: 48000, SSRC: 1})
	p.sfuA.addLocalPub(RemotePublicationInfo{RoomID: "r1", PublicationID: "v", Kind: "video", MimeType: "video/VP8", ClockRate: 90000, SSRC: 2})

	if _, err := p.b.Media().SubscribeRemote(context.Background(), "node-a", "r1", "a", "bob"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.b.Media().SubscribeRemote(context.Background(), "node-a", "r1", "v", "bob"); err != nil {
		t.Fatal(err)
	}
	if got := p.b.MetricsSnapshot().Media.SessionsCreated; got != 1 {
		t.Fatalf("expected 1 media session reused for both tracks, got %d created", got)
	}
}
