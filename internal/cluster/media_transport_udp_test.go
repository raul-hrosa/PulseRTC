package cluster

import (
	"net"
	"testing"
	"time"
)

func testMediaCfg() MediaConfig {
	return MediaConfig{
		Enabled: true, BindAddr: "127.0.0.1", Port: 0, AdvertiseHost: "127.0.0.1",
		RequestTimeout: time.Second, MaxPacketSize: 1500,
		SessionTimeout: 2 * time.Second, HeartbeatInterval: 200 * time.Millisecond,
		SendQueue: 256,
	}
}

// handshakeUDP dials A→B and B accepts, then waits for both connections to
// authenticate over the HELLO exchange.
func handshakeUDP(t *testing.T) (a, b *udpMediaTransport, ca, cb *udpMediaConn) {
	t.Helper()
	ma, mb := &Metrics{}, &Metrics{}
	var err error
	a, err = newUDPMediaTransport(testMediaCfg(), "secret", "node-a", ma, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err = newUDPMediaTransport(testMediaCfg(), "secret", "node-b", mb, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close(); b.Close() })

	var sid [16]byte
	copy(sid[:], []byte("0123456789abcdef"))

	bc, err := b.acceptSession(sid, "node-a", a.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	cb = bc
	mcA, err := a.Dial(sid, "node-b", b.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	ca = mcA.(*udpMediaConn)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ca.authenticated() && cb.authenticated() {
			return a, b, ca, cb
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("HELLO handshake did not authenticate both sides")
	return
}

func TestMediaTransport_HandshakeAndRTPRoundTrip(t *testing.T) {
	_, _, ca, cb := handshakeUDP(t)

	got := make(chan []byte, 4)
	cb.OnRTP(func(key string, pkt []byte) {
		if key == "pub-1" {
			cp := make([]byte, len(pkt))
			copy(cp, pkt)
			got <- cp
		}
	})

	rtp := []byte{0x80, 0x60, 0x00, 0x01, 0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4, 5, 6, 7, 8}
	if err := ca.SendRTP("pub-1", rtp); err != nil {
		t.Fatalf("send rtp: %v", err)
	}
	select {
	case p := <-got:
		if string(p) != string(rtp) {
			t.Fatalf("payload mismatch: %x", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RTP did not arrive at peer")
	}

	// RTCP the other way.
	rgot := make(chan []byte, 1)
	ca.OnRTCP(func(key string, pkt []byte) { cp := make([]byte, len(pkt)); copy(cp, pkt); rgot <- cp })
	rtcpPkt := []byte{0x81, 0xc9, 0x00, 0x07, 9, 9, 9, 9}
	if err := cb.SendRTCP("pub-1", rtcpPkt); err != nil {
		t.Fatalf("send rtcp: %v", err)
	}
	select {
	case p := <-rgot:
		if string(p) != string(rtcpPkt) {
			t.Fatalf("rtcp mismatch: %x", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RTCP did not arrive")
	}
}

func TestMediaTransport_OversizedRejected(t *testing.T) {
	_, _, ca, _ := handshakeUDP(t)
	err := ca.SendRTP("k", make([]byte, 4096))
	ce, _ := err.(*Error)
	if ce == nil || ce.Code != CodeMediaPacketTooLarge {
		t.Fatalf("want MEDIA_PACKET_TOO_LARGE, got %v", err)
	}
}

func TestMediaTransport_WrongSecretFailsAuth(t *testing.T) {
	ma, mb := &Metrics{}, &Metrics{}
	a, _ := newUDPMediaTransport(testMediaCfg(), "secretA", "node-a", ma, nil)
	b, _ := newUDPMediaTransport(testMediaCfg(), "secretB", "node-b", mb, nil)
	t.Cleanup(func() { a.Close(); b.Close() })

	var sid [16]byte
	copy(sid[:], []byte("ffffffffffffffff"))
	_, _ = b.acceptSession(sid, "node-a", a.LocalAddr())
	mc, _ := a.Dial(sid, "node-b", b.LocalAddr())
	ca := mc.(*udpMediaConn)

	deadline := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(deadline) {
		if ca.authenticated() {
			t.Fatal("handshake authenticated with mismatched secrets")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if mb.mediaAuthFailedC.Load() == 0 {
		t.Fatal("expected auth failures recorded on the receiver")
	}
}

func TestMediaTransport_UnknownSessionDropped(t *testing.T) {
	mb := &Metrics{}
	b, _ := newUDPMediaTransport(testMediaCfg(), "secret", "node-b", mb, nil)
	sender, _ := newUDPMediaTransport(testMediaCfg(), "secret", "node-x", &Metrics{}, nil)
	t.Cleanup(func() { b.Close(); sender.Close() })

	var sid [16]byte
	copy(sid[:], []byte("deadbeefdeadbeef"))
	// A raw RTP frame for a session B never agreed to.
	frame := encodeFrame(frameRTP, sid, encodeKeyed("pub", []byte{1, 2, 3}))
	raddr, _ := net.ResolveUDPAddr("udp", b.LocalAddr())
	_, _ = sender.conn.WriteToUDP(frame, raddr)

	time.Sleep(150 * time.Millisecond)
	if _, ok := b.connFor(sid); ok {
		t.Fatal("a stray packet created a session")
	}
	if mb.mediaRTPInvalidC.Load() == 0 {
		t.Fatal("stray packet not counted as invalid")
	}
}

func TestMediaTransport_HeartbeatTimeoutClosesSession(t *testing.T) {
	cfg := testMediaCfg()
	cfg.SessionTimeout = 400 * time.Millisecond
	cfg.HeartbeatInterval = 100 * time.Millisecond
	ma, mb := &Metrics{}, &Metrics{}
	a, _ := newUDPMediaTransport(cfg, "secret", "node-a", ma, nil)
	b, _ := newUDPMediaTransport(cfg, "secret", "node-b", mb, nil)
	t.Cleanup(func() { a.Close() })

	var sid [16]byte
	copy(sid[:], []byte("aaaaaaaaaaaaaaaa"))
	_, _ = b.acceptSession(sid, "node-a", a.LocalAddr())
	mc, _ := a.Dial(sid, "node-b", b.LocalAddr())
	ca := mc.(*udpMediaConn)

	lost := make(chan struct{}, 1)
	ca.setLostHandler(func() {
		select {
		case lost <- struct{}{}:
		default:
		}
	})

	// Wait for the handshake, then kill the peer so heartbeats stop arriving.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !ca.authenticated() {
		time.Sleep(10 * time.Millisecond)
	}
	if !ca.authenticated() {
		t.Fatal("handshake never completed")
	}
	b.Close()

	select {
	case <-lost:
	case <-time.After(3 * time.Second):
		t.Fatal("session did not detect the broken link")
	}
	if !ca.closed.Load() {
		t.Fatal("connection not closed after link loss")
	}
	_ = ma
}
