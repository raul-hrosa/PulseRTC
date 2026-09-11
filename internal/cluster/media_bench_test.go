package cluster

import (
	"testing"
	"time"
)

// Run:
//
//	go test ./internal/cluster/ -run=^$ -bench='Media|RTPForward|RTCPForward|PacketValidation|RemoteTrack' -benchmem
//
// The RTP forwarding path must avoid unnecessary allocations.

func BenchmarkMediaSession(b *testing.B) {
	var id [16]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := newMediaSession(id, "r", "a", "c", nil)
		s.setState(MediaSessionActive)
		s.Close()
	}
}

func BenchmarkRemoteTrack(b *testing.B) {
	info := RemotePublicationInfo{RoomID: "r", PublicationID: "p", Kind: "video"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rt := newRemoteTrack(info, [16]byte{})
		rt.setState(RemoteTrackActive)
		rt.end()
	}
}

func BenchmarkRTPForward(b *testing.B) {
	_, _, ca, cb := handshakeUDPB(b)
	cb.OnRTP(func(string, []byte) {})
	pkt := make([]byte, 1100)
	pkt[0] = 0x80
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ca.SendRTP("pub-1", pkt)
	}
}

func BenchmarkRTCPForward(b *testing.B) {
	_, _, ca, cb := handshakeUDPB(b)
	cb.OnRTCP(func(string, []byte) {})
	pkt := []byte{0x81, 0xc9, 0, 7, 1, 2, 3, 4}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ca.SendRTCP("pub-1", pkt)
	}
}

func BenchmarkPacketValidation(b *testing.B) {
	var sid [16]byte
	frame := encodeFrame(frameRTP, sid, encodeKeyed("pub-1", make([]byte, 1100)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, ok := decodeFrame(frame)
		if !ok {
			b.Fatal("decode")
		}
		if _, _, ok := decodeKeyed(f.payload); !ok {
			b.Fatal("key")
		}
	}
}

// handshakeUDPB is handshakeUDP for benchmarks (*testing.B).
func handshakeUDPB(b *testing.B) (ta, tb *udpMediaTransport, ca, cb *udpMediaConn) {
	b.Helper()
	ta, _ = newUDPMediaTransport(testMediaCfg(), "secret", "node-a", &Metrics{}, nil)
	tb, _ = newUDPMediaTransport(testMediaCfg(), "secret", "node-b", &Metrics{}, nil)
	b.Cleanup(func() { ta.Close(); tb.Close() })
	var sid [16]byte
	copy(sid[:], []byte("benchbenchbenchb"))
	bc, _ := tb.acceptSession(sid, "node-a", ta.LocalAddr())
	cb = bc
	mc, _ := ta.Dial(sid, "node-b", tb.LocalAddr())
	ca = mc.(*udpMediaConn)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !(ca.authenticated() && cb.authenticated()) {
		time.Sleep(5 * time.Millisecond)
	}
	return
}
