package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"
)

// MediaTransport is the dedicated SFU↔SFU media channel. It is
// completely separate from MessageTransport (signaling) and from Redis (state):
// only RTP/RTCP flow here, as binary datagrams over UDP. Nothing in
// this file parses SDP, touches Redis, or does HTTP.
type MediaTransport interface {
	// Dial opens (or reuses) a media connection toward a peer node at udpAddr,
	// bound to sessionID. The first datagram is an authenticated HELLO.
	Dial(sessionID [16]byte, nodeID, udpAddr string) (MediaConnection, error)
	// LocalAddr is the "host:port" other nodes send media to.
	LocalAddr() string
	Close() error
}

// MediaConnection is one logical SFU↔SFU pairing for a media session.
type MediaConnection interface {
	// SendRTP / SendRTCP enqueue a packet for the peer. key identifies the
	// track/publication. They never block indefinitely — a full queue
	// drops.
	SendRTP(key string, packet []byte) error
	SendRTCP(key string, packet []byte) error
	// OnRTP / OnRTCP register the inbound handlers.
	OnRTP(func(key string, packet []byte))
	OnRTCP(func(key string, packet []byte))
	SessionID() [16]byte
	Close() error
}

// --- wire format ----------------------------------------------------------
//
//	byte 0      magic 0x9C
//	byte 1      version 0x01
//	byte 2      frame kind (see below)
//	byte 3..18  sessionID (16 bytes)
//	byte 19..20 payload length (uint16 BE)
//	byte 21..   payload
//
// RTP/RTCP payload: u8 keyLen | key | packet bytes.
// HELLO payload:    u8 srcLen | src | u8 dstLen | dst | 32-byte HMAC-SHA256.

const (
	mediaMagic      = 0x9C
	mediaVersion    = 0x01
	mediaHeaderSize = 21

	frameHello     = 0x00
	frameRTP       = 0x01
	frameRTCP      = 0x02
	frameHeartbeat = 0x03
	frameBye       = 0x04
)

func helloMAC(secret string, sessionID [16]byte, src, dst string) []byte {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(sessionID[:])
	m.Write([]byte(src))
	m.Write([]byte{0})
	m.Write([]byte(dst))
	return m.Sum(nil)
}

func encodeFrame(kind byte, sessionID [16]byte, payload []byte) []byte {
	buf := make([]byte, mediaHeaderSize+len(payload))
	buf[0] = mediaMagic
	buf[1] = mediaVersion
	buf[2] = kind
	copy(buf[3:19], sessionID[:])
	binary.BigEndian.PutUint16(buf[19:21], uint16(len(payload)))
	copy(buf[21:], payload)
	return buf
}

type parsedFrame struct {
	kind      byte
	sessionID [16]byte
	payload   []byte
}

func decodeFrame(b []byte) (parsedFrame, bool) {
	if len(b) < mediaHeaderSize || b[0] != mediaMagic || b[1] != mediaVersion {
		return parsedFrame{}, false
	}
	plen := int(binary.BigEndian.Uint16(b[19:21]))
	if mediaHeaderSize+plen > len(b) {
		return parsedFrame{}, false
	}
	var f parsedFrame
	f.kind = b[2]
	copy(f.sessionID[:], b[3:19])
	f.payload = b[mediaHeaderSize : mediaHeaderSize+plen]
	return f, true
}

func encodeKeyed(key string, packet []byte) []byte {
	if len(key) > 255 {
		key = key[:255]
	}
	out := make([]byte, 1+len(key)+len(packet))
	out[0] = byte(len(key))
	copy(out[1:], key)
	copy(out[1+len(key):], packet)
	return out
}

func decodeKeyed(payload []byte) (string, []byte, bool) {
	if len(payload) < 1 {
		return "", nil, false
	}
	kl := int(payload[0])
	if 1+kl > len(payload) {
		return "", nil, false
	}
	return string(payload[1 : 1+kl]), payload[1+kl:], true
}

// --------------------------------------------------------------------------
// udpMediaTransport
// --------------------------------------------------------------------------

type udpMediaTransport struct {
	cfg     MediaConfig
	secret  string
	selfID  string
	conn    *net.UDPConn
	metrics *Metrics
	logger  logger

	mu     sync.RWMutex
	conns  map[[16]byte]*udpMediaConn // sessionID -> conn
	closed bool
	stop   chan struct{}
	wg     sync.WaitGroup
}

// logger is the tiny subset of *slog.Logger this file needs, so tests can pass
// nil without importing slog.
type logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}

func newUDPMediaTransport(cfg MediaConfig, secret, selfID string, m *Metrics, lg logger) (*udpMediaTransport, error) {
	addr := &net.UDPAddr{IP: net.ParseIP(cfg.bindAddr()), Port: cfg.Port}
	if addr.IP == nil {
		addr.IP = net.IPv4zero
	}
	c, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	t := &udpMediaTransport{
		cfg: cfg, secret: secret, selfID: selfID, conn: c,
		metrics: m, logger: lg,
		conns: make(map[[16]byte]*udpMediaConn),
		stop:  make(chan struct{}),
	}
	t.wg.Add(1)
	go t.readLoop()
	return t, nil
}

func (t *udpMediaTransport) LocalAddr() string { return t.conn.LocalAddr().String() }

func (t *udpMediaTransport) Dial(sessionID [16]byte, nodeID, udpAddr string) (MediaConnection, error) {
	raddr, err := net.ResolveUDPAddr("udp", udpAddr)
	if err != nil {
		return nil, &Error{Code: CodeMediaNodeUnavailable, Message: "bad media address", NodeID: nodeID}
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, &Error{Code: CodeMediaTransportClosed, Message: "transport closed"}
	}
	if existing, ok := t.conns[sessionID]; ok {
		t.mu.Unlock()
		return existing, nil
	}
	mc := newUDPMediaConn(t, sessionID, nodeID, raddr)
	t.conns[sessionID] = mc
	t.mu.Unlock()

	mc.sendHello()
	return mc, nil
}

func (t *udpMediaTransport) removeConn(sessionID [16]byte) {
	t.mu.Lock()
	delete(t.conns, sessionID)
	t.mu.Unlock()
}

func (t *udpMediaTransport) connFor(sessionID [16]byte) (*udpMediaConn, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	c, ok := t.conns[sessionID]
	return c, ok
}

func (t *udpMediaTransport) readLoop() {
	defer t.wg.Done()
	buf := make([]byte, 2048)
	for {
		select {
		case <-t.stop:
			return
		default:
		}
		_ = t.conn.SetReadDeadline(time.Now().Add(time.Second))
		n, src, err := t.conn.ReadFromUDP(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return
		}
		t.handleDatagram(buf[:n], src)
	}
}

func (t *udpMediaTransport) handleDatagram(b []byte, src *net.UDPAddr) {
	f, ok := decodeFrame(b)
	if !ok {
		t.metrics.mediaRTPInvalid()
		return
	}
	if t.cfg.MaxPacketSize > 0 && len(b) > t.cfg.MaxPacketSize+mediaHeaderSize {
		t.metrics.mediaOversized()
	}

	conn, known := t.connFor(f.sessionID)

	if f.kind == frameHello {
		t.handleHello(f, src, conn, known)
		return
	}
	// Every non-HELLO frame must belong to a known, authenticated session from
	// the confirmed source address. Unknown → drop, no log.
	if !known || !conn.authenticated() {
		t.metrics.mediaRTPInvalid()
		return
	}
	if !conn.matchesSource(src) {
		t.metrics.mediaRTPInvalid()
		return
	}
	conn.markRx()
	switch f.kind {
	case frameHeartbeat:
		// liveness only
	case frameBye:
		conn.peerClosed()
	case frameRTP:
		if key, pkt, ok := decodeKeyed(f.payload); ok {
			t.metrics.mediaRTPReceived()
			conn.deliverRTP(key, pkt)
		} else {
			t.metrics.mediaRTPInvalid()
		}
	case frameRTCP:
		if key, pkt, ok := decodeKeyed(f.payload); ok {
			t.metrics.mediaRTCPReceived()
			conn.deliverRTCP(key, pkt)
		} else {
			t.metrics.mediaRTPInvalid()
		}
	}
}

func (t *udpMediaTransport) handleHello(f parsedFrame, src *net.UDPAddr, conn *udpMediaConn, known bool) {
	src1, dst1, mac, ok := parseHello(f.payload)
	if !ok {
		t.metrics.mediaAuthFailed()
		return
	}
	// dst must be us; src must match the session's expected peer.
	want := helloMAC(t.secret, f.sessionID, src1, dst1)
	if subtle.ConstantTimeCompare(mac, want) != 1 || dst1 != t.selfID {
		t.metrics.mediaAuthFailed()
		return
	}
	if !known {
		// A HELLO for an unknown session is only valid if a handshake pre-created
		// it (Accept side). Without that, drop — a stray packet must not create a
		// session.
		t.metrics.mediaAuthFailed()
		return
	}
	if conn.remoteNodeID != src1 {
		t.metrics.mediaAuthFailed()
		return
	}
	conn.confirmSource(src)
	conn.markAuthenticated()
	conn.markRx()
	// Reply so the other side also confirms our address.
	conn.sendHello()
}

func parseHello(p []byte) (src, dst string, mac []byte, ok bool) {
	if len(p) < 1 {
		return "", "", nil, false
	}
	sl := int(p[0])
	if 1+sl+1 > len(p) {
		return "", "", nil, false
	}
	src = string(p[1 : 1+sl])
	off := 1 + sl
	dl := int(p[off])
	off++
	if off+dl+sha256.Size != len(p) {
		return "", "", nil, false
	}
	dst = string(p[off : off+dl])
	mac = p[off+dl:]
	return src, dst, mac, true
}

func (t *udpMediaTransport) writeTo(addr *net.UDPAddr, frame []byte) error {
	if addr == nil {
		return &Error{Code: CodeMediaNodeUnavailable, Message: "no peer address yet"}
	}
	_, err := t.conn.WriteToUDP(frame, addr)
	return err
}

// acceptSession pre-creates the accept-side connection so an inbound HELLO for
// sessionID is recognised (handshake). remoteAddr may be "" if not yet
// known — it is confirmed from the HELLO source.
func (t *udpMediaTransport) acceptSession(sessionID [16]byte, remoteNodeID, remoteAddr string) (*udpMediaConn, error) {
	var raddr *net.UDPAddr
	if remoteAddr != "" {
		a, err := net.ResolveUDPAddr("udp", remoteAddr)
		if err == nil {
			raddr = a
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, &Error{Code: CodeMediaTransportClosed, Message: "transport closed"}
	}
	if c, ok := t.conns[sessionID]; ok {
		return c, nil
	}
	mc := newUDPMediaConn(t, sessionID, remoteNodeID, raddr)
	t.conns[sessionID] = mc
	return mc, nil
}

func (t *udpMediaTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	conns := make([]*udpMediaConn, 0, len(t.conns))
	for _, c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.Unlock()

	for _, c := range conns {
		c.Close()
	}
	close(t.stop)
	err := t.conn.Close()
	t.wg.Wait()
	return err
}
