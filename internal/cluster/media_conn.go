package cluster

import (
	"encoding/hex"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func shortID(id [16]byte) string { return hex.EncodeToString(id[:4]) }

// udpMediaConn is one MediaConnection over the shared UDP socket. Outbound
// packets go through a bounded queue: a slow/lossy peer drops packets for THIS
// session only, never blocking the socket or other sessions.
type udpMediaConn struct {
	t            *udpMediaTransport
	sessionID    [16]byte
	remoteNodeID string

	mu         sync.RWMutex
	remoteAddr *net.UDPAddr
	srcOK      bool
	authOK     atomic.Bool
	closed     atomic.Bool
	lastRx     atomic.Int64 // unix nanos

	onRTP       atomic.Pointer[func(string, []byte)]
	onRTCP      atomic.Pointer[func(string, []byte)]
	lostHandler atomic.Pointer[func()]

	sendCh chan []byte
	stop   chan struct{}
	wg     sync.WaitGroup
	once   sync.Once
}

func newUDPMediaConn(t *udpMediaTransport, sessionID [16]byte, remoteNodeID string, raddr *net.UDPAddr) *udpMediaConn {
	depth := t.cfg.SendQueue
	if depth <= 0 {
		depth = 1024
	}
	c := &udpMediaConn{
		t: t, sessionID: sessionID, remoteNodeID: remoteNodeID, remoteAddr: raddr,
		sendCh: make(chan []byte, depth),
		stop:   make(chan struct{}),
	}
	c.markRx()
	c.wg.Add(1)
	go c.sendLoop()
	c.wg.Add(1)
	go c.heartbeatLoop()
	return c
}

func (c *udpMediaConn) SessionID() [16]byte { return c.sessionID }

func (c *udpMediaConn) OnRTP(fn func(string, []byte))  { c.onRTP.Store(&fn) }
func (c *udpMediaConn) OnRTCP(fn func(string, []byte)) { c.onRTCP.Store(&fn) }

func (c *udpMediaConn) authenticated() bool { return c.authOK.Load() }
func (c *udpMediaConn) markAuthenticated()  { c.authOK.Store(true) }

func (c *udpMediaConn) markRx() { c.lastRx.Store(time.Now().UnixNano()) }
func (c *udpMediaConn) idleFor() time.Duration {
	return time.Since(time.Unix(0, c.lastRx.Load()))
}

func (c *udpMediaConn) confirmSource(src *net.UDPAddr) {
	c.mu.Lock()
	c.remoteAddr = src
	c.srcOK = true
	c.mu.Unlock()
}

func (c *udpMediaConn) matchesSource(src *net.UDPAddr) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.srcOK || c.remoteAddr == nil {
		return false
	}
	return c.remoteAddr.IP.Equal(src.IP) && c.remoteAddr.Port == src.Port
}

func (c *udpMediaConn) addr() *net.UDPAddr {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.remoteAddr
}

func (c *udpMediaConn) sendHello() {
	src := c.t.selfID
	dst := c.remoteNodeID
	mac := helloMAC(c.t.secret, c.sessionID, src, dst)
	payload := make([]byte, 0, 2+len(src)+len(dst)+len(mac))
	payload = append(payload, byte(len(src)))
	payload = append(payload, src...)
	payload = append(payload, byte(len(dst)))
	payload = append(payload, dst...)
	payload = append(payload, mac...)
	c.enqueue(encodeFrame(frameHello, c.sessionID, payload))
}

func (c *udpMediaConn) SendRTP(key string, packet []byte) error {
	return c.sendKeyed(frameRTP, key, packet, true)
}

func (c *udpMediaConn) SendRTCP(key string, packet []byte) error {
	return c.sendKeyed(frameRTCP, key, packet, false)
}

func (c *udpMediaConn) sendKeyed(kind byte, key string, packet []byte, isRTP bool) error {
	if c.closed.Load() {
		return &Error{Code: CodeMediaTransportClosed, Message: "media connection closed"}
	}
	if !c.authOK.Load() {
		return &Error{Code: CodeMediaSessionInvalid, Message: "media session not established"}
	}
	if c.t.cfg.MaxPacketSize > 0 && len(packet) > c.t.cfg.MaxPacketSize {
		c.t.metrics.mediaOversized()
		return &Error{Code: CodeMediaPacketTooLarge, Message: "media packet exceeds MTU budget"}
	}
	frame := encodeFrame(kind, c.sessionID, encodeKeyed(key, packet))
	ok := c.enqueue(frame)
	if !ok {
		if isRTP {
			c.t.metrics.mediaRTPDropped()
		} else {
			c.t.metrics.mediaRTCPDropped()
		}
		return nil // controlled drop, not an error to the caller
	}
	if isRTP {
		c.t.metrics.mediaRTPSent()
	} else {
		c.t.metrics.mediaRTCPSent()
	}
	return nil
}

// enqueue returns false when the queue is full (packet dropped).
func (c *udpMediaConn) enqueue(frame []byte) bool {
	select {
	case c.sendCh <- frame:
		return true
	default:
		return false
	}
}

func (c *udpMediaConn) sendLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.stop:
			return
		case frame := <-c.sendCh:
			if addr := c.addr(); addr != nil {
				_ = c.t.writeTo(addr, frame)
			}
		}
	}
}

func (c *udpMediaConn) heartbeatLoop() {
	defer c.wg.Done()
	iv := c.t.cfg.HeartbeatInterval
	if iv <= 0 {
		iv = 2 * time.Second
	}
	timeout := c.t.cfg.SessionTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	tk := time.NewTicker(iv)
	defer tk.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-tk.C:
			if c.idleFor() > timeout {
				c.t.metrics.mediaSessionFailed()
				if c.t.logger != nil {
					c.t.logger.Warn("media_transport_timeout", "session", shortID(c.sessionID), "remoteNodeId", c.remoteNodeID)
				}
				c.notifyLost()
				c.Close()
				return
			}
			if c.authOK.Load() {
				c.enqueue(encodeFrame(frameHeartbeat, c.sessionID, nil))
			} else {
				c.sendHello() // keep trying the handshake
			}
		}
	}
}

func (c *udpMediaConn) deliverRTP(key string, pkt []byte) {
	if fn := c.onRTP.Load(); fn != nil {
		(*fn)(key, pkt)
	}
}

func (c *udpMediaConn) deliverRTCP(key string, pkt []byte) {
	if fn := c.onRTCP.Load(); fn != nil {
		(*fn)(key, pkt)
	}
}

// lostHandler is set by the media router so it can tear down remote tracks when
// the session dies.
func (c *udpMediaConn) setLostHandler(fn func()) { c.lostHandler.Store(&fn) }

func (c *udpMediaConn) notifyLost() {
	if fn := c.lostHandler.Load(); fn != nil {
		(*fn)()
	}
}

func (c *udpMediaConn) peerClosed() {
	c.notifyLost()
	c.Close()
}

func (c *udpMediaConn) Close() error {
	c.once.Do(func() {
		c.closed.Store(true)
		// Best-effort BYE.
		if addr := c.addr(); addr != nil && c.authOK.Load() {
			_ = c.t.writeTo(addr, encodeFrame(frameBye, c.sessionID, nil))
		}
		close(c.stop)
		c.t.removeConn(c.sessionID)
	})
	return nil
}
