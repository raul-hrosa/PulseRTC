package signaling

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/raulhrosa/pulsertc/internal/auth"
	"github.com/raulhrosa/pulsertc/internal/sfu"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	// defaultMaxMessageSize is the fallback when PULSERTC_MAX_MESSAGE_SIZE is
	// unset (see auth.Config). The largest legitimate message is an SDP offer.
	defaultMaxMessageSize = 64 * 1024
	sendBuffer            = 32
)

// Client represents a single WebSocket connection / participant.
type Client struct {
	id     string
	conn   *websocket.Conn
	server *Server

	send chan []byte

	// identity is the authenticated principal for this connection.
	// Never nil once newClient returns; a client can never mutate it.
	identity       *auth.Identity
	maxMessageSize int64
	msgLimiter     *auth.RateLimiter

	mu          sync.Mutex
	roomID      string
	closed      bool
	sfuPeer     *sfu.Participant
	expiryTimer *time.Timer

	// The logical session this connection currently backs.
	sessionID  string
	sessionGen uint64
}

func (c *Client) setSession(id string, gen uint64) {
	c.mu.Lock()
	c.sessionID = id
	c.sessionGen = gen
	c.mu.Unlock()
}

func (c *Client) session() (string, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID, c.sessionGen
}

// closeNow closes the underlying socket (used to evict a replaced session).
func (c *Client) closeNow() { _ = c.conn.Close() }

func newClient(id string, conn *websocket.Conn, server *Server, identity *auth.Identity) *Client {
	maxSize := int64(defaultMaxMessageSize)
	rate := 0
	if server.auth != nil {
		if m := server.auth.MaxMessageSize(); m > 0 {
			maxSize = m
		}
		rate = server.auth.MsgRatePerSec()
	}
	return &Client{
		id:             id,
		conn:           conn,
		server:         server,
		send:           make(chan []byte, sendBuffer),
		identity:       identity,
		maxMessageSize: maxSize,
		msgLimiter:     auth.NewRateLimiter(rate, time.Second),
	}
}

// ID implements room.Participant.
func (c *Client) ID() string { return c.id }

// Send implements room.Participant. It never blocks the caller: if the client's
// buffer is full the connection is considered too slow and gets dropped.
func (c *Client) Send(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	select {
	case c.send <- data:
	default:
		log.Printf("client %s send buffer full, closing", c.id)
		c.closed = true
		close(c.send)
	}
}

func (c *Client) sendJSON(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("marshal error for client %s: %v", c.id, err)
		return
	}
	c.Send(data)
}

// SendSFU implements sfu.Transport: it delivers an SFU signaling message to the
// browser over the same WebSocket used for control messages.
func (c *Client) SendSFU(msg any) { c.sendJSON(msg) }

func (c *Client) setSFUPeer(p *sfu.Participant) {
	c.mu.Lock()
	c.sfuPeer = p
	c.mu.Unlock()
}

func (c *Client) sfuPeerRef() *sfu.Participant {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sfuPeer
}

func (c *Client) currentRoom() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.roomID
}

func (c *Client) setRoom(id string) {
	c.mu.Lock()
	c.roomID = id
	c.mu.Unlock()
}

// scheduleTokenExpiry arranges a graceful disconnect when the token's exp is
// reached: the connection becomes unauthorized, the client is
// told once with an EXPIRED_TOKEN error, and the socket is closed. A no-op when
// auth is disabled or the token is already past exp.
func (c *Client) scheduleTokenExpiry() {
	if c.server.auth == nil || !c.server.auth.Enabled() || c.identity == nil {
		return
	}
	ttl := c.identity.ExpiresIn(time.Now())
	if ttl <= 0 {
		return
	}
	t := time.AfterFunc(ttl, func() {
		c.server.logger.Info("session_token_expired", "participant", c.id, "subject", c.identity.Subject)
		if c.server.auth != nil {
			c.server.auth.RecordAuthzFailure(auth.CodeExpiredToken)
		}
		c.sendJSON(newSecError(&auth.Error{Code: auth.CodeExpiredToken, Message: "session token expired"}))
		// Give writePump a moment to flush the notice, then drop the socket.
		time.AfterFunc(200*time.Millisecond, func() { _ = c.conn.Close() })
	})
	c.mu.Lock()
	c.expiryTimer = t
	c.mu.Unlock()
}

func (c *Client) stopTokenExpiry() {
	c.mu.Lock()
	if c.expiryTimer != nil {
		c.expiryTimer.Stop()
		c.expiryTimer = nil
	}
	c.mu.Unlock()
}

// readPump pumps messages from the WebSocket connection to the server.
func (c *Client) readPump() {
	defer c.server.disconnect(c)

	c.conn.SetReadLimit(c.maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("client %s read error: %v", c.id, err)
			}
			return
		}
		// Per-connection signaling rate limit. A client that
		// blows the budget is dropped rather than throttled — a well-behaved
		// browser never approaches it.
		if !c.msgLimiter.Allow("msg") {
			if c.server.auth != nil {
				c.server.auth.RecordAuthzFailure(auth.CodeRateLimited)
			}
			c.sendJSON(newSecError(&auth.Error{Code: auth.CodeRateLimited, Message: "signaling rate limit exceeded"}))
			return
		}
		c.server.handleMessage(c, data)
	}
}

// writePump pumps messages from the send channel to the WebSocket connection.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
