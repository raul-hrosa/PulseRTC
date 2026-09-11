package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

type Client struct {
	index      int
	cfg        Config
	publishing bool

	ws *websocket.Conn
	pc *webrtc.PeerConnection

	startedAt   time.Time
	connectedAt time.Time
	mediaAt     time.Time

	mu            sync.Mutex
	writeMu       sync.Mutex
	done          chan struct{}
	connected     chan struct{}
	pendingCands  []webrtc.ICECandidateInit
	remoteDescSet bool
	participantID string
	lastStats     *ClientSample
	lastStatsAt   time.Time

	// serverURL overrides cfg.ServerURL for this client (multi-node runs).
	serverURL    string
	clusterError string // last cluster error code (e.g. ROOM_ON_OTHER_NODE)
}

func (c *Client) target() string {
	if c.serverURL != "" {
		return c.serverURL
	}
	return c.cfg.ServerURL
}

// ClusterError returns the cluster error code this client received from its
// node, if any (ROOM_ON_OTHER_NODE, CLUSTER_STATE_UNAVAILABLE, …).
func (c *Client) ClusterError() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clusterError
}

func NewClient(index int, cfg Config, publishing bool) *Client {
	return &Client{
		index:      index,
		cfg:        cfg,
		publishing: publishing,
		done:       make(chan struct{}),
		connected:  make(chan struct{}),
	}
}

func (c *Client) Start(ctx context.Context) error {
	c.startedAt = time.Now()
	hdr := http.Header{}
	if tok, _ := c.cfg.Auth.tokenForClient(c.index, c.cfg.RoomID, c.publishing); tok != "" {
		hdr.Set("Sec-WebSocket-Protocol", "pulsertc, pulsertc.token."+tok)
	}
	ws, resp, err := websocket.DefaultDialer.DialContext(ctx, wsURL(c.target()), hdr)
	if err != nil {
		return err
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	c.ws = ws
	go c.readLoop()

	if err := c.send(map[string]any{"type": "join", "roomId": c.cfg.RoomID}); err != nil {
		return err
	}
	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	pc := c.pc
	ws := c.ws
	c.mu.Unlock()
	if pc != nil {
		_ = pc.Close()
	}
	if ws != nil {
		return ws.Close()
	}
	return nil
}

func (c *Client) WaitConnected(ctx context.Context) error {
	select {
	case <-c.connected:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("client closed before connecting")
	}
}

func (c *Client) Sample() ClientSample {
	c.mu.Lock()
	pc := c.pc
	id := c.participantID
	startedAt := c.startedAt
	connectedAt := c.connectedAt
	mediaAt := c.mediaAt
	prev := c.lastStats
	prevAt := c.lastStatsAt
	c.mu.Unlock()

	out := ClientSample{Participant: id}
	if !connectedAt.IsZero() {
		out.ConnectionTimeMs = float64(connectedAt.Sub(startedAt).Microseconds()) / 1000
	}
	if !mediaAt.IsZero() {
		out.MediaStartMs = float64(mediaAt.Sub(startedAt).Microseconds()) / 1000
	}
	if pc == nil {
		return out
	}
	out.ConnectionState = pc.ConnectionState().String()
	out.ICEState = pc.ICEConnectionState().String()
	now := time.Now()
	report := pc.GetStats()
	for _, raw := range report {
		switch st := raw.(type) {
		case webrtc.OutboundRTPStreamStats:
			out.PacketsSent += uint64(st.PacketsSent)
			out.BytesSent += st.BytesSent
		case webrtc.InboundRTPStreamStats:
			out.PacketsReceived += uint64(st.PacketsReceived)
			out.PacketsLost += int64(st.PacketsLost)
			out.BytesReceived += st.BytesReceived
			if st.Jitter > 0 {
				out.JitterMs = st.Jitter * 1000
			}
		case webrtc.ICECandidatePairStats:
			if st.Nominated && st.CurrentRoundTripTime > 0 {
				out.RTTMs = st.CurrentRoundTripTime * 1000
			}
		}
	}
	if prev != nil && !prevAt.IsZero() {
		dt := now.Sub(prevAt).Seconds()
		if dt > 0 {
			if out.BytesSent >= prev.BytesSent {
				out.BitrateOutBps = float64(out.BytesSent-prev.BytesSent) * 8 / dt
			}
			if out.BytesReceived >= prev.BytesReceived {
				out.BitrateInBps = float64(out.BytesReceived-prev.BytesReceived) * 8 / dt
			}
		}
	}
	c.mu.Lock()
	c.lastStats = &out
	c.lastStatsAt = now
	c.mu.Unlock()
	return out
}

func (c *Client) readLoop() {
	defer close(c.done)
	for {
		var msg map[string]json.RawMessage
		if err := c.ws.ReadJSON(&msg); err != nil {
			return
		}
		var typ string
		_ = json.Unmarshal(msg["type"], &typ)
		_ = c.handleMessage(typ, msg)
	}
}

func (c *Client) handleMessage(typ string, msg map[string]json.RawMessage) error {
	switch typ {
	case "welcome":
		var body struct {
			ParticipantID string `json:"participantId"`
		}
		_ = decodeMessage(msg, &body)
		c.mu.Lock()
		c.participantID = body.ParticipantID
		c.mu.Unlock()
	case "room_joined":
		return c.setupPeer()
	case "error":
		var body struct {
			Code string `json:"code"`
		}
		_ = decodeMessage(msg, &body)
		if body.Code != "" {
			c.mu.Lock()
			c.clusterError = body.Code
			c.mu.Unlock()
		}
	case "sfu_answer":
		var body struct {
			Payload struct {
				SDP string `json:"sdp"`
			} `json:"payload"`
		}
		_ = decodeMessage(msg, &body)
		c.mu.Lock()
		pc := c.pc
		c.mu.Unlock()
		if pc != nil {
			if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: body.Payload.SDP}); err != nil {
				return err
			}
			c.setRemoteDescSet()
			return c.drainCandidates()
		}
	case "sfu_offer":
		var body struct {
			Payload struct {
				SDP string `json:"sdp"`
			} `json:"payload"`
		}
		_ = decodeMessage(msg, &body)
		return c.answerOffer(body.Payload.SDP)
	case "sfu_ice_candidate":
		var body struct {
			Payload webrtc.ICECandidateInit `json:"payload"`
		}
		_ = decodeMessage(msg, &body)
		return c.addRemoteCandidate(body.Payload)
	case "publication_added":
		if !c.cfg.SubscribeAll {
			var body struct {
				PublicationID string `json:"publicationId"`
				ParticipantID string `json:"participantId"`
			}
			_ = decodeMessage(msg, &body)
			c.mu.Lock()
			self := c.participantID
			c.mu.Unlock()
			if body.PublicationID != "" && body.ParticipantID != self {
				return c.send(map[string]any{"type": "unsubscribe", "publicationId": body.PublicationID})
			}
		}
	}
	return nil
}

func (c *Client) setupPeer() error {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	})
	if err != nil {
		return err
	}

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate != nil {
			_ = c.send(map[string]any{"type": "sfu_ice_candidate", "payload": candidate.ToJSON()})
		}
	})
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		if st == webrtc.PeerConnectionStateConnected {
			c.mu.Lock()
			first := c.connectedAt.IsZero()
			if first {
				c.connectedAt = time.Now()
			}
			ch := c.connected
			c.mu.Unlock()
			if first {
				close(ch)
			}
		}
	})
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		c.mu.Lock()
		if c.mediaAt.IsZero() {
			c.mediaAt = time.Now()
		}
		c.mu.Unlock()
		go func() {
			for {
				if _, _, err := track.ReadRTP(); err != nil {
					return
				}
			}
		}()
	})

	c.mu.Lock()
	c.pc = pc
	c.mu.Unlock()

	if c.publishing && c.cfg.PublishAudio {
		if err := c.addAudioTrack(pc); err != nil {
			return err
		}
	} else if c.cfg.SubscribeAll {
		if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			return err
		}
	}
	if c.publishing && c.cfg.PublishVideo {
		if err := c.addVideoTrack(pc); err != nil {
			return err
		}
	} else if c.cfg.SubscribeAll {
		if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			return err
		}
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}
	return c.send(map[string]any{"type": "sfu_offer", "payload": map[string]string{"sdp": offer.SDP}})
}

func (c *Client) addAudioTrack(pc *webrtc.PeerConnection) error {
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		fmt.Sprintf("audio-%d", c.index),
		"loadtest",
	)
	if err != nil {
		return err
	}
	if _, err := pc.AddTrack(track); err != nil {
		return err
	}
	go c.writeSamples(track, 20*time.Millisecond, []byte{0xf8, 0xff, 0xfe})
	return nil
}

func (c *Client) addVideoTrack(pc *webrtc.PeerConnection) error {
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		fmt.Sprintf("video-%d", c.index),
		"loadtest",
	)
	if err != nil {
		return err
	}
	if _, err := pc.AddTrack(track); err != nil {
		return err
	}
	go c.writeSamples(track, 33*time.Millisecond, []byte{0x90, 0x80, 0x80, 0x80, 0x00})
	return nil
}

func (c *Client) writeSamples(track *webrtc.TrackLocalStaticSample, every time.Duration, payload []byte) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := track.WriteSample(media.Sample{Data: payload, Duration: every}); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *Client) answerOffer(sdp string) error {
	c.mu.Lock()
	pc := c.pc
	c.mu.Unlock()
	if pc == nil {
		return nil
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}); err != nil {
		return err
	}
	c.setRemoteDescSet()
	if err := c.drainCandidates(); err != nil {
		return err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		return err
	}
	return c.send(map[string]any{"type": "sfu_answer", "payload": map[string]string{"sdp": answer.SDP}})
}

func (c *Client) addRemoteCandidate(candidate webrtc.ICECandidateInit) error {
	c.mu.Lock()
	pc := c.pc
	ready := c.remoteDescSet
	if !ready {
		c.pendingCands = append(c.pendingCands, candidate)
	}
	c.mu.Unlock()
	if pc == nil || !ready {
		return nil
	}
	return pc.AddICECandidate(candidate)
}

func (c *Client) drainCandidates() error {
	c.mu.Lock()
	pc := c.pc
	pending := c.pendingCands
	c.pendingCands = nil
	c.mu.Unlock()
	if pc == nil {
		return nil
	}
	for _, cand := range pending {
		if err := pc.AddICECandidate(cand); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) setRemoteDescSet() {
	c.mu.Lock()
	c.remoteDescSet = true
	c.mu.Unlock()
}

func (c *Client) send(v any) error {
	c.mu.Lock()
	ws := c.ws
	c.mu.Unlock()
	if ws == nil {
		return errors.New("websocket is not connected")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return ws.WriteJSON(v)
}

func decodeMessage(msg map[string]json.RawMessage, dst any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dst)
}

func wsURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}
	u.Path = "/ws"
	u.RawQuery = ""
	return u.String()
}
