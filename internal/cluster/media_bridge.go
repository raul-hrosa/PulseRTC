package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// MediaBridge is the cross-node media orchestrator the signaling / SFU layers
// use. A disabled cluster returns a no-op implementation so callers never need a
// nil check.
type MediaBridge interface {
	Enabled() bool
	LocalMediaAddr() string
	SetSink(MediaSink)
	SetSource(MediaSource)
	// SubscribeRemote mirrors a publication living on remoteNodeID into the
	// local SFU for subscriberID.
	SubscribeRemote(ctx context.Context, remoteNodeID, roomID, publicationID, subscriberID string) (*RemoteTrack, error)
	UnsubscribeRemote(ctx context.Context, remoteNodeID, publicationID, subscriberID string)
	// PublicationEnded tells the bridge a locally-published, remotely-forwarded
	// track is gone.
	PublicationEnded(roomID, publicationID string)
}

type noopMediaBridge struct{}

func (noopMediaBridge) Enabled() bool          { return false }
func (noopMediaBridge) LocalMediaAddr() string { return "" }
func (noopMediaBridge) SetSink(MediaSink)      {}
func (noopMediaBridge) SetSource(MediaSource)  {}
func (noopMediaBridge) SubscribeRemote(context.Context, string, string, string, string) (*RemoteTrack, error) {
	return nil, &Error{Code: CodeMediaNodeUnavailable, Message: "cross-node media disabled"}
}
func (noopMediaBridge) UnsubscribeRemote(context.Context, string, string, string) {}
func (noopMediaBridge) PublicationEnded(string, string)                           {}

func (b *mediaBridge) Enabled() bool { return true }

// mediaBridge orchestrates SFU↔SFU media. It owns the UDP
// MediaTransport, the MediaSessions, and the RemoteTracks, and it drives the
// handshake over the MessageTransport. Media bytes never touch Redis
// or HTTP.
type mediaBridge struct {
	cfg      MediaConfig
	secret   string
	selfID   string
	logger   *slog.Logger
	metrics  *Metrics
	registry NodeRegistry

	transport *udpMediaTransport
	send      func(ctx context.Context, targetNodeID string, msg ClusterMessage) error
	newMsg    func(typ string, payload json.RawMessage) ClusterMessage

	mu            sync.Mutex
	byNode        map[string]*MediaSession
	bySID         map[string]*MediaSession
	pendingAccept map[string]chan mediaSessionAccept
	pendingSubAck map[string]chan mediaSubscribeAck

	sinkMu sync.RWMutex
	sink   MediaSink
	source MediaSource

	closed bool
}

func newMediaBridge(cfg Config, m *Metrics, reg NodeRegistry, logger *slog.Logger,
	send func(context.Context, string, ClusterMessage) error,
	newMsg func(string, json.RawMessage) ClusterMessage,
) (*mediaBridge, error) {
	if logger == nil {
		logger = slog.Default()
	}
	tr, err := newUDPMediaTransport(cfg.Media, cfg.Secret, cfg.NodeID, m, logger)
	if err != nil {
		return nil, err
	}
	return &mediaBridge{
		cfg: cfg.Media, secret: cfg.Secret, selfID: cfg.NodeID,
		logger: logger, metrics: m, registry: reg,
		transport:     tr,
		send:          send,
		newMsg:        newMsg,
		byNode:        map[string]*MediaSession{},
		bySID:         map[string]*MediaSession{},
		pendingAccept: map[string]chan mediaSessionAccept{},
		pendingSubAck: map[string]chan mediaSubscribeAck{},
	}, nil
}

// LocalMediaAddr is what peers should send media to.
func (b *mediaBridge) LocalMediaAddr() string {
	if b.cfg.AdvertiseHost != "" {
		_, port, _ := splitHostPort(b.transport.LocalAddr())
		return b.cfg.AdvertiseHost + ":" + port
	}
	return b.transport.LocalAddr()
}

func (b *mediaBridge) SetSink(s MediaSink)     { b.sinkMu.Lock(); b.sink = s; b.sinkMu.Unlock() }
func (b *mediaBridge) SetSource(s MediaSource) { b.sinkMu.Lock(); b.source = s; b.sinkMu.Unlock() }
func (b *mediaBridge) getSink() MediaSink      { b.sinkMu.RLock(); defer b.sinkMu.RUnlock(); return b.sink }
func (b *mediaBridge) getSource() MediaSource {
	b.sinkMu.RLock()
	defer b.sinkMu.RUnlock()
	return b.source
}

func newSessionID() [16]byte {
	var id [16]byte
	_, _ = rand.Read(id[:])
	return id
}

// EnsureSession returns an ACTIVE session to remoteNodeID, opening one via the
// handshake if needed.
func (b *mediaBridge) EnsureSession(ctx context.Context, remoteNodeID, roomID string) (*MediaSession, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, &Error{Code: CodeMediaTransportClosed, Message: "media bridge closed"}
	}
	if s, ok := b.byNode[remoteNodeID]; ok && s.State() != MediaSessionClosed {
		b.mu.Unlock()
		if err := b.waitActive(ctx, s); err != nil {
			return nil, err
		}
		return s, nil
	}

	info, ok := b.registry.Get(remoteNodeID)
	if !ok || info.MediaAddr == "" {
		b.mu.Unlock()
		return nil, &Error{Code: CodeMediaNodeUnavailable, Message: "remote node has no media address", NodeID: remoteNodeID}
	}
	if !info.State.AcceptsSessions() {
		b.mu.Unlock()
		return nil, &Error{Code: CodeMediaNodeUnavailable, Message: "remote node not ready", NodeID: remoteNodeID}
	}

	sid := newSessionID()
	conn, err := b.transport.Dial(sid, remoteNodeID, info.MediaAddr)
	if err != nil {
		b.mu.Unlock()
		return nil, err
	}
	sess := newMediaSession(sid, roomID, b.selfID, remoteNodeID, conn)
	sess.onClose = b.forget
	b.wireConn(sess, conn)
	b.byNode[remoteNodeID] = sess
	b.bySID[hex.EncodeToString(sid[:])] = sess
	ch := make(chan mediaSessionAccept, 1)
	b.pendingAccept[hex.EncodeToString(sid[:])] = ch
	b.mu.Unlock()

	b.metrics.mediaSessionCreated()
	b.logger.Info("media_session_created", "session", sess.IDHex, "remoteNodeId", remoteNodeID, "room", roomID)

	openMsg := b.newMsg(MsgMediaSessionOpen, mustJSON(mediaSessionOpen{
		SessionID: hex.EncodeToString(sid[:]), RoomID: roomID, MediaAddr: b.LocalMediaAddr(),
	}))
	sctx, cancel := context.WithTimeout(ctx, b.reqTimeout())
	defer cancel()
	if err := b.send(sctx, remoteNodeID, openMsg); err != nil {
		sess.Close()
		return nil, &Error{Code: CodeMediaNodeUnavailable, Message: "media session open failed", NodeID: remoteNodeID}
	}

	select {
	case <-ch:
	case <-sctx.Done():
		sess.Close()
		return nil, &Error{Code: CodeMediaTransportTimeout, Message: "media session accept timed out"}
	}
	if err := b.waitActive(ctx, sess); err != nil {
		sess.Close()
		return nil, err
	}
	return sess, nil
}

func (b *mediaBridge) waitActive(ctx context.Context, sess *MediaSession) error {
	deadline := time.Now().Add(b.reqTimeout())
	for {
		if sess.State() == MediaSessionActive {
			return nil
		}
		if sess.State() == MediaSessionClosed {
			return &Error{Code: CodeMediaSessionInvalid, Message: "media session closed during setup"}
		}
		if c, ok := sess.conn.(*udpMediaConn); ok && c.authenticated() {
			sess.setState(MediaSessionActive)
			b.logger.Info("media_session_connected", "session", sess.IDHex, "remoteNodeId", sess.RemoteNodeID)
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return &Error{Code: CodeMediaTransportTimeout, Message: "media session did not become active"}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (b *mediaBridge) reqTimeout() time.Duration {
	if b.cfg.RequestTimeout > 0 {
		return b.cfg.RequestTimeout
	}
	return 2 * time.Second
}

func (b *mediaBridge) wireConn(sess *MediaSession, conn MediaConnection) {
	conn.OnRTP(func(key string, pkt []byte) {
		if rt, ok := sess.track(key); ok {
			rt.writeRTP(pkt)
		}
	})
	conn.OnRTCP(func(key string, pkt []byte) {
		if rt, ok := sess.track(key); ok {
			rt.writeRTCP(pkt)
			return
		}
		if src := b.getSource(); src != nil {
			src.DeliverRTCPToPublisher(sess.RoomID, key, pkt)
		}
	})
	if c, ok := conn.(*udpMediaConn); ok {
		c.setLostHandler(func() { b.onSessionLost(sess) })
	}
}

func (b *mediaBridge) onSessionLost(sess *MediaSession) {
	b.logger.Warn("media_session_failed", "session", sess.IDHex, "remoteNodeId", sess.RemoteNodeID)
	b.metrics.mediaSessionFailed()
	sess.Close()
}

func (b *mediaBridge) forget(sess *MediaSession) {
	b.mu.Lock()
	if cur, ok := b.byNode[sess.RemoteNodeID]; ok && cur == sess {
		delete(b.byNode, sess.RemoteNodeID)
	}
	delete(b.bySID, hex.EncodeToString(sess.ID[:]))
	b.mu.Unlock()
	b.metrics.mediaSessionClosed()
	b.logger.Info("media_session_closed", "session", sess.IDHex, "remoteNodeId", sess.RemoteNodeID)
}

// SubscribeRemote mirrors publicationID (living on remoteNodeID) into the local
// SFU so subscriberID can receive it.
func (b *mediaBridge) SubscribeRemote(ctx context.Context, remoteNodeID, roomID, publicationID, subscriberID string) (*RemoteTrack, error) {
	sess, err := b.EnsureSession(ctx, remoteNodeID, roomID)
	if err != nil {
		return nil, err
	}
	if rt, ok := sess.track(publicationID); ok {
		return rt, nil
	}

	key := sess.IDHex + "|" + publicationID
	ch := make(chan mediaSubscribeAck, 1)
	b.mu.Lock()
	b.pendingSubAck[key] = ch
	b.mu.Unlock()
	defer func() { b.mu.Lock(); delete(b.pendingSubAck, key); b.mu.Unlock() }()

	sub := b.newMsg(MsgMediaSubscribe, mustJSON(mediaSubscribe{
		SessionID: sess.IDHex, RoomID: roomID, PublicationID: publicationID, SubscriberID: subscriberID,
	}))
	sctx, cancel := context.WithTimeout(ctx, b.reqTimeout())
	defer cancel()
	if err := b.send(sctx, remoteNodeID, sub); err != nil {
		return nil, &Error{Code: CodeMediaNodeUnavailable, Message: "media subscribe failed", NodeID: remoteNodeID}
	}

	var ack mediaSubscribeAck
	select {
	case ack = <-ch:
	case <-sctx.Done():
		return nil, &Error{Code: CodeMediaTransportTimeout, Message: "media subscribe timed out"}
	}
	if !ack.Accepted {
		return nil, &Error{Code: CodeRemoteTrackNotFound, Message: ack.Reason, RoomID: roomID}
	}

	info := RemotePublicationInfo{
		RoomID: roomID, PublicationID: publicationID, ParticipantID: ack.ParticipantID,
		OriginNodeID: remoteNodeID, Kind: ack.Kind, MimeType: ack.MimeType,
		ClockRate: ack.ClockRate, Channels: ack.Channels, PayloadType: ack.PayloadType, SSRC: ack.SSRC,
	}
	rt := newRemoteTrack(info, sess.ID)
	if sink := b.getSink(); sink != nil {
		handle, herr := sink.AddRemotePublication(info, func(pkt []byte) {
			_ = sess.conn.SendRTCP(publicationID, pkt)
		})
		if herr != nil {
			return nil, herr
		}
		rt.mu.Lock()
		rt.handle = handle
		rt.mu.Unlock()
	}
	rt.setState(RemoteTrackActive)
	sess.addTrack(rt)
	b.metrics.mediaRemoteTrackAdd()
	b.logger.Info("remote_track_created", "session", sess.IDHex, "publication", publicationID, "kind", info.Kind)
	return rt, nil
}

// UnsubscribeRemote stops mirroring a publication.
func (b *mediaBridge) UnsubscribeRemote(ctx context.Context, remoteNodeID, publicationID, subscriberID string) {
	b.mu.Lock()
	sess := b.byNode[remoteNodeID]
	b.mu.Unlock()
	if sess == nil {
		return
	}
	sess.removeTrack(publicationID)
	b.metrics.mediaRemoteTrackDel()
	msg := b.newMsg(MsgMediaUnsubscribe, mustJSON(mediaUnsubscribe{
		SessionID: sess.IDHex, PublicationID: publicationID, SubscriberID: subscriberID,
	}))
	sctx, cancel := context.WithTimeout(ctx, b.reqTimeout())
	defer cancel()
	_ = b.send(sctx, remoteNodeID, msg)
	b.logger.Info("remote_track_removed", "session", sess.IDHex, "publication", publicationID)
}

// handleControl processes an inbound media.* ClusterMessage.
func (b *mediaBridge) handleControl(ctx context.Context, msg ClusterMessage) error {
	switch msg.Type {
	case MsgMediaSessionOpen:
		return b.onSessionOpen(ctx, msg)
	case MsgMediaSessionAccept:
		return b.resolveAccept(msg)
	case MsgMediaSessionReject:
		return b.resolveReject(msg)
	case MsgMediaSessionClose:
		return b.onSessionClose(msg)
	case MsgMediaSubscribe:
		return b.onSubscribe(ctx, msg)
	case MsgMediaSubscribeAck:
		return b.resolveSubAck(msg)
	case MsgMediaUnsubscribe:
		return b.onUnsubscribe(msg)
	case MsgMediaTrackEnded:
		return b.onTrackEnded(msg)
	}
	return &Error{Code: CodeClusterMessageInvalid, Message: "unknown media message"}
}

func (b *mediaBridge) onSessionOpen(_ context.Context, msg ClusterMessage) error {
	var p mediaSessionOpen
	if json.Unmarshal(msg.Payload, &p) != nil || p.SessionID == "" {
		return &Error{Code: CodeMediaSessionInvalid, Message: "bad session open"}
	}
	sid, err := hex.DecodeString(p.SessionID)
	if err != nil || len(sid) != 16 {
		return &Error{Code: CodeMediaSessionInvalid, Message: "bad session id"}
	}
	var id [16]byte
	copy(id[:], sid)

	conn, cerr := b.transport.acceptSession(id, msg.SourceNodeID, p.MediaAddr)
	if cerr != nil {
		return cerr
	}
	sess := newMediaSession(id, p.RoomID, b.selfID, msg.SourceNodeID, conn)
	sess.onClose = b.forget
	b.wireConn(sess, conn)

	b.mu.Lock()
	if _, exists := b.bySID[p.SessionID]; !exists {
		b.byNode[msg.SourceNodeID] = sess
		b.bySID[p.SessionID] = sess
		b.metrics.mediaSessionCreated()
	} else {
		sess = b.bySID[p.SessionID]
	}
	b.mu.Unlock()

	b.logger.Info("media_session_created", "session", sess.IDHex, "remoteNodeId", msg.SourceNodeID, "room", p.RoomID)
	go b.promoteWhenReady(sess)

	accept := b.newMsg(MsgMediaSessionAccept, mustJSON(mediaSessionAccept{
		SessionID: p.SessionID, MediaAddr: b.LocalMediaAddr(),
	}))
	sctx, cancel := context.WithTimeout(context.Background(), b.reqTimeout())
	defer cancel()
	return b.send(sctx, msg.SourceNodeID, accept)
}

func (b *mediaBridge) promoteWhenReady(sess *MediaSession) {
	deadline := time.Now().Add(b.reqTimeout() + 2*time.Second)
	for time.Now().Before(deadline) {
		if sess.State() == MediaSessionClosed {
			return
		}
		if c, ok := sess.conn.(*udpMediaConn); ok && c.authenticated() {
			sess.setState(MediaSessionActive)
			b.logger.Info("media_session_connected", "session", sess.IDHex, "remoteNodeId", sess.RemoteNodeID)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (b *mediaBridge) resolveAccept(msg ClusterMessage) error {
	var p mediaSessionAccept
	if json.Unmarshal(msg.Payload, &p) != nil {
		return &Error{Code: CodeMediaSessionInvalid, Message: "bad accept"}
	}
	b.mu.Lock()
	ch := b.pendingAccept[p.SessionID]
	delete(b.pendingAccept, p.SessionID)
	b.mu.Unlock()
	if ch != nil {
		ch <- p
	}
	return nil
}

func (b *mediaBridge) resolveReject(msg ClusterMessage) error {
	var p mediaSessionReject
	_ = json.Unmarshal(msg.Payload, &p)
	b.mu.Lock()
	if s, ok := b.bySID[p.SessionID]; ok {
		go s.Close()
	}
	delete(b.pendingAccept, p.SessionID)
	b.mu.Unlock()
	return nil
}

func (b *mediaBridge) onSessionClose(msg ClusterMessage) error {
	var p mediaSessionClose
	_ = json.Unmarshal(msg.Payload, &p)
	b.mu.Lock()
	s := b.bySID[p.SessionID]
	b.mu.Unlock()
	if s != nil {
		s.Close()
	}
	return nil
}

func (b *mediaBridge) onSubscribe(_ context.Context, msg ClusterMessage) error {
	var p mediaSubscribe
	if json.Unmarshal(msg.Payload, &p) != nil {
		return &Error{Code: CodeMediaSessionInvalid, Message: "bad subscribe"}
	}
	b.mu.Lock()
	sess := b.bySID[p.SessionID]
	b.mu.Unlock()
	reject := func(reason string) error {
		ack := b.newMsg(MsgMediaSubscribeAck, mustJSON(mediaSubscribeAck{
			SessionID: p.SessionID, PublicationID: p.PublicationID, Accepted: false, Reason: reason,
		}))
		sctx, cancel := context.WithTimeout(context.Background(), b.reqTimeout())
		defer cancel()
		return b.send(sctx, msg.SourceNodeID, ack)
	}
	if sess == nil {
		return reject(CodeMediaSessionNotFound)
	}
	src := b.getSource()
	if src == nil {
		return reject(CodeRemoteTrackNotFound)
	}
	info, detach, ok := src.AttachForwarder(p.RoomID, p.PublicationID, func(pkt []byte) {
		_ = sess.conn.SendRTP(p.PublicationID, pkt)
	})
	if !ok {
		return reject(CodeRemoteTrackNotFound)
	}
	sess.addForwarder(p.PublicationID, detach)
	b.logger.Info("remote_track_created", "session", sess.IDHex, "publication", p.PublicationID, "role", "source")

	ack := b.newMsg(MsgMediaSubscribeAck, mustJSON(mediaSubscribeAck{
		SessionID: p.SessionID, PublicationID: p.PublicationID, ParticipantID: info.ParticipantID,
		Kind: info.Kind, MimeType: info.MimeType, ClockRate: info.ClockRate, Channels: info.Channels,
		PayloadType: info.PayloadType, SSRC: info.SSRC, Accepted: true,
	}))
	sctx, cancel := context.WithTimeout(context.Background(), b.reqTimeout())
	defer cancel()
	return b.send(sctx, msg.SourceNodeID, ack)
}

func (b *mediaBridge) resolveSubAck(msg ClusterMessage) error {
	var p mediaSubscribeAck
	if json.Unmarshal(msg.Payload, &p) != nil {
		return &Error{Code: CodeMediaSessionInvalid, Message: "bad subscribe ack"}
	}
	b.mu.Lock()
	sess := b.bySID[p.SessionID]
	var ch chan mediaSubscribeAck
	if sess != nil {
		ch = b.pendingSubAck[sess.IDHex+"|"+p.PublicationID]
	}
	b.mu.Unlock()
	if ch != nil {
		ch <- p
	}
	return nil
}

func (b *mediaBridge) onUnsubscribe(msg ClusterMessage) error {
	var p mediaUnsubscribe
	if json.Unmarshal(msg.Payload, &p) != nil {
		return &Error{Code: CodeMediaSessionInvalid, Message: "bad unsubscribe"}
	}
	b.mu.Lock()
	sess := b.bySID[p.SessionID]
	b.mu.Unlock()
	if sess != nil {
		sess.removeForwarder(p.PublicationID)
	}
	return nil
}

func (b *mediaBridge) onTrackEnded(msg ClusterMessage) error {
	var p mediaTrackEnded
	if json.Unmarshal(msg.Payload, &p) != nil {
		return &Error{Code: CodeMediaSessionInvalid, Message: "bad track ended"}
	}
	b.mu.Lock()
	sess := b.bySID[p.SessionID]
	b.mu.Unlock()
	if sess != nil {
		sess.removeTrack(p.PublicationID)
		b.metrics.mediaRemoteTrackDel()
	}
	return nil
}

// PublicationEnded is called by the publisher-side SFU when a local publication
// that is being forwarded goes away. It stops every forwarder and tells
// the subscribing nodes.
func (b *mediaBridge) PublicationEnded(roomID, publicationID string) {
	b.mu.Lock()
	sessions := make([]*MediaSession, 0, len(b.byNode))
	for _, s := range b.byNode {
		sessions = append(sessions, s)
	}
	b.mu.Unlock()
	for _, s := range sessions {
		s.mu.Lock()
		_, has := s.forwarders[publicationID]
		s.mu.Unlock()
		if !has {
			continue
		}
		s.removeForwarder(publicationID)
		msg := b.newMsg(MsgMediaTrackEnded, mustJSON(mediaTrackEnded{SessionID: s.IDHex, PublicationID: publicationID}))
		sctx, cancel := context.WithTimeout(context.Background(), b.reqTimeout())
		_ = b.send(sctx, s.RemoteNodeID, msg)
		cancel()
	}
}

func (b *mediaBridge) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	sessions := make([]*MediaSession, 0, len(b.bySID))
	for _, s := range b.bySID {
		sessions = append(sessions, s)
	}
	b.mu.Unlock()
	for _, s := range sessions {
		s.Close()
	}
	return b.transport.Close()
}

func splitHostPort(hp string) (host, port string, ok bool) {
	for i := len(hp) - 1; i >= 0; i-- {
		if hp[i] == ':' {
			return hp[:i], hp[i+1:], true
		}
	}
	return hp, "", false
}
