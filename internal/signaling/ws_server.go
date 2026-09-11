package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"

	"github.com/raulhrosa/pulsertc/internal/auth"
	"github.com/raulhrosa/pulsertc/internal/cluster"
	"github.com/raulhrosa/pulsertc/internal/history"
	"github.com/raulhrosa/pulsertc/internal/quality"
	"github.com/raulhrosa/pulsertc/internal/room"
	"github.com/raulhrosa/pulsertc/internal/sfu"
)

// Server wires WebSocket connections to the control plane (room manager), the
// media plane (SFU) and the QoE engine. They share only participant ids.
type Server struct {
	rooms    *room.Manager
	sfu      *sfu.SFU
	quality  *quality.Engine
	auth     *auth.Authenticator
	cluster  *cluster.Cluster
	logger   *slog.Logger
	started  time.Time
	upgrader websocket.Upgrader

	// clients indexes locally-connected participants by id so cross-node
	// signaling can deliver a routed message to the right socket.
	clientsMu sync.RWMutex
	clients   map[string]*Client

	// Session recovery.
	recovery  SessionRecoveryConfig
	sessions  *SessionRegistry
	sessionMx *sessionMetrics

	// Call history: an aggregated RoomSummary / ParticipantSummary emitted as
	// a structured log when a room is torn down. Never on the RTP path.
	history *history.Recorder

	// Cross-node media. remotePubs maps "room|pub" to the node that
	// hosts a publication; mirrorSubs counts local interest in each mirror.
	remoteMediaMu sync.Mutex
	remotePubs    map[string]remotePubRef
	mirrorSubs    map[string]map[string]bool

	// Optional integration-API event sink. Set by cmd/server when
	// webhooks are configured; nil otherwise (zero overhead). Called off the hot
	// path, on join / leave only.
	onParticipantEvent func(evType, roomID, participantID string)
}

// SetParticipantEventHook registers a callback invoked on participant join /
// leave (webhooks). Safe to call once at startup, before serving.
func (s *Server) SetParticipantEventHook(fn func(evType, roomID, participantID string)) {
	s.onParticipantEvent = fn
}

// participantName returns the display name of a locally-connected participant
// (from its validated token), or "" when the id is unknown on this node or the
// token carried no name. Remote participants always return "" — their name is
// baked into the events the owning node emits.
func (s *Server) participantName(id string) string {
	s.clientsMu.RLock()
	c := s.clients[id]
	s.clientsMu.RUnlock()
	if c != nil && c.identity != nil {
		return c.identity.Name
	}
	return ""
}

// nameOf returns a client's display name, tolerating a nil identity.
func nameOf(c *Client) string {
	if c != nil && c.identity != nil {
		return c.identity.Name
	}
	return ""
}

// logicalID is the identity that stays stable across reconnects — the token
// subject. It falls back to the per-connection id when there is no identity
// (auth disabled in a bare test), so call history still has a key.
func logicalID(c *Client) string {
	if c == nil {
		return ""
	}
	if c.identity != nil && c.identity.Subject != "" {
		return c.identity.Subject
	}
	return c.id
}

func (s *Server) fireParticipantEvent(evType, roomID, participantID string) {
	if s.onParticipantEvent != nil {
		s.onParticipantEvent(evType, roomID, participantID)
	}
}

// NewServer builds a signaling server with an empty room manager, a fresh
// single-node SFU and an Authenticator configured from the PULSERTC_* env vars.
// It fails if authentication is enabled without a signing secret.
// A nil logger falls back to slog.Default().
func NewServer(logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}
	authenticator, err := auth.Build(auth.FromEnv(), logger)
	if err != nil {
		return nil, err
	}
	clu, err := cluster.New(cluster.FromEnv(), logger)
	if err != nil {
		return nil, err
	}
	return NewServerWithDeps(logger, authenticator, clu)
}

// NewServerWithAuthenticator builds a server with an explicit Authenticator and
// a disabled (single-node) cluster. Used by the existing tests.
func NewServerWithAuthenticator(logger *slog.Logger, authenticator *auth.Authenticator) (*Server, error) {
	clu, err := cluster.New(cluster.Config{}, logger)
	if err != nil {
		return nil, err
	}
	return NewServerWithDeps(logger, authenticator, clu)
}

// NewServerWithDeps builds a server with an explicit Authenticator and Cluster.
// Used by tests to inject a known signing secret / a multi-node cluster without
// touching the process environment.
func NewServerWithDeps(logger *slog.Logger, authenticator *auth.Authenticator, clu *cluster.Cluster) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}

	media, err := sfu.New(logger)
	if err != nil {
		return nil, err
	}

	recovery := sessionRecoveryConfigFromEnv()
	sessionMx := &sessionMetrics{}
	sessions := newSessionRegistry(recovery)
	sessions.onStarted = func() { sessionMx.started.Add(1) }
	sessions.onReplaced = func() { sessionMx.replaced.Add(1) }
	sessions.onTimeout = func() { sessionMx.timeout.Add(1); sessionMx.failed.Add(1) }
	sessions.onReconnect = func(d time.Duration) {
		sessionMx.attempts.Add(1)
		sessionMx.success.Add(1)
		sessionMx.observeDuration(d.Milliseconds())
	}

	srv := &Server{
		rooms:     room.NewManager(),
		sfu:       media,
		quality:   quality.New(quality.DefaultConfig()),
		auth:      authenticator,
		cluster:   clu,
		logger:    logger,
		started:   time.Now(),
		clients:   make(map[string]*Client),
		recovery:  recovery,
		sessions:  sessions,
		sessionMx: sessionMx,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// The token travels as the "pulsertc.token.*" subprotocol
			// for browsers; the server negotiates the plain "pulsertc".
			Subprotocols: []string{auth.WSProtocol},
			// Origin checks are out of scope for; local testing may
			// connect from file://.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
	// Call-history recorder. The negotiation figures come from the SFU's own
	// official counters, diffed over the room's lifetime.
	srv.history = history.NewRecorder(logger, func() (int64, int64) {
		m := srv.sfu.Metrics()
		return m.Negotiations.Started, m.Negotiations.Failed
	})
	// The cluster routes messages destined for a participant on this
	// node back through the signaling layer.
	clu.SetLocalDelivery((*clusterDelivery)(srv))
	// Wire the SFU to the cross-node media bridge.
	srv.wireCrossNodeMedia()
	// Feed the cluster load reporter this node's live counts.
	srv.wireLoadProvider()
	// D3: notify a subscriber over its WebSocket when the SFU tears its
	// subscription down after a persistent media-transport failure. Wired
	// unconditionally — single-node subscriptions fail too.
	srv.sfu.SetSubscriptionFailedHook(srv.onSubscriptionFailed)
	return srv, nil
}

// onSubscriptionFailed is the sfu.SetSubscriptionFailedHook callback (D3): the
// SFU has torn down subscriberID's subscription to publicationID after a
// persistent media-transport failure. If the subscriber is connected to this
// node, tell it so it can decide to re-subscribe.
func (s *Server) onSubscriptionFailed(roomID, subscriberID, publicationID string) {
	c, ok := s.localClient(subscriberID)
	if !ok || c == nil {
		return
	}
	s.logger.Warn("subscription_failed_notified",
		"room", roomID, "subscriber", subscriberID, "publication", publicationID)
	c.sendJSON(SubscriptionFailedMsg{
		Type:          TypeSubscriptionFailed,
		PublicationID: publicationID,
		Reason:        "media_transport_failed",
	})
}

// Protected wraps an HTTP handler so it requires a valid bearer token unless
// auth is disabled or PULSERTC_METRICS_PUBLIC=true. The
// health check is never wrapped so Docker / orchestration keep working.
func (s *Server) Protected(h http.HandlerFunc) http.HandlerFunc {
	return s.auth.ProtectHTTP(h)
}

// SweepRateLimiters drops idle per-IP rate-limit buckets. Call periodically.
func (s *Server) SweepRateLimiters() { s.auth.SweepRateLimiters() }

// Cluster exposes the multi-node coordinator so main can register its HTTP
// routes and drive its lifecycle.
func (s *Server) Cluster() *cluster.Cluster { return s.cluster }

// HandleHealth answers the health check endpoint.
func (s *Server) HandleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// HandleSFUStats returns a diagnostics snapshot for the room named in ?room=.
// It distinguishes each participant's role (publisher / subscriber) and reports
// both the Publisher->SFU and SFU->Subscriber legs.
func (s *Server) HandleSFUStats(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("room")
	w.Header().Set("Content-Type", "application/json")
	if roomID == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"room query parameter is required"}`))
		return
	}
	_ = json.NewEncoder(w).Encode(s.sfu.Stats(roomID))
}

// HandleQualityStats returns the QoE snapshot for every participant in ?room=.
// The verdicts are derived from browser-reported WebRTC stats.
func (s *Server) HandleQualityStats(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("room")
	w.Header().Set("Content-Type", "application/json")
	if roomID == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"room query parameter is required"}`))
		return
	}

	// Drop verdicts for streams that stopped reporting (unpublished tracks,
	// gone participants) before answering.
	s.quality.Prune(30 * time.Second)

	ids := make([]string, 0)
	for _, p := range s.sfu.Stats(roomID).Participants {
		ids = append(ids, p.ID)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"room":         roomID,
		"participants": s.quality.SnapshotRoom(ids),
	})
}

// HandleWS authenticates the request, upgrades it to a WebSocket connection and
// registers a client. Authentication happens BEFORE the upgrade:
// a bad token gets a plain 401 and the socket is never established.
func (s *Server) HandleWS(w http.ResponseWriter, r *http.Request) {
	identity, secErr := s.auth.AuthenticateRequest(r)
	if secErr != nil {
		status := http.StatusUnauthorized
		if secErr.Code == auth.CodeRateLimited {
			status = http.StatusTooManyRequests
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"` + secErr.Code + `"}`))
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("ws_upgrade", "err", err)
		return
	}

	// The server is the sole authority on identity: the participant id is
	// derived from the validated token subject here, never from client input.
	client := newClient(auth.ParticipantID(identity), conn, s, identity)
	s.logger.Info("participant_connected",
		"participant", client.id, "subject", identity.Subject)

	client.sendJSON(map[string]any{
		"type":          "welcome",
		"participantId": client.id,
		"userId":        identity.UserID(),
		"name":          identity.Name,
		"iceServers":    s.sfu.ICEServers(),
	})

	go client.writePump()
	go client.readPump()
	client.scheduleTokenExpiry()
}

func (s *Server) handleMessage(c *Client, data []byte) {
	var in Inbound
	if err := json.Unmarshal(data, &in); err != nil {
		c.sendJSON(newError("invalid message: not valid JSON"))
		return
	}

	switch {
	case in.Type == TypeJoin:
		s.handleJoin(c, in)
	case in.Type == TypeSessionResume:
		s.handleSessionResume(c, in)
	case in.Type == TypeQualityReport:
		s.handleQualityReport(c, data)
	case in.Type == TypeControl:
		s.handleControl(c, in)
	case sfuTypes[in.Type]:
		s.handleSFU(c, in)
	case forwardableTypes[in.Type]:
		s.handleForward(c, in)
	default:
		c.sendJSON(newError("unknown message type: " + in.Type))
	}
}

func (s *Server) handleJoin(c *Client, in Inbound) {
	if in.RoomID == "" {
		c.sendJSON(newError("join requires a non-empty roomId"))
		return
	}
	if c.currentRoom() != "" {
		c.sendJSON(newError("already joined a room; open a new connection to join another"))
		return
	}

	// Authorize the join (JOIN permission + room claim) before
	// any room / SFU resource is created.
	if err := auth.AuthorizeJoin(c.identity, in.RoomID); err != nil {
		e := auth.AsError(err)
		s.auth.RecordAuthzFailure(e.Code)
		s.logger.Warn("join_denied",
			"participant", c.id, "subject", c.identity.Subject,
			"room", in.RoomID, "reason", e.Code)
		c.sendJSON(newSecError(e))
		return
	}

	// Resolve room ownership before creating any local
	// state. When the cluster is disabled this always claims locally and is a
	// no-op difference from the earlier flow. When another node owns the
	// room, the client is told where to go (ROOM_ON_OTHER_NODE) and no local
	// room / SFU peer is created here.
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	res, cerr := s.cluster.ClaimRoom(ctx, in.RoomID)
	cancel()
	if cerr != nil {
		var ce *cluster.Error
		if errors.As(cerr, &ce) {
			s.logger.Info("join_room_not_local",
				"participant", c.id, "room", in.RoomID, "reason", ce.Code, "owner", ce.NodeID)
			c.sendJSON(newClusterError(ce))
		} else {
			c.sendJSON(newError("could not resolve room ownership"))
		}
		return
	}
	_ = res

	// Open (or resume) the logical session for this identity in this
	// room. Enforces one active session per (identity, room) on this node and
	// generation fencing. A stale resume is refused here, before
	// any room / SFU state is created.
	var openRes OpenResult
	if s.recovery.Enabled {
		var serr *SessionError
		openRes, serr = s.sessions.Open(c.identity.Subject, in.RoomID, c.id, s.cluster.NodeID(), in.Resume)
		if serr != nil {
			s.sessionMx.stale.Add(1)
			s.sessionMx.attempts.Add(1)
			s.sessionMx.failed.Add(1)
			s.logger.Warn("session resume rejected",
				"participantId", c.id, "room", in.RoomID, "reason", serr.Code)
			c.sendJSON(SessionEvent{Type: TypeSessionStale, Reason: serr.Code, RoomID: in.RoomID})
			return
		}
		c.setSession(openRes.Session.SessionID, openRes.Session.Generation)
		if openRes.Kick != "" {
			s.replaceSession(openRes.Kick, in.RoomID)
		}
	}

	r := s.rooms.GetOrCreate(in.RoomID)

	// Capture existing membership before adding ourselves so room_joined lists
	// only the participants that were already there, and so the joining
	// participant does not receive its own participant_joined.
	existing := r.ParticipantIDs()

	r.Add(c)
	c.setRoom(in.RoomID)
	s.trackClient(c)

	// Create this participant's dedicated PeerConnection with the SFU. The
	// browser will start the WebRTC negotiation with an sfu_offer.
	peer, err := s.sfu.Room(in.RoomID).JoinWithPermissions(c.id, c, sfu.Permissions{
		Publish:   c.identity.Permissions.Publish,
		Subscribe: c.identity.Permissions.Subscribe,
	})
	if err != nil {
		s.logger.Error("sfu_join", "participant", c.id, "room", in.RoomID, "err", err)
		c.sendJSON(newError("could not attach to the media server"))
		r.Remove(c.id)
		c.setRoom("")
		if r.Size() == 0 {
			s.cluster.ReleaseRoom(in.RoomID)
		}
		return
	}
	c.setSFUPeer(peer)

	// Record this participant's location. Only the location is
	// shared cluster-wide — the WebRTC session stays on this node.
	s.cluster.RegisterParticipant(c.id, in.RoomID)

	// Open (or extend) the call-history summary for this room.
	s.history.EnsureRoom(in.RoomID)
	s.history.ParticipantJoined(in.RoomID, logicalID(c), nameOf(c),
		s.recovery.Enabled && openRes.Reconnected)

	participants := make([]ParticipantInfo, 0, len(existing))
	for _, id := range existing {
		participants = append(participants, ParticipantInfo{
			ParticipantID: id,
			Name:          s.participantName(id),
		})
	}
	rj := RoomJoined{
		Type:         TypeRoomJoined,
		RoomID:       in.RoomID,
		Participants: participants,
		Quality:      s.roomQualityStates(existing),
	}
	if s.recovery.Enabled {
		// Fold the current LOGICAL room state into room_joined so a
		// reconnecting client can reconcile against it — no PeerConnection / ICE
		// / DTLS / SRTP / Pion state, built from the live node.
		snap := s.buildRoomSnapshot(in.RoomID)
		rj.SessionID = openRes.Session.SessionID
		rj.Generation = openRes.Session.Generation
		rj.RoomGeneration = snap.RoomGeneration
		rj.OwnerNodeID = s.cluster.NodeID()
		rj.Reconnected = openRes.Reconnected
		rj.Reconnect = s.recovery.ReconnectHints.wire()
		rj.Snapshot = &snap
	}
	c.sendJSON(rj)

	if s.recovery.Enabled && openRes.Reconnected {
		s.quality.SetRecovering(c.id, false)
		c.sendJSON(SessionEvent{
			Type: TypeSessionReconnected, RoomID: in.RoomID,
			SessionID: openRes.Session.SessionID,
		})
		s.sessionMx.rtcReconn.Add(1)
		s.logger.Info("session reconnected",
			"participantId", c.id, "room", in.RoomID,
			"newGeneration", openRes.Session.Generation,
			"duration", openRes.RecoveryDur.String())
	}

	joined, _ := json.Marshal(ParticipantEvent{
		Type: TypeParticipantJoined, ParticipantID: c.id, Name: c.identity.Name,
	})
	r.Broadcast(joined, c.id)
	// Let participants of this room on other nodes learn about
	// the newcomer too.
	s.broadcastRoomEventRemote(in.RoomID, joined)

	// Tell the newcomer which publications already exist so its UI can list
	// them; the SFU auto-subscribes once the PeerConnection is connected.
	peer.SendExistingPublications()

	s.logger.Info("participant_joined",
		"participant", c.id, "room", in.RoomID, "participants", r.Size())
	s.fireParticipantEvent("participant.joined", in.RoomID, c.id)
}

// handleSFU consumes the browser side of the SFU PeerConnection negotiation.
// The media (SDP/ICE) never leaves this process — it terminates at the SFU.
func (s *Server) handleSFU(c *Client, in Inbound) {
	peer := c.sfuPeerRef()
	if peer == nil {
		c.sendJSON(newError("join a room before sending " + in.Type + " messages"))
		return
	}
	// Negotiation messages carry an SDP/ICE payload; pub/sub control messages
	// carry publicationId/muted at the top level instead.
	switch in.Type {
	case TypeSFUOffer, TypeSFUAnswer, TypeSFUICECandidate:
		if len(in.Payload) == 0 {
			c.sendJSON(newError(in.Type + " requires a payload"))
			return
		}
	}

	var err error
	switch in.Type {
	case TypeSFUOffer:
		var p struct {
			SDP string `json:"sdp"`
		}
		if json.Unmarshal(in.Payload, &p) != nil || p.SDP == "" {
			c.sendJSON(newError("invalid sfu_offer payload"))
			return
		}
		err = peer.HandleOffer(p.SDP)
	case TypeSFUAnswer:
		var p struct {
			SDP string `json:"sdp"`
		}
		if json.Unmarshal(in.Payload, &p) != nil || p.SDP == "" {
			c.sendJSON(newError("invalid sfu_answer payload"))
			return
		}
		err = peer.HandleAnswer(p.SDP)
	case TypeSFUICECandidate:
		var cand webrtc.ICECandidateInit
		if json.Unmarshal(in.Payload, &cand) != nil {
			c.sendJSON(newError("invalid sfu_ice_candidate payload"))
			return
		}
		err = peer.HandleICECandidate(cand)

	case TypeSubscribe:
		if in.PublicationID == "" {
			c.sendJSON(newError("subscribe requires a publicationId"))
			return
		}
		// Authorize before the SFU allocates the subscription.
		if aerr := auth.AuthorizeSubscribe(c.identity); aerr != nil {
			e := auth.AsError(aerr)
			s.auth.RecordAuthzFailure(e.Code)
			s.logger.Warn("subscribe_denied",
				"participant", c.id, "subject", c.identity.Subject, "reason", e.Code)
			c.sendJSON(newSecError(e))
			return
		}
		// If the publication lives on another node, mirror it in via
		// the cluster media bridge before the local SFU subscribes.
		if merr := s.ensureRemoteMirror(c.currentRoom(), in.PublicationID, c.id); merr != nil {
			c.sendJSON(*merr)
			return
		}
		err = peer.Subscribe(in.PublicationID)
		if err == nil {
			s.recordSubIntent(c, in.PublicationID, true)
		}
	case TypeUnsubscribe:
		if in.PublicationID == "" {
			c.sendJSON(newError("unsubscribe requires a publicationId"))
			return
		}
		peer.Unsubscribe(in.PublicationID)
		s.releaseRemoteMirror(c.currentRoom(), in.PublicationID, c.id)
		s.recordSubIntent(c, in.PublicationID, false)
	case TypePublish:
		// Source declaration for a track the browser is about to add
		// (screen share). No reply; the SFU stamps the Publication when the
		// track arrives. Reuses the existing publish/negotiation flow.
		if in.TrackID == "" || in.Source == "" {
			c.sendJSON(newError("publish requires trackId and source"))
			return
		}
		peer.DeclareSource(in.TrackID, in.Source)
	case TypeUnpublish:
		if in.PublicationID == "" {
			c.sendJSON(newError("unpublish requires a publicationId"))
			return
		}
		peer.Unpublish(in.PublicationID)
	case TypeSetMute:
		if in.PublicationID == "" {
			c.sendJSON(newError("set_mute requires a publicationId"))
			return
		}
		err = peer.SetMute(in.PublicationID, in.Muted)
		if err == nil && s.recovery.Enabled && c.currentRoom() != "" {
			kind := in.Kind
			s.sessions.UpdatePublishIntent(c.identity.Subject, c.currentRoom(),
				TrackIntent{TrackID: in.PublicationID, Kind: kind, Muted: in.Muted})
		}
	}
	if err != nil {
		s.logger.Error("sfu_signal", "participant", c.id, "type", in.Type, "err", err)
		c.sendJSON(newError(in.Type + ": " + err.Error()))
	}
}

// handleControl gates administrative commands over other participants.
// No concrete command exists yet: an identity WITHOUT the CONTROL
// permission is refused with CONTROL_NOT_ALLOWED; one WITH it is told the
// feature is not implemented. This keeps the authorization edge in place and
// testable ahead of the commands themselves.
func (s *Server) handleControl(c *Client, in Inbound) {
	if c.currentRoom() == "" {
		c.sendJSON(newError("join a room before sending control messages"))
		return
	}
	if err := auth.AuthorizeControl(c.identity); err != nil {
		e := auth.AsError(err)
		s.auth.RecordAuthzFailure(e.Code)
		s.logger.Warn("control_denied",
			"participant", c.id, "subject", c.identity.Subject, "reason", e.Code)
		c.sendJSON(newSecError(e))
		return
	}
	c.sendJSON(newError("control commands are not implemented yet"))
}

// handleForward relays "signal" and legacy "webrtc_*" messages to a single
// target in the sender's own room. The payload is never inspected. Cross-room
// delivery is impossible because the target lookup never leaves the sender's room.
func (s *Server) handleForward(c *Client, in Inbound) {
	roomID := c.currentRoom()
	if roomID == "" {
		c.sendJSON(newError("join a room before sending " + in.Type + " messages"))
		return
	}
	if in.Target == "" {
		c.sendJSON(newError(in.Type + " requires a target participant id"))
		return
	}
	if in.Target == c.id {
		c.sendJSON(newError("cannot send " + in.Type + " to yourself"))
		return
	}
	if len(in.Payload) == 0 {
		c.sendJSON(newError(in.Type + " requires a payload"))
		return
	}

	r, ok := s.rooms.Get(roomID)
	if !ok {
		c.sendJSON(newError("your room no longer exists"))
		return
	}

	fwd, _ := json.Marshal(ForwardedMessage{
		Type:    in.Type,
		From:    c.id,
		Target:  in.Target,
		Payload: in.Payload,
	})

	if target, ok := r.Get(in.Target); ok {
		target.Send(fwd)
		return
	}

	// Not in the local room. With a cluster, the target may be a participant of
	// the same room living on another node — route the message there.
	// Media never travels this path; this is a signaling frame.
	if s.cluster.Enabled() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		loc, found := s.cluster.LocateParticipant(ctx, in.Target)
		if !found || loc.RoomID != roomID {
			c.sendJSON(newError("target participant is not in your room"))
			return
		}
		msg := s.cluster.NewClusterMessage(cluster.MsgParticipantMessage, roomID, in.Target, fwd)
		if err := s.cluster.RouteMessage(ctx, in.Target, msg); err != nil {
			var ce *cluster.Error
			if errors.As(err, &ce) {
				s.logger.Warn("cross_node_forward_failed",
					"from", c.id, "target", in.Target, "room", roomID, "reason", ce.Code)
				c.sendJSON(newClusterError(ce))
				return
			}
			c.sendJSON(newError("could not deliver message to the target participant"))
			return
		}
		return
	}

	c.sendJSON(newError("target participant is not in your room"))
}

// disconnect removes a client from its room and the SFU, notifies the remaining
// participants and releases the connection resources. Safe to call once per client.
func (s *Server) disconnect(c *Client) {
	c.stopTokenExpiry()
	roomID := c.currentRoom()
	if roomID != "" {
		if r, ok := s.rooms.Get(roomID); ok {
			left, _ := json.Marshal(ParticipantEvent{
				Type: TypeParticipantLeft, ParticipantID: c.id, Name: nameOf(c),
			})
			r.Broadcast(left, c.id)
			s.broadcastRoomEventRemote(roomID, left)
		}
		s.sfu.Leave(roomID, c.id)
		s.rooms.RemoveParticipant(roomID, c.id)
		s.history.ParticipantLeft(roomID, logicalID(c))
		// Drop the participant's location, and release cluster
		// ownership of the room once it is empty on this node. A future
		// rejoin re-claims it.
		s.cluster.ForgetParticipant(c.id)
		if _, stillHere := s.rooms.Get(roomID); !stillHere {
			s.cluster.ReleaseRoom(roomID)
			// No local participants left → drop any inter-node media
			// mirrors for this room.
			s.dropRoomMirrors(roomID)
			// Finalize + emit the call-history summary before the live
			// Room / SFU / Quality state is forgotten below.
			s.history.RoomClosed(roomID)
		}
	}
	// Park the logical session in RECOVERING so a fast client
	// reconnect can resume it with its generation and intent intact; the
	// registry sweeper closes it if no resume arrives within the timeout.
	// A session already taken over by a newer connection is left untouched.
	if s.recovery.Enabled && roomID != "" && c.identity != nil {
		if s.sessions.MarkRecovering(c.identity.Subject, roomID, c.id) {
			s.quality.SetRecovering(c.id, true)
			s.logger.Info("session recovery started",
				"participantId", c.id, "room", roomID, "reason", "connection lost")
		}
	}

	if roomID != "" {
		s.fireParticipantEvent("participant.left", roomID, c.id)
	}
	s.untrackClient(c.id)
	s.quality.Forget(c.id)

	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.send)
	}
	c.mu.Unlock()

	s.logger.Info("participant_left", "participant", c.id)
}
