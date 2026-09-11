// Package sfu implements the PulseRTC Selective Forwarding Unit.
//
// The SFU owns one webrtc.PeerConnection per participant and forwards RTP
// packets from a publisher to every subscriber in the same room WITHOUT
// decoding or re-encoding the media. The media stays encrypted/encoded end to
// end; the SFU only copies RTP packets between tracks.
//
// Responsibility split:
//
//	signaling  -> rooms, participants, negotiation transport, control messages
//	sfu        -> PeerConnections, Tracks, Publications, Subscriptions, forwarding
//
// The signaling layer never touches SDP/RTP; this package never touches the
// WebSocket. They meet only through the Transport interface.
package sfu

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/nack"
	"github.com/pion/webrtc/v4"
)

// Signaling message types exchanged between a browser and its SFU peer.
// SDP/ICE messages travel inside the {type, payload} envelope; the pub/sub
// events carry their fields at the top level (see docs/networking/protocol.md).
const (
	// WebRTC negotiation (perfect negotiation: both sides may offer).
	MsgOffer        = "sfu_offer"
	MsgAnswer       = "sfu_answer"
	MsgICECandidate = "sfu_ice_candidate"

	// Pub/sub events, server -> browser.
	MsgPublicationAdded    = "publication_added"
	MsgPublicationRemoved  = "publication_removed"
	MsgPublicationMuted    = "publication_muted"
	MsgSubscriptionAdded   = "subscription_added"
	MsgSubscriptionRemoved = "subscription_removed"

	// Authorization refusals, server -> browser.
	MsgPublishDenied   = "publish_denied"
	MsgSubscribeDenied = "subscribe_denied"
)

// Transport pushes a message to a single browser participant. The concrete
// implementation lives in the signaling package (a WebSocket client). msg is
// always JSON-serialisable.
type Transport interface {
	SendSFU(msg any)
}

// signalEnvelope carries SDP/ICE: a discriminating "type" plus a "payload".
type signalEnvelope struct {
	Type    string `json:"type"`
	Payload any    `json:"payload"`
}

type sdpPayload struct {
	SDP string `json:"sdp"`
}

// publicationEvent is a server -> browser pub/sub notification.
type publicationEvent struct {
	Type          string `json:"type"`
	PublicationID string `json:"publicationId"`
	ParticipantID string `json:"participantId"`
	Kind          string `json:"kind"`
	Source        string `json:"source,omitempty"`
	Muted         bool   `json:"muted"`
}

// subscriptionEvent acknowledges a subscribe / unsubscribe for one publication.
type subscriptionEvent struct {
	Type          string `json:"type"`
	PublicationID string `json:"publicationId"`
	ParticipantID string `json:"participantId"`
	Kind          string `json:"kind"`
	Source        string `json:"source,omitempty"`
}

// SFU is the media plane. One instance per server process (single-node).
type SFU struct {
	api    *webrtc.API
	config webrtc.Configuration
	logger *slog.Logger

	negLimiter *negotiationLimiter

	subQueueAudioCap int
	subQueueVideoCap int

	mu    sync.RWMutex
	rooms map[string]*Room

	roomsCreated           atomic.Int64
	roomsClosed            atomic.Int64
	peerConnectionsMade    atomic.Int64
	peerConnectionsClose   atomic.Int64
	publicationsMade       atomic.Int64
	publicationsRemoved    atomic.Int64
	screenPublicationsMade atomic.Int64
	subscriptionsMade      atomic.Int64
	subscriptionsRemoved   atomic.Int64
	subscriptionsFailed    atomic.Int64

	// negotiation counters.
	negotiationsStarted   atomic.Int64
	negotiationsCompleted atomic.Int64
	negotiationsCoalesced atomic.Int64
	negotiationsFailed    atomic.Int64

	// Part B: fixed-bucket histogram of SFU-side negotiation duration.
	negotiationDur *durationHistogram

	// ADR 012 per-subscriber send-queue counters.
	subQueueDroppedAudio atomic.Int64
	subQueueDroppedVideo atomic.Int64
	subQueueResyncs      atomic.Int64

	// Fired when a locally published track that may be forwarded
	// cross-node ends, so the cluster media bridge can tear down remote mirrors.
	mediaPubEnded atomic.Pointer[func(roomID, publicationID string)]
	// Fired on add/remove of a LOCAL publication so the signaling
	// layer can announce it to other nodes in the room.
	pubEvent atomic.Pointer[func(roomID string, added bool, info PublicationInfo)]
	// Part D: fired when a subscription is torn down after a persistent
	// media-transport failure on its write loop.
	subFailedHook atomic.Pointer[func(roomID, subscriberID, publicationID string)]
}

// PublicationInfo is the minimal cross-node announcement of a publication.
type PublicationInfo struct {
	PublicationID string
	ParticipantID string
	Kind          string
}

// SetPublicationEventHook registers the callback described on pubEvent.
func (s *SFU) SetPublicationEventHook(fn func(roomID string, added bool, info PublicationInfo)) {
	s.pubEvent.Store(&fn)
}

// SetSubscriptionFailedHook registers the callback described on subFailedHook.
func (s *SFU) SetSubscriptionFailedHook(fn func(roomID, subscriberID, publicationID string)) {
	s.subFailedHook.Store(&fn)
}

// FireSubscriptionFailedHook invokes the registered subscription-failed hook (if
// any) directly and reports whether one was registered. It lets a wiring test in
// another package prove the hook is connected without inducing a real
// media-transport failure. Production code fires the hook via onSubscriptionFatal.
func (s *SFU) FireSubscriptionFailedHook(roomID, subscriberID, publicationID string) bool {
	fn := s.subFailedHook.Load()
	if fn == nil {
		return false
	}
	(*fn)(roomID, subscriberID, publicationID)
	return true
}

func (s *SFU) firePublicationEvent(roomID string, added bool, pub *Publication) {
	if pub.remote {
		return
	}
	if fn := s.pubEvent.Load(); fn != nil {
		(*fn)(roomID, added, PublicationInfo{
			PublicationID: pub.id, ParticipantID: pub.participantID, Kind: pub.Kind(),
		})
	}
}

// New builds an SFU with the default codecs and the default interceptors
// (RTCP reports, NACK generator/responder, TWCC). Those interceptors are what
// make RTCP feedback keep working through the SFU without any custom code.
// The NACK responder's retained-packet buffer size is tuned down from Pion's
// default (see nackResponderSize) — a heap profile under load showed it as
// ~85% of retained heap, scaling with subscription count.
func New(logger *slog.Logger) (*SFU, error) {
	if logger == nil {
		logger = slog.Default()
	}

	cfg := configFromEnv(logger)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	me := &webrtc.MediaEngine{}
	if err := me.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}

	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptorsWithOptions(me, ir,
		webrtc.WithNackResponderOptions(nack.ResponderSize(cfg.NackBufferSize)),
	); err != nil {
		return nil, err
	}

	// SettingEngine lets us pin ICE to a single, well-known UDP port and
	// advertise a reachable IP. This is what makes the SFU work from behind
	// Docker's port mapping: without it Pion would only offer the container's
	// private IP, which the browser on the host cannot reach.
	se := webrtc.SettingEngine{}
	if err := configureNetwork(&se, logger); err != nil {
		return nil, err
	}

	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(me),
		webrtc.WithInterceptorRegistry(ir),
		webrtc.WithSettingEngine(se),
	)

	return &SFU{
		api:              api,
		config:           webrtc.Configuration{ICEServers: cfg.ICEServers},
		logger:           logger,
		rooms:            make(map[string]*Room),
		negLimiter:       newNegotiationLimiter(cfg.NegotiationConcurrency),
		negotiationDur:   newDurationHistogram(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
		subQueueAudioCap: cfg.SubQueueAudio,
		subQueueVideoCap: cfg.SubQueueVideo,
	}, nil
}

// subQueueCap returns the send-queue capacity for a media kind.
func (s *SFU) subQueueCap(kind queueKind) int {
	if kind == queueKindVideo {
		return s.subQueueVideoCap
	}
	return s.subQueueAudioCap
}

// addSubQueueDrops rolls per-subscriber queue drops into SFU-wide counters.
func (s *SFU) addSubQueueDrops(kind queueKind, n int64) {
	if kind == queueKindVideo {
		s.subQueueDroppedVideo.Add(n)
	} else {
		s.subQueueDroppedAudio.Add(n)
	}
}

// Room returns the room with the given id, creating it if necessary.
func (s *SFU) Room(id string) *Room {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[id]
	if !ok {
		r = newRoom(id, s)
		s.rooms[id] = r
		s.roomsCreated.Add(1)
		s.logger.Info("sfu_room_created", "room", id)
	}
	return r
}

// GetRoom returns the room with the given id if it exists.
func (s *SFU) GetRoom(id string) (*Room, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rooms[id]
	return r, ok
}

// Leave removes a participant from a room and releases its resources. Safe to
// call for an unknown room/participant.
func (s *SFU) Leave(roomID, participantID string) {
	r, ok := s.GetRoom(roomID)
	if !ok {
		return
	}
	r.Leave(participantID)
}

func (s *SFU) removeRoomIfEmpty(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.rooms[id]; ok && r.size() == 0 {
		delete(s.rooms, id)
		s.roomsClosed.Add(1)
		s.logger.Info("sfu_room_closed", "room", id)
	}
}

// RoomCount reports the number of live SFU rooms.
func (s *SFU) RoomCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rooms)
}

// LoadCounts walks the live rooms and returns active (rooms, participants,
// publications, subscriptions) for load reporting. It excludes remote
// (mirrored) publications — those are load on the origin node, not here.
func (s *SFU) LoadCounts() (rooms, participants, publications, subscriptions int) {
	s.mu.RLock()
	roomList := make([]*Room, 0, len(s.rooms))
	for _, r := range s.rooms {
		roomList = append(roomList, r)
	}
	s.mu.RUnlock()

	rooms = len(roomList)
	for _, r := range roomList {
		for _, p := range r.snapshot() {
			participants++
			p.mu.Lock()
			publications += len(p.publications)
			subscriptions += len(p.subscriptions)
			p.mu.Unlock()
		}
	}
	return
}
