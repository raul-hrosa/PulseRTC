package sfu

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

var errNotFound = errors.New("publication not found in this room")

// Participant owns one PeerConnection between a browser and the SFU. The same
// connection carries the participant's Publications (browser -> SFU) and all of
// its Subscriptions (SFU -> browser).
//
// Negotiation: "perfect negotiation" (MDN). Both sides may send an offer — the
// browser when it adds/removes a track (dynamic publish/unpublish), the SFU
// when it adds/removes a forwarded track (subscribe/unsubscribe). The SFU is
// the impolite peer: on an offer collision it ignores the incoming offer and
// its own offer wins; the browser rolls back and retries.
type Participant struct {
	id     string
	room   *Room
	pc     *webrtc.PeerConnection
	tport  Transport
	logger *slog.Logger
	perms  Permissions

	mu             sync.Mutex
	publications   map[string]*Publication  // key: publication id
	subscriptions  map[string]*Subscription // key: publication id
	optedOut       map[string]bool          // publication ids the user unsubscribed from
	pendingSources map[string]string        // browser trackID -> declared source, consumed by onTrack
	pendingCands   []webrtc.ICECandidateInit
	makingOffer    bool
	needNegotiate  bool
	negotiateTimer *time.Timer // pending debounced negotiate
	bootstrapped   bool
	closed         bool
}

func newParticipant(id string, r *Room, t Transport) (*Participant, error) {
	pc, err := r.sfu.api.NewPeerConnection(r.sfu.config)
	if err != nil {
		return nil, err
	}

	p := &Participant{
		id:             id,
		room:           r,
		pc:             pc,
		tport:          t,
		logger:         r.sfu.logger,
		perms:          FullPermissions(),
		publications:   make(map[string]*Publication),
		subscriptions:  make(map[string]*Subscription),
		optedOut:       make(map[string]bool),
		pendingSources: make(map[string]string),
	}

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		p.tport.SendSFU(signalEnvelope{Type: MsgICECandidate, Payload: c.ToJSON()})
	})

	pc.OnTrack(func(remote *webrtc.TrackRemote, recv *webrtc.RTPReceiver) {
		p.onTrack(remote, recv)
	})

	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		p.logger.Info("peer_connection_state",
			"room", r.id, "participant", id, "state", st.String())

		if st == webrtc.PeerConnectionStateConnected {
			p.mu.Lock()
			first := !p.bootstrapped
			p.bootstrapped = true
			p.mu.Unlock()
			if first {
				p.bootstrap()
			}
		}
	})

	pc.OnICEConnectionStateChange(func(st webrtc.ICEConnectionState) {
		p.logger.Debug("ice_connection_state",
			"room", r.id, "participant", id, "state", st.String())
	})

	return p, nil
}

// ID returns the participant id (the same UUID the signaling layer assigned).
func (p *Participant) ID() string { return p.id }

// DeclareSource records that the next incoming track whose browser id is
// trackID carries the given media source ("screen"). The browser sends this
// just before it adds the track; onTrack consumes it. An empty source or
// trackID is ignored. Reusing an existing publication flow, no new pipeline.
func (p *Participant) DeclareSource(trackID, source string) {
	if trackID == "" || source == "" {
		return
	}
	p.mu.Lock()
	p.pendingSources[trackID] = source
	p.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Perfect negotiation
// ---------------------------------------------------------------------------

// HandleOffer applies a browser offer and answers it. Used for the first
// negotiation and for browser-initiated renegotiation (dynamic publish).
func (p *Participant) HandleOffer(sdp string) error {
	p.mu.Lock()
	collision := p.makingOffer || p.pc.SignalingState() != webrtc.SignalingStateStable
	p.mu.Unlock()
	if collision {
		// Impolite peer: ignore the colliding offer. Our own offer wins; the
		// browser rolls back and its OnNegotiationNeeded fires again.
		p.logger.Debug("glare: ignoring browser offer", "participant", p.id)
		return nil
	}

	// The expensive part (SDP parsing) is gated by the SFU-wide
	// negotiation limiter, so a burst of participants joining at once queues
	// instead of fighting for every core simultaneously.
	p.room.sfu.negLimiter.acquire()
	err := p.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: sdp,
	})
	if err != nil {
		p.room.sfu.negLimiter.release()
		return err
	}
	p.flushPendingCandidates()

	answer, err := p.pc.CreateAnswer(nil)
	if err != nil {
		p.room.sfu.negLimiter.release()
		return err
	}
	err = p.pc.SetLocalDescription(answer)
	p.room.sfu.negLimiter.release()
	if err != nil {
		return err
	}
	p.tport.SendSFU(signalEnvelope{Type: MsgAnswer, Payload: sdpPayload{SDP: answer.SDP}})
	return nil
}

// HandleAnswer applies the browser's answer to an SFU offer.
func (p *Participant) HandleAnswer(sdp string) error {
	p.room.sfu.negLimiter.acquire()
	err := p.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer, SDP: sdp,
	})
	p.room.sfu.negLimiter.release()
	if err != nil {
		return err
	}
	p.flushPendingCandidates()

	p.mu.Lock()
	p.makingOffer = false
	again := p.needNegotiate
	p.needNegotiate = false
	p.mu.Unlock()

	if again {
		p.negotiate()
	}
	return nil
}

// HandleICECandidate adds a trickled ICE candidate, buffering it until the
// remote description exists.
func (p *Participant) HandleICECandidate(c webrtc.ICECandidateInit) error {
	p.mu.Lock()
	if p.pc.RemoteDescription() == nil {
		p.pendingCands = append(p.pendingCands, c)
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()
	return p.pc.AddICECandidate(c)
}

func (p *Participant) flushPendingCandidates() {
	p.mu.Lock()
	pend := p.pendingCands
	p.pendingCands = nil
	p.mu.Unlock()
	for _, c := range pend {
		if err := p.pc.AddICECandidate(c); err != nil {
			p.logger.Error("add_ice_candidate", "participant", p.id, "err", err)
		}
	}
}

// scheduleNegotiate requests an SFU-side renegotiation after a short debounce
// window instead of immediately. Several subscribe/
// unsubscribe() calls landing on this Participant within the window — e.g.
// bootstrap-subscribing to many publications at once — collapse into a single
// negotiate() call. A second call while one is already pending is a no-op:
// the pending timer will see every change made in the meantime, since
// negotiate() always captures the PC's current state at fire time.
func (p *Participant) scheduleNegotiate() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if p.negotiateTimer != nil {
		p.mu.Unlock()
		p.room.sfu.negotiationsCoalesced.Add(1)
		return
	}
	p.negotiateTimer = time.AfterFunc(negotiationDebounce, func() {
		p.mu.Lock()
		p.negotiateTimer = nil
		p.mu.Unlock()
		p.negotiate()
	})
	p.mu.Unlock()
}

// negotiate creates an SFU-side offer. Called (after debounce) once the SFU
// has added or removed a forwarded track. Deferred if a negotiation is
// already in flight.
func (p *Participant) negotiate() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if p.makingOffer || p.pc.SignalingState() != webrtc.SignalingStateStable {
		p.needNegotiate = true
		p.mu.Unlock()
		p.room.sfu.negotiationsCoalesced.Add(1)
		return
	}
	p.makingOffer = true
	p.mu.Unlock()
	p.room.sfu.negotiationsStarted.Add(1)

	start := time.Now()
	p.room.sfu.negLimiter.acquire()
	offer, err := p.pc.CreateOffer(nil)
	if err != nil {
		p.room.sfu.negLimiter.release()
		p.room.sfu.negotiationsFailed.Add(1)
		p.logger.Error("create_offer", "participant", p.id, "err", err)
		p.clearMakingOffer()
		return
	}
	err = p.pc.SetLocalDescription(offer)
	p.room.sfu.negLimiter.release()
	if err != nil {
		p.room.sfu.negotiationsFailed.Add(1)
		p.logger.Error("set_local_description", "participant", p.id, "err", err)
		p.clearMakingOffer()
		return
	}
	p.room.sfu.negotiationDur.Observe(time.Since(start))
	p.room.sfu.negotiationsCompleted.Add(1)
	p.tport.SendSFU(signalEnvelope{Type: MsgOffer, Payload: sdpPayload{SDP: offer.SDP}})
}

func (p *Participant) clearMakingOffer() {
	p.mu.Lock()
	p.makingOffer = false
	p.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Track ingestion (publisher -> SFU)
// ---------------------------------------------------------------------------

func (p *Participant) onTrack(remote *webrtc.TrackRemote, recv *webrtc.RTPReceiver) {
	// Authorization happens before any Publication / fan-out
	// track is allocated. Without PUBLISH the incoming RTP is drained and
	// dropped, and the browser is told its track was refused.
	if !p.perms.Publish {
		p.logger.Warn("publish_denied",
			"room", p.room.id, "participant", p.id, "kind", remote.Kind().String())
		p.tport.SendSFU(signalEnvelope{Type: MsgPublishDenied, Payload: sdpPayload{}})
		go drainReceiverRTCP(recv)
		go func() {
			buf := make([]byte, 1500)
			for {
				if _, _, err := remote.Read(buf); err != nil {
					return
				}
			}
		}()
		return
	}

	// Mint the Publication first (it generates the id). Each subscriber gets its
	// own fan-out track when it subscribes (ADR 012), using the publication id
	// as the TrackLocal ID and the publisher's participant id as the StreamID —
	// that is how a subscribing browser maps an incoming track back to
	// (participant, publication).
	p.mu.Lock()
	source := p.pendingSources[remote.ID()]
	delete(p.pendingSources, remote.ID())
	p.mu.Unlock()

	publication := newPublication(p.id, remote, source)
	publication.room = p.room

	p.mu.Lock()
	p.publications[publication.id] = publication
	p.mu.Unlock()
	p.room.sfu.publicationsMade.Add(1)
	if publication.source == SourceScreen {
		p.room.sfu.screenPublicationsMade.Add(1)
	}

	p.logger.Info("publication_created",
		"room", p.room.id, "participant", p.id, "publication", publication.id,
		"kind", publication.Kind(), "source", publication.Source(), "ssrc", uint32(publication.ssrc),
		"codec", remote.Codec().MimeType)

	// Tell the publisher its own publication id (the room notifies everyone
	// else). The browser needs it to mute/unpublish this exact track.
	p.sendPublicationEvent(MsgPublicationAdded, publication)

	go drainReceiverRTCP(recv)
	go p.ingestLoop(remote, publication)

	p.room.onPublicationAdded(publication)
}

// ingestLoop copies RTP packets from the publisher's remote track into every
// subscriber's send queue, byte for byte — no decode, no re-encode. It never
// blocks on a subscriber: a slow one drops on its own queue (ADR 012).
func (p *Participant) ingestLoop(remote *webrtc.TrackRemote, pub *Publication) {
	buf := make([]byte, maxRTPPacketSize)
	for {
		n, _, err := remote.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.logger.Debug("ingest_read", "participant", p.id, "publication", pub.id, "err", err)
			}
			break
		}
		pub.writeRTP(buf[:n])
		// Also fan this packet to any cross-node subscribers. Cheap
		// no-op (one atomic load) when the publication has no remote sinks.
		if pub.remoteSinks.Load() != nil {
			pub.fanRemote(buf[:n])
		}
	}
	p.removePublication(pub.id)
}

// removePublication retires a publication: notifies subscribers, drops their
// subscriptions and frees the fan-out track.
func (p *Participant) removePublication(pubID string) {
	p.mu.Lock()
	pub, ok := p.publications[pubID]
	if ok {
		delete(p.publications, pubID)
	}
	p.mu.Unlock()
	if !ok {
		return
	}
	p.room.sfu.publicationsRemoved.Add(1)

	p.logger.Info("publication_removed",
		"room", p.room.id, "participant", p.id, "publication", pubID, "kind", pub.Kind())
	p.room.onPublicationRemoved(pub)
	// Tell the cluster media bridge so it stops any inter-node
	// forwarders and notifies subscribing nodes.
	p.room.sfu.firePublicationEnded(p.room.id, pubID)
}

// Unpublish removes a publication on request (e.g. the browser stopped sending
// a track without a full renegotiation). Normally the ingest loop detects the
// track ending and calls removePublication itself.
func (p *Participant) Unpublish(pubID string) { p.removePublication(pubID) }

// ---------------------------------------------------------------------------
// Mute / unmute (producer side)
// ---------------------------------------------------------------------------

// SetMute flips the mute flag of one of this participant's own publications and
// tells the room. It does not stop forwarding — a disabled track simply carries
// little or no media.
func (p *Participant) SetMute(pubID string, muted bool) error {
	p.mu.Lock()
	pub, ok := p.publications[pubID]
	p.mu.Unlock()
	if !ok {
		return errors.New("not your publication")
	}
	pub.muted.Store(muted)
	p.logger.Info("publication_muted",
		"room", p.room.id, "participant", p.id, "publication", pubID, "muted", muted)
	p.room.onPublicationMuted(pub)
	return nil
}

// ---------------------------------------------------------------------------
// Subscriptions (SFU -> subscriber)
// ---------------------------------------------------------------------------

// errSubscribeDenied is returned when the participant lacks the SUBSCRIBE
// permission.
var errSubscribeDenied = errors.New("subscribe not allowed")

// Subscribe adds an explicit subscription to a publication in the same room.
func (p *Participant) Subscribe(pubID string) error {
	if !p.perms.Subscribe {
		p.logger.Warn("subscribe_denied", "room", p.room.id, "participant", p.id, "publication", pubID)
		p.tport.SendSFU(signalEnvelope{Type: MsgSubscribeDenied, Payload: sdpPayload{}})
		return errSubscribeDenied
	}
	pub, ok := p.room.findPublication(pubID)
	if !ok {
		return errNotFound
	}
	if pub.participantID == p.id {
		return errors.New("cannot subscribe to your own publication")
	}

	p.mu.Lock()
	delete(p.optedOut, pubID)
	_, already := p.subscriptions[pubID]
	p.mu.Unlock()

	if already {
		p.sendSubscriptionEvent(MsgSubscriptionAdded, pub)
		return nil
	}
	p.subscribe(pub)
	return nil
}

// Unsubscribe removes a subscription and remembers the opt-out so the
// publication is not auto-subscribed again.
func (p *Participant) Unsubscribe(pubID string) {
	p.mu.Lock()
	p.optedOut[pubID] = true
	p.mu.Unlock()
	p.unsubscribe(pubID)
}

// maybeAutoSubscribe subscribes to a publication unless the user opted out.
func (p *Participant) maybeAutoSubscribe(pub *Publication) {
	if !p.perms.Subscribe {
		return
	}
	p.mu.Lock()
	out := p.optedOut[pub.id]
	_, already := p.subscriptions[pub.id]
	closed := p.closed
	p.mu.Unlock()
	if out || already || closed {
		return
	}
	p.subscribe(pub)
}

func (p *Participant) subscribe(pub *Publication) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if _, ok := p.subscriptions[pub.id]; ok {
		p.mu.Unlock()
		return
	}
	local, err := webrtc.NewTrackLocalStaticRTP(pub.codec.RTPCodecCapability, pub.id, pub.participantID)
	if err != nil {
		p.mu.Unlock()
		p.logger.Error("new_sub_track", "participant", p.id, "publication", pub.id, "err", err)
		return
	}
	sender, err := p.pc.AddTrack(local)
	if err != nil {
		p.mu.Unlock()
		p.logger.Error("add_track", "participant", p.id, "publication", pub.id, "err", err)
		return
	}
	sfu := p.room.sfu
	kind := pub.queueKind()
	q := newSendQueue(kind, sfu.subQueueCap(kind))
	q.onDrop = func(n int64) { sfu.addSubQueueDrops(kind, n) }
	sub := &Subscription{subscriberID: p.id, publication: pub, sender: sender, local: local, queue: q}
	sub.detach = pub.attachSink(q)
	p.subscriptions[pub.id] = sub
	p.mu.Unlock()
	sfu.subscriptionsMade.Add(1)

	p.logger.Info("subscription_created",
		"room", p.room.id, "subscriber", p.id, "publication", pub.id, "publisher", pub.participantID)

	go runSubWriteLoop(q, local, func() {
		sfu.subQueueResyncs.Add(1)
		pub.requestKeyframe()
	}, func() {
		p.onSubscriptionFatal(pub.id)
	})
	go p.subscriberRTCPLoop(pub, sender)
	p.sendSubscriptionEvent(MsgSubscriptionAdded, pub)
	p.scheduleNegotiate()

	// A new subscriber to a live video publication needs a keyframe to render.
	// The publication's shared rate limiter coalesces a burst of subscribers
	// into a single request to the publisher.
	go pub.requestKeyframe()
}

// onSubscriptionFatal runs on the subscription's write-loop goroutine when the
// media transport has failed persistently. It tears the subscription down,
// counts it, and fires the SFU's subscription-failed hook (D3 turns that into a
// client-facing message). Calling unsubscribe here is safe: the write loop
// holds no lock, and closing the queue it drains just makes its next pop return
// ok=false before the loop returns.
func (p *Participant) onSubscriptionFatal(pubID string) {
	p.logger.Warn("subscription_media_failed",
		"room", p.room.id, "subscriber", p.id, "publication", pubID)
	p.unsubscribe(pubID)
	p.room.sfu.subscriptionsFailed.Add(1)
	if fn := p.room.sfu.subFailedHook.Load(); fn != nil {
		(*fn)(p.room.id, p.id, pubID)
	}
}

func (p *Participant) unsubscribe(pubID string) {
	p.mu.Lock()
	sub, ok := p.subscriptions[pubID]
	if !ok {
		p.mu.Unlock()
		return
	}
	delete(p.subscriptions, pubID)
	err := p.pc.RemoveTrack(sub.sender)
	pub := sub.publication
	p.mu.Unlock()

	if sub.detach != nil {
		sub.detach()
	}
	sub.queue.close() // ends the write loop goroutine
	p.room.sfu.subscriptionsRemoved.Add(1)

	if err != nil {
		p.logger.Debug("remove_track", "participant", p.id, "publication", pubID, "err", err)
	}
	p.logger.Info("subscription_removed",
		"room", p.room.id, "subscriber", p.id, "publication", pubID)
	p.sendSubscriptionEvent(MsgSubscriptionRemoved, pub)
	p.scheduleNegotiate()
}

// subscriberRTCPLoop forwards a subscriber's keyframe requests to the publisher.
// requestKeyframe routes to the local publisher or, for a mirrored remote
// publication, back to the origin node, and rate-limits the combined
// stream of requests from every subscriber.
func (p *Participant) subscriberRTCPLoop(pub *Publication, sender *webrtc.RTPSender) {
	for {
		pkts, _, err := sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, pkt := range pkts {
			switch pkt.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				pub.requestKeyframe()
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Bootstrap / notifications
// ---------------------------------------------------------------------------

// bootstrap auto-subscribes a freshly-connected participant to every
// publication already live in the room (the default "receive everyone").
func (p *Participant) bootstrap() {
	for _, sp := range p.room.othersOf(p.id) {
		for _, pub := range sp.listPublications() {
			p.maybeAutoSubscribe(pub)
		}
	}
}

// SendExistingPublications tells a just-joined participant which publications
// already exist, so its UI can list them before subscriptions are wired.
func (p *Participant) SendExistingPublications() {
	for _, sp := range p.room.othersOf(p.id) {
		for _, pub := range sp.listPublications() {
			p.tport.SendSFU(publicationEvent{
				Type:          MsgPublicationAdded,
				PublicationID: pub.id,
				ParticipantID: pub.participantID,
				Kind:          pub.Kind(),
				Source:        pub.Source(),
				Muted:         pub.Muted(),
			})
		}
	}
}

func (p *Participant) sendPublicationEvent(typ string, pub *Publication) {
	p.tport.SendSFU(publicationEvent{
		Type:          typ,
		PublicationID: pub.id,
		ParticipantID: pub.participantID,
		Kind:          pub.Kind(),
		Source:        pub.Source(),
		Muted:         pub.Muted(),
	})
}

func (p *Participant) sendSubscriptionEvent(typ string, pub *Publication) {
	p.tport.SendSFU(subscriptionEvent{
		Type:          typ,
		PublicationID: pub.id,
		ParticipantID: pub.participantID,
		Kind:          pub.Kind(),
		Source:        pub.Source(),
	})
}

// ---------------------------------------------------------------------------
// Introspection / teardown
// ---------------------------------------------------------------------------

func (p *Participant) listPublications() []*Publication {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*Publication, 0, len(p.publications))
	for _, pub := range p.publications {
		out = append(out, pub)
	}
	return out
}

func (p *Participant) publicationIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.publications))
	for id := range p.publications {
		out = append(out, id)
	}
	return out
}

func (p *Participant) subscriptionIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.subscriptions))
	for id := range p.subscriptions {
		out = append(out, id)
	}
	return out
}

// close tears down the PeerConnection. It never walks p.subscriptions to call
// unsubscribe() one by one (pointless work when the whole connection is going
// away), but it does still account for them: the leaving participant's own
// subscriptions were never removed the "normal" way, and previously the
// cumulative subscriptionsRemoved counter simply never learned about them —
// observability fix. subscriptions.active is unaffected: it is a
// live count over remaining room participants, so it was always correct.
func (p *Participant) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	subs := p.subscriptions
	removed := int64(len(subs))
	p.subscriptions = make(map[string]*Subscription)
	if p.negotiateTimer != nil {
		p.negotiateTimer.Stop()
		p.negotiateTimer = nil
	}
	p.mu.Unlock()

	// Detach every subscription's sink and close its queue so the per-subscriber
	// write loop goroutines exit (ADR 012). No RemoveTrack — the whole
	// PeerConnection is going away.
	for _, sub := range subs {
		if sub.detach != nil {
			sub.detach()
		}
		sub.queue.close()
	}

	if removed > 0 {
		p.room.sfu.subscriptionsRemoved.Add(removed)
	}

	if err := p.pc.Close(); err != nil {
		p.logger.Debug("pc_close", "participant", p.id, "err", err)
	}
}

func drainReceiverRTCP(recv *webrtc.RTPReceiver) {
	buf := make([]byte, 1500)
	for {
		if _, _, err := recv.Read(buf); err != nil {
			return
		}
	}
}
