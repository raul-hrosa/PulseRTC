package sfu

import (
	"sync"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"

	"github.com/raulhrosa/pulsertc/internal/cluster"
)

// Room is an isolation boundary in the media plane. A publication in one room
// can never be found, subscribed to or forwarded into another: every lookup and
// every fan-out walks only this room's participant set.
type Room struct {
	id  string
	sfu *SFU

	mu           sync.RWMutex
	participants map[string]*Participant

	// remotePubs are synthetic publications whose media originates on another
	// node and arrives via the cluster media bridge. They have no
	// local publisher Participant.
	remoteMu   sync.RWMutex
	remotePubs map[string]*Publication
}

func newRoom(id string, s *SFU) *Room {
	return &Room{
		id:           id,
		sfu:          s,
		participants: make(map[string]*Participant),
		remotePubs:   make(map[string]*Publication),
	}
}

// addRemotePublication builds a synthetic publication fed by the media bridge
// and fans it to local subscribers.
func (r *Room) addRemotePublication(info cluster.RemotePublicationInfo, rtcpToOrigin func(pkt []byte)) (cluster.RemotePublication, error) {
	kind := webrtc.RTPCodecTypeAudio
	if info.Kind == "video" {
		kind = webrtc.RTPCodecTypeVideo
	}
	capab := webrtc.RTPCodecCapability{
		MimeType: info.MimeType, ClockRate: info.ClockRate, Channels: info.Channels,
	}
	trackID := info.PublicationID
	pub := &Publication{
		id:             info.PublicationID,
		participantID:  info.ParticipantID,
		kind:           kind,
		ssrc:           webrtc.SSRC(info.SSRC),
		codec:          webrtc.RTPCodecParameters{RTPCodecCapability: capab, PayloadType: webrtc.PayloadType(info.PayloadType)},
		remoteTrackID:  trackID + "-" + uuid.NewString()[:8],
		remote:         true,
		remoteRTCPSink: rtcpToOrigin,
		room:           r,
	}

	r.remoteMu.Lock()
	if existing, ok := r.remotePubs[pub.id]; ok {
		r.remoteMu.Unlock()
		return &remotePubHandle{room: r, pub: existing}, nil
	}
	r.remotePubs[pub.id] = pub
	r.remoteMu.Unlock()

	r.sfu.publicationsMade.Add(1)
	r.sfu.logger.Info("remote_publication_added",
		"room", r.id, "publication", pub.id, "origin_participant", pub.participantID,
		"origin_node", info.OriginNodeID, "kind", pub.Kind())

	r.onPublicationAdded(pub)
	return &remotePubHandle{room: r, pub: pub}, nil
}

func (r *Room) removeRemotePublication(pubID string) {
	r.remoteMu.Lock()
	pub, ok := r.remotePubs[pubID]
	delete(r.remotePubs, pubID)
	r.remoteMu.Unlock()
	if !ok {
		return
	}
	r.sfu.publicationsRemoved.Add(1)
	r.sfu.logger.Info("remote_publication_removed", "room", r.id, "publication", pubID)
	r.onPublicationRemoved(pub)
}

func (r *Room) remotePublications() []*Publication {
	r.remoteMu.RLock()
	defer r.remoteMu.RUnlock()
	out := make([]*Publication, 0, len(r.remotePubs))
	for _, pub := range r.remotePubs {
		out = append(out, pub)
	}
	return out
}

// ID returns the room identifier.
func (r *Room) ID() string { return r.id }

// Join creates a Participant with its own dedicated PeerConnection to the SFU,
// granting full media permissions. callers that enforce authorization
// use JoinWithPermissions instead.
func (r *Room) Join(id string, t Transport) (*Participant, error) {
	return r.JoinWithPermissions(id, t, FullPermissions())
}

// JoinWithPermissions is Join with an explicit media-plane authorization set.
// A participant without Publish cannot create a Publication; one
// without Subscribe cannot create a Subscription.
func (r *Room) JoinWithPermissions(id string, t Transport, perms Permissions) (*Participant, error) {
	p, err := newParticipant(id, r, t)
	if err != nil {
		return nil, err
	}
	p.perms = perms

	r.mu.Lock()
	r.participants[id] = p
	r.mu.Unlock()

	r.sfu.peerConnectionsMade.Add(1)
	r.sfu.logger.Info("peer_connection_created", "room", r.id, "participant", id)
	return p, nil
}

// Leave tears a participant down: retires each of its publications (which
// unsubscribes everyone and notifies them), closes its PeerConnection and frees
// the room if it became empty.
func (r *Room) Leave(id string) {
	r.mu.Lock()
	p, ok := r.participants[id]
	if ok {
		delete(r.participants, id)
	}
	r.mu.Unlock()
	if !ok {
		return
	}

	for _, pub := range p.listPublications() {
		r.onPublicationRemoved(pub)
	}

	p.close()
	r.sfu.peerConnectionsClose.Add(1)
	r.sfu.logger.Info("peer_connection_closed", "room", r.id, "participant", id)
	r.sfu.removeRoomIfEmpty(r.id)
}

// ---------------------------------------------------------------------------
// Pub/sub coordination
// ---------------------------------------------------------------------------

// onPublicationAdded notifies every other participant of a new publication and
// auto-subscribes those that have not opted out.
func (r *Room) onPublicationAdded(pub *Publication) {
	for _, sp := range r.othersOf(pub.participantID) {
		sp.sendPublicationEvent(MsgPublicationAdded, pub)
		sp.maybeAutoSubscribe(pub)
	}
	r.sfu.firePublicationEvent(r.id, true, pub)
}

// onPublicationRemoved drops every subscription to a publication and notifies
// the room.
func (r *Room) onPublicationRemoved(pub *Publication) {
	for _, sp := range r.othersOf(pub.participantID) {
		sp.unsubscribe(pub.id)
		sp.sendPublicationEvent(MsgPublicationRemoved, pub)
	}
	r.sfu.firePublicationEvent(r.id, false, pub)
}

// onPublicationMuted propagates a producer-side mute/unmute to the room.
func (r *Room) onPublicationMuted(pub *Publication) {
	for _, sp := range r.othersOf(pub.participantID) {
		sp.sendPublicationEvent(MsgPublicationMuted, pub)
	}
}

// findPublication locates a publication by id anywhere in THIS room — a local
// publisher's, or a remote one mirrored from another node.
func (r *Room) findPublication(pubID string) (*Publication, bool) {
	for _, sp := range r.snapshot() {
		for _, pub := range sp.listPublications() {
			if pub.id == pubID {
				return pub, true
			}
		}
	}
	r.remoteMu.RLock()
	pub, ok := r.remotePubs[pubID]
	r.remoteMu.RUnlock()
	return pub, ok
}

// ---------------------------------------------------------------------------
// Introspection
// ---------------------------------------------------------------------------

func (r *Room) participant(id string) (*Participant, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.participants[id]
	return p, ok
}

func (r *Room) size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.participants)
}

// othersOf returns every participant except the one with the given id. It does
// not hold r.mu while the caller runs, so callers may take participant locks.
func (r *Room) othersOf(id string) []*Participant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Participant, 0, len(r.participants))
	for pid, sp := range r.participants {
		if pid == id {
			continue
		}
		out = append(out, sp)
	}
	return out
}

func (r *Room) snapshot() []*Participant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Participant, 0, len(r.participants))
	for _, sp := range r.participants {
		out = append(out, sp)
	}
	return out
}
