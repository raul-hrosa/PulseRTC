package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"time"

	"github.com/raulhrosa/pulsertc/internal/cluster"
	"github.com/raulhrosa/pulsertc/internal/sfu"
)

// Cross-node media wiring in the signaling layer. It keeps a small
// index of publications that live on OTHER nodes (learned via room
// events) and drives the cluster media bridge to mirror them into the local SFU
// on demand.

type remotePubRef struct {
	NodeID        string
	ParticipantID string
	Kind          string
}

// crossNodePubEvent is the room-event payload announcing a publication to nodes
// that hold other participants of the room.
type crossNodePubEvent struct {
	Type          string `json:"type"` // "remote_publication_added" | "remote_publication_removed"
	RoomID        string `json:"roomId"`
	PublicationID string `json:"publicationId"`
	ParticipantID string `json:"participantId"`
	Kind          string `json:"kind"`
	OriginNodeID  string `json:"originNodeId"`
}

// wireLoadProvider gives the cluster load reporter this node's live counts.
// Cheap: it is called once per PULSERTC_LOAD_REPORT_INTERVAL,
// never per packet.
func (s *Server) wireLoadProvider() {
	s.cluster.SetLoadProvider(func() cluster.NodeLoad {
		rooms, participants, publications, subscriptions := s.sfu.LoadCounts()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return cluster.NodeLoad{
			Rooms:         rooms,
			Participants:  participants,
			Publications:  publications,
			Subscriptions: subscriptions,
			MemoryBytes:   ms.HeapAlloc,
			Goroutines:    runtime.NumGoroutine(),
		}
	})
}

func (s *Server) wireCrossNodeMedia() {
	if !s.cluster.Media().Enabled() {
		return
	}
	s.remotePubs = map[string]remotePubRef{}
	s.mirrorSubs = map[string]map[string]bool{}

	s.cluster.Media().SetSink(s.sfu)
	s.cluster.Media().SetSource(s.sfu)
	s.sfu.SetMediaPublicationEndedHook(s.cluster.Media().PublicationEnded)
	s.sfu.SetPublicationEventHook(s.onLocalPublicationEvent)
}

func mediaKey(roomID, pubID string) string { return roomID + "|" + pubID }

// onLocalPublicationEvent announces a local publication to the other nodes that
// hold participants of this room.
func (s *Server) onLocalPublicationEvent(roomID string, added bool, info sfu.PublicationInfo) {
	// Record the publisher's logical publish state so a reconnecting
	// client / a room snapshot can reflect it. Keyed by kind, which
	// is stable across sessions unlike the physical publication id.
	if s.recovery.Enabled {
		s.clientsMu.RLock()
		owner := s.clients[info.ParticipantID]
		s.clientsMu.RUnlock()
		if owner != nil {
			if added {
				s.sessions.UpdatePublishIntent(owner.identity.Subject, roomID,
					TrackIntent{TrackID: info.Kind, Kind: info.Kind})
			} else {
				s.sessions.RemovePublishIntent(owner.identity.Subject, roomID, info.Kind)
			}
		}
	}

	if !s.cluster.Media().Enabled() {
		return
	}
	typ := "remote_publication_added"
	if !added {
		typ = "remote_publication_removed"
	}
	payload, _ := json.Marshal(crossNodePubEvent{
		Type: typ, RoomID: roomID, PublicationID: info.PublicationID,
		ParticipantID: info.ParticipantID, Kind: info.Kind,
		OriginNodeID: s.cluster.NodeID(),
	})
	s.broadcastRoomEventRemote(roomID, payload)
}

// handleRemotePublicationEvent is called from DeliverRoomEvent when a room event
// turns out to be a cross-node publication announcement.
func (s *Server) handleRemotePublicationEvent(ev crossNodePubEvent) bool {
	if ev.Type != "remote_publication_added" && ev.Type != "remote_publication_removed" {
		return false
	}
	key := mediaKey(ev.RoomID, ev.PublicationID)
	if ev.Type == "remote_publication_removed" {
		s.remoteMediaMu.Lock()
		delete(s.remotePubs, key)
		s.remoteMediaMu.Unlock()
		s.tearDownMirror(ev.RoomID, ev.PublicationID)
		return true
	}

	s.remoteMediaMu.Lock()
	s.remotePubs[key] = remotePubRef{NodeID: ev.OriginNodeID, ParticipantID: ev.ParticipantID, Kind: ev.Kind}
	s.remoteMediaMu.Unlock()

	// If a local participant is already in the room, mirror the publication now
	// so the "receive everyone" default keeps working across nodes.
	if r, ok := s.rooms.Get(ev.RoomID); ok && r.Size() > 0 {
		_ = s.ensureRemoteMirror(ev.RoomID, ev.PublicationID, "__node__")
	}
	return true
}

// ensureRemoteMirror makes sure a synthetic publication for a remote pub exists
// in the local SFU. Returns nil when the publication is local or already
// mirrored. subscriberID may be "__node__" for the room-level auto mirror.
func (s *Server) ensureRemoteMirror(roomID, pubID, subscriberID string) *ErrorMessage {
	if roomID == "" || pubID == "" || !s.cluster.Media().Enabled() {
		return nil
	}
	if s.sfu.RoomHasPublication(roomID, pubID) {
		return nil // local publication, or already mirrored
	}
	s.remoteMediaMu.Lock()
	ref, known := s.remotePubs[mediaKey(roomID, pubID)]
	if !known {
		s.remoteMediaMu.Unlock()
		return nil // not a known remote pub — let the SFU return "not found"
	}
	subs := s.mirrorSubs[mediaKey(roomID, pubID)]
	if subs == nil {
		subs = map[string]bool{}
		s.mirrorSubs[mediaKey(roomID, pubID)] = subs
	}
	firstSub := len(subs) == 0
	subs[subscriberID] = true
	s.remoteMediaMu.Unlock()

	if !firstSub {
		return nil // mirror already established by another subscriber
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if _, err := s.cluster.Media().SubscribeRemote(ctx, ref.NodeID, roomID, pubID, subscriberID); err != nil {
		s.remoteMediaMu.Lock()
		delete(s.mirrorSubs[mediaKey(roomID, pubID)], subscriberID)
		s.remoteMediaMu.Unlock()
		var ce *cluster.Error
		if errors.As(err, &ce) {
			return &ErrorMessage{Type: TypeError, Code: ce.Code, Message: ce.Message, RoomID: roomID}
		}
		return &ErrorMessage{Type: TypeError, Message: "could not mirror remote publication"}
	}
	return nil
}

// releaseRemoteMirror drops one subscriber's interest in a mirrored publication
// and tears the mirror down when the last one goes.
func (s *Server) releaseRemoteMirror(roomID, pubID, subscriberID string) {
	if !s.cluster.Media().Enabled() {
		return
	}
	key := mediaKey(roomID, pubID)
	s.remoteMediaMu.Lock()
	subs := s.mirrorSubs[key]
	if subs == nil {
		s.remoteMediaMu.Unlock()
		return
	}
	delete(subs, subscriberID)
	// Keep the mirror alive as long as the node-level auto mirror wants it.
	remaining := len(subs)
	ref := s.remotePubs[key]
	s.remoteMediaMu.Unlock()
	if remaining == 0 {
		s.remoteMediaMu.Lock()
		delete(s.mirrorSubs, key)
		s.remoteMediaMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.cluster.Media().UnsubscribeRemote(ctx, ref.NodeID, pubID, subscriberID)
	}
}

func (s *Server) tearDownMirror(roomID, pubID string) {
	key := mediaKey(roomID, pubID)
	s.remoteMediaMu.Lock()
	ref := s.remotePubs[key]
	delete(s.mirrorSubs, key)
	s.remoteMediaMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.cluster.Media().UnsubscribeRemote(ctx, ref.NodeID, pubID, "__node__")
}

// dropRoomMirrors tears down every mirror for a room once it has no local
// participants.
func (s *Server) dropRoomMirrors(roomID string) {
	if s.mirrorSubs == nil {
		return
	}
	s.remoteMediaMu.Lock()
	var keys []string
	for k := range s.mirrorSubs {
		if len(k) > len(roomID) && k[:len(roomID)+1] == roomID+"|" {
			keys = append(keys, k)
		}
	}
	pending := make([]struct{ node, pub string }, 0, len(keys))
	for _, k := range keys {
		ref := s.remotePubs[k]
		pending = append(pending, struct{ node, pub string }{ref.NodeID, k[len(roomID)+1:]})
		delete(s.mirrorSubs, k)
	}
	s.remoteMediaMu.Unlock()
	for _, p := range pending {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		s.cluster.Media().UnsubscribeRemote(ctx, p.node, p.pub, "__node__")
		cancel()
	}
}
