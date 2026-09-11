package signaling

import (
	"context"
	"encoding/json"
	"time"

	"github.com/raulhrosa/pulsertc/internal/cluster"
)

// clusterDelivery is the signaling layer seen through the cluster.LocalDelivery
// interface. It hands a message routed to this node to the
// locally-connected participant, or fans a room event out to local room
// members. It is a distinct type over *Server purely to keep these
// interface methods namespaced.
type clusterDelivery Server

func (d *clusterDelivery) srv() *Server { return (*Server)(d) }

// DeliverToParticipant delivers a participant-scoped message to the local
// socket. Returns PARTICIPANT_NOT_FOUND when the participant is not on this
// node so the router can re-resolve a stale location.
func (d *clusterDelivery) DeliverToParticipant(ctx context.Context, msg cluster.ClusterMessage) error {
	s := d.srv()
	c, ok := s.localClient(msg.ParticipantID)
	if !ok {
		return &cluster.Error{
			Code:          cluster.CodeParticipantNotFound,
			Message:       "participant is not on this node",
			ParticipantID: msg.ParticipantID,
		}
	}
	switch msg.Type {
	case cluster.MsgParticipantDisconnect:
		s.logger.Info("cross_node_disconnect", "participant", c.id, "sourceNodeId", msg.SourceNodeID)
		go s.disconnect(c)
	default:
		// participant.message / participant.control: the payload is already a
		// ready-to-send signaling frame produced on the origin node.
		if len(msg.Payload) > 0 {
			c.Send(msg.Payload)
		}
	}
	return nil
}

// DeliverRoomEvent fans a room.event out to the local participants of the room.
// A cross-node publication announcement is consumed here instead of
// being broadcast to browsers verbatim.
func (d *clusterDelivery) DeliverRoomEvent(_ context.Context, msg cluster.ClusterMessage) error {
	s := d.srv()
	if len(msg.Payload) > 0 {
		var pe crossNodePubEvent
		if json.Unmarshal(msg.Payload, &pe) == nil && s.handleRemotePublicationEvent(pe) {
			return nil
		}
	}
	if r, ok := s.rooms.Get(msg.RoomID); ok && len(msg.Payload) > 0 {
		r.Broadcast(msg.Payload, "")
	}
	return nil
}

// --- local client index -----------------------------------------------------

func (s *Server) trackClient(c *Client) {
	s.clientsMu.Lock()
	s.clients[c.id] = c
	s.clientsMu.Unlock()
}

func (s *Server) untrackClient(id string) {
	s.clientsMu.Lock()
	delete(s.clients, id)
	s.clientsMu.Unlock()
}

func (s *Server) localClient(id string) (*Client, bool) {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()
	c, ok := s.clients[id]
	return c, ok
}

// broadcastRoomEventRemote pushes a room event to the same room's participants
// on other nodes. No-op without a cluster.
func (s *Server) broadcastRoomEventRemote(roomID string, payload []byte) {
	if !s.cluster.Enabled() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.cluster.BroadcastRoomEvent(ctx, roomID, payload)
}
