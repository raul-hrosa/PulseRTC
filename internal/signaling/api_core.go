package signaling

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/raulhrosa/pulsertc/internal/api"
	"github.com/raulhrosa/pulsertc/internal/quality"
)

// apiCore adapts *Server to api.Core. It is a thin read/close view:
// every value comes from state the core already keeps (SFU stats, quality
// engine, session registry, cluster locator) — nothing is duplicated for the
// API, and it never touches media, RTP or the WebSocket protocol.
type apiCore struct{ s *Server }

// NewAPICore returns the api.Core implementation backed by this server.
func NewAPICore(s *Server) api.Core { return &apiCore{s: s} }

// resolveOwner reports whether roomID is served by this node. When the cluster
// is enabled and another node owns the room it returns that node id so the API
// can answer ROOM_ON_OTHER_NODE.
func (a *apiCore) resolveOwner(roomID string) (ownerNodeID string, local bool, remote bool) {
	if !a.s.cluster.Enabled() {
		return "", true, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, ok := a.s.cluster.ResolveRoom(ctx, roomID)
	if !ok {
		// nobody owns it yet — treat as local (a join will claim it here).
		return a.s.cluster.NodeID(), true, false
	}
	if res.Local {
		return a.s.cluster.NodeID(), true, false
	}
	return res.OwnerID, false, true
}

func (a *apiCore) Room(roomID string) (api.CoreRoom, error) {
	owner, local, remote := a.resolveOwner(roomID)
	if remote {
		return api.CoreRoom{}, &api.ErrRoomRemote{OwnerNodeID: owner}
	}

	cr := api.CoreRoom{OwnerNodeID: owner, Generation: a.s.roomGeneration(roomID)}
	if r, ok := a.s.rooms.Get(roomID); ok {
		cr.Exists = true
		cr.Participants = r.Size()
	}
	if a.s.recovery.Enabled && a.s.sessions != nil {
		total, recovering := a.s.sessions.RoomStats(roomID)
		if total > 0 {
			cr.Exists = true
		}
		cr.Recovering = recovering > 0
	}
	_ = local
	return cr, nil
}

func (a *apiCore) Participants(roomID string) ([]api.CoreParticipant, error) {
	if owner, _, remote := a.resolveOwner(roomID); remote {
		return nil, &api.ErrRoomRemote{OwnerNodeID: owner}
	}
	stats := a.s.sfu.Stats(roomID)
	out := make([]api.CoreParticipant, 0, len(stats.Participants))
	for _, p := range stats.Participants {
		cp := api.CoreParticipant{
			Identity: p.ID,
			Subject:  subjectOf(p.ID),
			Name:     a.s.participantName(p.ID),
			State:    participantState(p.ConnectionState),
			Role:     p.Role,
			Tracks:   make([]api.CoreTrack, 0, len(p.Publications)),
		}
		for _, pub := range p.Publications {
			cp.Tracks = append(cp.Tracks, api.CoreTrack{
				PublicationID: pub.ID, Kind: pub.Kind, Muted: pub.Muted,
			})
		}
		out = append(out, cp)
	}
	return out, nil
}

func (a *apiCore) Quality(roomID string) ([]api.CoreQuality, error) {
	if owner, _, remote := a.resolveOwner(roomID); remote {
		return nil, &api.ErrRoomRemote{OwnerNodeID: owner}
	}
	a.s.quality.Prune(30 * time.Second)
	ids := make([]string, 0)
	for _, p := range a.s.sfu.Stats(roomID).Participants {
		ids = append(ids, p.ID)
	}
	out := make([]api.CoreQuality, 0, len(ids))
	for _, pq := range a.s.quality.SnapshotRoom(ids) {
		out = append(out, toCoreQuality(pq))
	}
	return out, nil
}

func (a *apiCore) Session(roomID, subject string) (api.CoreSession, error) {
	if owner, _, remote := a.resolveOwner(roomID); remote {
		return api.CoreSession{}, &api.ErrRoomRemote{OwnerNodeID: owner}
	}
	if !a.s.recovery.Enabled || a.s.sessions == nil {
		return api.CoreSession{}, nil
	}
	sess, ok := a.s.sessions.Get(subject, roomID)
	if !ok {
		return api.CoreSession{}, nil
	}
	return api.CoreSession{
		Exists:      true,
		SessionID:   sess.SessionID,
		State:       string(sess.State),
		Generation:  sess.Generation,
		Recoverable: sess.State != SessionClosed,
	}, nil
}

func (a *apiCore) CloseRoom(roomID, reason string) (int, error) {
	if owner, _, remote := a.resolveOwner(roomID); remote {
		return 0, &api.ErrRoomRemote{OwnerNodeID: owner}
	}
	return a.s.CloseRoom(roomID, reason), nil
}

// CloseRoom notifies every locally-connected participant of roomID with a
// room_closed event, then closes their sockets after a short flush window. The
// normal disconnect path releases the SFU peer and cluster ownership — nothing
// is torn down abruptly (design).
func (s *Server) CloseRoom(roomID, reason string) int {
	r, ok := s.rooms.Get(roomID)
	if !ok {
		return 0
	}
	ids := r.ParticipantIDs()
	notice, _ := json.Marshal(map[string]any{
		"type": "room_closed", "roomId": roomID, "reason": reason,
	})
	n := 0
	for _, id := range ids {
		if p, ok := r.Get(id); ok {
			p.Send(notice)
			n++
		}
	}
	go func() {
		time.Sleep(250 * time.Millisecond)
		for _, id := range ids {
			s.clientsMu.RLock()
			c := s.clients[id]
			s.clientsMu.RUnlock()
			if c != nil {
				c.closeNow()
			}
		}
	}()
	s.logger.Info("room_closed_by_api", "room", roomID, "reason", reason, "notified", n)
	return n
}

func subjectOf(participantID string) string {
	if i := strings.LastIndexByte(participantID, '.'); i > 0 {
		return participantID[:i]
	}
	return participantID
}

func participantState(connState string) string {
	switch strings.ToLower(connState) {
	case "connected", "completed":
		return "CONNECTED"
	case "new", "connecting", "checking":
		return "CONNECTING"
	default:
		return "DISCONNECTED"
	}
}

func toCoreQuality(pq quality.ParticipantQuality) api.CoreQuality {
	cq := api.CoreQuality{
		Identity: pq.ParticipantID,
		Status:   string(pq.Overall),
		Reason:   pq.Reason,
	}

	legOf := func(sq quality.StreamQuality) *api.CoreQualityLeg {
		leg := &api.CoreQualityLeg{Status: string(sq.Status)}
		if len(sq.Problems) > 0 {
			leg.Reason = sq.Problems[0]
		}
		return leg
	}
	worseLeg := func(cur *api.CoreQualityLeg, sq quality.StreamQuality) *api.CoreQualityLeg {
		cand := legOf(sq)
		if cur == nil || qualityRank(cand.Status) > qualityRank(cur.Status) {
			return cand
		}
		return cur
	}

	minScore := -1
	consider := func(sq quality.StreamQuality) {
		if sq.Status == quality.Unknown {
			return
		}
		if minScore < 0 || sq.Score < minScore {
			minScore = sq.Score
		}
	}

	if pq.Connection.Status != quality.Unknown {
		cq.Connection = legOf(pq.Connection)
		consider(pq.Connection)
	}
	for _, sq := range append(append([]quality.StreamQuality{}, pq.Outbound...), pq.Inbound...) {
		consider(sq)
		switch sq.Kind {
		case quality.Audio:
			cq.Audio = worseLeg(cq.Audio, sq)
		case quality.Video:
			cq.Video = worseLeg(cq.Video, sq)
		}
	}
	if minScore >= 0 {
		cq.Score = minScore
	}
	if cq.Reason == "" {
		switch {
		case cq.Video != nil && cq.Video.Reason != "" && qualityRank(cq.Video.Status) >= qualityRank(cq.Status):
			cq.Reason = cq.Video.Reason
		case cq.Audio != nil && cq.Audio.Reason != "":
			cq.Reason = cq.Audio.Reason
		case cq.Connection != nil && cq.Connection.Reason != "":
			cq.Reason = cq.Connection.Reason
		}
	}
	return cq
}

func qualityRank(status string) int {
	switch status {
	case "GOOD":
		return 0
	case "WARNING":
		return 1
	case "POOR":
		return 2
	default:
		return -1
	}
}
