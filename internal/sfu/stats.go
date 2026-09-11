package sfu

import "github.com/pion/webrtc/v4"

// RoomStats is a diagnostics snapshot of one room's media plane. The dashboard
// polls this to show, per participant, both legs of every track:
//
//	Publisher -> SFU   (inbound-rtp on the publisher's PeerConnection)
//	SFU -> Subscriber  (outbound-rtp on the subscriber's PeerConnection)
type RoomStats struct {
	Room         string             `json:"room"`
	Participants []ParticipantStats `json:"participants"`
}

// ParticipantStats describes one participant and distinguishes its role.
type ParticipantStats struct {
	ID              string            `json:"id"`
	Role            string            `json:"role"` // publisher / subscriber / publisher+subscriber / idle
	ConnectionState string            `json:"connectionState"`
	ICEState        string            `json:"iceState"`
	Publications    []PublicationStat `json:"publications"`
	Subscriptions   []string          `json:"subscriptions"` // publication ids
	Inbound         []RTPStats        `json:"inbound"`       // media the SFU receives from this participant
	Outbound        []RTPStats        `json:"outbound"`      // media the SFU sends to this participant
}

// PublicationStat is the identity + state of one published track.
type PublicationStat struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Source string `json:"source,omitempty"`
	SSRC   uint32 `json:"ssrc"`
	Muted  bool   `json:"muted"`
}

// RTPStats is a per-stream slice of the WebRTC statistics, reusing the
// vocabulary.
type RTPStats struct {
	Kind            string  `json:"kind"`
	SSRC            uint32  `json:"ssrc"`
	PacketsReceived uint64  `json:"packetsReceived,omitempty"`
	PacketsSent     uint64  `json:"packetsSent,omitempty"`
	PacketsLost     int64   `json:"packetsLost,omitempty"`
	BytesReceived   uint64  `json:"bytesReceived,omitempty"`
	BytesSent       uint64  `json:"bytesSent,omitempty"`
	Jitter          float64 `json:"jitter,omitempty"`
	NACKCount       uint32  `json:"nackCount,omitempty"`
	PLICount        uint32  `json:"pliCount,omitempty"`
	RoundTripTime   float64 `json:"roundTripTime,omitempty"`
}

// Stats returns a diagnostics snapshot for a single room. Returns an empty
// snapshot (not nil) for an unknown room.
func (s *SFU) Stats(roomID string) RoomStats {
	out := RoomStats{Room: roomID, Participants: []ParticipantStats{}}
	r, ok := s.GetRoom(roomID)
	if !ok {
		return out
	}
	for _, p := range r.snapshot() {
		out.Participants = append(out.Participants, p.stats())
	}
	return out
}

func (p *Participant) stats() ParticipantStats {
	pubs := p.listPublications()
	subIDs := p.subscriptionIDs()

	role := "idle"
	switch {
	case len(pubs) > 0 && len(subIDs) > 0:
		role = "publisher+subscriber"
	case len(pubs) > 0:
		role = "publisher"
	case len(subIDs) > 0:
		role = "subscriber"
	}

	pubStats := make([]PublicationStat, 0, len(pubs))
	for _, pub := range pubs {
		pubStats = append(pubStats, PublicationStat{
			ID: pub.id, Kind: pub.Kind(), Source: pub.Source(), SSRC: uint32(pub.ssrc), Muted: pub.Muted(),
		})
	}

	ps := ParticipantStats{
		ID:              p.id,
		Role:            role,
		ConnectionState: p.pc.ConnectionState().String(),
		ICEState:        p.pc.ICEConnectionState().String(),
		Publications:    pubStats,
		Subscriptions:   subIDs,
		Inbound:         []RTPStats{},
		Outbound:        []RTPStats{},
	}

	report := p.pc.GetStats()
	rtt := roundTripTime(report)

	for _, v := range report {
		switch st := v.(type) {
		case webrtc.InboundRTPStreamStats:
			ps.Inbound = append(ps.Inbound, RTPStats{
				Kind:            string(st.Kind),
				SSRC:            uint32(st.SSRC),
				PacketsReceived: uint64(st.PacketsReceived),
				PacketsLost:     int64(st.PacketsLost),
				BytesReceived:   st.BytesReceived,
				Jitter:          st.Jitter,
				NACKCount:       st.NACKCount,
				PLICount:        st.PLICount,
			})
		case webrtc.OutboundRTPStreamStats:
			ps.Outbound = append(ps.Outbound, RTPStats{
				Kind:          string(st.Kind),
				SSRC:          uint32(st.SSRC),
				PacketsSent:   uint64(st.PacketsSent),
				BytesSent:     st.BytesSent,
				NACKCount:     st.NACKCount,
				PLICount:      st.PLICount,
				RoundTripTime: rtt,
			})
		}
	}
	return ps
}

func roundTripTime(report webrtc.StatsReport) float64 {
	for _, v := range report {
		if pair, ok := v.(webrtc.ICECandidatePairStats); ok && pair.Nominated {
			return pair.CurrentRoundTripTime
		}
	}
	return 0
}
