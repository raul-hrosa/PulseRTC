package sfu

// MetricsSnapshot is the aggregate SFU state used by benchmarks. Active counts
// come from the live room snapshot; cumulative counts come from monotonic
// counters updated in the lifecycle paths.
type MetricsSnapshot struct {
	Rooms         EntityMetrics      `json:"rooms"`
	PeerConns     EntityMetrics      `json:"peerConnections"`
	Tracks        EntityMetrics      `json:"tracks"`
	Publications  EntityMetrics      `json:"publications"`
	Subscriptions EntityMetrics      `json:"subscriptions"`
	Negotiations  NegotiationMetrics `json:"negotiations"`
	RTP           RTPAggregate       `json:"rtp"`
	SubQueue      SubQueueMetrics    `json:"subscriberQueue"`

	// ScreenPublicationsTotal counts screen-share publications ever created
	// (source == "screen"). Low cardinality — no room / participant labels.
	ScreenPublicationsTotal int64 `json:"screenPublicationsTotal"`

	NegotiationDuration HistogramSnapshot `json:"negotiationDuration"`
}

// HistogramSnapshot is a point-in-time view of a durationHistogram.
type HistogramSnapshot struct {
	Bounds     []float64 `json:"bounds"`
	Counts     []uint64  `json:"counts"`
	SumSeconds float64   `json:"sumSeconds"`
	Count      uint64    `json:"count"`
}

// SubQueueMetrics are ADR 012 per-subscriber send-queue counters.
type SubQueueMetrics struct {
	DroppedAudioPackets int64 `json:"droppedAudioPackets"`
	DroppedVideoPackets int64 `json:"droppedVideoPackets"`
	KeyframeResyncs     int64 `json:"keyframeResyncs"`
	Failed              int64 `json:"failed"`
}

// NegotiationMetrics are counters for the renegotiation path.
type NegotiationMetrics struct {
	Started   int64 `json:"started"`
	Completed int64 `json:"completed"`
	Coalesced int64 `json:"coalesced"`
	Failed    int64 `json:"failed"`
}

type EntityMetrics struct {
	Active  int   `json:"active"`
	Created int64 `json:"created"`
	Closed  int64 `json:"closed,omitempty"`
	Removed int64 `json:"removed,omitempty"`
}

type RTPAggregate struct {
	PacketsReceived uint64 `json:"packetsReceived"`
	PacketsSent     uint64 `json:"packetsSent"`
	PacketsLost     int64  `json:"packetsLost"`
	BytesReceived   uint64 `json:"bytesReceived"`
	BytesSent       uint64 `json:"bytesSent"`
}

// Metrics aggregates all rooms. It is intentionally cheap enough to poll
// during local benchmarks.
func (s *SFU) Metrics() MetricsSnapshot {
	s.mu.RLock()
	roomIDs := make([]string, 0, len(s.rooms))
	for id := range s.rooms {
		roomIDs = append(roomIDs, id)
	}
	s.mu.RUnlock()

	out := MetricsSnapshot{
		Rooms: EntityMetrics{
			Active:  len(roomIDs),
			Created: s.roomsCreated.Load(),
			Closed:  s.roomsClosed.Load(),
		},
		PeerConns: EntityMetrics{
			Created: s.peerConnectionsMade.Load(),
			Closed:  s.peerConnectionsClose.Load(),
		},
		Tracks: EntityMetrics{
			Created: s.publicationsMade.Load(),
			Removed: s.publicationsRemoved.Load(),
		},
		Publications: EntityMetrics{
			Created: s.publicationsMade.Load(),
			Removed: s.publicationsRemoved.Load(),
		},
		Subscriptions: EntityMetrics{
			Created: s.subscriptionsMade.Load(),
			Removed: s.subscriptionsRemoved.Load(),
		},
		ScreenPublicationsTotal: s.screenPublicationsMade.Load(),
		Negotiations: NegotiationMetrics{
			Started:   s.negotiationsStarted.Load(),
			Completed: s.negotiationsCompleted.Load(),
			Coalesced: s.negotiationsCoalesced.Load(),
			Failed:    s.negotiationsFailed.Load(),
		},
		SubQueue: SubQueueMetrics{
			DroppedAudioPackets: s.subQueueDroppedAudio.Load(),
			DroppedVideoPackets: s.subQueueDroppedVideo.Load(),
			KeyframeResyncs:     s.subQueueResyncs.Load(),
			Failed:              s.subscriptionsFailed.Load(),
		},
	}

	b, c, sum, total := s.negotiationDur.Snapshot()
	out.NegotiationDuration = HistogramSnapshot{Bounds: b, Counts: c, SumSeconds: sum, Count: total}

	for _, id := range roomIDs {
		st := s.Stats(id)
		out.PeerConns.Active += len(st.Participants)
		for _, p := range st.Participants {
			out.Publications.Active += len(p.Publications)
			out.Tracks.Active += len(p.Publications)
			out.Subscriptions.Active += len(p.Subscriptions)
			for _, rtp := range p.Inbound {
				out.RTP.PacketsReceived += rtp.PacketsReceived
				out.RTP.PacketsLost += rtp.PacketsLost
				out.RTP.BytesReceived += rtp.BytesReceived
			}
			for _, rtp := range p.Outbound {
				out.RTP.PacketsSent += rtp.PacketsSent
				out.RTP.BytesSent += rtp.BytesSent
			}
		}
	}
	return out
}
