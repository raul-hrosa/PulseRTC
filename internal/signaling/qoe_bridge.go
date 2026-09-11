package signaling

import (
	"encoding/json"

	"github.com/raulhrosa/pulsertc/internal/quality"
)

// qualityReport is the browser's periodic QoE stats push. Counters
// are cumulative; the engine derives rates. Every numeric field is optional.
type qualityReport struct {
	Samples []qualitySample `json:"samples"`
}

type qualitySample struct {
	PublicationID string `json:"publicationId"`
	Kind          string `json:"kind"`      // "audio" | "video" | "connection"
	Direction     string `json:"direction"` // "inbound" | "outbound" | "" (connection)
	TMs           int64  `json:"tMs"`

	PacketsSent     *float64 `json:"packetsSent"`
	PacketsReceived *float64 `json:"packetsReceived"`
	PacketsLost     *float64 `json:"packetsLost"`
	BytesSent       *float64 `json:"bytesSent"`
	BytesReceived   *float64 `json:"bytesReceived"`
	FramesDecoded   *float64 `json:"framesDecoded"`
	FramesDropped   *float64 `json:"framesDropped"`

	JitterMs *float64 `json:"jitterMs"`
	RTTMs    *float64 `json:"rttMs"`
	FPS      *float64 `json:"fps"`
	Width    *float64 `json:"width"`
	Height   *float64 `json:"height"`

	Enabled   *bool  `json:"enabled"`
	ConnState string `json:"connState"`
	ICEState  string `json:"iceState"`
}

// roomQualityStates returns the consolidated QoE verdict for each of the given
// participants that already has one. Used to seed a newcomer's room_joined so it
// learns the room's current quality without waiting for the next transition
// (QoE propagation). Participants still at UNKNOWN are omitted.
func (s *Server) roomQualityStates(participantIDs []string) []ParticipantQualityState {
	out := make([]ParticipantQualityState, 0, len(participantIDs))
	for _, id := range participantIDs {
		pq := s.quality.Snapshot(id)
		if pq.Overall == quality.Unknown {
			continue
		}
		out = append(out, ParticipantQualityState{
			ParticipantID: id,
			Status:        string(pq.Overall),
			Reason:        pq.Reason,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

var validKind = map[string]quality.Kind{
	"audio":      quality.Audio,
	"video":      quality.Video,
	"connection": quality.Connection,
}

var validDirection = map[string]quality.Direction{
	"inbound":  quality.Inbound,
	"outbound": quality.Outbound,
}

// handleQualityReport validates a browser stats push, feeds every sample to the
// quality engine, and fans out a quality_* event for each stable transition to
// every participant of the room — local and cross-node (QoE propagation).
//
// The quality belongs to the OBSERVED participant (ev.ParticipantID, always the
// server-side id of the reporter), so "participant B = POOR" reaches A, B and C
// alike. The raw quality_report is never forwarded — only the consolidated
// verdict, and only when hysteresis commits a change, so a stable stream
// produces no traffic.
func (s *Server) handleQualityReport(c *Client, data []byte) {
	roomID := c.currentRoom()
	if roomID == "" {
		c.sendJSON(newError("join a room before sending quality_report"))
		return
	}

	var rep qualityReport
	if err := json.Unmarshal(data, &rep); err != nil {
		c.sendJSON(newError("invalid quality_report: not valid JSON"))
		return
	}

	for _, sm := range rep.Samples {
		kind, ok := validKind[sm.Kind]
		if !ok {
			continue // ignore unknown media kinds rather than erroring
		}
		dir := quality.Direction("")
		if kind != quality.Connection {
			dir, ok = validDirection[sm.Direction]
			if !ok {
				continue
			}
		}
		trackID := sm.PublicationID
		if kind == quality.Connection {
			trackID = ""
		}

		sample := quality.Sample{
			Key: quality.StreamKey{
				// Identity is the server's UUID, never the message body.
				Participant: c.id,
				Direction:   dir,
				Kind:        kind,
				TrackID:     trackID,
			},
			Source:          quality.SourceBrowser,
			AtMillis:        sm.TMs,
			PacketsSent:     sm.PacketsSent,
			PacketsReceived: sm.PacketsReceived,
			PacketsLost:     sm.PacketsLost,
			BytesSent:       sm.BytesSent,
			BytesReceived:   sm.BytesReceived,
			FramesDecoded:   sm.FramesDecoded,
			FramesDropped:   sm.FramesDropped,
			JitterMs:        sm.JitterMs,
			RTTMs:           sm.RTTMs,
			FPS:             sm.FPS,
			Width:           sm.Width,
			Height:          sm.Height,
			Enabled:         sm.Enabled,
			ConnState:       sm.ConnState,
			ICEState:        sm.ICEState,
		}

		// Feed RTT / jitter into the call-history media aggregates.
		s.history.RecordMediaSample(roomID, logicalID(c), sm.RTTMs, sm.JitterMs)

		for _, ev := range s.quality.Ingest(sample) {
			s.logger.Info(ev.Type,
				"participant", ev.ParticipantID, "media", ev.MediaType,
				"direction", ev.Direction, "from", ev.From, "status", ev.Status,
				"reason", ev.Reason)
			s.history.RecordQualityEvent(roomID, logicalID(c), ev.Type, ev.Reason, string(ev.Status))
			payload, err := json.Marshal(ev) // ev.Type is already "quality_degraded" / etc.
			if err != nil {
				continue
			}
			// Fan out to the whole room, including the reporter itself: no
			// special-casing of the affected participant at this stage.
			if r, ok := s.rooms.Get(roomID); ok {
				r.Broadcast(payload, "")
			}
			s.broadcastRoomEventRemote(roomID, payload)
		}
	}
}
