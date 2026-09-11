package quality

import "time"

// Event types emitted on a stable status transition.
const (
	EventDegraded  = "quality_degraded"
	EventRecovered = "quality_recovered"
	EventChanged   = "quality_changed"
)

// Event is a stable status transition for one stream. Events are not emitted
// per sample — only when hysteresis commits a change.
type Event struct {
	Type          string    `json:"type"`
	ParticipantID string    `json:"participantId"`
	Direction     Direction `json:"direction,omitempty"`
	MediaType     Kind      `json:"mediaType,omitempty"`
	TrackID       string    `json:"trackId,omitempty"`
	From          Status    `json:"from"`
	Status        Status    `json:"status"`
	Reason        string    `json:"reason,omitempty"`
	At            time.Time `json:"at"`
}

// makeEvent classifies a transition. Acquiring the first verdict (from Unknown)
// is reported as quality_changed, never as degraded/recovered.
func makeEvent(key StreamKey, from, to Status, a Analysis, at time.Time) Event {
	e := Event{
		ParticipantID: key.Participant,
		Direction:     key.Direction,
		MediaType:     key.Kind,
		TrackID:       key.TrackID,
		From:          from,
		Status:        to,
		At:            at,
	}
	if len(a.Problems) > 0 {
		e.Reason = a.Problems[0]
	}

	switch {
	case rank(from) < 0:
		e.Type = EventChanged
	case rank(to) > rank(from):
		e.Type = EventDegraded
	case to == Good:
		e.Type = EventRecovered
	default:
		e.Type = EventChanged
	}
	return e
}
