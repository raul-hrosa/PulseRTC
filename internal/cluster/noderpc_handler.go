package cluster

import (
	"context"
	"strings"
	"time"
)

// ClusterMessageHandler processes a message that arrived from another node.
// Authentication is already done at the HTTP edge; everything
// else — envelope validation, target check, idempotency, dispatch — lives here,
// not in the HTTP handler.
type ClusterMessageHandler interface {
	Handle(ctx context.Context, message ClusterMessage) error
}

type clusterMessageHandler struct {
	self     string
	maxAge   time.Duration
	maxSize  int
	dedup    *dedupCache
	delivery func() LocalDelivery
	metrics  *Metrics
	now      func() time.Time
	// media handles media.* control messages; nil when disabled.
	media func(context.Context, ClusterMessage) error
}

func (h *clusterMessageHandler) Handle(ctx context.Context, msg ClusterMessage) error {
	now := time.Now
	if h.now != nil {
		now = h.now
	}

	// 1. envelope validation.
	if e := msg.Validate(now(), h.maxAge, h.maxSize); e != nil {
		h.metrics.msgInvalid()
		h.metrics.msgRejected()
		return e
	}

	// 2. target / loop-prevention: this must be the addressed node.
	if msg.TargetNodeID != h.self {
		h.metrics.msgRejected()
		return &Error{Code: CodeClusterTargetMismatch, Message: "message addressed to a different node", NodeID: msg.TargetNodeID}
	}

	h.metrics.msgReceived()

	// 6. idempotency: a duplicate is an idempotent success — no side effects.
	if h.dedup.markSeen(msg.RequestID) {
		h.metrics.msgDuplicated()
		return nil
	}

	// Media session control goes to the media bridge, not to a
	// participant. (Media bytes themselves never come through here — only over
	// the UDP MediaTransport.)
	if strings.HasPrefix(msg.Type, "media.") {
		if h.media == nil {
			return &Error{Code: CodeMediaNodeUnavailable, Message: "cross-node media disabled"}
		}
		return h.media(ctx, msg)
	}

	// 7-9. dispatch to the local component.
	d := h.deliveryOrNil()
	if d == nil {
		return &Error{Code: CodeParticipantNotFound, Message: "node has no local delivery target"}
	}
	switch msg.Type {
	case MsgParticipantMessage, MsgParticipantControl, MsgParticipantDisconnect:
		return d.DeliverToParticipant(ctx, msg)
	case MsgRoomEvent:
		return d.DeliverRoomEvent(ctx, msg)
	default:
		h.metrics.msgInvalid()
		return &Error{Code: CodeClusterMessageInvalid, Message: "unhandled message type"}
	}
}

func (h *clusterMessageHandler) deliveryOrNil() LocalDelivery {
	if h.delivery == nil {
		return nil
	}
	return h.delivery()
}
