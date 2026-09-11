package cluster

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// RegisterRoutes wires the cluster's HTTP surface onto mux:
//
//	GET /ready - readiness (public;)
//	GET /internal/node             - this node's NodeInfo         (cluster auth)
//	GET /internal/nodes            - every known node             (cluster auth)
//	GET /internal/rooms/{roomId}   - owner of a room, 404 if none (cluster auth)
//	GET /internal/participants/{id}- a participant's location     (cluster auth)
//
// The /internal endpoints are refused entirely when the cluster is disabled.
func (c *Cluster) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/ready", c.handleReady)

	guard := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !c.cfg.Enabled {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "cluster disabled"})
				return
			}
			c.InternalAuthMiddleware(h)(w, r)
		}
	}
	mux.HandleFunc("/internal/node", guard(c.handleInternalNode))
	mux.HandleFunc("/internal/nodes", guard(c.handleInternalNodes))
	mux.HandleFunc("/internal/rooms/", guard(c.handleInternalRoom))
	mux.HandleFunc("/internal/participants/", guard(c.handleInternalParticipant))
	// Cross-node signaling ingress.
	mux.HandleFunc("/internal/cluster/messages", guard(c.handleClusterMessage))
	// Cluster-wide load snapshot.
	mux.HandleFunc("/internal/cluster/load", guard(c.handleInternalLoad))
	// Failure-detection & recovery diagnostics. Internal only.
	mux.HandleFunc("/internal/cluster/failures", guard(c.handleInternalFailures))
	mux.HandleFunc("/internal/cluster/recovery", guard(c.handleInternalRecovery))
}

func (c *Cluster) handleInternalFailures(w http.ResponseWriter, _ *http.Request) {
	failures := []NodeFailure{}
	known := true
	if c.failureDet != nil {
		failures = c.failureDet.Failures()
		known = c.failureDet.BackendKnown()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"nodeId":       c.cfg.NodeID,
		"backendKnown": known,
		"nodes":        failures,
	})
}

func (c *Cluster) handleInternalRecovery(w http.ResponseWriter, _ *http.Request) {
	snap := c.MetricsSnapshot().Recovery
	inProgress := int64(0)
	if c.recovery != nil {
		inProgress = c.recovery.InProgress()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"active":         !c.cfg.RecoveryDisabled && c.recovery != nil,
		"inProgress":     inProgress,
		"started":        snap.Started,
		"completed":      snap.Completed,
		"failed":         snap.Failed,
		"rooms":          snap.Rooms,
		"roomsRecovered": snap.RoomsRecovered,
		"roomsSkipped":   snap.RoomsSkipped,
		"conflicts":      snap.Conflicts,
	})
}

func (c *Cluster) handleInternalLoad(w http.ResponseWriter, _ *http.Request) {
	snap, err := c.state.Load().LoadSnapshot()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": CodeClusterStateUnavailable})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodeId": c.cfg.NodeID, "load": snap})
}

// handleClusterMessage receives a ClusterMessage from a peer node. Auth is
// already enforced by the guard; here we bound the body, decode, run the
// loop-prevention target check, then hand off to the ClusterMessageHandler —
// the HTTP layer holds no processing logic.
func (c *Cluster) handleClusterMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": CodeBadRequest})
		return
	}
	limit := int64(c.cfg.MaxMessageSize)
	if limit <= 0 {
		limit = 64 * 1024
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit+1024) // headroom for envelope fields
	var msg ClusterMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			c.metrics.msgRejected()
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": CodeClusterMessageTooLarge})
			return
		}
		c.metrics.msgInvalid()
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": CodeClusterMessageInvalid})
		return
	}

	if msg.TargetNodeID != "" && msg.TargetNodeID != c.cfg.NodeID {
		c.metrics.msgRejected()
		c.logger.Warn("cluster_message_rejected", "reason", CodeClusterTargetMismatch,
			"sourceNodeId", msg.SourceNodeID, "targetNodeId", msg.TargetNodeID, "messageType", msg.Type)
		writeJSON(w, http.StatusConflict, map[string]string{"error": CodeClusterTargetMismatch})
		return
	}
	msg.TargetNodeID = c.cfg.NodeID

	err := c.handleInboundClusterMessage(r.Context(), msg)
	if err == nil {
		c.logger.Info("cluster_message_received",
			"sourceNodeId", msg.SourceNodeID, "targetNodeId", msg.TargetNodeID,
			"messageType", msg.Type, "requestId", msg.RequestID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	ce, ok := err.(*Error)
	if !ok {
		ce = &Error{Code: "INTERNAL", Message: "processing failed"}
	}
	c.logger.Warn("cluster_message_failed",
		"sourceNodeId", msg.SourceNodeID, "messageType", msg.Type, "reason", ce.Code)
	writeJSON(w, ce.HTTPStatus(), map[string]string{"error": ce.Code})
}

func (c *Cluster) handleReady(w http.ResponseWriter, _ *http.Request) {
	status := http.StatusOK
	if !c.Ready() {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{
		"ready":  c.Ready(),
		"nodeId": c.cfg.NodeID,
		"state":  string(c.State()),
	})
}

func (c *Cluster) handleInternalNode(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, c.Self())
}

func (c *Cluster) handleInternalNodes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"nodes": c.registry.List()})
}

func (c *Cluster) handleInternalRoom(w http.ResponseWriter, r *http.Request) {
	roomID := strings.TrimPrefix(r.URL.Path, "/internal/rooms/")
	if roomID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": CodeBadRequest})
		return
	}
	owner, ok := c.rooms.GetOwner(roomID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": CodeNotFound, "roomId": roomID})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"roomId": roomID, "nodeId": owner})
}

func (c *Cluster) handleInternalParticipant(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/internal/participants/")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": CodeBadRequest})
		return
	}
	loc, ok := c.participants.Lookup(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": CodeNotFound, "participantId": id})
		return
	}
	writeJSON(w, http.StatusOK, loc)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
