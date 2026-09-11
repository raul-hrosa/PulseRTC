package signaling

import "time"

// replaceSession evicts a still-connected session that a new connection for the
// same identity+room just superseded: the old socket is told once
// with SESSION_REPLACED, then closed. Never relies on timeout alone.
func (s *Server) replaceSession(physicalID, roomID string) {
	s.clientsMu.RLock()
	old := s.clients[physicalID]
	s.clientsMu.RUnlock()
	if old == nil {
		return
	}
	s.logger.Info("session replaced",
		"participantId", physicalID, "room", roomID, "reason", "new session for same identity")
	old.sendJSON(SessionEvent{Type: TypeSessionReplacedNotice, Reason: CodeSessionReplaced, RoomID: roomID})
	// give writePump a moment to flush the notice, then drop the socket.
	go func() {
		time.Sleep(100 * time.Millisecond)
		old.closeNow()
	}()
}

// recordSubIntent stores the client's desired subscription state so it can be
// reported in a room snapshot / restored on reconnect. Idempotent.
func (s *Server) recordSubIntent(c *Client, publicationID string, enabled bool) {
	if !s.recovery.Enabled || c.currentRoom() == "" {
		return
	}
	s.sessions.UpdateSubIntent(c.identity.Subject, c.currentRoom(),
		SubIntent{PublicationID: publicationID, Enabled: enabled})
}

// SweepSessions closes sessions stuck in RECOVERING past the timeout. Call
// it periodically alongside SweepRateLimiters.
func (s *Server) SweepSessions() {
	if s.sessions == nil {
		return
	}
	if n := s.sessions.SweepExpired(); n > 0 {
		s.logger.Info("session recovery failed", "reason", "timeout", "closed", n)
	}
}

// handleSessionResume answers a standalone session.resume frame. Normally
// resume information rides along the join message; this path lets a client that
// is already joined re-assert its session and pick up the current room snapshot,
// and lets the server fence a stale generation.
func (s *Server) handleSessionResume(c *Client, in Inbound) {
	if !s.recovery.Enabled {
		c.sendJSON(SessionEvent{Type: TypeSessionRecoveryFailed, Reason: CodeRecoveryDisabled})
		return
	}
	roomID := c.currentRoom()
	if roomID == "" {
		c.sendJSON(newError("join a room before sending session.resume"))
		return
	}
	sess, ok := s.sessions.Get(c.identity.Subject, roomID)
	if !ok {
		c.sendJSON(SessionEvent{Type: TypeSessionRecoveryFailed, Reason: "NO_SESSION", RoomID: roomID})
		return
	}
	if in.Resume != nil && in.Resume.Generation != 0 && in.Resume.Generation < sess.Generation {
		s.sessionMx.stale.Add(1)
		c.sendJSON(SessionEvent{Type: TypeSessionStale, Reason: CodeStaleSession, RoomID: roomID})
		return
	}
	c.sendJSON(s.buildRoomSnapshot(roomID))
	c.sendJSON(SessionEvent{
		Type: TypeSessionReconnected, RoomID: roomID, SessionID: sess.SessionID,
	})
}
