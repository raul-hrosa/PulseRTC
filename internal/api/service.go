package api

import (
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

// Service orchestrates the integration API: it owns the room metadata registry,
// the token issuer and the webhook dispatcher, and reads live state through
// Core. Handlers hold no logic beyond decoding / encoding.
type Service struct {
	cfg      Config
	core     Core
	registry *RoomRegistry
	issuer   *TokenIssuer
	hooks    *WebhookDispatcher
	logger   *slog.Logger
}

// NewService wires a Service. core must not be nil.
func NewService(cfg Config, core Core, issuer *TokenIssuer, hooks *WebhookDispatcher, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		cfg:      cfg,
		core:     core,
		registry: NewRoomRegistry(cfg.RoomRetention),
		issuer:   issuer,
		hooks:    hooks,
		logger:   logger,
	}
}

// Registry exposes the room registry so the server can schedule its sweep.
func (s *Service) Registry() *RoomRegistry { return s.registry }

// SweepRateLimiters is a hook for the periodic maintenance loop.
func (s *Service) Sweep() { s.registry.Sweep() }

// ---- rooms --------------------------------------------------------------

func (s *Service) createRoom(req createRoomRequest) (roomResponse, bool, *APIError) {
	roomID := strings.TrimSpace(req.RoomID)
	if roomID == "" {
		roomID = GenerateRoomID()
	} else if err := ValidateRoomID(roomID); err != nil {
		return roomResponse{}, false, err.(*APIError)
	}
	if err := ValidateMetadata(req.Metadata); err != nil {
		return roomResponse{}, false, err.(*APIError)
	}

	rec, created := s.registry.GetOrCreate(roomID, req.Metadata)
	cr, cerr := s.core.Room(roomID)
	if isRemote(cerr) {
		return roomResponse{}, false, remoteErr(cerr)
	}

	resp := s.roomResponse(rec, cr, true)
	if created {
		s.emit("room.created", roomID, map[string]any{"metadata": rec.Metadata})
	}
	return resp, created, nil
}

func (s *Service) getRoom(roomID string) (roomResponse, *APIError) {
	cr, cerr := s.core.Room(roomID)
	if isRemote(cerr) {
		return roomResponse{}, remoteErr(cerr)
	}
	if errors.Is(cerr, ErrClusterStateUnavailable) {
		return roomResponse{}, apiErr(CodeClusterStateUnavailable, "cluster state unavailable")
	}
	rec, known := s.registry.Get(roomID)
	if !known && !cr.Exists {
		return roomResponse{}, apiErr(CodeRoomNotFound, "room not found")
	}
	if rec == nil {
		rec = &RoomRecord{RoomID: roomID, CreatedAt: time.Now()}
	}
	return s.roomResponse(rec, cr, false), nil
}

func (s *Service) closeRoom(roomID string) (closeRoomResponse, *APIError) {
	rec, known := s.registry.Get(roomID)
	cr, cerr := s.core.Room(roomID)
	if isRemote(cerr) {
		return closeRoomResponse{}, remoteErr(cerr)
	}
	if !known && !cr.Exists {
		return closeRoomResponse{}, apiErr(CodeRoomNotFound, "room not found")
	}
	if rec != nil && rec.Closed {
		return closeRoomResponse{RoomID: roomID, Status: RoomClosed}, nil
	}

	n, err := s.core.CloseRoom(roomID, "closed_by_api")
	if err != nil && !isRemote(err) {
		s.logger.Warn("api_close_room", "room", roomID, "err", err)
	}
	s.registry.EnsureExists(roomID)
	s.registry.MarkClosed(roomID)
	s.emit("room.closed", roomID, map[string]any{"participantsNotified": n})
	return closeRoomResponse{RoomID: roomID, Status: RoomClosed, ParticipantsNotified: n}, nil
}

// ---- tokens -----------------------------------------------------------

func (s *Service) mintToken(roomID string, req createTokenRequest) (tokenResponse, *APIError) {
	if err := ValidateRoomID(roomID); err != nil {
		return tokenResponse{}, err.(*APIError)
	}
	identity := strings.TrimSpace(req.Identity)
	if identity == "" {
		return tokenResponse{}, apiErr(CodeInvalidRequest, "identity is required")
	}
	if len(identity) > 256 {
		return tokenResponse{}, apiErr(CodeInvalidRequest, "identity is too long")
	}
	if err := ValidateMetadata(req.Metadata); err != nil {
		return tokenResponse{}, err.(*APIError)
	}

	perms := auth.Permissions{Join: true, Publish: true, Subscribe: true, Control: false}
	if p := req.Permissions; p != nil {
		perms.Join = boolOr(p.Join, perms.Join)
		perms.Publish = boolOr(p.Publish, perms.Publish)
		perms.Subscribe = boolOr(p.Subscribe, perms.Subscribe)
		perms.Control = boolOr(p.Control, false)
	}
	if perms.Control && !s.cfg.AllowControlTokens {
		return tokenResponse{}, apiErr(CodeForbidden,
			"control tokens are disabled (set PULSERTC_API_ALLOW_CONTROL_TOKENS=true)")
	}

	var ttl time.Duration
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}

	minted, err := s.issuer.Mint(TokenParams{
		RoomID: roomID, Identity: identity, Name: strings.TrimSpace(req.Name),
		Permissions: perms, TTL: ttl,
	})
	if err != nil {
		if errors.Is(err, errNoSigningSecret) {
			return tokenResponse{}, apiErr(CodeNotConfigured, "token signing is not configured")
		}
		s.logger.Error("api_mint_token", "room", roomID, "err", err)
		return tokenResponse{}, apiErr(CodeInternalError, "could not mint token")
	}

	// Make a GET on the room work before the first join.
	s.registry.EnsureExists(roomID)

	return tokenResponse{
		Token: minted.Token, Identity: minted.Identity,
		RoomID: minted.RoomID, ExpiresAt: minted.ExpiresAt,
	}, nil
}

// ---- connection -----------------------------------------------------

func (s *Service) connection(roomID string) (connectionInfo, *APIError) {
	if s.cfg.PublicWSURL == "" {
		return connectionInfo{}, apiErr(CodeNotConfigured, "PULSERTC_PUBLIC_WS_URL is not set")
	}
	return connectionInfo{ServerURL: s.cfg.PublicWSURL, RoomID: roomID}, nil
}

// ---- participants -------------------------------------------------

func (s *Service) participants(roomID string) (participantListResponse, *APIError) {
	list, err := s.core.Participants(roomID)
	if isRemote(err) {
		return participantListResponse{}, remoteErr(err)
	}
	if err := s.assertRoomKnown(roomID, len(list) > 0); err != nil {
		return participantListResponse{}, err
	}
	out := participantListResponse{Participants: make([]participantSummary, 0, len(list))}
	for _, p := range list {
		out.Participants = append(out.Participants, participantSummary{
			Identity: p.Identity, Name: p.Name, State: p.State, Role: p.Role, Tracks: len(p.Tracks),
		})
	}
	return out, nil
}

func (s *Service) participant(roomID, identity string) (participantDetailResponse, *APIError) {
	list, err := s.core.Participants(roomID)
	if isRemote(err) {
		return participantDetailResponse{}, remoteErr(err)
	}
	p, ambiguous, ok := resolveParticipant(list, identity)
	if !ok {
		return participantDetailResponse{}, apiErr(CodeParticipantNotFound, "participant not found")
	}
	return participantDetailResponse{
		Identity: p.Identity, Name: p.Name, State: p.State, Role: p.Role,
		Tracks: nonNilTracks(p.Tracks), Ambiguous: ambiguous,
	}, nil
}

func (s *Service) session(roomID, identity string) (sessionResponse, *APIError) {
	list, err := s.core.Participants(roomID)
	if isRemote(err) {
		return sessionResponse{}, remoteErr(err)
	}
	subject := identity
	if p, _, ok := resolveParticipant(list, identity); ok {
		subject = p.Subject
	}
	cs, serr := s.core.Session(roomID, subject)
	if isRemote(serr) {
		return sessionResponse{}, remoteErr(serr)
	}
	if !cs.Exists {
		return sessionResponse{}, apiErr(CodeSessionNotFound, "no session for this participant")
	}
	return sessionResponse{
		State: cs.State, Generation: cs.Generation,
		Recoverable: cs.Recoverable, SessionID: cs.SessionID,
	}, nil
}

// ---- quality ------------------------------------------------------

func (s *Service) roomQuality(roomID string) (roomQualityResponse, *APIError) {
	qs, err := s.core.Quality(roomID)
	if isRemote(err) {
		return roomQualityResponse{}, remoteErr(err)
	}
	if err := s.assertRoomKnown(roomID, len(qs) > 0); err != nil {
		return roomQualityResponse{}, err
	}
	out := roomQualityResponse{Participants: make([]roomQualityEntry, 0, len(qs))}
	for _, q := range qs {
		out.Participants = append(out.Participants, roomQualityEntry{
			Identity: q.Identity, Status: q.Status, Score: q.Score, Reason: q.Reason,
		})
	}
	return out, nil
}

func (s *Service) participantQuality(roomID, identity string) (participantQualityResponse, *APIError) {
	list, perr := s.core.Participants(roomID)
	if isRemote(perr) {
		return participantQualityResponse{}, remoteErr(perr)
	}
	target := identity
	if p, _, ok := resolveParticipant(list, identity); ok {
		target = p.Identity
	}
	qs, err := s.core.Quality(roomID)
	if isRemote(err) {
		return participantQualityResponse{}, remoteErr(err)
	}
	for _, q := range qs {
		if q.Identity != target {
			continue
		}
		return participantQualityResponse{
			Identity: q.Identity, Status: q.Status, Score: q.Score, Reason: q.Reason,
			Audio: q.Audio, Video: q.Video, Connection: q.Connection,
		}, nil
	}
	return participantQualityResponse{}, apiErr(CodeParticipantNotFound, "participant not found")
}

// ---- helpers -----------------------------------------------------

func (s *Service) assertRoomKnown(roomID string, hasLive bool) *APIError {
	if hasLive {
		return nil
	}
	if _, known := s.registry.Get(roomID); known {
		return nil
	}
	cr, err := s.core.Room(roomID)
	if isRemote(err) {
		return remoteErr(err)
	}
	if !cr.Exists {
		return apiErr(CodeRoomNotFound, "room not found")
	}
	return nil
}

func (s *Service) roomResponse(rec *RoomRecord, cr CoreRoom, withConnection bool) roomResponse {
	resp := roomResponse{
		RoomID:       rec.RoomID,
		Status:       roomStatus(rec, cr),
		Generation:   cr.Generation,
		Participants: cr.Participants,
		Metadata:     rec.Metadata,
		CreatedAt:    rec.CreatedAt,
		OwnerNodeID:  cr.OwnerNodeID,
	}
	if withConnection && s.cfg.PublicWSURL != "" {
		resp.Connection = &connectionInfo{ServerURL: s.cfg.PublicWSURL, RoomID: rec.RoomID}
	}
	return resp
}

func (s *Service) emit(evType, roomID string, data map[string]any) {
	if s.hooks == nil {
		return
	}
	s.hooks.Emit(Event{Type: evType, RoomID: roomID, Data: data})
}

func roomStatus(rec *RoomRecord, cr CoreRoom) RoomStatus {
	if rec != nil && rec.Closed {
		return RoomClosed
	}
	if cr.Recovering {
		return RoomRecovering
	}
	if cr.Participants > 0 {
		return RoomActive
	}
	return RoomEmpty
}

func resolveParticipant(list []CoreParticipant, identity string) (CoreParticipant, bool, bool) {
	identity = strings.TrimSpace(identity)
	// exact participant-id match first
	for _, p := range list {
		if p.Identity == identity {
			return p, false, true
		}
	}
	// then subject / subject-prefix match
	var matches []CoreParticipant
	for _, p := range list {
		if p.Subject == identity || strings.HasPrefix(p.Identity, identity+".") {
			matches = append(matches, p)
		}
	}
	switch len(matches) {
	case 0:
		return CoreParticipant{}, false, false
	case 1:
		return matches[0], false, true
	default:
		best := matches[0]
		for _, m := range matches[1:] {
			if m.Identity < best.Identity {
				best = m
			}
		}
		return best, true, true
	}
}

func nonNilTracks(t []CoreTrack) []CoreTrack {
	if t == nil {
		return []CoreTrack{}
	}
	return t
}

func boolOr(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}

func isRemote(err error) bool {
	if err == nil {
		return false
	}
	var re *ErrRoomRemote
	return errors.As(err, &re)
}

func remoteErr(err error) *APIError {
	var re *ErrRoomRemote
	if errors.As(err, &re) {
		return &APIError{
			Code:    CodeRoomOnOtherNode,
			Message: "room is owned by another node",
			Extra:   map[string]any{"ownerNodeId": re.OwnerNodeID},
		}
	}
	return apiErr(CodeInternalError, "internal error")
}
