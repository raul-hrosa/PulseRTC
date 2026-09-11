package api

import (
	"log/slog"
	"net/http"
)

// RegisterRoutes mounts the /v1 integration API on mux. Every route runs
// through the middleware chain (request id, API-key auth, per-key rate limit,
// panic recovery) and requires the scope noted below.
//
//	POST   /v1/rooms                                          rooms:create
//	GET    /v1/rooms/{roomId}                                 rooms:read
//	DELETE /v1/rooms/{roomId}                                 rooms:close
//	GET    /v1/rooms/{roomId}/connection                      rooms:read
//	POST   /v1/rooms/{roomId}/tokens                          tokens:create
//	GET    /v1/rooms/{roomId}/participants                    participants:read
//	GET    /v1/rooms/{roomId}/participants/{identity}         participants:read
//	GET    /v1/rooms/{roomId}/participants/{identity}/session participants:read
//	GET    /v1/rooms/{roomId}/quality                         quality:read
//	GET    /v1/rooms/{roomId}/participants/{identity}/quality quality:read
//
// The /internal cluster routes and /health, /ready are untouched.
func RegisterRoutes(mux *http.ServeMux, svc *Service, cfg Config, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	m := newMiddleware(cfg, logger)

	mux.HandleFunc("POST /v1/rooms", m.wrap(ScopeRoomsCreate, svc.handleCreateRoom))
	mux.HandleFunc("GET /v1/rooms/{roomId}", m.wrap(ScopeRoomsRead, svc.handleGetRoom))
	mux.HandleFunc("DELETE /v1/rooms/{roomId}", m.wrap(ScopeRoomsClose, svc.handleCloseRoom))
	mux.HandleFunc("GET /v1/rooms/{roomId}/connection", m.wrap(ScopeRoomsRead, svc.handleConnection))
	mux.HandleFunc("POST /v1/rooms/{roomId}/tokens", m.wrap(ScopeTokensCreate, svc.handleCreateToken))
	mux.HandleFunc("GET /v1/rooms/{roomId}/participants", m.wrap(ScopeParticipantsRead, svc.handleListParticipants))
	mux.HandleFunc("GET /v1/rooms/{roomId}/participants/{identity}", m.wrap(ScopeParticipantsRead, svc.handleGetParticipant))
	mux.HandleFunc("GET /v1/rooms/{roomId}/participants/{identity}/session", m.wrap(ScopeParticipantsRead, svc.handleGetSession))
	mux.HandleFunc("GET /v1/rooms/{roomId}/quality", m.wrap(ScopeQualityRead, svc.handleRoomQuality))
	mux.HandleFunc("GET /v1/rooms/{roomId}/participants/{identity}/quality", m.wrap(ScopeQualityRead, svc.handleParticipantQuality))

	logger.Info("integration_api_registered", "enabled", cfg.Enabled, "keys", len(cfg.Keys),
		"publicWsUrl", cfg.PublicWSURL != "", "webhooks", cfg.WebhookURL != "")
}
