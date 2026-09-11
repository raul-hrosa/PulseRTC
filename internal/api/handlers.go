package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

const maxBodyBytes = 64 * 1024

// decodeJSON reads and strictly decodes the request body into v.
func decodeJSON(r *http.Request, v any) *APIError {
	r.Body = http.MaxBytesReader(nil, r.Body, maxBodyBytes)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return apiErr(CodeInvalidRequest, "request body too large or unreadable")
	}
	if len(data) == 0 {
		return nil // empty body is allowed; caller validates required fields
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return apiErr(CodeInvalidRequest, "invalid JSON: "+err.Error())
	}
	return nil
}

func (s *Service) handleCreateRoom(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())

	raw, _ := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxBodyBytes))
	var req createRoomRequest
	if len(raw) > 0 {
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeError(w, reqID, apiErr(CodeInvalidRequest, "invalid JSON: "+err.Error()))
			return
		}
	}

	idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if cached, status, hit, conflict := s.registry.idempotencyLookup(idemKey, raw); conflict {
		writeError(w, reqID, apiErr(CodeIdempotencyConflict, "Idempotency-Key reused with a different body"))
		return
	} else if hit {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(cached)
		return
	}

	resp, created, aerr := s.createRoom(req)
	if aerr != nil {
		writeError(w, reqID, aerr)
		return
	}
	body, _ := json.Marshal(resp)
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	s.registry.idempotencyStore(idemKey, raw, body, status)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (s *Service) handleGetRoom(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())
	resp, aerr := s.getRoom(r.PathValue("roomId"))
	if aerr != nil {
		writeError(w, reqID, aerr)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) handleCloseRoom(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())
	resp, aerr := s.closeRoom(r.PathValue("roomId"))
	if aerr != nil {
		writeError(w, reqID, aerr)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) handleCreateToken(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())
	var req createTokenRequest
	if aerr := decodeJSON(r, &req); aerr != nil {
		writeError(w, reqID, aerr)
		return
	}
	resp, aerr := s.mintToken(r.PathValue("roomId"), req)
	if aerr != nil {
		writeError(w, reqID, aerr)
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Service) handleConnection(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())
	resp, aerr := s.connection(r.PathValue("roomId"))
	if aerr != nil {
		writeError(w, reqID, aerr)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) handleListParticipants(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())
	resp, aerr := s.participants(r.PathValue("roomId"))
	if aerr != nil {
		writeError(w, reqID, aerr)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) handleGetParticipant(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())
	resp, aerr := s.participant(r.PathValue("roomId"), r.PathValue("identity"))
	if aerr != nil {
		writeError(w, reqID, aerr)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) handleGetSession(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())
	resp, aerr := s.session(r.PathValue("roomId"), r.PathValue("identity"))
	if aerr != nil {
		writeError(w, reqID, aerr)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) handleRoomQuality(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())
	resp, aerr := s.roomQuality(r.PathValue("roomId"))
	if aerr != nil {
		writeError(w, reqID, aerr)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) handleParticipantQuality(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())
	resp, aerr := s.participantQuality(r.PathValue("roomId"), r.PathValue("identity"))
	if aerr != nil {
		writeError(w, reqID, aerr)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
