package api

import (
	"encoding/json"
	"net/http"
)

// Error codes (design). These stable strings are the only error identifiers
// a caller sees; messages are generic and never carry secrets or stack traces.
const (
	CodeUnauthorized            = "UNAUTHORIZED"
	CodeForbidden               = "FORBIDDEN"
	CodeInvalidRequest          = "INVALID_REQUEST"
	CodeRoomNotFound            = "ROOM_NOT_FOUND"
	CodeParticipantNotFound     = "PARTICIPANT_NOT_FOUND"
	CodeSessionNotFound         = "SESSION_NOT_FOUND"
	CodeRoomClosed              = "ROOM_CLOSED"
	CodeRoomOnOtherNode         = "ROOM_ON_OTHER_NODE"
	CodeIdempotencyConflict     = "IDEMPOTENCY_CONFLICT"
	CodeRateLimited             = "RATE_LIMITED"
	CodeNodeUnavailable         = "NODE_UNAVAILABLE"
	CodeClusterStateUnavailable = "CLUSTER_STATE_UNAVAILABLE"
	CodeNotConfigured           = "NOT_CONFIGURED"
	CodeInternalError           = "INTERNAL_ERROR"
)

var codeStatus = map[string]int{
	CodeUnauthorized:            http.StatusUnauthorized,
	CodeForbidden:               http.StatusForbidden,
	CodeInvalidRequest:          http.StatusBadRequest,
	CodeRoomNotFound:            http.StatusNotFound,
	CodeParticipantNotFound:     http.StatusNotFound,
	CodeSessionNotFound:         http.StatusNotFound,
	CodeRoomClosed:              http.StatusConflict,
	CodeRoomOnOtherNode:         http.StatusConflict,
	CodeIdempotencyConflict:     http.StatusConflict,
	CodeRateLimited:             http.StatusTooManyRequests,
	CodeNodeUnavailable:         http.StatusServiceUnavailable,
	CodeClusterStateUnavailable: http.StatusServiceUnavailable,
	CodeNotConfigured:           http.StatusServiceUnavailable,
	CodeInternalError:           http.StatusInternalServerError,
}

// statusFor maps an error code to its HTTP status (500 for anything unknown).
func statusFor(code string) int {
	if s, ok := codeStatus[code]; ok {
		return s
	}
	return http.StatusInternalServerError
}

// APIError is a handler-level failure carrying a stable code and optional
// extra top-level fields (e.g. ownerNodeId for ROOM_ON_OTHER_NODE).
type APIError struct {
	Code    string
	Message string
	Extra   map[string]any
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func apiErr(code, msg string) *APIError { return &APIError{Code: code, Message: msg} }

// errorBody is the wire shape: {"error":{"code","message","requestId"}}.
type errorBody struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"requestId"`
	} `json:"error"`
}

// writeError renders an *APIError (or a generic INTERNAL_ERROR) as JSON.
func writeError(w http.ResponseWriter, requestID string, err error) {
	ae, ok := err.(*APIError)
	if !ok {
		ae = apiErr(CodeInternalError, "internal error")
	}
	var body errorBody
	body.Error.Code = ae.Code
	body.Error.Message = ae.Message
	body.Error.RequestID = requestID

	// Merge Extra into a generic map so callers still get {"error":{...}} plus
	// any top-level hints.
	out := map[string]any{"error": map[string]any{
		"code": ae.Code, "message": ae.Message, "requestId": requestID,
	}}
	for k, v := range ae.Extra {
		out[k] = v
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusFor(ae.Code))
	_ = json.NewEncoder(w).Encode(out)
}

// writeJSON renders v with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
