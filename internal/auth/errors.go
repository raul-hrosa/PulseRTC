package auth

// Error is a security failure with a stable, client-safe code. The Message is
// deliberately generic: it never carries crypto details, secret material or the
// token itself.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Security error codes. These are the only strings a client sees.
const (
	CodeUnauthenticated  = "UNAUTHENTICATED"
	CodeInvalidToken     = "INVALID_TOKEN"
	CodeExpiredToken     = "EXPIRED_TOKEN"
	CodeInvalidIssuer    = "INVALID_ISSUER"
	CodeInvalidAudience  = "INVALID_AUDIENCE"
	CodeRoomAccessDenied = "ROOM_ACCESS_DENIED"
	CodePublishDenied    = "PUBLISH_NOT_ALLOWED"
	CodeSubscribeDenied  = "SUBSCRIBE_NOT_ALLOWED"
	CodeControlDenied    = "CONTROL_NOT_ALLOWED"
	CodeJoinDenied       = "JOIN_NOT_ALLOWED"
	CodeRateLimited      = "RATE_LIMITED"
	CodeMessageTooLarge  = "MESSAGE_TOO_LARGE"
)

var (
	errMissing     = &Error{CodeUnauthenticated, "authentication required"}
	errMalformed   = &Error{CodeInvalidToken, "token is malformed"}
	errBadSig      = &Error{CodeInvalidToken, "token signature is invalid"}
	errBadAlg      = &Error{CodeInvalidToken, "unexpected token algorithm"}
	errExpired     = &Error{CodeExpiredToken, "token has expired"}
	errNotYetValid = &Error{CodeInvalidToken, "token is not valid yet"}
	errNoIat       = &Error{CodeInvalidToken, "token is missing iat"}
	errStale       = &Error{CodeInvalidToken, "token is too old"}
	errNoSub       = &Error{CodeInvalidToken, "token is missing sub"}
	errIssuer      = &Error{CodeInvalidIssuer, "token issuer is not accepted"}
	errAudience    = &Error{CodeInvalidAudience, "token audience is not accepted"}
)

// AsError returns err as an *Error, or a generic INVALID_TOKEN wrapper.
func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		return e
	}
	return &Error{CodeInvalidToken, "token could not be validated"}
}
