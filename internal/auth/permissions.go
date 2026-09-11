package auth

// The authorization layer. These functions never look at
// anything a client sent — only the Identity derived from the validated token.
// Each returns a typed *Error so the caller can surface a stable code and bump
// the matching metric.

// AuthorizeJoin checks the JOIN permission and the room claim in one step: a
// participant may establish a session only in a room its token allows.
func AuthorizeJoin(id *Identity, roomID string) error {
	if id == nil {
		return errMissing
	}
	if !id.Permissions.Join {
		return &Error{CodeJoinDenied, "join is not permitted for this identity"}
	}
	if !id.RoomAllowed(roomID) {
		return &Error{CodeRoomAccessDenied, "token does not grant access to this room"}
	}
	return nil
}

// AuthorizePublish gates creating a Publication. Enforced before any
// expensive media resource is allocated.
func AuthorizePublish(id *Identity) error {
	if id == nil {
		return errMissing
	}
	if !id.Permissions.Publish {
		return &Error{CodePublishDenied, "publishing is not permitted for this identity"}
	}
	return nil
}

// AuthorizeSubscribe gates creating a Subscription.
func AuthorizeSubscribe(id *Identity) error {
	if id == nil {
		return errMissing
	}
	if !id.Permissions.Subscribe {
		return &Error{CodeSubscribeDenied, "subscribing is not permitted for this identity"}
	}
	return nil
}

// AuthorizeControl gates administrative commands over other participants:
// mute, remove, disable-publication and the like. CONTROL defaults to false.
func AuthorizeControl(id *Identity) error {
	if id == nil {
		return errMissing
	}
	if !id.Permissions.Control {
		return &Error{CodeControlDenied, "control operations are not permitted for this identity"}
	}
	return nil
}
