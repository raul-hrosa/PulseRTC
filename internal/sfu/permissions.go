package sfu

// Permissions is the media-plane view of a participant's authorization. The
// signaling layer resolves these from the authenticated identity and
// passes them in at Join time; the SFU never sees a token. Publish gates
// creating a Publication; Subscribe gates creating a Subscription.
type Permissions struct {
	Publish   bool
	Subscribe bool
}

// FullPermissions grants everything. Used by Room.Join (the earlier entry
// point) and by tests that are not exercising authorization.
func FullPermissions() Permissions { return Permissions{Publish: true, Subscribe: true} }
