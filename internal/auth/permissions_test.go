package auth

import "testing"

func id(p Permissions, room string) *Identity {
	return &Identity{Subject: "u", Room: room, Permissions: p}
}

func TestAuthorizeMatrix(t *testing.T) {
	all := Permissions{Join: true, Publish: true, Subscribe: true, Control: true}
	none := Permissions{}

	cases := []struct {
		name  string
		fn    func() error
		allow bool
	}{
		{"join allowed", func() error { return AuthorizeJoin(id(all, ""), "r") }, true},
		{"join denied", func() error { return AuthorizeJoin(id(none, ""), "r") }, false},
		{"publish allowed", func() error { return AuthorizePublish(id(all, "")) }, true},
		{"publish denied", func() error { return AuthorizePublish(id(none, "")) }, false},
		{"subscribe allowed", func() error { return AuthorizeSubscribe(id(all, "")) }, true},
		{"subscribe denied", func() error { return AuthorizeSubscribe(id(none, "")) }, false},
		{"control allowed", func() error { return AuthorizeControl(id(all, "")) }, true},
		{"control denied", func() error { return AuthorizeControl(id(none, "")) }, false},
		{"room match", func() error { return AuthorizeJoin(id(all, "room-a"), "room-a") }, true},
		{"room mismatch", func() error { return AuthorizeJoin(id(all, "room-a"), "room-b") }, false},
		{"room wildcard", func() error { return AuthorizeJoin(id(all, ""), "anything") }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if tc.allow && err != nil {
				t.Fatalf("expected allow, got %v", err)
			}
			if !tc.allow && err == nil {
				t.Fatalf("expected deny")
			}
		})
	}
}

func TestRoomMismatchCode(t *testing.T) {
	err := AuthorizeJoin(id(Permissions{Join: true}, "room-a"), "room-b")
	if AsError(err).Code != CodeRoomAccessDenied {
		t.Fatalf("want ROOM_ACCESS_DENIED, got %v", err)
	}
}

func TestNilIdentityDenied(t *testing.T) {
	for _, fn := range []func() error{
		func() error { return AuthorizeJoin(nil, "r") },
		func() error { return AuthorizePublish(nil) },
		func() error { return AuthorizeSubscribe(nil) },
		func() error { return AuthorizeControl(nil) },
	} {
		if fn() == nil {
			t.Fatalf("nil identity must be denied")
		}
	}
}
