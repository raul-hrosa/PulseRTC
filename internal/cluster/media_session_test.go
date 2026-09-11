package cluster

import "testing"

func TestMediaSession_LifecycleAndIdempotentClose(t *testing.T) {
	var id [16]byte
	copy(id[:], []byte("sessionsessionss"))
	s := newMediaSession(id, "r1", "a", "b", nil)
	if s.State() != MediaSessionCreating {
		t.Fatalf("initial state = %s", s.State())
	}
	s.setState(MediaSessionActive)

	var detached int
	s.addForwarder("p1", func() { detached++ })
	rt := newRemoteTrack(RemotePublicationInfo{RoomID: "r1", PublicationID: "p2"}, id)
	s.addTrack(rt)

	// Close Close Close must not panic and cleans up once.
	s.Close()
	s.Close()
	s.Close()
	if s.State() != MediaSessionClosed {
		t.Fatalf("state after close = %s", s.State())
	}
	if detached != 1 {
		t.Fatalf("forwarder detached %d times, want 1", detached)
	}
	if rt.State() != RemoteTrackEnded {
		t.Fatalf("track not ended: %s", rt.State())
	}
}

func TestRemoteTrack_States(t *testing.T) {
	rt := newRemoteTrack(RemotePublicationInfo{PublicationID: "p"}, [16]byte{})
	if rt.State() != RemoteTrackCreating {
		t.Fatal("want CREATING")
	}
	rt.setState(RemoteTrackActive)
	rt.end()
	rt.end() // idempotent
	if rt.State() != RemoteTrackEnded {
		t.Fatal("want ENDED")
	}
	// writes after end are no-ops (no panic, no handle).
	rt.writeRTP([]byte{1})
	rt.writeRTCP([]byte{1})
}

func TestMediaMessageTypesRegistered(t *testing.T) {
	for _, ty := range []string{
		MsgMediaSessionOpen, MsgMediaSessionAccept, MsgMediaSubscribe,
		MsgMediaSubscribeAck, MsgMediaUnsubscribe, MsgMediaTrackEnded,
	} {
		if !knownMessageTypes[ty] {
			t.Fatalf("%s not registered as a known cluster message type", ty)
		}
	}
}
