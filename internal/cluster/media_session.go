package cluster

import (
	"encoding/hex"
	"sync"
	"time"
)

// MediaSessionState is the lifecycle of an SFU↔SFU media session.
type MediaSessionState string

const (
	MediaSessionCreating MediaSessionState = "CREATING"
	MediaSessionActive   MediaSessionState = "ACTIVE"
	MediaSessionClosed   MediaSessionState = "CLOSED"
)

// MediaSession is one SFU↔SFU pairing. It owns the MediaConnection and every
// RemoteTrack flowing over it, and cleans them all up on Close.
type MediaSession struct {
	ID           [16]byte
	IDHex        string
	RoomID       string
	LocalNodeID  string
	RemoteNodeID string
	CreatedAt    time.Time

	conn MediaConnection

	mu    sync.Mutex
	state MediaSessionState
	// publisher side: publicationID -> detach forwarder.
	forwarders map[string]func()
	// subscriber side: publicationID -> RemoteTrack.
	tracks map[string]*RemoteTrack

	closeOnce sync.Once
	onClose   func(*MediaSession)
}

func newMediaSession(id [16]byte, roomID, local, remote string, conn MediaConnection) *MediaSession {
	return &MediaSession{
		ID: id, IDHex: hex.EncodeToString(id[:]), RoomID: roomID,
		LocalNodeID: local, RemoteNodeID: remote,
		CreatedAt:  time.Now(),
		conn:       conn,
		state:      MediaSessionCreating,
		forwarders: map[string]func(){},
		tracks:     map[string]*RemoteTrack{},
	}
}

// State returns the session state.
func (s *MediaSession) State() MediaSessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *MediaSession) setState(st MediaSessionState) {
	s.mu.Lock()
	s.state = st
	s.mu.Unlock()
}

func (s *MediaSession) addForwarder(pubID string, detach func()) {
	s.mu.Lock()
	if old, ok := s.forwarders[pubID]; ok {
		s.mu.Unlock()
		old()
		s.mu.Lock()
	}
	s.forwarders[pubID] = detach
	s.mu.Unlock()
}

func (s *MediaSession) removeForwarder(pubID string) {
	s.mu.Lock()
	detach, ok := s.forwarders[pubID]
	delete(s.forwarders, pubID)
	s.mu.Unlock()
	if ok {
		detach()
	}
}

func (s *MediaSession) addTrack(rt *RemoteTrack) {
	s.mu.Lock()
	s.tracks[rt.Info.PublicationID] = rt
	s.mu.Unlock()
}

func (s *MediaSession) track(pubID string) (*RemoteTrack, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.tracks[pubID]
	return rt, ok
}

func (s *MediaSession) removeTrack(pubID string) {
	s.mu.Lock()
	rt, ok := s.tracks[pubID]
	delete(s.tracks, pubID)
	s.mu.Unlock()
	if ok {
		rt.end()
	}
}

// Close tears the session down: stops forwarders, ends remote tracks, closes the
// connection. Idempotent — Close Close Close must not panic.
func (s *MediaSession) Close() {
	s.closeOnce.Do(func() {
		s.setState(MediaSessionClosed)

		s.mu.Lock()
		fwds := s.forwarders
		tracks := s.tracks
		s.forwarders = map[string]func(){}
		s.tracks = map[string]*RemoteTrack{}
		s.mu.Unlock()

		for _, detach := range fwds {
			detach()
		}
		for _, rt := range tracks {
			rt.end()
		}
		if s.conn != nil {
			_ = s.conn.Close()
		}
		if s.onClose != nil {
			s.onClose(s)
		}
	})
}
