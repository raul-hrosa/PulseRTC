package cluster

import (
	"sync"
	"time"
)

// RemoteTrackState is the lifecycle of a publication mirrored from another node.
type RemoteTrackState string

const (
	RemoteTrackCreating RemoteTrackState = "CREATING"
	RemoteTrackActive   RemoteTrackState = "ACTIVE"
	RemoteTrackMuted    RemoteTrackState = "MUTED"
	RemoteTrackEnded    RemoteTrackState = "ENDED"
)

// RemotePublicationInfo describes a publication that physically lives on another
// node but is subscribable locally. Identity is stable
// (publicationId/participantId); SSRC is only an RTP property.
type RemotePublicationInfo struct {
	RoomID        string
	PublicationID string
	ParticipantID string
	OriginNodeID  string
	Kind          string // "audio" | "video"
	MimeType      string
	ClockRate     uint32
	Channels      uint16
	PayloadType   uint8
	SSRC          uint32
}

// RemoteTrack is the media bridge's record of one mirrored publication.
type RemoteTrack struct {
	Info      RemotePublicationInfo
	SessionID [16]byte
	CreatedAt time.Time

	mu    sync.Mutex
	state RemoteTrackState
	// handle is the SFU-side synthetic publication (nil in pure transport tests).
	handle RemotePublication
}

func newRemoteTrack(info RemotePublicationInfo, sessionID [16]byte) *RemoteTrack {
	return &RemoteTrack{
		Info: info, SessionID: sessionID, CreatedAt: time.Now(),
		state: RemoteTrackCreating,
	}
}

func (rt *RemoteTrack) setState(s RemoteTrackState) {
	rt.mu.Lock()
	rt.state = s
	rt.mu.Unlock()
}

// State returns the current lifecycle state.
func (rt *RemoteTrack) State() RemoteTrackState {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.state
}

func (rt *RemoteTrack) writeRTP(pkt []byte) {
	rt.mu.Lock()
	h := rt.handle
	ended := rt.state == RemoteTrackEnded
	rt.mu.Unlock()
	if ended || h == nil {
		return
	}
	_ = h.WriteRTP(pkt)
}

func (rt *RemoteTrack) writeRTCP(pkt []byte) {
	rt.mu.Lock()
	h := rt.handle
	rt.mu.Unlock()
	if h != nil {
		_ = h.WriteRTCP(pkt)
	}
}

// end retires the remote track and its SFU handle. Idempotent.
func (rt *RemoteTrack) end() {
	rt.mu.Lock()
	if rt.state == RemoteTrackEnded {
		rt.mu.Unlock()
		return
	}
	rt.state = RemoteTrackEnded
	h := rt.handle
	rt.handle = nil
	rt.mu.Unlock()
	if h != nil {
		h.Close()
	}
}

// RemotePublication is the SFU-side handle for a mirrored publication: the
// bridge writes inbound RTP/RTCP into it and the SFU fans it out to local
// subscribers exactly like a normal Publication.
type RemotePublication interface {
	WriteRTP(pkt []byte) error
	WriteRTCP(pkt []byte) error
	Close()
}

// MediaSink is the SFU as seen by the media bridge (subscriber side). It creates
// a local synthetic publication fed from another node. rtcpToOrigin is invoked
// by the SFU with subscriber feedback (PLI/FIR/NACK) that must travel back to
// the publishing node.
type MediaSink interface {
	AddRemotePublication(info RemotePublicationInfo, rtcpToOrigin func(pkt []byte)) (RemotePublication, error)
}

// MediaSource is the SFU as seen by the media bridge (publisher side).
type MediaSource interface {
	// AttachForwarder starts copying a local publication's RTP to onRTP. It
	// returns the publication parameters, a detach func, and ok=false if the
	// publication does not exist locally.
	AttachForwarder(roomID, publicationID string, onRTP func(pkt []byte)) (info RemotePublicationInfo, detach func(), ok bool)
	// DeliverRTCPToPublisher injects remote-subscriber feedback toward the local
	// publisher.
	DeliverRTCPToPublisher(roomID, publicationID string, pkt []byte)
}
