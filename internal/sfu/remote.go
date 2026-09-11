package sfu

import (
	"github.com/pion/rtcp"

	"github.com/raulhrosa/pulsertc/internal/cluster"
)

// Cross-node media. The SFU implements cluster.MediaSource (its
// publisher side: hand a local publication's RTP to the bridge) and
// cluster.MediaSink (its subscriber side: inject a remote publication into a
// local room). Media bytes flow over the cluster's UDP MediaTransport; this
// file only wires the SFU's existing fan-out to it.

var (
	_ cluster.MediaSource = (*SFU)(nil)
	_ cluster.MediaSink   = (*SFU)(nil)
)

// SetMediaPublicationEndedHook registers a callback fired when a locally
// published track that may be forwarded cross-node goes away.
func (s *SFU) SetMediaPublicationEndedHook(fn func(roomID, publicationID string)) {
	s.mediaPubEnded.Store(&fn)
}

func (s *SFU) firePublicationEnded(roomID, pubID string) {
	if fn := s.mediaPubEnded.Load(); fn != nil {
		(*fn)(roomID, pubID)
	}
}

// AttachForwarder (cluster.MediaSource) starts copying a local publication's RTP
// to onRTP and returns its parameters so the remote node can build a matching
// synthetic track.
func (s *SFU) AttachForwarder(roomID, publicationID string, onRTP func(pkt []byte)) (cluster.RemotePublicationInfo, func(), bool) {
	r, ok := s.GetRoom(roomID)
	if !ok {
		return cluster.RemotePublicationInfo{}, nil, false
	}
	pub, ok := r.findPublication(publicationID)
	if !ok || pub.IsRemote() {
		return cluster.RemotePublicationInfo{}, nil, false
	}
	detach := pub.addRemoteSink(onRTP)
	info := cluster.RemotePublicationInfo{
		RoomID:        roomID,
		PublicationID: pub.id,
		ParticipantID: pub.participantID,
		Kind:          pub.Kind(),
		MimeType:      pub.codec.RTPCodecCapability.MimeType,
		ClockRate:     pub.codec.RTPCodecCapability.ClockRate,
		Channels:      pub.codec.RTPCodecCapability.Channels,
		PayloadType:   uint8(pub.codec.PayloadType),
		SSRC:          uint32(pub.ssrc),
	}
	return info, detach, true
}

// DeliverRTCPToPublisher (cluster.MediaSource) injects remote-subscriber
// feedback toward the local publisher. Only keyframe requests are translated;
// other RTCP is dropped here (the local NACK responder
// handles subscriber-side loss on the remote node).
func (s *SFU) DeliverRTCPToPublisher(roomID, publicationID string, pkt []byte) {
	r, ok := s.GetRoom(roomID)
	if !ok {
		return
	}
	pub, ok := r.findPublication(publicationID)
	if !ok || pub.IsRemote() {
		return
	}
	pubPeer, ok := r.participant(pub.participantID)
	if !ok {
		return
	}
	pkts, err := rtcp.Unmarshal(pkt)
	if err != nil {
		return
	}
	for _, p := range pkts {
		switch p.(type) {
		case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
			_ = pubPeer.pc.WriteRTCP([]rtcp.Packet{
				&rtcp.PictureLossIndication{MediaSSRC: uint32(pub.ssrc)},
			})
		}
	}
}

// RoomHasPublication reports whether a room currently exposes a publication
// (local or a remote mirror) with the given id (signaling helper).
func (s *SFU) RoomHasPublication(roomID, pubID string) bool {
	r, ok := s.GetRoom(roomID)
	if !ok {
		return false
	}
	_, found := r.findPublication(pubID)
	return found
}

// AddRemotePublication (cluster.MediaSink) creates a synthetic publication in a
// local room fed by the bridge, and fans it to local subscribers exactly like a
// normal publication.
func (s *SFU) AddRemotePublication(info cluster.RemotePublicationInfo, rtcpToOrigin func(pkt []byte)) (cluster.RemotePublication, error) {
	r := s.Room(info.RoomID)
	return r.addRemotePublication(info, rtcpToOrigin)
}

// remotePubHandle is the cluster.RemotePublication returned to the bridge.
type remotePubHandle struct {
	room *Room
	pub  *Publication
}

func (h *remotePubHandle) WriteRTP(pkt []byte) error {
	h.pub.writeRTP(pkt)
	return nil
}

func (h *remotePubHandle) WriteRTCP([]byte) error { return nil }

func (h *remotePubHandle) Close() { h.room.removeRemotePublication(h.pub.id) }
