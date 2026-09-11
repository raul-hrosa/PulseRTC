package sfu

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

// Media sources a Publication can originate from. "screen" is a screen /
// window / tab capture (getDisplayMedia); the SFU treats it as an ordinary
// video Publication and only carries the label through to subscribers.
const (
	SourceCamera     = "camera"
	SourceMicrophone = "microphone"
	SourceScreen     = "screen"
)

// keyframeRateLimit caps how often the SFU asks a publisher for a fresh keyframe,
// no matter how many subscribers ask at once (a new subscriber, a downstream
// PLI/FIR, a video send-queue resync all route through the same limiter).
const keyframeRateLimit = time.Second

// Publication is ONE media track a participant makes available through the SFU.
//
// A participant that publishes audio AND video therefore owns TWO independent
// Publications, each with its own id. Audio and video are never merged into a
// single "publication per participant".
//
// Identity (see docs/architecture/pubsub.md, docs/architecture/sfu.md) — these are four different
// things and none is derived from another:
//
//	ParticipantID  UUID from signaling            stable per connection
//	PublicationID  "pub-" + UUID, minted here     stable per published track
//	remoteTrackID  the browser's MediaStreamTrack the publisher chose it
//	SSRC           RTP transport number           per PeerConnection, may change
type Publication struct {
	id            string
	participantID string
	kind          webrtc.RTPCodecType
	ssrc          webrtc.SSRC
	codec         webrtc.RTPCodecParameters
	remoteTrackID string

	// source is "camera" / "microphone" / "screen". It is metadata only —
	// the SFU forwards a screen track exactly like a camera track — propagated
	// to subscribers so their UI can tell a shared screen from a webcam.
	source string

	// muted is producer-side enable/disable (mic mute, camera off). The SFU
	// keeps forwarding whatever RTP still arrives; this flag is metadata that
	// is propagated to subscribers so their UI can react.
	muted atomic.Bool

	// remote is set only on a "remote publication" — a publication whose media
	// originates on another node and arrives via the cluster media bridge.
	// For such a publication there is no local publisher
	// PeerConnection; RTP is written straight into local by the bridge and
	// subscriber RTCP feedback is sent back through remoteRTCPSink.
	remote         bool
	remoteRTCPSink func(pkt []byte)

	// room is the room this publication lives in, used to reach the local
	// publisher for keyframe requests. nil for a bare publication in tests.
	room *Room

	// lastKeyframe is the unix-nano timestamp of the last PLI sent toward the
	// publisher; requestKeyframe uses it to rate-limit (keyframeRateLimit).
	lastKeyframe atomic.Int64

	// remoteSinks fan a LOCAL publication's RTP out to cluster media sessions so
	// remote nodes can subscribe to it (publisher side). Copy-on-write
	// so the RTP hot path reads with a single atomic load and no lock.
	remoteSinks atomic.Pointer[[]remoteSink]
	sinkSeq     atomic.Uint64

	// subSinks fan this publication's RTP out to each subscriber's bounded send
	// queue (ADR 012). A slow subscriber overflows and drops on its own queue;
	// it never back-pressures the ingest loop. Copy-on-write, same as
	// remoteSinks — the hot path reads with one atomic load and no lock.
	subSinks atomic.Pointer[[]subSinkEntry]
	subSeq   atomic.Uint64
}

type subSinkEntry struct {
	id uint64
	q  *sendQueue
}

// attachSink registers a subscriber send queue and returns an idempotent detach.
func (p *Publication) attachSink(q *sendQueue) (detach func()) {
	id := p.subSeq.Add(1)
	for {
		cur := p.subSinks.Load()
		next := make([]subSinkEntry, 0, sinkCount(cur)+1)
		if cur != nil {
			next = append(next, *cur...)
		}
		next = append(next, subSinkEntry{id: id, q: q})
		if p.subSinks.CompareAndSwap(cur, &next) {
			break
		}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for {
				cur := p.subSinks.Load()
				if cur == nil {
					return
				}
				next := make([]subSinkEntry, 0, len(*cur))
				for _, e := range *cur {
					if e.id != id {
						next = append(next, e)
					}
				}
				if p.subSinks.CompareAndSwap(cur, &next) {
					return
				}
			}
		})
	}
}

func sinkCount(s *[]subSinkEntry) int {
	if s == nil {
		return 0
	}
	return len(*s)
}

// requestKeyframe asks the publisher for a fresh keyframe: the local publisher's
// PeerConnection, or the origin node for a mirrored remote publication. It is a
// no-op for audio and is rate-limited to at most one request per
// keyframeRateLimit regardless of how many callers ask.
func (p *Publication) requestKeyframe() {
	if p.kind != webrtc.RTPCodecTypeVideo {
		return
	}
	now := time.Now().UnixNano()
	last := p.lastKeyframe.Load()
	if now-last < int64(keyframeRateLimit) {
		return
	}
	if !p.lastKeyframe.CompareAndSwap(last, now) {
		return
	}

	pli := []rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(p.ssrc)}}

	if p.remote {
		if sink := p.remoteRTCPSink; sink != nil {
			if raw, err := rtcp.Marshal(pli); err == nil {
				sink(raw)
			}
		}
		return
	}
	if p.room == nil {
		return
	}
	if pubPeer, ok := p.room.participant(p.participantID); ok {
		_ = pubPeer.pc.WriteRTCP(pli)
	}
}

// writeRTP fans one RTP packet to every subscriber send queue. Never blocks.
func (p *Publication) writeRTP(pkt []byte) {
	if s := p.subSinks.Load(); s != nil {
		for _, e := range *s {
			e.q.push(pkt)
		}
	}
}

type remoteSink struct {
	id uint64
	fn func(pkt []byte)
}

// IsRemote reports whether this publication's media originates on another node.
func (p *Publication) IsRemote() bool { return p.remote }

// addRemoteSink registers an inter-node forwarder for this (local) publication
// and returns a detach func (idempotent).
func (p *Publication) addRemoteSink(fn func(pkt []byte)) (remove func()) {
	id := p.sinkSeq.Add(1)
	for {
		cur := p.remoteSinks.Load()
		next := make([]remoteSink, 0, sinkLen(cur)+1)
		if cur != nil {
			next = append(next, *cur...)
		}
		next = append(next, remoteSink{id: id, fn: fn})
		if p.remoteSinks.CompareAndSwap(cur, &next) {
			break
		}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for {
				cur := p.remoteSinks.Load()
				if cur == nil {
					return
				}
				next := make([]remoteSink, 0, len(*cur))
				for _, s := range *cur {
					if s.id != id {
						next = append(next, s)
					}
				}
				if p.remoteSinks.CompareAndSwap(cur, &next) {
					return
				}
			}
		})
	}
}

func sinkLen(s *[]remoteSink) int {
	if s == nil {
		return 0
	}
	return len(*s)
}

func (p *Publication) hasRemoteSinks() bool {
	s := p.remoteSinks.Load()
	return s != nil && len(*s) > 0
}

func (p *Publication) fanRemote(pkt []byte) {
	if s := p.remoteSinks.Load(); s != nil {
		for _, sink := range *s {
			sink.fn(pkt)
		}
	}
}

// ID returns the publication identifier.
func (p *Publication) ID() string { return p.id }

// ParticipantID returns the publisher's participant id.
func (p *Publication) ParticipantID() string { return p.participantID }

// Kind returns "audio" or "video".
func (p *Publication) Kind() string { return p.kind.String() }

// Muted reports the producer-side mute state.
func (p *Publication) Muted() bool { return p.muted.Load() }

// Source returns "camera" / "microphone" / "screen". It falls back to the
// kind-derived default when the publisher never declared one (older client, or
// a cross-node mirror).
func (p *Publication) Source() string {
	if p.source != "" {
		return p.source
	}
	return defaultSource(p.kind)
}

func defaultSource(kind webrtc.RTPCodecType) string {
	if kind == webrtc.RTPCodecTypeVideo {
		return SourceCamera
	}
	return SourceMicrophone
}

func newPublication(participantID string, remote *webrtc.TrackRemote, source string) *Publication {
	if source == "" {
		source = defaultSource(remote.Kind())
	}
	return &Publication{
		id:            "pub-" + uuid.NewString(),
		participantID: participantID,
		kind:          remote.Kind(),
		ssrc:          remote.SSRC(),
		codec:         remote.Codec(),
		remoteTrackID: remote.ID(),
		source:        source,
	}
}

// queueKindOf maps a publication's media kind to a send-queue drop policy.
func (p *Publication) queueKind() queueKind {
	if p.kind == webrtc.RTPCodecTypeVideo {
		return queueKindVideo
	}
	return queueKindAudio
}

// Subscription is one Publication being forwarded to one subscriber. Each
// subscription owns a private fan-out track and a bounded send queue drained by
// a single writer goroutine (ADR 012), so a slow subscriber cannot stall the
// publisher's ingest loop or any other subscriber.
type Subscription struct {
	subscriberID string
	publication  *Publication
	sender       *webrtc.RTPSender
	local        *webrtc.TrackLocalStaticRTP
	queue        *sendQueue
	detach       func()
}
