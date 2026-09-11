package sfu

import (
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

func TestPublicationRequestKeyframeSendsPLIToOrigin(t *testing.T) {
	var got [][]byte
	pub := &Publication{
		kind:           webrtc.RTPCodecTypeVideo,
		ssrc:           0xDEADBEEF,
		remote:         true,
		remoteRTCPSink: func(pkt []byte) { got = append(got, pkt) },
	}

	pub.requestKeyframe()

	if len(got) != 1 {
		t.Fatalf("sink called %d times, want 1", len(got))
	}
	pkts, err := rtcp.Unmarshal(got[0])
	if err != nil {
		t.Fatalf("unmarshal RTCP: %v", err)
	}
	pli, ok := pkts[0].(*rtcp.PictureLossIndication)
	if !ok {
		t.Fatalf("packet type %T, want *rtcp.PictureLossIndication", pkts[0])
	}
	if pli.MediaSSRC != 0xDEADBEEF {
		t.Fatalf("PLI MediaSSRC = %#x, want 0xDEADBEEF", pli.MediaSSRC)
	}
}

func TestPublicationRequestKeyframeRateLimited(t *testing.T) {
	var count int
	pub := &Publication{
		kind:           webrtc.RTPCodecTypeVideo,
		remote:         true,
		remoteRTCPSink: func([]byte) { count++ },
	}

	pub.requestKeyframe()
	pub.requestKeyframe() // within the rate-limit window -> suppressed
	pub.requestKeyframe()

	if count != 1 {
		t.Fatalf("keyframe requests sent = %d, want 1 (rate-limited)", count)
	}
}

func TestPublicationRequestKeyframeAllowedAgainAfterWindow(t *testing.T) {
	var count int
	pub := &Publication{
		kind:           webrtc.RTPCodecTypeVideo,
		remote:         true,
		remoteRTCPSink: func([]byte) { count++ },
	}

	pub.requestKeyframe()
	pub.lastKeyframe.Store(time.Now().Add(-2 * time.Second).UnixNano())
	pub.requestKeyframe()

	if count != 2 {
		t.Fatalf("keyframe requests sent = %d, want 2", count)
	}
}

func TestPublicationRequestKeyframeAudioIsNoop(t *testing.T) {
	var count int
	pub := &Publication{
		kind:           webrtc.RTPCodecTypeAudio,
		remote:         true,
		remoteRTCPSink: func([]byte) { count++ },
	}

	pub.requestKeyframe()

	if count != 0 {
		t.Fatalf("audio publication sent %d keyframe requests, want 0", count)
	}
}
