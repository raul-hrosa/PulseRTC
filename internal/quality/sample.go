package quality

// Direction distinguishes the two legs of a participant's media. This is the
// key to a useful diagnosis: "A → SFU good, SFU → A poor" points at A's
// downlink, not at A's uplink.
type Direction string

const (
	Outbound Direction = "outbound" // participant → SFU (uplink)
	Inbound  Direction = "inbound"  // SFU → participant (downlink)
)

// Kind is the media kind, plus the synthetic "connection" kind for the
// transport-level verdict.
type Kind string

const (
	Audio      Kind = "audio"
	Video      Kind = "video"
	Connection Kind = "connection"
)

// Source records where a Sample came from. The browser is the source of truth
// today; the field exists so SFU/network samples can be added later without
// changing the model.
type Source string

const (
	SourceBrowser Source = "browser"
	SourceServer  Source = "server"
)

// StreamKey identifies one thing the engine tracks a verdict for: a media
// track in one direction, or a participant's connection.
type StreamKey struct {
	Participant string
	Direction   Direction
	Kind        Kind
	TrackID     string // publication id; empty for Connection
}

// Sample is one normalised observation. Every counter is cumulative since the
// stream started; the engine derives rates from consecutive samples. All
// fields are optional — a nil pointer means "the browser did not report it".
type Sample struct {
	Key      StreamKey
	Source   Source
	AtMillis int64 // monotonic clock of the reporter, in ms

	// Cumulative counters.
	PacketsSent     *float64
	PacketsReceived *float64
	PacketsLost     *float64
	BytesSent       *float64
	BytesReceived   *float64
	FramesDecoded   *float64
	FramesDropped   *float64

	// Instantaneous values (already point-in-time in getStats()).
	JitterMs *float64
	RTTMs    *float64
	FPS      *float64
	Width    *float64
	Height   *float64

	// Track / transport state.
	Enabled   *bool  // MediaStreamTrack.enabled; false => mute / camera off
	ConnState string // RTCPeerConnectionState
	ICEState  string // RTCIceConnectionState
}

// Derived is one interval's worth of rates, ready to analyse. Nil means the
// metric could not be computed from the sample pair.
type Derived struct {
	HasData   bool
	Enabled   bool
	DtSeconds float64

	LossPct      *float64
	BitrateBps   *float64
	JitterMs     *float64
	RTTMs        *float64
	FPS          *float64
	FrameDropPct *float64
	Width        *float64
	Height       *float64

	ConnState string
	ICEState  string
}

func ptr[T any](v T) *T { return &v }

func fval(p *float64) (float64, bool) {
	if p == nil {
		return 0, false
	}
	return *p, true
}

// derive turns a (prev, cur) sample pair into interval rates. With no prev it
// still surfaces the instantaneous values (jitter/rtt/fps) so a verdict is
// possible on the very first interval for those.
func derive(prev, cur *Sample) Derived {
	d := Derived{ConnState: cur.ConnState, ICEState: cur.ICEState}
	d.Enabled = cur.Enabled == nil || *cur.Enabled

	if cur.JitterMs != nil {
		d.JitterMs = ptr(*cur.JitterMs)
	}
	if cur.RTTMs != nil {
		d.RTTMs = ptr(*cur.RTTMs)
	}
	if cur.FPS != nil {
		d.FPS = ptr(*cur.FPS)
	}
	if cur.Width != nil {
		d.Width = ptr(*cur.Width)
	}
	if cur.Height != nil {
		d.Height = ptr(*cur.Height)
	}

	if prev != nil && cur.AtMillis > prev.AtMillis {
		dt := float64(cur.AtMillis-prev.AtMillis) / 1000
		d.DtSeconds = dt

		if s := deltaNonNeg(prev.BytesReceived, cur.BytesReceived); s != nil {
			d.BitrateBps = ptr(*s * 8 / dt)
		} else if s := deltaNonNeg(prev.BytesSent, cur.BytesSent); s != nil {
			d.BitrateBps = ptr(*s * 8 / dt)
		}

		// Packet loss % over the interval. Uses received on the inbound leg;
		// on the outbound leg the browser fills PacketsReceived from the
		// remote-inbound-rtp report (packets the SFU actually got).
		dLost := deltaNonNeg(prev.PacketsLost, cur.PacketsLost)
		dRecv := deltaNonNeg(prev.PacketsReceived, cur.PacketsReceived)
		if dLost != nil && dRecv != nil {
			total := *dLost + *dRecv
			if total > 0 {
				d.LossPct = ptr(*dLost / total * 100)
			} else {
				d.LossPct = ptr(0.0)
			}
		}

		dDrop := deltaNonNeg(prev.FramesDropped, cur.FramesDropped)
		dDec := deltaNonNeg(prev.FramesDecoded, cur.FramesDecoded)
		if dDrop != nil && dDec != nil {
			total := *dDrop + *dDec
			if total > 0 {
				d.FrameDropPct = ptr(*dDrop / total * 100)
			} else {
				d.FrameDropPct = ptr(0.0)
			}
		}
	}

	d.HasData = d.LossPct != nil || d.BitrateBps != nil || d.JitterMs != nil ||
		d.RTTMs != nil || d.FPS != nil || d.ConnState != ""
	return d
}

func deltaNonNeg(prev, cur *float64) *float64 {
	pv, pok := fval(prev)
	cv, cok := fval(cur)
	if !pok || !cok {
		return nil
	}
	if cv < pv { // counter reset (renegotiation, track replaced)
		return nil
	}
	return ptr(cv - pv)
}
