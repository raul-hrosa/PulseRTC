package loadtest

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

type ServerMetrics struct {
	Timestamp string  `json:"timestamp"`
	UptimeSec float64 `json:"uptimeSec"`
	Host      struct {
		OS     string `json:"os"`
		Arch   string `json:"arch"`
		CPUs   int    `json:"cpus"`
		Go     string `json:"go"`
		Commit string `json:"commit"`
	} `json:"host"`
	Runtime struct {
		Goroutines      int     `json:"goroutines"`
		HeapAllocBytes  uint64  `json:"heapAllocBytes"`
		HeapSysBytes    uint64  `json:"heapSysBytes"`
		AllocBytes      uint64  `json:"allocBytes"`
		TotalAllocBytes uint64  `json:"totalAllocBytes"`
		SysBytes        uint64  `json:"sysBytes"`
		NumGC           uint32  `json:"numGC"`
		GoCPUSeconds    float64 `json:"goCpuSeconds"`
	} `json:"runtime"`
	SFU struct {
		Rooms         EntityMetrics      `json:"rooms"`
		PeerConns     EntityMetrics      `json:"peerConnections"`
		Tracks        EntityMetrics      `json:"tracks"`
		Publications  EntityMetrics      `json:"publications"`
		Subscriptions EntityMetrics      `json:"subscriptions"`
		Negotiations  NegotiationMetrics `json:"negotiations"`
		RTP           RTPAggregate       `json:"rtp"`
	} `json:"sfu"`
}

type NegotiationMetrics struct {
	Started   int64 `json:"started"`
	Completed int64 `json:"completed"`
	Coalesced int64 `json:"coalesced"`
	Failed    int64 `json:"failed"`
}

type EntityMetrics struct {
	Active  int   `json:"active"`
	Created int64 `json:"created"`
	Closed  int64 `json:"closed,omitempty"`
	Removed int64 `json:"removed,omitempty"`
}

type RTPAggregate struct {
	PacketsReceived uint64 `json:"packetsReceived"`
	PacketsSent     uint64 `json:"packetsSent"`
	PacketsLost     int64  `json:"packetsLost"`
	BytesReceived   uint64 `json:"bytesReceived"`
	BytesSent       uint64 `json:"bytesSent"`
}

type ClientSample struct {
	Participant      string  `json:"participant"`
	ConnectionState  string  `json:"connectionState"`
	ICEState         string  `json:"iceState"`
	PacketsSent      uint64  `json:"packetsSent"`
	PacketsReceived  uint64  `json:"packetsReceived"`
	PacketsLost      int64   `json:"packetsLost"`
	BytesSent        uint64  `json:"bytesSent"`
	BytesReceived    uint64  `json:"bytesReceived"`
	RTTMs            float64 `json:"rttMs"`
	JitterMs         float64 `json:"jitterMs"`
	FPS              float64 `json:"fps"`
	BitrateOutBps    float64 `json:"bitrateOutBps"`
	BitrateInBps     float64 `json:"bitrateInBps"`
	ConnectionTimeMs float64 `json:"connectionTimeMs"`
	MediaStartMs     float64 `json:"mediaStartMs"`
}

type SummaryStats struct {
	Average float64 `json:"avg"`
	Min     float64 `json:"min"`
	Max     float64 `json:"max"`
	P50     float64 `json:"p50"`
	P95     float64 `json:"p95"`
	P99     float64 `json:"p99"`
}

type Result struct {
	Timestamp     string          `json:"timestamp"`
	Scenario      string          `json:"scenario"`
	Configuration Config          `json:"configuration"`
	Host          ResultHost      `json:"host"`
	ServerStart   *ServerMetrics  `json:"serverStart,omitempty"`
	ServerEnd     *ServerMetrics  `json:"serverEnd,omitempty"`
	Summary       ResultSummary   `json:"summary"`
	ServerSamples []ServerMetrics `json:"serverSamples,omitempty"`
	ClientSamples []ClientSample  `json:"clientSamples,omitempty"`
	// NodeClusters holds each target node's /metrics "cluster" block at the end
	// of the run: proof that Node A and Node B agree on
	// ownership and participant location through Redis.
	NodeClusters map[string]json.RawMessage `json:"nodeClusters,omitempty"`
}

type ResultHost struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPUs      int    `json:"cpus"`
	Go        string `json:"go"`
	GitCommit string `json:"gitCommit"`
}

type ResultSummary struct {
	Participants          int                `json:"participants"`
	FailedConnections     int                `json:"failedConnections"`
	RedirectedConnections int                `json:"redirectedConnections"` // got ROOM_ON_OTHER_NODE etc (S10)
	LocalMessages         int64              `json:"localMessages"`         // cross-node signaling
	RemoteMessages        int64              `json:"remoteMessages"`
	RemoteMessageErrors   int64              `json:"remoteMessageErrors"`
	PeerConnections       EntityMetrics      `json:"peerConnections"`
	Tracks                EntityMetrics      `json:"tracks"`
	Publications          EntityMetrics      `json:"publications"`
	Subscriptions         EntityMetrics      `json:"subscriptions"`
	Negotiations          NegotiationMetrics `json:"negotiations"`
	Goroutines            SummaryStats       `json:"goroutines"`
	HeapAllocMB           SummaryStats       `json:"heapAllocMb"`
	CPUPercent            SummaryStats       `json:"cpuPercent"`
	NetworkInMbps         SummaryStats       `json:"networkInMbps"`
	NetworkOutMbps        SummaryStats       `json:"networkOutMbps"`
	PacketsSent           uint64             `json:"packetsSent"`
	PacketsReceived       uint64             `json:"packetsReceived"`
	PacketsLost           int64              `json:"packetsLost"`
	PacketLossPct         float64            `json:"packetLossPct"`
	RTTMs                 SummaryStats       `json:"rttMs"`
	JitterMs              SummaryStats       `json:"jitterMs"`
	BitrateInMbps         SummaryStats       `json:"clientBitrateInMbps"`
	BitrateOutMbps        SummaryStats       `json:"clientBitrateOutMbps"`
	ConnectionTimeMs      SummaryStats       `json:"connectionTimeMs"`
	MediaStartTimeMs      SummaryStats       `json:"mediaStartTimeMs"`
	AudioQuality          string             `json:"audioQuality"`
	VideoQuality          string             `json:"videoQuality"`
	ConnectionQuality     string             `json:"connectionQuality"`
	OverallQuality        string             `json:"overallQuality"`
	LikelyBottleneck      string             `json:"likelyBottleneck"`
}

func Summarize(cfg Config, server []ServerMetrics, clients []ClientSample) Result {
	res := Result{
		Timestamp:     time.Now().UTC().Format(time.RFC3339Nano),
		Scenario:      cfg.Scenario,
		Configuration: cfg,
		Host: ResultHost{
			OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(),
			Go: runtime.Version(), GitCommit: "unavailable",
		},
		ServerSamples: server,
		ClientSamples: clients,
	}
	if len(server) > 0 {
		res.ServerStart = &server[0]
		res.ServerEnd = &server[len(server)-1]
		res.Host.GitCommit = server[len(server)-1].Host.Commit
		res.Summary.PeerConnections = server[len(server)-1].SFU.PeerConns
		res.Summary.Tracks = server[len(server)-1].SFU.Tracks
		res.Summary.Publications = server[len(server)-1].SFU.Publications
		res.Summary.Subscriptions = server[len(server)-1].SFU.Subscriptions
		res.Summary.Negotiations = server[len(server)-1].SFU.Negotiations
	}
	res.Summary.Participants = cfg.Participants
	res.Summary.Goroutines = summarize(serverValues(server, func(m ServerMetrics) float64 { return float64(m.Runtime.Goroutines) }))
	res.Summary.HeapAllocMB = summarize(serverValues(server, func(m ServerMetrics) float64 { return bytesToMB(m.Runtime.HeapAllocBytes) }))
	res.Summary.CPUPercent = summarizeCPU(server)
	res.Summary.NetworkInMbps, res.Summary.NetworkOutMbps = summarizeNetwork(server)

	var rtt, jitter, brIn, brOut, conn, media []float64
	for _, s := range clients {
		res.Summary.PacketsSent += s.PacketsSent
		res.Summary.PacketsReceived += s.PacketsReceived
		res.Summary.PacketsLost += s.PacketsLost
		rtt = appendPositive(rtt, s.RTTMs)
		jitter = appendPositive(jitter, s.JitterMs)
		brIn = appendPositive(brIn, s.BitrateInBps/1_000_000)
		brOut = appendPositive(brOut, s.BitrateOutBps/1_000_000)
		conn = appendPositive(conn, s.ConnectionTimeMs)
		media = appendPositive(media, s.MediaStartMs)
	}
	totalPackets := float64(res.Summary.PacketsReceived) + float64(res.Summary.PacketsLost)
	if totalPackets > 0 {
		res.Summary.PacketLossPct = float64(res.Summary.PacketsLost) / totalPackets * 100
	}
	res.Summary.RTTMs = summarize(rtt)
	res.Summary.JitterMs = summarize(jitter)
	res.Summary.BitrateInMbps = summarize(brIn)
	res.Summary.BitrateOutMbps = summarize(brOut)
	res.Summary.ConnectionTimeMs = summarize(conn)
	res.Summary.MediaStartTimeMs = summarize(media)
	res.Summary.AudioQuality = inferQuality(res.Summary.PacketLossPct, res.Summary.JitterMs.P95, res.Summary.RTTMs.P95)
	res.Summary.VideoQuality = inferQuality(res.Summary.PacketLossPct, res.Summary.JitterMs.P95, res.Summary.RTTMs.P95)
	res.Summary.ConnectionQuality = inferQuality(res.Summary.PacketLossPct, 0, res.Summary.RTTMs.P95)
	res.Summary.OverallQuality = worstQuality(res.Summary.AudioQuality, res.Summary.VideoQuality, res.Summary.ConnectionQuality)
	res.Summary.LikelyBottleneck = inferBottleneck(res.Summary)
	return res
}

func WriteResult(dir string, r Result) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	name := r.Scenario
	if name == "" {
		name = "custom"
	}
	path := filepath.Join(dir, name+"-"+time.Now().UTC().Format("20060102T150405Z")+".json")
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return path, enc.Encode(r)
}

func summarize(values []float64) SummaryStats {
	if len(values) == 0 {
		return SummaryStats{}
	}
	sort.Float64s(values)
	var sum float64
	for _, v := range values {
		sum += v
	}
	return SummaryStats{
		Average: sum / float64(len(values)),
		Min:     values[0],
		Max:     values[len(values)-1],
		P50:     percentile(values, 50),
		P95:     percentile(values, 95),
		P99:     percentile(values, 99),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	pos := (p / 100) * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func appendPositive(values []float64, v float64) []float64 {
	if v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0) {
		return append(values, v)
	}
	return values
}

func bytesToMB(v uint64) float64 { return float64(v) / 1024 / 1024 }

func serverValues(samples []ServerMetrics, fn func(ServerMetrics) float64) []float64 {
	out := make([]float64, 0, len(samples))
	for _, s := range samples {
		out = append(out, fn(s))
	}
	return out
}

func summarizeCPU(samples []ServerMetrics) SummaryStats {
	var values []float64
	for i := 1; i < len(samples); i++ {
		prev, cur := samples[i-1], samples[i]
		dt := cur.UptimeSec - prev.UptimeSec
		if dt <= 0 {
			continue
		}
		dcpu := cur.Runtime.GoCPUSeconds - prev.Runtime.GoCPUSeconds
		values = appendPositive(values, (dcpu/dt)*100)
	}
	return summarize(values)
}

func summarizeNetwork(samples []ServerMetrics) (SummaryStats, SummaryStats) {
	var in, out []float64
	for i := 1; i < len(samples); i++ {
		prev, cur := samples[i-1], samples[i]
		dt := cur.UptimeSec - prev.UptimeSec
		if dt <= 0 {
			continue
		}
		if cur.SFU.RTP.BytesReceived >= prev.SFU.RTP.BytesReceived {
			in = appendPositive(in, float64(cur.SFU.RTP.BytesReceived-prev.SFU.RTP.BytesReceived)*8/dt/1_000_000)
		}
		if cur.SFU.RTP.BytesSent >= prev.SFU.RTP.BytesSent {
			out = appendPositive(out, float64(cur.SFU.RTP.BytesSent-prev.SFU.RTP.BytesSent)*8/dt/1_000_000)
		}
	}
	return summarize(in), summarize(out)
}

func inferQuality(lossPct, jitterP95, rttP95 float64) string {
	switch {
	case lossPct >= 5 || jitterP95 >= 80 || rttP95 >= 500:
		return "POOR"
	case lossPct >= 1 || jitterP95 >= 40 || rttP95 >= 250:
		return "WARNING"
	default:
		return "GOOD"
	}
}

func worstQuality(values ...string) string {
	out := "GOOD"
	for _, v := range values {
		if v == "POOR" {
			return "POOR"
		}
		if v == "WARNING" {
			out = "WARNING"
		}
	}
	return out
}

func inferBottleneck(s ResultSummary) string {
	if s.CPUPercent.P95 >= 80 {
		return "CPU"
	}
	if s.NetworkOutMbps.P95 > 0 && s.PacketLossPct >= 1 {
		return "network"
	}
	if s.HeapAllocMB.Max > s.HeapAllocMB.Min*1.5 && s.HeapAllocMB.Max-s.HeapAllocMB.Min > 128 {
		return "memory"
	}
	return "not identified"
}
