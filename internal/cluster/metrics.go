package cluster

import (
	"sync/atomic"
	"time"
)

// Metrics are the cluster counters. Cumulative
// unless noted; the gauges (node/room counts) are computed live in Snapshot.
type Metrics struct {
	ownershipClaimed  atomic.Int64
	ownershipConflict atomic.Int64
	internalRequests  atomic.Int64
	internalFailed    atomic.Int64
	roomRedirects     atomic.Int64

	// Redis / shared-state coordination.
	redisErrors     atomic.Int64
	redisReconnects atomic.Int64
	redisUnavail    atomic.Int64
	roomClaimFail   atomic.Int64
	roomRelease     atomic.Int64
	participantReg  atomic.Int64
	participantRem  atomic.Int64

	// Cross-node signaling. Low-cardinality: no per-message,
	// per-room or per-participant labels.
	msgSentC       atomic.Int64
	msgReceivedC   atomic.Int64
	msgFailedC     atomic.Int64
	msgRetriedC    atomic.Int64
	msgDuplicatedC atomic.Int64
	msgRejectedC   atomic.Int64
	msgInvalidC    atomic.Int64
	msgTimeoutC    atomic.Int64
	transportErrC  atomic.Int64
	routeLocalC    atomic.Int64
	routeRemoteC   atomic.Int64
	routeNotFoundC atomic.Int64
	transportLatNs atomic.Int64
	transportLatN  atomic.Int64

	// Cross-node media. Low cardinality: no ssrc/track/room labels.
	mediaSessCreated  atomic.Int64
	mediaSessClosed   atomic.Int64
	mediaSessFailed   atomic.Int64
	mediaSessActive   atomic.Int64 // gauge
	mediaRTPSentC     atomic.Int64
	mediaRTPRecvC     atomic.Int64
	mediaRTPDropC     atomic.Int64
	mediaRTPInvalidC  atomic.Int64
	mediaRTCPSentC    atomic.Int64
	mediaRTCPRecvC    atomic.Int64
	mediaRTCPDropC    atomic.Int64
	mediaOversizedC   atomic.Int64
	mediaAuthFailedC  atomic.Int64
	mediaRemoteTracks atomic.Int64 // gauge

	// Routing / load.
	roomSelections     atomic.Int64
	roomSelectionFails atomic.Int64
	roomClaimsRouted   atomic.Int64
	roomClaimRetries   atomic.Int64
	selLocal           atomic.Int64
	selLeastLoaded     atomic.Int64
	selCapacity        atomic.Int64
	selFallback        atomic.Int64
	loadReports        atomic.Int64
	loadStaleC         atomic.Int64

	// Failure detection & recovery. Low cardinality: no
	// room/participant/node labels.
	nodesDetectedStale  atomic.Int64
	nodesRecovered      atomic.Int64
	nodesRecoveryFailed atomic.Int64
	recoveryStarted     atomic.Int64
	recoveryCompleted   atomic.Int64
	recoveryFailedC     atomic.Int64
	recoveryRoomsC      atomic.Int64
	recoveryRoomsRecov  atomic.Int64
	recoveryRoomsSkip   atomic.Int64
	recoveryConflictsC  atomic.Int64
	routingStaleNodeC   atomic.Int64
	routingUnavailNodeC atomic.Int64
}

func (m *Metrics) nodeDetectedStale()   { m.addIf(&m.nodesDetectedStale) }
func (m *Metrics) nodeRecovered()       { m.addIf(&m.nodesRecovered) }
func (m *Metrics) nodeRecoveryFailed()  { m.addIf(&m.nodesRecoveryFailed) }
func (m *Metrics) recoveryBegan()       { m.addIf(&m.recoveryStarted) }
func (m *Metrics) recoveryDone()        { m.addIf(&m.recoveryCompleted) }
func (m *Metrics) recoveryFailed()      { m.addIf(&m.recoveryFailedC) }
func (m *Metrics) recoveryRoom()        { m.addIf(&m.recoveryRoomsC) }
func (m *Metrics) recoveryRoomRecov()   { m.addIf(&m.recoveryRoomsRecov) }
func (m *Metrics) recoveryRoomSkipped() { m.addIf(&m.recoveryRoomsSkip) }
func (m *Metrics) recoveryConflict()    { m.addIf(&m.recoveryConflictsC) }
func (m *Metrics) routingStaleNode()    { m.addIf(&m.routingStaleNodeC) }
func (m *Metrics) routingUnavailNode()  { m.addIf(&m.routingUnavailNodeC) }

func (m *Metrics) claimed()         { m.ownershipClaimed.Add(1) }
func (m *Metrics) conflict()        { m.ownershipConflict.Add(1) }
func (m *Metrics) internalReq()     { m.internalRequests.Add(1) }
func (m *Metrics) internalReqFail() { m.internalFailed.Add(1) }
func (m *Metrics) redirect()        { m.roomRedirects.Add(1) }

func (m *Metrics) redisError()            { m.redisErrors.Add(1) }
func (m *Metrics) redisReconnect()        { m.redisReconnects.Add(1) }
func (m *Metrics) redisUnavailable()      { m.redisUnavail.Add(1) }
func (m *Metrics) roomClaimFailed()       { m.roomClaimFail.Add(1) }
func (m *Metrics) roomReleased()          { m.roomRelease.Add(1) }
func (m *Metrics) participantRegistered() { m.participantReg.Add(1) }
func (m *Metrics) participantRemoved()    { m.participantRem.Add(1) }

func (m *Metrics) msgSent()        { m.addIf(&m.msgSentC) }
func (m *Metrics) msgReceived()    { m.addIf(&m.msgReceivedC) }
func (m *Metrics) msgFailed()      { m.addIf(&m.msgFailedC) }
func (m *Metrics) msgRetried()     { m.addIf(&m.msgRetriedC) }
func (m *Metrics) msgDuplicated()  { m.addIf(&m.msgDuplicatedC) }
func (m *Metrics) msgRejected()    { m.addIf(&m.msgRejectedC) }
func (m *Metrics) msgInvalid()     { m.addIf(&m.msgInvalidC) }
func (m *Metrics) msgTimeout()     { m.addIf(&m.msgTimeoutC) }
func (m *Metrics) transportError() { m.addIf(&m.transportErrC) }
func (m *Metrics) routeLocal()     { m.addIf(&m.routeLocalC) }
func (m *Metrics) routeRemote()    { m.addIf(&m.routeRemoteC) }
func (m *Metrics) routeNotFound()  { m.addIf(&m.routeNotFoundC) }

func (m *Metrics) addIf(c *atomic.Int64) {
	if m == nil {
		return
	}
	c.Add(1)
}

func (m *Metrics) observeTransportLatency(d time.Duration) {
	if m == nil {
		return
	}
	m.transportLatNs.Add(int64(d))
	m.transportLatN.Add(1)
}

func (m *Metrics) mediaSessionCreated() { m.addIf(&m.mediaSessCreated); m.mediaSessActive.Add(1) }
func (m *Metrics) mediaSessionClosed()  { m.addIf(&m.mediaSessClosed); m.mediaSessActive.Add(-1) }
func (m *Metrics) mediaSessionFailed()  { m.addIf(&m.mediaSessFailed) }
func (m *Metrics) mediaRTPSent()        { m.addIf(&m.mediaRTPSentC) }
func (m *Metrics) mediaRTPReceived()    { m.addIf(&m.mediaRTPRecvC) }
func (m *Metrics) mediaRTPDropped()     { m.addIf(&m.mediaRTPDropC) }
func (m *Metrics) mediaRTPInvalid()     { m.addIf(&m.mediaRTPInvalidC) }
func (m *Metrics) mediaRTCPSent()       { m.addIf(&m.mediaRTCPSentC) }
func (m *Metrics) mediaRTCPReceived()   { m.addIf(&m.mediaRTCPRecvC) }
func (m *Metrics) mediaRTCPDropped()    { m.addIf(&m.mediaRTCPDropC) }
func (m *Metrics) mediaOversized()      { m.addIf(&m.mediaOversizedC) }
func (m *Metrics) mediaAuthFailed()     { m.addIf(&m.mediaAuthFailedC) }
func (m *Metrics) mediaRemoteTrackAdd() { m.addIf(&m.mediaRemoteTracks) }
func (m *Metrics) mediaRemoteTrackDel() {
	if m != nil {
		m.mediaRemoteTracks.Add(-1)
	}
}

func (m *Metrics) roomSelected(reason string) {
	m.addIf(&m.roomSelections)
	switch reason {
	case "local", "assigned":
		m.addIf(&m.selLocal)
	case "least_loaded":
		m.addIf(&m.selLeastLoaded)
	case "capacity":
		m.addIf(&m.selCapacity)
	default:
		m.addIf(&m.selFallback)
	}
}
func (m *Metrics) roomSelectionFailed() { m.addIf(&m.roomSelectionFails) }
func (m *Metrics) roomClaimRouted()     { m.addIf(&m.roomClaimsRouted) }
func (m *Metrics) roomClaimRetry()      { m.addIf(&m.roomClaimRetries) }
func (m *Metrics) loadReported()        { m.addIf(&m.loadReports) }
func (m *Metrics) loadStale()           { m.addIf(&m.loadStaleC) }

func (m *Metrics) transportLatencyAvgMs() float64 {
	n := m.transportLatN.Load()
	if n == 0 {
		return 0
	}
	return float64(m.transportLatNs.Load()) / float64(n) / 1e6
}

// Snapshot is the JSON shape under "cluster" in /metrics.
type Snapshot struct {
	Enabled bool   `json:"enabled"`
	NodeID  string `json:"nodeId"`
	State   string `json:"state"`
	Backend string `json:"backend"`

	Nodes       int `json:"nodes"`
	NodesActive int `json:"nodesActive"`
	NodesStale  int `json:"nodesStale"`

	Rooms       int `json:"rooms"`
	RoomsLocal  int `json:"roomsLocal"`
	RoomsRemote int `json:"roomsRemote"`

	OwnershipClaimed  int64 `json:"ownershipClaimed"`
	OwnershipConflict int64 `json:"ownershipConflict"`
	RoomRedirects     int64 `json:"roomRedirects"`

	InternalRequests       int64 `json:"internalRequests"`
	InternalRequestsFailed int64 `json:"internalRequestsFailed"`

	Participants int `json:"participantsLocated"`

	RedisConnected     bool  `json:"redisConnected"`
	RedisErrors        int64 `json:"redisErrors"`
	RedisReconnects    int64 `json:"redisReconnects"`
	RedisUnavailable   int64 `json:"redisUnavailable"`
	RoomClaimFailed    int64 `json:"roomClaimFailed"`
	RoomReleased       int64 `json:"roomReleased"`
	ParticipantRegs    int64 `json:"participantRegistrations"`
	ParticipantRemoves int64 `json:"participantRemovals"`

	Messages       MessageMetrics `json:"messages"`
	Routing        RoutingMetrics `json:"routing"`
	TransportErrs  int64          `json:"transportErrors"`
	TransportLatMs float64        `json:"transportLatencyMsAvg"`

	Media MediaMetrics `json:"media"`

	Scheduler RoutingSchedMetrics `json:"scheduler"`
	Load      LoadMetrics         `json:"load"`

	Recovery RecoveryMetrics `json:"recovery"`
}

// RecoveryMetrics is the cluster.recovery.* / cluster.nodes.* block.
type RecoveryMetrics struct {
	NodesDetectedStale  int64 `json:"nodesDetectedStale"`
	NodesRecovered      int64 `json:"nodesRecovered"`
	NodesRecoveryFailed int64 `json:"nodesRecoveryFailed"`
	Started             int64 `json:"started"`
	Completed           int64 `json:"completed"`
	Failed              int64 `json:"failed"`
	Rooms               int64 `json:"rooms"`
	RoomsRecovered      int64 `json:"roomsRecovered"`
	RoomsSkipped        int64 `json:"roomsSkipped"`
	Conflicts           int64 `json:"conflicts"`
	RoutingStaleNode    int64 `json:"routingStaleNode"`
	RoutingUnavailNode  int64 `json:"routingUnavailableNode"`
}

// RoutingSchedMetrics is the cluster.routing.* block.
type RoutingSchedMetrics struct {
	RoomSelections        int64 `json:"roomSelections"`
	RoomSelectionFailures int64 `json:"roomSelectionFailures"`
	RoomClaimsRouted      int64 `json:"roomClaims"`
	RoomClaimRetries      int64 `json:"roomClaimRetries"`
	SelectLocal           int64 `json:"selectLocal"`
	SelectLeastLoaded     int64 `json:"selectLeastLoaded"`
	SelectCapacity        int64 `json:"selectCapacity"`
	SelectFallback        int64 `json:"selectFallback"`
}

// LoadMetrics is the cluster.load.* block.
type LoadMetrics struct {
	Reports int64 `json:"reports"`
	Stale   int64 `json:"stale"`
}

// MediaMetrics is the cluster.media.* block.
type MediaMetrics struct {
	Enabled          bool   `json:"enabled"`
	LocalAddr        string `json:"localAddr,omitempty"`
	SessionsActive   int64  `json:"sessionsActive"`
	SessionsCreated  int64  `json:"sessionsCreated"`
	SessionsClosed   int64  `json:"sessionsClosed"`
	SessionsFailed   int64  `json:"sessionsFailed"`
	RemoteTracks     int64  `json:"remoteTracks"`
	RTPSent          int64  `json:"rtpSent"`
	RTPReceived      int64  `json:"rtpReceived"`
	RTPDropped       int64  `json:"rtpDropped"`
	RTPInvalid       int64  `json:"rtpInvalid"`
	RTCPSent         int64  `json:"rtcpSent"`
	RTCPReceived     int64  `json:"rtcpReceived"`
	RTCPDropped      int64  `json:"rtcpDropped"`
	OversizedPackets int64  `json:"oversizedPackets"`
	AuthFailures     int64  `json:"authFailures"`
}

// MessageMetrics is the cluster.messages.* block.
type MessageMetrics struct {
	Sent       int64 `json:"sent"`
	Received   int64 `json:"received"`
	Failed     int64 `json:"failed"`
	Retried    int64 `json:"retried"`
	Duplicated int64 `json:"duplicated"`
	Rejected   int64 `json:"rejected"`
	Invalid    int64 `json:"invalid"`
	Timeout    int64 `json:"timeout"`
}

// RoutingMetrics is the cluster.routing.* block.
type RoutingMetrics struct {
	Local    int64 `json:"local"`
	Remote   int64 `json:"remote"`
	NotFound int64 `json:"notFound"`
}
