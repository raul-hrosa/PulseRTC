"use strict";

// PulseRTC — Sprint 3: WebRTC statistics collection layer.
//
// Wraps RTCPeerConnection.getStats() and turns the raw RTCStatsReport into a
// flat snapshot that is easy to render. Cumulative counters (bytes) are turned
// into rates by comparing two consecutive snapshots.
//
// Bitrate formula (documented in docs/networking/webrtc-stats.md):
//
//     bitrate_bps = (bytes_now - bytes_prev) * 8 / (t_now - t_prev)   [seconds]
//
// getStats() timestamps are in milliseconds (DOMHighResTimeStamp).

function pick(obj, keys) {
  const out = {};
  for (const k of keys) if (obj[k] !== undefined) out[k] = obj[k];
  return out;
}

// Parse one RTCStatsReport into { video, audio, connection } with raw values.
function parseReport(report) {
  const snap = {
    timestamp: performance.now(),
    video: { in: null, out: null },
    audio: { in: null, out: null },
    connection: { pair: null, local: null, remote: null, transport: null },
  };

  const byId = new Map();
  report.forEach((s) => byId.set(s.id, s));

  report.forEach((s) => {
    switch (s.type) {
      case "inbound-rtp":
        snap[s.kind].in = pick(s, [
          "timestamp", "packetsReceived", "packetsLost", "bytesReceived",
          "jitter", "framesReceived", "framesDropped", "framesPerSecond",
          "frameWidth", "frameHeight", "framesDecoded",
        ]);
        break;
      case "outbound-rtp":
        snap[s.kind].out = pick(s, [
          "timestamp", "packetsSent", "bytesSent", "framesSent",
          "framesPerSecond", "frameWidth", "frameHeight", "framesEncoded",
        ]);
        break;
      case "candidate-pair":
        // "selected" is Firefox; nominated+succeeded is Chrome's signal.
        if (s.selected || (s.nominated && s.state === "succeeded")) {
          snap.connection.pair = pick(s, [
            "currentRoundTripTime", "availableOutgoingBitrate",
            "availableIncomingBitrate", "state", "bytesSent", "bytesReceived",
          ]);
          snap.connection.local = byId.get(s.localCandidateId) || null;
          snap.connection.remote = byId.get(s.remoteCandidateId) || null;
        }
        break;
      case "transport":
        snap.connection.transport = pick(s, ["dtlsState", "iceState", "selectedCandidatePairId"]);
        break;
    }
  });

  // Chrome exposes the selected pair via transport.selectedCandidatePairId.
  if (!snap.connection.pair && snap.connection.transport?.selectedCandidatePairId) {
    const s = byId.get(snap.connection.transport.selectedCandidatePairId);
    if (s) {
      snap.connection.pair = pick(s, [
        "currentRoundTripTime", "availableOutgoingBitrate",
        "availableIncomingBitrate", "state", "bytesSent", "bytesReceived",
      ]);
      snap.connection.local = byId.get(s.localCandidateId) || null;
      snap.connection.remote = byId.get(s.remoteCandidateId) || null;
    }
  }

  return snap;
}

// bitrate in bits/s between two counter samples, or null if not computable.
function rate(nowBytes, prevBytes, dtSeconds) {
  if (nowBytes === undefined || prevBytes === undefined || !(dtSeconds > 0)) return null;
  return Math.max(0, ((nowBytes - prevBytes) * 8) / dtSeconds);
}

function derive(cur, prev) {
  const out = { video: {}, audio: {} };
  if (!prev) return out;
  const dt = (cur.timestamp - prev.timestamp) / 1000;

  for (const kind of ["video", "audio"]) {
    const ci = cur[kind].in, pi = prev[kind].in;
    const co = cur[kind].out, po = prev[kind].out;
    out[kind] = {
      recvBitrate: ci && pi ? rate(ci.bytesReceived, pi.bytesReceived, dt) : null,
      sendBitrate: co && po ? rate(co.bytesSent, po.bytesSent, dt) : null,
    };
  }
  return out;
}

function createStatsCollector(pc, render, intervalMs = 1000) {
  let prev = null;
  let timer = null;

  async function tick() {
    if (!pc || pc.connectionState === "closed") return;
    try {
      const report = await pc.getStats();
      const snap = parseReport(report);
      render(snap, derive(snap, prev));
      prev = snap;
    } catch (err) {
      console.warn("getStats failed", err);
    }
  }

  return {
    start() {
      if (timer) return;
      tick();
      timer = setInterval(tick, intervalMs);
    },
    stop() {
      clearInterval(timer);
      timer = null;
      prev = null;
    },
  };
}

window.PulseStats = { createStatsCollector };
