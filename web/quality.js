"use strict";

// PulseRTC — Sprint 6: QoE client.
//
// Two jobs, both thin:
//   1. reporting — read getStats() from the SFU PeerConnection once per second,
//      normalise it to cumulative "samples", and push a quality_report. All the
//      analysis (scores, status, hysteresis, events) is done by the Go engine.
//   2. dashboard — poll GET /quality?room= and render the per-participant QoE
//      view; surface quality_* events in the log.

(function () {
  const REPORT_MS = 1000;
  const DASH_MS = 2000;

  let reportTimer = null;
  let dashTimer = null;

  const BADGE = { GOOD: "🟢", WARNING: "🟡", POOR: "🔴", UNKNOWN: "⚪" };
  const s2ms = (v) => (typeof v === "number" ? v * 1000 : undefined);

  // ---- reporting ---------------------------------------------------------

  async function collect(pc, localPubs) {
    const report = await pc.getStats();
    const byId = new Map();
    report.forEach((s) => byId.set(s.id, s));

    const samples = [];
    const now = Math.round(performance.now());

    report.forEach((s) => {
      if (s.type === "outbound-rtp" && !s.isRemote) {
        const remote = s.remoteId ? byId.get(s.remoteId) : null;
        samples.push({
          publicationId: localPubs()[s.kind] || "",
          kind: s.kind,
          direction: "outbound",
          tMs: now,
          packetsSent: s.packetsSent,
          bytesSent: s.bytesSent,
          // loss / rtt / jitter as seen by the SFU come from the RR report
          packetsReceived: remote ? num(s.packetsSent) - num(remote.packetsLost) : undefined,
          packetsLost: remote ? remote.packetsLost : undefined,
          rttMs: remote ? s2ms(remote.roundTripTime) : undefined,
          jitterMs: remote ? s2ms(remote.jitter) : undefined,
          enabled: senderEnabled(pc, s.kind),
        });
      } else if (s.type === "inbound-rtp") {
        samples.push({
          publicationId: s.trackIdentifier || "",
          kind: s.kind,
          direction: "inbound",
          tMs: now,
          packetsReceived: s.packetsReceived,
          packetsLost: s.packetsLost,
          bytesReceived: s.bytesReceived,
          framesDecoded: s.framesDecoded,
          framesDropped: s.framesDropped,
          jitterMs: s2ms(s.jitter),
          fps: s.framesPerSecond,
          width: s.frameWidth,
          height: s.frameHeight,
        });
      } else if (s.type === "candidate-pair" && (s.selected || (s.nominated && s.state === "succeeded"))) {
        samples.push({
          kind: "connection",
          tMs: now,
          rttMs: s2ms(s.currentRoundTripTime),
          connState: pc.connectionState,
          iceState: pc.iceConnectionState,
        });
      }
    });

    // Guarantee at least a connection sample so the engine always has a verdict.
    if (!samples.some((x) => x.kind === "connection")) {
      samples.push({
        kind: "connection", tMs: now,
        connState: pc.connectionState, iceState: pc.iceConnectionState,
      });
    }
    return samples;
  }

  const num = (v) => (typeof v === "number" ? v : 0);

  function senderEnabled(pc, kind) {
    const snd = pc.getSenders().find((x) => x.track && x.track.kind === kind);
    return snd ? snd.track.enabled : undefined;
  }

  function startReporting(pc, opts) {
    if (reportTimer) return;
    const tick = async () => {
      try {
        const samples = await collect(pc, opts.localPubs);
        if (samples.length) opts.send({ type: "quality_report", samples });
      } catch (e) { /* transient */ }
    };
    tick();
    reportTimer = setInterval(tick, REPORT_MS);
  }

  function stopReporting() {
    if (reportTimer) { clearInterval(reportTimer); reportTimer = null; }
  }

  // ---- dashboard --------------------------------------------------------

  function startDashboard(getRoomId, getMyId) {
    if (dashTimer) return;
    const tick = async () => {
      try {
        const res = await fetch(`/quality?room=${encodeURIComponent(getRoomId())}`);
        if (res.ok) render((await res.json()).participants || [], getMyId());
      } catch (e) { /* transient */ }
    };
    tick();
    dashTimer = setInterval(tick, DASH_MS);
  }

  function stopDashboard() {
    if (dashTimer) { clearInterval(dashTimer); dashTimer = null; }
    const el = document.getElementById("qoePanel");
    if (el) el.innerHTML = "";
  }

  function streamLine(s) {
    const m = s.metrics || {};
    const bits = [];
    if (m.packetLossPct !== undefined) bits.push(`loss ${m.packetLossPct}%`);
    if (m.jitterMs !== undefined) bits.push(`jitter ${m.jitterMs}ms`);
    if (m.rttMs !== undefined) bits.push(`rtt ${m.rttMs}ms`);
    if (m.bitrateBps !== undefined) bits.push(`${Math.round(m.bitrateBps / 1000)}kbps`);
    if (m.fps !== undefined) bits.push(`${Math.round(m.fps)}fps`);
    if (m.frameDropPct !== undefined) bits.push(`drop ${m.frameDropPct}%`);
    const label = s.kind === "connection" ? "connection"
      : `${s.direction === "outbound" ? "↑" : "↓"} ${s.kind}`;
    const probs = (s.problems || []).length ? ` — ${s.problems.join(", ")}` : "";
    return `<div class="qrow">${BADGE[s.status] || "⚪"} <b>${label}</b> `
      + `<span class="qscore">${s.score}</span> `
      + `<span class="qmet">${bits.join("  ")}</span>`
      + `<span class="qprob">${probs}</span></div>`;
  }

  function timeline(points) {
    if (!points || !points.length) return "";
    const blocks = points.slice(-40).map((p) => {
      const c = { GOOD: "#2e7d32", WARNING: "#f9a825", POOR: "#c62828", UNKNOWN: "#bbb" }[p.status] || "#bbb";
      return `<span title="${p.status} @ ${new Date(p.at).toLocaleTimeString()}" style="background:${c}"></span>`;
    }).join("");
    return `<div class="qtl">${blocks}</div>`;
  }

  function render(participants, myId) {
    const el = document.getElementById("qoePanel");
    if (!el) return;
    if (!participants.length) { el.textContent = "(sem dados de qualidade ainda)"; return; }

    el.innerHTML = participants.map((p) => {
      const streams = [p.connection, ...(p.outbound || []), ...(p.inbound || [])]
        .filter((s) => s && s.status !== undefined);
      return `<div class="qcard">
        <div class="qhead">${BADGE[p.overall] || "⚪"} <b>${p.participantId.slice(0, 8)}`
        + `${p.participantId === myId ? " (você)" : ""}</b> — overall ${p.overall}</div>
        ${streams.map(streamLine).join("")}
        ${timeline(p.timeline)}
      </div>`;
    }).join("");
  }

  // liveStatus tracks the last consolidated status reported for each participant
  // (QoE propagation). The quality belongs to msg.participantId — the OBSERVED
  // participant — never to whoever received the event.
  const liveStatus = new Map();

  function onEvent(msg) {
    if (msg.participantId && msg.status) liveStatus.set(msg.participantId, msg.status);
    const who = msg.participantId ? msg.participantId.slice(0, 8) : "?";
    const where = [msg.direction, msg.mediaType].filter(Boolean).join(" ");
    const reason = msg.reason ? ` (${msg.reason})` : "";
    const line = `${BADGE[msg.status] || ""} ${msg.type} ${who}: ${where} ${msg.from}→${msg.status}${reason}`;
    if (window.pulseLog) window.pulseLog(line); else console.log(line);
  }

  window.PulseQoE = { startReporting, stopReporting, startDashboard, stopDashboard, onEvent, liveStatus };
})();
