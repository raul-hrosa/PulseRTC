// Full-featured demo/test client for @pulsertc/client.
//
// Everything below goes through the SDK's public surface only — no direct
// WebSocket, RTCPeerConnection or getUserMedia. Compare with the hand-rolled
// client at "/" (web/app.js) which drives the raw /ws protocol.

import { Room, RoomEvent, ConnectionState } from "/sdk/index.js";

const $ = (id) => document.getElementById(id);
const short = (id) => (id.length > 12 ? id.slice(0, 12) + "…" : id);

const room = new Room();
window.room = room; // handy for poking the SDK from the devtools console

// identity -> { figure, video, stream, badges }
const tiles = new Map();
// identity -> ParticipantQuality
const qoe = new Map();

let micEnabled = true;
let camEnabled = true;

// --------------------------------------------------------------------------
// logging
// --------------------------------------------------------------------------

function log(msg) {
  const el = $("log");
  el.textContent += `[${new Date().toLocaleTimeString()}] ${msg}\n`;
  el.scrollTop = el.scrollHeight;
}

// --------------------------------------------------------------------------
// connection lifecycle
// --------------------------------------------------------------------------

room.on(RoomEvent.ConnectionStateChanged, (state) => {
  const badge = $("conn");
  badge.textContent = state;
  badge.className = `badge state-${state}`;
  log(`connection: ${state}`);

  if (state === ConnectionState.Reconnecting) {
    // The SDK rebuilds the media session from scratch; drop the remote tiles
    // and let them re-appear from the post-reconnect events.
    for (const id of [...tiles.keys()]) removeTile(id);
    renderParticipants();
  }
  if (state === ConnectionState.Connected) {
    showLocalPreview();
    renderParticipants();
  }
});

room.on(RoomEvent.Disconnected, (reason) => {
  log(`disconnected (${reason})`);
  teardownUI();
});

room.on(RoomEvent.Error, (err) => log(`error: ${err.name} — ${err.message}`));
room.on(RoomEvent.SubscribeDenied, (info) =>
  log(`subscribe denied${info.publicationId ? ` (${info.publicationId})` : ""}`),
);

// --------------------------------------------------------------------------
// participants
// --------------------------------------------------------------------------

room.on(RoomEvent.ParticipantConnected, (p) => {
  log(`participant joined: ${p.name ?? p.identity}`);
  renderParticipants();
});

room.on(RoomEvent.ParticipantDisconnected, (p) => {
  log(`participant left: ${p.name ?? p.identity}`);
  removeTile(p.identity);
  qoe.delete(p.identity);
  renderParticipants();
  renderQoe();
});

function renderParticipants() {
  const ul = $("participantList");
  ul.innerHTML = "";
  const rows = [];
  const lp = room.localParticipant;
  if (lp.identity) rows.push({ label: `${lp.name ?? short(lp.identity)} (you)`, pubs: lp.publications.size });
  for (const p of room.remoteParticipants.values()) {
    rows.push({ label: p.name ?? short(p.identity), pubs: p.publications.size });
  }
  for (const r of rows) {
    const li = document.createElement("li");
    li.textContent = `${r.label} — ${r.pubs} track${r.pubs === 1 ? "" : "s"}`;
    ul.append(li);
  }
  $("participantCount").textContent = String(rows.length);
}

// --------------------------------------------------------------------------
// remote media tiles
// --------------------------------------------------------------------------

room.on(RoomEvent.TrackSubscribed, (track, pub, p) => {
  log(`subscribed: ${p.name ?? short(p.identity)} ${pub.source ?? pub.kind}`);
  const tile = ensureTile(p.identity, p.name ?? short(p.identity));
  if (pub.source === "screen") {
    tile.screenStream.addTrack(track);
    tile.screenFigure.hidden = false;
    tile.screenVideo.play().catch(() => {});
  } else {
    tile.stream.addTrack(track);
    tile.video.play().catch(() => {});
  }
  updateBadges(p);
  renderParticipants();
});

room.on(RoomEvent.TrackUnsubscribed, (pub, p) => {
  log(`unsubscribed: ${p.name ?? short(p.identity)} ${pub.source ?? pub.kind}`);
  const tile = tiles.get(p.identity);
  if (tile) {
    const target = pub.source === "screen" ? tile.screenStream : tile.stream;
    for (const t of target.getTracks()) {
      if (t.kind === pub.kind) target.removeTrack(t);
    }
    if (pub.source === "screen" && tile.screenStream.getTracks().length === 0) {
      tile.screenFigure.hidden = true;
    }
  }
  updateBadges(p);
  renderParticipants();
});

room.on(RoomEvent.TrackMuted, (pub, p) => {
  log(`${p.name ?? short(p.identity)} ${pub.kind} ${pub.muted ? "muted" : "unmuted"}`);
  updateBadges(p);
});

function ensureTile(identity, label) {
  let tile = tiles.get(identity);
  if (tile) return tile;

  const figure = document.createElement("figure");
  const video = document.createElement("video");
  video.autoplay = video.playsInline = true;
  const stream = new MediaStream();
  video.srcObject = stream;
  const badges = document.createElement("figcaption");
  badges.dataset.name = label;
  figure.append(video, badges);
  $("tiles").append(figure);

  // A dedicated figure for this participant's screen share, hidden until one
  // arrives. `.screen-tile` makes it span the grid — screen shares are the
  // focus, like Meet.
  const screenFigure = document.createElement("figure");
  screenFigure.className = "screen-tile";
  screenFigure.hidden = true;
  const screenVideo = document.createElement("video");
  screenVideo.autoplay = screenVideo.playsInline = true;
  const screenStream = new MediaStream();
  screenVideo.srcObject = screenStream;
  const screenCap = document.createElement("figcaption");
  screenCap.textContent = `${label} · screen`;
  screenFigure.append(screenVideo, screenCap);
  $("tiles").append(screenFigure);

  tile = { figure, video, stream, badges, screenFigure, screenVideo, screenStream };
  tiles.set(identity, tile);
  return tile;
}

function removeTile(identity) {
  const tile = tiles.get(identity);
  if (!tile) return;
  tile.video.srcObject = null;
  tile.figure.remove();
  tile.screenVideo.srcObject = null;
  tile.screenFigure.remove();
  tiles.delete(identity);
}

function updateBadges(p) {
  const tile = tiles.get(p.identity);
  if (!tile) return;
  let audio, video, screen;
  for (const pub of p.publications.values()) {
    if (pub.kind === "audio") audio = pub;
    else if (pub.source === "screen") screen = pub;
    else if (pub.kind === "video") video = pub;
  }
  const parts = [tile.badges.dataset.name];
  parts.push(!audio ? "· no mic" : audio.muted ? "· 🔇" : "· 🎤");
  parts.push(!video ? "· no cam" : video.muted ? "· 📷🚫" : "· 📹");
  if (screen) parts.push("· 🖥️");
  tile.badges.textContent = parts.join(" ");
}

// --------------------------------------------------------------------------
// quality (QoE)
// --------------------------------------------------------------------------

room.on(RoomEvent.QualityChanged, (quality, participant) => {
  qoe.set(participant.identity, quality);
  const where = [quality.direction, quality.mediaType].filter(Boolean).join(" ") || "overall";
  log(`quality: ${participant.name ?? short(participant.identity)} ${where} → ${quality.status}` +
    (quality.reason ? ` (${quality.reason})` : ""));
  renderQoe();
});

function renderQoe() {
  const body = $("qoeBody");
  if (qoe.size === 0) {
    body.innerHTML = `<tr><td colspan="4" class="muted">no reports yet</td></tr>`;
    return;
  }
  body.innerHTML = "";
  for (const [identity, q] of qoe) {
    const tr = document.createElement("tr");
    const where = [q.direction, q.mediaType].filter(Boolean).join(" ") || "overall";
    tr.innerHTML =
      `<td>${short(identity)}</td>` +
      `<td class="q-${q.status}">${q.status}</td>` +
      `<td>${where}</td>` +
      `<td>${q.reason ?? "—"}</td>`;
    body.append(tr);
  }
}

// --------------------------------------------------------------------------
// local preview + controls
// --------------------------------------------------------------------------

function showLocalPreview() {
  const pubs = [...room.localParticipant.publications.values()];
  const camTracks = pubs.filter((p) => p.source !== "screen").map((p) => p.track).filter(Boolean);
  $("localVideo").srcObject = camTracks.length ? new MediaStream(camTracks) : null;
  $("localId").textContent = room.localParticipant.identity
    ? `(${short(room.localParticipant.identity)})`
    : "";
  showLocalScreen();
  renderLocalBadges();
}

function showLocalScreen() {
  const scr = [...room.localParticipant.publications.values()]
    .find((p) => p.source === "screen" && p.kind === "video")?.track;
  // Only the video — the local capture audio is not played back to yourself.
  $("localScreen").srcObject = scr ? new MediaStream([scr]) : null;
  $("localScreenFigure").hidden = !scr;
}

function renderLocalBadges() {
  const sharing = room.localParticipant.isScreenSharing;
  $("localBadges").textContent =
    `${micEnabled ? "🎤" : "🔇"}  ${camEnabled ? "📹" : "📷🚫"}${sharing ? "  🖥️" : ""}`;
  $("mic").textContent = micEnabled ? "🎤 Mute" : "🎤 Unmute";
  $("cam").textContent = camEnabled ? "📹 Camera off" : "📹 Camera on";
  $("screen").textContent = sharing ? "🛑 Stop screen" : "🖥️ Share screen";
}

$("screen").onclick = async () => {
  const lp = room.localParticipant;
  try {
    if (lp.isScreenSharing) await lp.stopScreenShare();
    else await lp.startScreenShare();
  } catch (e) {
    log(`screen share: ${e.message}`);
  }
  showLocalScreen();
  renderLocalBadges();
  renderParticipants();
};

$("mic").onclick = async () => {
  micEnabled = !micEnabled;
  renderLocalBadges();
  try { await room.localParticipant.setMicrophoneEnabled(micEnabled); }
  catch (e) { log(`setMicrophoneEnabled: ${e.message}`); }
};

$("cam").onclick = async () => {
  camEnabled = !camEnabled;
  renderLocalBadges();
  try { await room.localParticipant.setCameraEnabled(camEnabled); }
  catch (e) { log(`setCameraEnabled: ${e.message}`); }
};

// --------------------------------------------------------------------------
// join / leave
// --------------------------------------------------------------------------

$("join").onclick = async () => {
  $("join").disabled = true;
  const url = $("url").value.trim();
  const roomId = $("roomId").value.trim();
  const token = $("token").value.trim();
  if (!url || !roomId) { $("join").disabled = false; return alert("URL and Room ID are required"); }

  try {
    await room.connect(url, token, roomId);
    micEnabled = camEnabled = true;
    await room.localParticipant.enableCameraAndMicrophone();
    showLocalPreview();
    $("stage").hidden = false;
    $("leave").disabled = false;
    log("joined and publishing");
  } catch (e) {
    log(`join failed: ${e.name} — ${e.message}${e.code ? ` [${e.code}]` : ""}`);
    $("join").disabled = false;
  }
};

$("leave").onclick = () => room.disconnect();

function teardownUI() {
  for (const id of [...tiles.keys()]) removeTile(id);
  qoe.clear();
  renderQoe();
  renderParticipants();
  $("localVideo").srcObject = null;
  $("stage").hidden = true;
  $("join").disabled = false;
  $("leave").disabled = true;
}
