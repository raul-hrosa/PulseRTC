"use strict";

// PulseRTC — Sprint 5: multi-track publish/subscribe SFU client.
//
// One RTCPeerConnection with the SFU. The browser publishes its mic + camera as
// two independent Publications and receives the Publications of the other
// participants. Negotiation is "perfect negotiation" (MDN): both the browser
// and the SFU may send an offer; the browser is the polite peer and rolls back
// on a collision.

// Fallback until the server sends its ICE config in the `welcome` message
// (the server is the source of truth, including any TURN credentials).
const DEFAULT_ICE_SERVERS = [{ urls: "stun:stun.l.google.com:19302" }];
let ICE_SERVERS = DEFAULT_ICE_SERVERS;
const $ = (id) => document.getElementById(id);

let ws = null;
let pc = null;
let localStream = null;
let myId = null;
let roomId = null;
let statsTimer = null;

// Sprint 14B — session recovery / reconnection.
let wantConnected = false;          // true between joinRoom() and leaveRoom()
let sessionInfo = null;             // { sessionId, generation } from room_joined
let reconnectHints = null;          // backoff hints from the server
let reconnectAttempt = 0;
let reconnectTimer = null;

let makingOffer = false;
let ignoreOffer = false;
let remoteDescSet = false;
const pendingCandidates = [];

const senders = { audio: null, video: null };
const localPubs = { audio: null, video: null, screen: null, screenAudio: null };
// Screen share is a SEPARATE lane from the camera: its own getDisplayMedia
// stream / sender(s) / publication(s), so camera and screen coexist. When the
// capture carries audio it is published too (screenAudio).
let localScreen = null; // { stream, video, audio, videoSender, audioSender }
const participants = new Set();
// participantId -> display name (from the token "name" claim). Populated from
// welcome / room_joined / participant_joined.
const participantNames = new Map();
let myName = null;

function nameFor(id) {
  return participantNames.get(id) || id.slice(0, 8);
}

// publicationId -> { participantId, kind, source, muted, subscribed }
const pubs = new Map();
// streamId(publisherId) -> { figure, video }
const tiles = new Map();

function log(msg, obj) {
  const line = `[${new Date().toLocaleTimeString()}] ${msg}` +
    (obj !== undefined ? ` ${typeof obj === "string" ? obj : JSON.stringify(obj)}` : "");
  const el = $("log");
  el.textContent += line + "\n";
  el.scrollTop = el.scrollHeight;
}
const setStatus = (s) => { $("status").textContent = s; };
const send = (m) => ws.send(JSON.stringify(m));
window.pulseLog = (line) => log(line);

// ---------------------------------------------------------------------------
// Join / leave
// ---------------------------------------------------------------------------

async function joinRoom() {
  roomId = $("roomId").value.trim();
  if (!roomId) { alert("Room ID é obrigatório"); return; }

  $("joinBtn").disabled = true;
  if (!(await getLocalMedia())) { $("joinBtn").disabled = false; return; }

  wantConnected = true;
  reconnectAttempt = 0;
  sessionInfo = null;
  openSocket(false);

  $("leaveBtn").disabled = false;
  $("mediaControls").hidden = false;
}

// openSocket opens a WebSocket and (on open) sends a join. When resume is true
// and we hold a prior sessionInfo, the join carries session.resume so the server
// reconnects the logical session instead of creating a fresh one (Sprint 14B).
function openSocket(resume) {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  // Sprint 8: the browser cannot set an Authorization header on a WebSocket, so
  // the token rides along as the "pulsertc.token.*" subprotocol.
  const token = ($("token").value || "").trim();
  const url = `${proto}://${location.host}/ws`;
  ws = token
    ? new WebSocket(url, ["pulsertc", `pulsertc.token.${token}`])
    : new WebSocket(url);
  ws.onopen = () => {
    log("websocket conectado");
    const j = { type: "join", roomId };
    if (resume && sessionInfo) {
      j.resume = { sessionId: sessionInfo.sessionId, generation: sessionInfo.generation };
      log(`retomando sessão ${sessionInfo.sessionId} (geração ${sessionInfo.generation})`);
    }
    send(j);
  };
  ws.onclose = () => onSocketClose();
  ws.onerror = () => log("erro no websocket");
  ws.onmessage = (ev) => handleMessage(JSON.parse(ev.data));
}

function onSocketClose() {
  if (!wantConnected) { log("websocket fechado"); teardown(); resetUI(); return; }
  // Unexpected drop — likely the SFU node failed. Keep the local camera/mic,
  // tear down just the dead PeerConnection, and reconnect with backoff (§8).
  log("conexão perdida — recuperando sessão");
  setStatus("recovering");
  teardownForReconnect();
  scheduleReconnect();
}

function scheduleReconnect() {
  const h = reconnectHints || { initialDelayMs: 500, maxDelayMs: 10000, jitter: 0.2 };
  const base = Math.min(h.initialDelayMs * 2 ** reconnectAttempt, h.maxDelayMs);
  const delay = base + Math.random() * base * (h.jitter || 0);
  reconnectAttempt++;
  log(`reconnect em ${Math.round(delay)}ms (tentativa ${reconnectAttempt})`);
  clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(() => { if (wantConnected) openSocket(true); }, delay);
}

function leaveRoom() {
  wantConnected = false;
  clearTimeout(reconnectTimer);
  if (ws) ws.close();
}

async function getLocalMedia() {
  try {
    localStream = await navigator.mediaDevices.getUserMedia({ audio: true, video: true });
    $("localVideo").srcObject = localStream;
    log("mídia local obtida (câmera + microfone)");
    return true;
  } catch (err) {
    log(`erro ao acessar mídia: ${err.name} — ${err.message}`);
    return false;
  }
}

// ---------------------------------------------------------------------------
// Signaling
// ---------------------------------------------------------------------------

async function handleMessage(msg) {
  switch (msg.type) {
    case "welcome":
      myId = msg.participantId;
      myName = msg.name || null;
      if (Array.isArray(msg.iceServers) && msg.iceServers.length) {
        ICE_SERVERS = msg.iceServers;
        log(`ICE servers do servidor: ${msg.iceServers.map((s) => s.urls).flat().join(", ")}`);
      }
      if (myName) participantNames.set(myId, myName);
      $("myId").textContent = myName ? `${myName} (${myId})` : myId;
      break;

    case "room_joined":
      log(`entrou na room ${msg.roomId}`, msg.participants.map((p) => p.participantId));
      // Sprint 14B: remember session id/generation + backoff hints for recovery.
      if (msg.sessionId) sessionInfo = { sessionId: msg.sessionId, generation: msg.generation || 0 };
      if (msg.reconnect) reconnectHints = msg.reconnect;
      if (msg.reconnected) {
        reconnectAttempt = 0;
        log(`sessão recuperada (geração ${msg.generation}, room gen ${msg.roomGeneration || 0})`);
      }
      if (msg.snapshot && msg.snapshot.participants) {
        const tracks = msg.snapshot.participants.reduce((n, p) => n + (p.tracks ? p.tracks.length : 0), 0);
        log(`room snapshot: ${msg.snapshot.participants.length} participantes, ${tracks} tracks`);
      }
      msg.participants.forEach((p) => {
        participants.add(p.participantId);
        if (p.name) participantNames.set(p.participantId, p.name);
      });
      (msg.snapshot?.participants || []).forEach((p) => {
        if (p.name) participantNames.set(p.participantId, p.name);
      });
      // QoE propagation: estado de qualidade atual dos que já estavam na room.
      (msg.quality || []).forEach((q) => PulseQoE.onEvent({
        type: "quality_changed", participantId: q.participantId,
        from: "UNKNOWN", status: q.status, reason: q.reason,
      }));
      renderParticipants();
      await startPublishing();
      startStatsPolling();
      PulseQoE.startReporting(pc, {
        send,
        localPubs: () => localPubs,
      });
      PulseQoE.startDashboard(() => roomId, () => myId);
      break;

    case "participant_joined":
      participants.add(msg.participantId);
      if (msg.name) participantNames.set(msg.participantId, msg.name);
      renderParticipants();
      log(`participante entrou: ${msg.name || msg.participantId}`);
      break;

    case "participant_left":
      participants.delete(msg.participantId);
      renderParticipants();
      removeTile(msg.participantId);
      log(`participante saiu: ${participantNames.get(msg.participantId) || msg.participantId}`);
      participantNames.delete(msg.participantId);
      break;

    // --- perfect negotiation --------------------------------------------
    case "sfu_offer":
      await onRemoteOffer(msg.payload.sdp);
      break;
    case "sfu_answer":
      if (pc.signalingState !== "have-local-offer") { log("answer inesperado, ignorado"); break; }
      await pc.setRemoteDescription({ type: "answer", sdp: msg.payload.sdp });
      remoteDescSet = true;
      await drainCandidates();
      break;
    case "sfu_ice_candidate":
      await addRemoteCandidate(msg.payload);
      break;

    // --- pub/sub -------------------------------------------------------
    case "publication_added": {
      if (msg.participantId === myId) {
        if (msg.source === "screen") localPubs[msg.kind === "audio" ? "screenAudio" : "screen"] = msg.publicationId;
        else localPubs[msg.kind] = msg.publicationId;
        break;
      }
      pubs.set(msg.publicationId, {
        participantId: msg.participantId, kind: msg.kind, source: msg.source || (msg.kind === "audio" ? "microphone" : "camera"),
        muted: msg.muted, subscribed: true,
      });
      renderPublications();
      updateTileBadges(msg.participantId);
      if (msg.source === "screen") attachRemoteScreen(msg.participantId);
      log(`publication_added: ${msg.participantId} ${msg.source || msg.kind}`);
      break;
    }
    case "publication_removed": {
      const gone = pubs.get(msg.publicationId);
      pubs.delete(msg.publicationId);
      renderPublications();
      updateTileBadges(msg.participantId);
      if (gone && gone.source === "screen") detachRemoteScreen(msg.participantId);
      log(`publication_removed: ${msg.publicationId}`);
      break;
    }
    case "publication_muted": {
      const p = pubs.get(msg.publicationId);
      if (p) { p.muted = msg.muted; renderPublications(); updateTileBadges(p.participantId); }
      log(`publication_muted: ${msg.publicationId} = ${msg.muted}`);
      break;
    }
    case "subscription_added": {
      const p = pubs.get(msg.publicationId);
      if (p) { p.subscribed = true; renderPublications(); }
      break;
    }
    case "subscription_removed": {
      const p = pubs.get(msg.publicationId);
      if (p) { p.subscribed = false; renderPublications(); }
      break;
    }

    case "quality_degraded":
    case "quality_recovered":
    case "quality_changed":
      PulseQoE.onEvent(msg);
      break;

    case "error":
      log(msg.code ? `erro de segurança [${msg.code}]` : "erro do servidor", msg.message);
      break;

    // --- session recovery (Sprint 14B) -------------------------------------
    case "session.reconnected":
      log(`sessão reconectada: ${msg.sessionId}`);
      setStatus("connected");
      break;
    case "session.stale":
      // Our prior session is unrecoverable — rejoin fresh on the next attempt.
      log(`sessão obsoleta (${msg.reason}) — reingressando do zero`);
      sessionInfo = null;
      break;
    case "session.replaced":
      log("sessão substituída por outra conexão — não reconectando");
      wantConnected = false;
      clearTimeout(reconnectTimer);
      break;
    case "session.recovery":
      log(`recuperação de sessão: ${msg.reason || "?"}`);
      break;
    case "room.snapshot":
      log(`room snapshot (gen ${msg.roomGeneration || 0})`);
      break;

    case "publish_denied":
      log("publicação negada: o token não concede PUBLISH");
      break;

    case "subscribe_denied":
      log("assinatura negada: o token não concede SUBSCRIBE");
      break;
  }
}

// ---------------------------------------------------------------------------
// WebRTC (browser <-> SFU), perfect negotiation
// ---------------------------------------------------------------------------

async function startPublishing() {
  pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

  // A screen share never survives a fresh PeerConnection (reconnect): drop it.
  if (localScreen) {
    localScreen.stream.getTracks().forEach((t) => t.stop());
    localScreen = null;
  }
  localPubs.screen = localPubs.screenAudio = null;
  $("localScreenFigure").hidden = true;
  $("localScreen").srcObject = null;

  senders.audio = pc.addTrack(localStream.getAudioTracks()[0], localStream);
  senders.video = pc.addTrack(localStream.getVideoTracks()[0], localStream);

  pc.onicecandidate = (ev) => {
    if (ev.candidate) send({ type: "sfu_ice_candidate", payload: ev.candidate.toJSON() });
  };

  pc.ontrack = (ev) => {
    const stream = ev.streams[0];
    if (!stream) return;
    addTile(stream); // stream.id === publisher participant id
    updateTileBadges(stream.id);
    // If this track belongs to a screen publication, (re)build the tile's
    // dedicated screen element from all of that publisher's screen tracks.
    if ([...pubs.values()].some((p) => p.participantId === stream.id && p.source === "screen")) {
      attachRemoteScreen(stream.id);
    }
    stream.onremovetrack = () => {
      if (stream.getTracks().length === 0) removeTile(stream.id);
    };
  };

  pc.onnegotiationneeded = async () => {
    try {
      makingOffer = true;
      await pc.setLocalDescription();
      send({ type: "sfu_offer", payload: { sdp: pc.localDescription.sdp } });
      log("sfu_offer enviado (browser)");
    } catch (err) {
      log("erro em negotiationneeded", err.message);
    } finally {
      makingOffer = false;
    }
  };

  pc.onconnectionstatechange = () => {
    setStatus(pc.connectionState);
    $("d-conn").textContent = pc.connectionState;
    log(`connectionState: ${pc.connectionState}`);
  };
  pc.oniceconnectionstatechange = () => { $("d-ice").textContent = pc.iceConnectionState; };

  renderLocalState();
}

async function onRemoteOffer(sdp) {
  const offerCollision = makingOffer || pc.signalingState !== "stable";
  ignoreOffer = offerCollision; // browser is polite: still accept, after rollback
  if (offerCollision) {
    await Promise.all([
      pc.setLocalDescription({ type: "rollback" }),
      pc.setRemoteDescription({ type: "offer", sdp }),
    ]);
  } else {
    await pc.setRemoteDescription({ type: "offer", sdp });
  }
  remoteDescSet = true;
  await drainCandidates();
  await pc.setLocalDescription(await pc.createAnswer());
  send({ type: "sfu_answer", payload: { sdp: pc.localDescription.sdp } });
  log("sfu_answer enviado (browser)");
}

async function addRemoteCandidate(init) {
  if (!pc || !remoteDescSet) { pendingCandidates.push(init); return; }
  try { await pc.addIceCandidate(init); }
  catch (err) { if (!ignoreOffer) log("erro ao adicionar ICE candidate", err.message); }
}

async function drainCandidates() {
  while (pendingCandidates.length) {
    try { await pc.addIceCandidate(pendingCandidates.shift()); }
    catch (err) { if (!ignoreOffer) log("erro ao adicionar ICE candidate pendente", err.message); }
  }
}

// ---------------------------------------------------------------------------
// Media controls
// ---------------------------------------------------------------------------

function toggleMute() {
  const track = localStream.getAudioTracks()[0];
  if (!track) return;
  track.enabled = !track.enabled;
  if (localPubs.audio) send({ type: "set_mute", publicationId: localPubs.audio, muted: !track.enabled });
  renderLocalState();
  log(track.enabled ? "microfone ativado" : "microfone mutado");
}

function toggleCamera() {
  const track = localStream.getVideoTracks()[0];
  if (!track) return;
  track.enabled = !track.enabled;
  if (localPubs.video) send({ type: "set_mute", publicationId: localPubs.video, muted: !track.enabled });
  renderLocalState();
  log(track.enabled ? "câmera ligada" : "câmera desligada (track desabilitada, publication mantida)");
}

// --- Screen share (separate lane from the camera) --------------------------

async function startScreenShare() {
  if (localScreen || !pc) return;
  let stream;
  try {
    // audio: true -> captura áudio da aba / tela (Chromium, checkbox "compartilhar áudio")
    stream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true });
  } catch (err) {
    log(`compartilhamento de tela cancelado: ${err.message}`);
    return;
  }
  const video = stream.getVideoTracks()[0];
  if (!video) { stream.getTracks().forEach((t) => t.stop()); return; }
  const audio = stream.getAudioTracks()[0]; // undefined em janela / sem "compartilhar áudio"
  video.addEventListener("ended", stopScreenShare); // "Stop sharing" da UI do navegador
  localScreen = { stream, video, audio, videoSender: null, audioSender: null };
  // Preview local primeiro — não depende do round-trip do SFU.
  $("localScreen").srcObject = new MediaStream([video]); // só o vídeo; áudio não volta pra você
  $("localScreenFigure").hidden = false;
  $("localScreen").play().catch(() => {});
  // Declara a origem ANTES da track chegar no SFU.
  send({ type: "publish", trackId: video.id, kind: "video", source: "screen" });
  if (audio) send({ type: "publish", trackId: audio.id, kind: "audio", source: "screen" });
  try {
    localScreen.videoSender = pc.addTrack(video, stream);
    if (audio) localScreen.audioSender = pc.addTrack(audio, stream);
  } catch (err) {
    log(`erro ao publicar a tela: ${err.message}`);
  }
  renderLocalState();
  log(`compartilhamento de tela iniciado${audio ? " (com áudio)" : ""}`);
}

function stopScreenShare() {
  if (!localScreen) return;
  const { stream, video, videoSender, audioSender } = localScreen;
  localScreen = null;
  video.removeEventListener("ended", stopScreenShare);
  stream.getTracks().forEach((t) => t.stop());
  for (const s of [videoSender, audioSender]) {
    if (s && pc) { try { pc.removeTrack(s); } catch { /* pc may be gone */ } }
  }
  for (const k of ["screen", "screenAudio"]) {
    if (localPubs[k]) { send({ type: "unpublish", publicationId: localPubs[k] }); localPubs[k] = null; }
  }
  $("localScreen").srcObject = null;
  $("localScreenFigure").hidden = true;
  renderLocalState();
  log("compartilhamento de tela encerrado");
}

// Combine every screen-share track (video + optional audio) from one publisher
// into a single element so the screen plays with sound.
function attachRemoteScreen(participantId) {
  const tile = tiles.get(participantId);
  if (!tile || !pc) return;
  const screenPubIds = new Set(
    [...pubs].filter(([, p]) => p.participantId === participantId && p.source === "screen").map(([id]) => id),
  );
  if (screenPubIds.size === 0) return;
  const ms = new MediaStream();
  for (const r of pc.getReceivers()) {
    if (r.track && screenPubIds.has(r.track.id)) ms.addTrack(r.track);
  }
  if (ms.getTracks().length === 0) return;
  tile.screenVideo.srcObject = ms;
  tile.screenVideo.hidden = false;
  tile.screenVideo.muted = false; // áudio da tela remota deve tocar
  tile.screenVideo.play().catch(() => {});
}

function detachRemoteScreen(participantId) {
  const tile = tiles.get(participantId);
  if (!tile) return;
  // ainda há outra publication de tela (ex.: removeu só o áudio)? re-attacha.
  const stillSharing = [...pubs.values()].some(
    (p) => p.participantId === participantId && p.source === "screen",
  );
  if (stillSharing) { attachRemoteScreen(participantId); return; }
  tile.screenVideo.srcObject = null;
  tile.screenVideo.hidden = true;
}

async function togglePublishVideo() {
  if (senders.video && senders.video.track) {
    // Unpublish: remove the track entirely -> publication is removed.
    const pubId = localPubs.video;
    pc.removeTrack(senders.video);
    senders.video = null;
    localPubs.video = null;
    if (pubId) send({ type: "unpublish", publicationId: pubId });
    localStream.getVideoTracks().forEach((t) => t.stop());
    log("vídeo despublicado (publication removida)");
  } else {
    // Re-publish: acquire a fresh camera track and add it back.
    try {
      const s = await navigator.mediaDevices.getUserMedia({ video: true });
      const track = s.getVideoTracks()[0];
      localStream.addTrack(track);
      $("localVideo").srcObject = localStream;
      senders.video = pc.addTrack(track, localStream);
      log("vídeo republicado — aguardando nova publication");
    } catch (err) {
      log(`erro ao republicar vídeo: ${err.message}`);
    }
  }
  renderLocalState();
}

// ---------------------------------------------------------------------------
// Selective subscription
// ---------------------------------------------------------------------------

function toggleSubscription(pubId, wantSubscribed) {
  send({ type: wantSubscribed ? "subscribe" : "unsubscribe", publicationId: pubId });
}

// ---------------------------------------------------------------------------
// UI
// ---------------------------------------------------------------------------

function renderParticipants() {
  const all = new Set(participants);
  if (myId) all.add(myId);
  $("participantCount").textContent = String(all.size);
  const ul = $("participantList");
  ul.innerHTML = "";
  for (const id of all) {
    const li = document.createElement("li");
    const label = nameFor(id);
    li.textContent = id === myId ? `${label} (você)` : label;
    li.title = id;
    ul.append(li);
  }
}

function renderLocalState() {
  const a = localStream && localStream.getAudioTracks()[0];
  const v = localStream && localStream.getVideoTracks()[0];
  $("localAudioState").textContent = a ? (a.enabled ? "enabled" : "disabled") : "—";
  $("localVideoState").textContent = v && senders.video ? (v.enabled ? "enabled" : "disabled") : "não publicado";
  $("muteBtn").textContent = a && a.enabled ? "🎤 Mute" : "🎤 Unmute";
  $("cameraBtn").textContent = v && v.enabled ? "📹 Camera Off" : "📹 Camera On";
  $("publishVideoBtn").textContent = senders.video && senders.video.track ? "⛔ Unpublish vídeo" : "▶ Publish vídeo";
  $("localScreenState").textContent = localScreen ? "compartilhando" : "—";
  $("screenShareBtn").textContent = localScreen ? "🛑 Parar tela" : "🖥️ Compartilhar tela";
}

function renderPublications() {
  const box = $("publicationList");
  box.innerHTML = "";
  const byParticipant = new Map();
  for (const [id, p] of pubs) {
    if (!byParticipant.has(p.participantId)) byParticipant.set(p.participantId, []);
    byParticipant.get(p.participantId).push([id, p]);
  }
  if (byParticipant.size === 0) { box.textContent = "(nenhuma publicação de outros participantes)"; return; }
  for (const [pid, list] of byParticipant) {
    const h = document.createElement("div");
    h.innerHTML = `<strong>${nameFor(pid)}</strong>`;
    h.title = pid;
    box.append(h);
    for (const [id, p] of list) {
      const label = document.createElement("label");
      label.style.display = "block";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = p.subscribed;
      cb.addEventListener("change", () => toggleSubscription(id, cb.checked));
      const what = p.source === "screen" ? `tela ${p.kind === "audio" ? "🔊" : "🖥️"}` : p.kind;
      label.append(cb, document.createTextNode(
        ` ${what}${p.muted ? " (muted)" : ""} — ${id.slice(0, 12)}`));
      box.append(label);
    }
  }
}

function addTile(stream) {
  if (tiles.has(stream.id)) { tiles.get(stream.id).video.srcObject = stream; return; }
  const figure = document.createElement("figure");
  const caption = document.createElement("figcaption");
  caption.textContent = nameFor(stream.id);
  caption.title = stream.id;
  const video = document.createElement("video");
  video.autoplay = true; video.playsInline = true;
  video.srcObject = stream;
  const screenVideo = document.createElement("video");
  screenVideo.autoplay = true; screenVideo.playsInline = true; screenVideo.hidden = true;
  screenVideo.className = "screen";
  const badges = document.createElement("div");
  badges.className = "badges";
  figure.append(caption, video, screenVideo, badges);
  $("remoteVideos").append(figure);
  tiles.set(stream.id, { figure, video, screenVideo, badges });
  video.play().catch(() => { $("playRemoteBtn").hidden = false; });
  log("tile remoto criado", stream.id.slice(0, 8));
}

function removeTile(participantId) {
  const tile = tiles.get(participantId);
  if (!tile) return;
  tile.video.srcObject = null;
  tile.figure.remove();
  tiles.delete(participantId);
}

function updateTileBadges(participantId) {
  const tile = tiles.get(participantId);
  if (!tile) return;
  let audio = null, video = null, screen = null;
  for (const p of pubs.values()) {
    if (p.participantId !== participantId) continue;
    if (p.source === "screen") { if (p.kind === "video") screen = p; continue; }
    if (p.kind === "audio") audio = p;
    if (p.kind === "video") video = p;
  }
  const parts = [];
  if (!audio) parts.push("sem áudio");
  else if (audio.muted) parts.push("🔇");
  if (!video) parts.push("sem vídeo");
  else if (video.muted) parts.push("📷🚫");
  if (screen) parts.push("🖥️");
  tile.badges.textContent = parts.join("  ");
}

// ---------------------------------------------------------------------------
// Diagnostics
// ---------------------------------------------------------------------------

function startStatsPolling() {
  if (statsTimer) return;
  const poll = async () => {
    try {
      const res = await fetch(`/sfu/stats?room=${encodeURIComponent(roomId)}`);
      if (res.ok) renderSFUStats(await res.json());
    } catch (_) { /* transient */ }
  };
  poll();
  statsTimer = setInterval(poll, 1000);
}
function stopStatsPolling() { if (statsTimer) { clearInterval(statsTimer); statsTimer = null; } }

function renderSFUStats(data) {
  const tbody = $("sfuStatsBody");
  tbody.innerHTML = "";
  for (const p of data.participants) {
    const pubTxt = p.publications.map((s) => `${s.kind}${s.muted ? "(m)" : ""}`).join(", ") || "—";
    const inTxt = p.inbound.map((s) => `${s.kind}: ${s.packetsReceived || 0}pkt lost ${s.packetsLost || 0}`).join("; ") || "—";
    const outTxt = p.outbound.map((s) => `${s.kind}: ${s.packetsSent || 0}pkt`).join("; ") || "—";
    const tr = document.createElement("tr");
    tr.innerHTML =
      `<td>${p.id.slice(0, 8)}${p.id === myId ? " (você)" : ""}</td>` +
      `<td>${p.role}</td>` +
      `<td>${p.connectionState}</td>` +
      `<td>${pubTxt}</td>` +
      `<td>${p.subscriptions.length}</td>` +
      `<td>${inTxt}</td>` +
      `<td>${outTxt}</td>`;
    tbody.append(tr);
  }
}

// ---------------------------------------------------------------------------
// Teardown
// ---------------------------------------------------------------------------

function teardown() {
  stopStatsPolling();
  PulseQoE.stopReporting();
  PulseQoE.stopDashboard();
  for (const id of [...tiles.keys()]) removeTile(id);
  if (pc) {
    pc.getSenders().forEach((s) => { try { s.track && s.track.stop(); } catch (_) {} });
    pc.onicecandidate = pc.ontrack = pc.onnegotiationneeded = pc.onconnectionstatechange = null;
    pc.close();
    pc = null;
  }
  pendingCandidates.length = 0;
  remoteDescSet = false; makingOffer = false;
  senders.audio = senders.video = null;
  localPubs.audio = localPubs.video = null;
  pubs.clear();
  participants.clear();
  setStatus("closed");
}

// teardownForReconnect drops the dead PeerConnection and remote tiles but KEEPS
// the local camera/mic MediaStreamTracks so the recovered session can re-add
// them without re-prompting for permissions (Sprint 14B §48).
function teardownForReconnect() {
  stopStatsPolling();
  PulseQoE.stopReporting();
  PulseQoE.stopDashboard();
  for (const id of [...tiles.keys()]) removeTile(id);
  if (pc) {
    pc.onicecandidate = pc.ontrack = pc.onnegotiationneeded = pc.onconnectionstatechange = null;
    try { pc.close(); } catch (_) {}
    pc = null;
  }
  pendingCandidates.length = 0;
  remoteDescSet = false; makingOffer = false;
  senders.audio = senders.video = null;
  localPubs.audio = localPubs.video = null;
  pubs.clear();
  participants.clear();
  renderParticipants();
  renderPublications();
}

function resetUI() {
  if (localScreen) { localScreen.stream.getTracks().forEach((t) => t.stop()); localScreen = null; }
  localPubs.screen = localPubs.screenAudio = null;
  $("localScreenFigure").hidden = true;
  $("localScreen").srcObject = null;
  if (localStream) { localStream.getTracks().forEach((t) => t.stop()); localStream = null; }
  $("localVideo").srcObject = null;
  $("joinBtn").disabled = false;
  $("leaveBtn").disabled = true;
  $("mediaControls").hidden = true;
  $("participantCount").textContent = "0";
  $("participantList").innerHTML = "";
  $("publicationList").innerHTML = "";
  for (const id of ["d-conn", "d-ice"]) $(id).textContent = "—";
  ws = null;
}

$("playRemoteBtn").addEventListener("click", () => {
  tiles.forEach((t) => t.video.play());
  $("playRemoteBtn").hidden = true;
});
$("joinBtn").addEventListener("click", joinRoom);
$("leaveBtn").addEventListener("click", leaveRoom);
$("muteBtn").addEventListener("click", toggleMute);
$("cameraBtn").addEventListener("click", toggleCamera);
$("publishVideoBtn").addEventListener("click", togglePublishVideo);
$("screenShareBtn").addEventListener("click", () => (localScreen ? stopScreenShare() : startScreenShare()));
