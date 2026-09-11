import { defaultDeps, type RoomDeps } from "./deps.js";
import {
  ConnectionState, RoomEvent, type DisconnectReason, type RoomEventMap,
} from "./events.js";
import { ConnectionError, TokenError, RoomOnOtherNodeError } from "./errors.js";
import { Signaling } from "./signaling.js";
import { Negotiator } from "./negotiator.js";
import { QoEReporter, parseQualityEvent } from "./qoe.js";
import { Reconnector, decideRejoin } from "./reconnector.js";
import {
  ParticipantStore, LocalParticipant, type RemoteParticipant, type Participant,
} from "./participants.js";
import type { ServerMessage } from "./protocol.js";

export interface RoomOptions { qoe?: boolean }

const TOKEN_ERROR_CODES = new Set([
  "INVALID_TOKEN", "EXPIRED_TOKEN", "INVALID_ISSUER", "INVALID_AUDIENCE",
  "ROOM_ACCESS_DENIED", "JOIN_NOT_ALLOWED", "UNAUTHENTICATED",
]);
const MAX_NODE_REDIRECTS = 5;

export class Room extends EventTarget {
  private readonly deps: RoomDeps;
  private readonly qoeEnabled: boolean;

  private readonly sig: Signaling;
  private readonly neg: Negotiator;
  private readonly store = new ParticipantStore();
  private readonly reconnector = new Reconnector();
  private qoe: QoEReporter | null = null;

  private _state = ConnectionState.Disconnected;
  private _local = new LocalParticipant("");
  private serverUrl = "";
  private token = "";
  private roomId = "";
  private nodeRedirects = 0;
  private resume: { sessionId: string; generation: number } | null = null;
  private connecting: { resolve: () => void; reject: (e: unknown) => void } | null = null;
  private wantConnected = false;
  /**
   * Server-message serialization. Only the SDP cases of {@link onMessage} are
   * asynchronous; while one of those is in flight (`busy`) every later message
   * waits in `pending`. Without this, two `sfu_offer`s arriving back to back
   * interleave — the second reads `signalingState` while the first is awaiting
   * `createAnswer()` and issues a concurrent rollback (`InvalidStateError` in
   * Chromium). Messages that resolve synchronously are still handled inline, so
   * observers see the same ordering the socket delivered.
   */
  private busy = false;
  private pending: ServerMessage[] = [];

  constructor(options: RoomOptions = {}, deps: Partial<RoomDeps> = {}) {
    super();
    this.deps = { ...defaultDeps, ...deps };
    this.qoeEnabled = options.qoe !== false;
    this.sig = new Signaling(this.deps.wsFactory);
    this.neg = new Negotiator(this.deps.pcFactory, (m) => this.sig.send(m));
    this.sig.onOpen = () => this.sendJoin();
    this.sig.onMessage = (m) => { this.pending.push(m); this.pump(); };
    this.sig.onClose = (info) => this.onClose(info);
    this.neg.onTrack = (track, streamId, trackId) => this.onRemoteTrack(track, streamId, trackId);
  }

  /**
   * listener -> event name -> the wrappers registered for it. Keyed by event as
   * well as by function so that the same callback can be attached to several
   * events (or to one event twice) and `off()` removes only the intended
   * registration instead of orphaning the others.
   */
  private eventWrappers = new WeakMap<Function, Map<string, EventListener[]>>();

  on<E extends RoomEvent>(event: E, listener: (...args: RoomEventMap[E]) => void): void {
    const wrapper: EventListener = (ev) => listener(...(ev as CustomEvent).detail);
    let byEvent = this.eventWrappers.get(listener);
    if (!byEvent) { byEvent = new Map(); this.eventWrappers.set(listener, byEvent); }
    const wrappers = byEvent.get(event);
    if (wrappers) wrappers.push(wrapper);
    else byEvent.set(event, [wrapper]);
    this.addEventListener(event, wrapper);
  }
  off<E extends RoomEvent>(event: E, listener: (...args: RoomEventMap[E]) => void): void {
    const byEvent = this.eventWrappers.get(listener);
    const wrappers = byEvent?.get(event);
    const wrapper = wrappers?.pop();
    if (!wrapper) return;
    this.removeEventListener(event, wrapper);
    if (wrappers && wrappers.length === 0) byEvent?.delete(event);
  }
  private emit<E extends RoomEvent>(event: E, ...args: RoomEventMap[E]): void {
    this.dispatchEvent(new CustomEvent(event, { detail: args }));
  }

  get state(): ConnectionState { return this._state; }
  get localParticipant(): LocalParticipant { return this._local; }
  get remoteParticipants(): ReadonlyMap<string, RemoteParticipant> { return this.store.remote; }
  /** Read-only escape hatch for diagnostics. `pc` is null before `welcome`. */
  get engine(): { pc: RTCPeerConnection | null } { return { pc: this.neg.pcOrNull }; }

  connect(serverUrl: string, token: string, roomId: string): Promise<void> {
    this.serverUrl = serverUrl;
    this.token = token;
    this.roomId = roomId;
    this.wantConnected = true;
    this.setState(ConnectionState.Connecting);
    return new Promise((resolve, reject) => {
      this.connecting = { resolve, reject };
      this.sig.connect(serverUrl, token);
    });
  }

  async disconnect(): Promise<void> {
    this.wantConnected = false;
    this.reconnector.reset();
    this.qoe?.stop();
    this.qoe = null;
    this.sig.close();
    this.closeMedia();
    // `wantConnected` is already false, so the synchronous onClose above bailed
    // out early — settle a pending connect() here or it would hang forever.
    this.connecting?.reject(new ConnectionError("disconnected"));
    this.connecting = null;
    // A Room can be reconnected after disconnect(): drop every trace of the old
    // session so the next connect() starts fresh (no resume, no ghosts).
    this.resetSession();
    this.setState(ConnectionState.Disconnected);
    this.emit(RoomEvent.Disconnected, "client");
  }

  /** Forget the per-session state so this Room can be `connect()`ed again. */
  private resetSession(): void {
    this._local = new LocalParticipant("");
    this.store.clear();
    this.resume = null;
    this.nodeRedirects = 0;
    this.pending = [];
  }

  /** Close the peer connection and drop the remote track handles it owned. */
  private closeMedia(): void {
    this.neg.close();
    this.store.clearTracks();
  }

  private setState(s: ConnectionState): void {
    if (this._state === s) return;
    this._state = s;
    this.emit(RoomEvent.ConnectionStateChanged, s);
  }

  private sendJoin(): void {
    const j: { type: "join"; roomId: string; resume?: { sessionId: string; generation: number } } =
      { type: "join", roomId: this.roomId };
    const d = decideRejoin(this.resume);
    if (d.type === "resume") j.resume = d.resume;
    this.sig.send(j);
  }

  private failConnect(err: unknown): void {
    this.connecting?.reject(err);
    this.connecting = null;
    this.wantConnected = false;
    this.setState(ConnectionState.Disconnected);
  }

  /** Drain {@link pending}, pausing while an asynchronous handler is in flight. */
  private pump(): void {
    while (!this.busy) {
      const m = this.pending.shift();
      if (m === undefined) return;
      let r: void | Promise<void>;
      try { r = this.onMessage(m); } catch (e) { this.onFatal(e); continue; }
      if (!r) continue;
      this.busy = true;
      r.then(undefined, (e: unknown) => this.onFatal(e)).then(() => { this.busy = false; this.pump(); });
    }
  }

  /**
   * Last-resort error sink for anything thrown out of message handling. Nothing
   * `void`s a rejected promise into the void: a failure before CONNECTED fails
   * the pending `connect()`, and afterwards it surfaces as {@link RoomEvent.Error}.
   */
  private onFatal(e: unknown): void {
    const err = e instanceof Error ? e : new Error(String(e));
    if (this.connecting) { this.failConnect(err); return; }
    this.emit(RoomEvent.Error, err);
  }

  /**
   * Handle one server message. Returns a promise ONLY for the cases that really
   * are asynchronous (SDP), so the common synchronous cases keep their inline
   * ordering; see {@link busy}.
   */
  private onMessage(m: ServerMessage): void | Promise<void> {
    switch (m.type) {
      case "welcome":
        this.store.applyWelcome(m);
        // A fresh connect starts with `new LocalParticipant("")`; a reconnect
        // keeps the existing participant (and its captured stream) so its
        // tracks can be re-published against the new pc.
        if (this._local.identity === "") this._local = new LocalParticipant(m.participantId, m.name);
        else this._local._resetForReconnect();
        this._local._wire({
          getUserMedia: this.deps.getUserMedia,
          getDisplayMedia: this.deps.getDisplayMedia,
          addLocalTrack: (t, s) => this.neg.addLocalTrack(t, s),
          removeLocalTrack: (s) => this.neg.removeLocalTrack(s),
          send: (m2) => this.sig.send(m2),
        });
        this.neg.create(m.iceServers ?? []);
        break;

      case "room_joined": {
        this.roomId = m.roomId;
        this.nodeRedirects = 0;
        if (m.reconnect) this.reconnector.setHints(m.reconnect);
        if (m.sessionId) this.resume = { sessionId: m.sessionId, generation: m.generation ?? 0 };
        // room_joined carries the FULL membership, so it is a reconciliation:
        // only genuinely new participants are "connected", and anyone the
        // server dropped while we were away has left.
        const { added, removed } = this.store.applyRoomJoined(m);
        for (const rp of removed) {
          for (const pub of rp.publications.values()) {
            if (pub.track) this.emit(RoomEvent.TrackUnsubscribed, pub, rp);
          }
          this.emit(RoomEvent.ParticipantDisconnected, rp);
        }
        for (const rp of added) this.emit(RoomEvent.ParticipantConnected, rp);
        this.reconnector.reset();
        this.setState(ConnectionState.Connected);
        this.connecting?.resolve();
        this.connecting = null;
        if (m.reconnected) {
          // media session is rebuilt from scratch: re-publish local tracks.
          void this._local._republish().catch((e: unknown) => this.onFatal(e));
        }
        if (this.qoeEnabled) this.startQoE();
        break;
      }

      case "participant_joined":
        this.emit(RoomEvent.ParticipantConnected, this.store.applyParticipantJoined(m));
        break;

      case "participant_left": {
        const rp = this.store.applyParticipantLeft(m);
        if (rp) this.emit(RoomEvent.ParticipantDisconnected, rp);
        break;
      }

      case "sfu_offer":
      case "sfu_answer":
      case "sfu_ice_candidate":
        return this.neg.handleSignal(m);

      case "publication_added":
      case "publication_removed":
      case "publication_muted": {
        // Echoes of our own publications never reach the ParticipantStore —
        // all three kinds are applied to the LocalParticipant instead.
        if (m.participantId === this._local.identity) {
          if (m.type === "publication_added") {
            if (m.source === "screen") this._local._onScreenPublicationAdded(m.publicationId, m.kind);
            else this._local._onPublicationAdded(m.kind, m.publicationId);
          }
          else if (m.type === "publication_removed") this._local._onPublicationRemoved(m.publicationId);
          else this._local._onPublicationMuted(m.publicationId, m.muted);
          break;
        }
        const r = this.store.applyPublicationEvent(m);
        if (!r) break;
        if (r.removed) this.emit(RoomEvent.TrackUnsubscribed, r.publication, r.participant);
        else if (m.type === "publication_muted") this.emit(RoomEvent.TrackMuted, r.publication, r.participant);
        else if (r.publication.track) this.emit(RoomEvent.TrackSubscribed, r.publication.track, r.publication, r.participant);
        break;
      }

      case "subscription_added":
      case "subscription_removed":
        this.store.applySubscriptionEvent(m);
        break;

      case "quality_degraded":
      case "quality_recovered":
      case "quality_changed": {
        const who: Participant = m.participantId === this._local.identity
          ? this._local
          : this.store.remote.get(m.participantId) ?? { identity: m.participantId };
        this.emit(RoomEvent.QualityChanged, parseQualityEvent(m), who);
        break;
      }

      case "publish_denied":
        this._local._onPublishDenied();
        break;

      case "subscribe_denied":
        this.emit(RoomEvent.SubscribeDenied, { publicationId: m.publicationId });
        break;

      case "room_closed":
        this.teardown("room_closed");
        break;

      case "session.stale":
        // The server refused the resume and ABORTED the join without closing
        // the socket (ws_server.go: sendJSON(session.stale) then `return`), so
        // no room_joined and no onClose is ever coming. Drop the dead resume
        // token and immediately retry a fresh join on this same socket —
        // otherwise the client waits forever.
        this.resume = null;
        this.sendJoin();
        break;

      case "session.reconnected":
        // no-op: connection state is driven by the following room_joined.
        break;

      case "session.recovery":
        // Advisory only: the server put our session into RECOVERING. The socket
        // may well still be usable, so do not pre-emptively change state.
        break;

      case "session.recovery_failed":
        // The RECOVERING session blew its timeout server-side; the resume token
        // is dead. The next attempt has to join fresh.
        this.resume = null;
        break;

      case "session.replaced":
        this.teardown("replaced");
        break;

      case "error":
        this.handleError(m.code, m.message, m.nodeId);
        break;

      case "room.snapshot":
        // Delivered inline in room_joined; a standalone one carries nothing new.
        break;

      default: {
        // Exhaustive over ServerMessage at compile time; the wire is still open
        // (isServerMessage only checks `type`), so warn on anything genuinely new.
        const unhandled: never = m;
        console.warn("[pulsertc] unhandled server message", (unhandled as ServerMessage).type);
        break;
      }
    }
  }

  private handleError(code: string | undefined, message: string | undefined, nodeId: string | undefined): void {
    if (this.connecting) {
      if (code && TOKEN_ERROR_CODES.has(code)) this.failConnect(new TokenError(message ?? code, code));
      else if (code === "ROOM_ON_OTHER_NODE") this.failConnect(new RoomOnOtherNodeError(nodeId ?? "?"));
      else this.failConnect(new ConnectionError(message ?? code ?? "connection error"));
      return;
    }
    if (code === "EXPIRED_TOKEN") { this.teardown("token_expired"); return; }
    if (code === "ROOM_ON_OTHER_NODE") {
      // After CONNECTED: the LB routed us to the wrong node. Retry the same URL;
      // give up after maxNodeRedirects consecutive misses.
      this.nodeRedirects += 1;
      if (this.nodeRedirects > MAX_NODE_REDIRECTS) { this.teardown("error"); return; }
      this.sig.close();
      this.setState(ConnectionState.Reconnecting);
      this.reconnector.arm(() => this.sig.connect(this.serverUrl, this.token));
      return;
    }
    // An error we do not have a recovery path for: surface it rather than drop it.
    this.emit(RoomEvent.Error, new ConnectionError(message ?? code ?? "server error"));
  }

  private teardown(reason: DisconnectReason): void {
    this.wantConnected = false;
    this.reconnector.reset();
    this.qoe?.stop();
    this.qoe = null;
    this.sig.close();
    this.closeMedia();
    this.connecting?.reject(new ConnectionError(`disconnected (${reason})`));
    this.connecting = null;
    this.resetSession();
    this.setState(ConnectionState.Disconnected);
    this.emit(RoomEvent.Disconnected, reason);
  }

  private onClose(info: { code: number; clean: boolean }): void {
    if (!this.wantConnected) return;
    if (this.connecting) { this.failConnect(new ConnectionError(`socket closed (${info.code})`)); return; }
    this.setState(ConnectionState.Reconnecting);
    this.closeMedia();
    this.reconnector.arm(() => this.sig.connect(this.serverUrl, this.token));
  }

  private onRemoteTrack(track: MediaStreamTrack, streamId: string, trackId: string): void {
    const r = this.store.attachTrack(streamId, track, trackId);
    if (r) this.emit(RoomEvent.TrackSubscribed, track, r.publication, r.participant);
  }

  private startQoE(): void {
    this.qoe?.stop();
    this.qoe = new QoEReporter({
      getStats: () => this.neg.pc.getStats(),
      send: (samples) => this.sig.send({ type: "quality_report", samples }),
      localPubId: (kind) => {
        for (const p of this._local.publications.values()) if (p.kind === kind) return p.id;
        return undefined;
      },
      now: this.deps.now,
    });
    this.qoe.start();
  }
}
