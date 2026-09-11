import type {
  ParticipantJoinedMsg, ParticipantLeftMsg, PublicationEventMsg,
  RoomJoinedMsg, SnapshotTrack, SubscriptionEventMsg, WelcomeMsg,
} from "./protocol.js";
import { PublishError } from "./errors.js";
import type { MediaSource } from "./protocol.js";

export interface Participant { readonly identity: string; readonly name?: string }

const defaultSource = (kind: "audio" | "video"): MediaSource => (kind === "audio" ? "microphone" : "camera");

export class RemotePublication {
  muted = false;
  track?: MediaStreamTrack;
  subscribed = false;
  readonly source: MediaSource;
  constructor(readonly id: string, readonly kind: "audio" | "video", source?: MediaSource) {
    this.source = source ?? defaultSource(kind);
  }
}

export class LocalPublication {
  muted = false;
  readonly source: MediaSource;
  constructor(readonly id: string, readonly kind: "audio" | "video", readonly track: MediaStreamTrack, source?: MediaSource) {
    this.source = source ?? defaultSource(kind);
  }
}

export class RemoteParticipant implements Participant {
  readonly publications = new Map<string, RemotePublication>();
  constructor(readonly identity: string, public name?: string) {}
  getTrack(kind: "audio" | "video"): MediaStreamTrack | undefined {
    for (const p of this.publications.values()) if (p.kind === kind && p.track) return p.track;
    return undefined;
  }
  /**
   * The first published track of the given source that has media attached.
   * Pass `kind` to disambiguate a screen share that carries both video and
   * audio.
   */
  getTrackBySource(source: MediaSource, kind?: "audio" | "video"): MediaStreamTrack | undefined {
    for (const p of this.publications.values()) {
      if (p.source === source && p.track && (kind === undefined || p.kind === kind)) return p.track;
    }
    return undefined;
  }
}

export interface LocalWiring {
  getUserMedia(c: MediaStreamConstraints): Promise<MediaStream>;
  getDisplayMedia(c: MediaStreamConstraints): Promise<MediaStream>;
  addLocalTrack(track: MediaStreamTrack, stream: MediaStream): RTCRtpSender;
  removeLocalTrack(sender: RTCRtpSender): void;
  send(m: { type: "set_mute"; publicationId: string; muted: boolean; kind: "audio" | "video" }
        | { type: "unpublish"; publicationId: string }
        | { type: "publish"; trackId: string; kind: "audio" | "video"; source: MediaSource }): void;
}

export class LocalParticipant implements Participant {
  readonly publications = new Map<string, LocalPublication>();
  private w: LocalWiring | null = null;
  private stream: MediaStream | null = null;
  private senders = new Map<"audio" | "video", RTCRtpSender>();
  private waiters = new Map<"audio" | "video", { resolve: () => void; reject: (e: unknown) => void }>();
  /** publication_added echoes that arrived before the local stream was ready */
  private pendingPubs = new Map<"audio" | "video", string>();
  private denied = false;

  /**
   * Screen share is a SEPARATE lane from camera/mic: its own getDisplayMedia
   * stream, its own sender(s), its own publication(s). It is never merged into
   * {@link stream} / {@link senders} so camera and screen coexist and a camera
   * mute never touches the screen. When the capture carries system/tab audio
   * (Chromium "Share audio"), that audio is published too — a second
   * publication, `kind: "audio"`, `source: "screen"`.
   */
  private screen: {
    stream: MediaStream;
    video: MediaStreamTrack;
    audio?: MediaStreamTrack;
    videoSender?: RTCRtpSender;
    audioSender?: RTCRtpSender;
    videoPubId?: string;
    audioPubId?: string;
    onended: () => void;
    /** resolves when the VIDEO publication is acknowledged (audio is best-effort) */
    waiter?: { resolve: () => void; reject: (e: unknown) => void };
  } | null = null;
  /** screen publication_added echoes that landed before the lane was ready */
  private pendingScreenPubs: { audio?: string; video?: string } = {};

  constructor(public identity: string, public name?: string) {}

  /** @internal */ _wire(w: LocalWiring): void { this.w = w; }

  /** Whether a screen share is currently being published. */
  get isScreenSharing(): boolean { return this.screen !== null; }

  /**
   * @internal
   * Reset the publish state after a socket reconnect: the media session is
   * rebuilt from scratch by the SFU, so the old publicationIds / senders /
   * waiters are stale. The captured {@link stream} is KEPT so `republish()`
   * can re-add its tracks against the fresh peer connection.
   */
  _resetForReconnect(): void {
    this.publications.clear();
    this.senders.clear();
    this.waiters.clear();
    this.pendingPubs.clear();
    this.denied = false;
    // The browser almost never lets a getDisplayMedia track survive a new
    // PeerConnection, so screen share is NOT auto-restored on reconnect
    // (spec §18): drop it and let the app / user start it again.
    this._teardownScreen(false);
  }

  /**
   * @internal
   * Re-add the tracks of the captured local stream against the (freshly
   * created) peer connection and resolve once their `publication_added`
   * echoes land. No-op when nothing was published.
   */
  async _republish(): Promise<void> {
    if (!this.stream) return;
    const w = this.wiring();
    const stream = this.stream;
    const audio = stream.getAudioTracks()[0];
    const video = stream.getVideoTracks()[0];
    const pending: Promise<void>[] = [];
    if (audio) { this.senders.set("audio", w.addLocalTrack(audio, stream)); pending.push(this.waitFor("audio")); this.flushPub("audio"); }
    if (video) { this.senders.set("video", w.addLocalTrack(video, stream)); pending.push(this.waitFor("video")); this.flushPub("video"); }
    await Promise.all(pending);
  }

  /** @internal */ _onPublicationAdded(kind: "audio" | "video", publicationId: string): void {
    this.pendingPubs.set(kind, publicationId);
    this.flushPub(kind);
  }

  /** @internal */ _onPublicationRemoved(publicationId: string): void {
    this.publications.delete(publicationId);
    // The SFU dropped one of our screen publications (ingest loop ended, e.g.
    // the track died). Tear the whole lane down without a second unpublish.
    if (this.screen && (this.screen.videoPubId === publicationId || this.screen.audioPubId === publicationId)) {
      this._teardownScreen(false);
    }
  }

  /** @internal */ _onScreenPublicationAdded(publicationId: string, kind: "audio" | "video"): void {
    this.pendingScreenPubs[kind] = publicationId;
    this.flushScreenPub();
  }

  private flushScreenPub(): void {
    const sc = this.screen;
    if (!sc) return;
    const vId = this.pendingScreenPubs.video;
    if (vId && !sc.videoPubId) {
      sc.videoPubId = vId;
      this.publications.set(vId, new LocalPublication(vId, "video", sc.video, "screen"));
      delete this.pendingScreenPubs.video;
      sc.waiter?.resolve();
      sc.waiter = undefined;
    }
    const aId = this.pendingScreenPubs.audio;
    if (aId && sc.audio && !sc.audioPubId) {
      sc.audioPubId = aId;
      this.publications.set(aId, new LocalPublication(aId, "audio", sc.audio, "screen"));
      delete this.pendingScreenPubs.audio;
    }
  }

  /** @internal */ _onPublicationMuted(publicationId: string, muted: boolean): void {
    const p = this.publications.get(publicationId);
    if (p) p.muted = muted;
  }

  /** @internal */ _onPublishDenied(): void {
    this.denied = true;
    for (const w of this.waiters.values()) w.reject(new PublishError("publish denied by server"));
    this.waiters.clear();
    if (this.screen?.waiter) this._teardownScreen(false);
  }

  private flushPub(kind: "audio" | "video"): void {
    const publicationId = this.pendingPubs.get(kind);
    if (publicationId === undefined) return;
    const track = kind === "audio" ? this.stream?.getAudioTracks()[0] : this.stream?.getVideoTracks()[0];
    if (!track) return; // local stream not acquired yet — retried after getUserMedia resolves
    this.publications.set(publicationId, new LocalPublication(publicationId, kind, track));
    this.pendingPubs.delete(kind);
    this.waiters.get(kind)?.resolve();
    this.waiters.delete(kind);
  }

  async enableCameraAndMicrophone(): Promise<void> {
    const w = this.wiring();
    this.denied = false;
    let stream: MediaStream;
    try { stream = await w.getUserMedia({ audio: true, video: true }); }
    catch (e) { throw new PublishError("getUserMedia failed", e); }
    this.stream = stream;
    const audio = stream.getAudioTracks()[0];
    const video = stream.getVideoTracks()[0];
    const pending: Promise<void>[] = [];
    if (audio) { this.senders.set("audio", w.addLocalTrack(audio, stream)); pending.push(this.waitFor("audio")); this.flushPub("audio"); }
    if (video) { this.senders.set("video", w.addLocalTrack(video, stream)); pending.push(this.waitFor("video")); this.flushPub("video"); }
    if (this.denied) this._onPublishDenied();
    await Promise.all(pending);
  }

  async setMicrophoneEnabled(enabled: boolean): Promise<void> { this.setKind("audio", enabled); }
  async setCameraEnabled(enabled: boolean): Promise<void> { this.setKind("video", enabled); }

  private setKind(kind: "audio" | "video", enabled: boolean): void {
    const track = kind === "audio" ? this.stream?.getAudioTracks()[0] : this.stream?.getVideoTracks()[0];
    if (!track) return;
    track.enabled = enabled;
    for (const p of this.publications.values()) {
      // Only camera/mic — a screen publication is a separate video track and
      // must not be muted by a camera toggle.
      if (p.kind === kind && p.source !== "screen") {
        p.muted = !enabled;
        this.wiring().send({ type: "set_mute", publicationId: p.id, muted: !enabled, kind });
      }
    }
  }

  /**
   * Start sharing a screen / window / tab. Prompts the browser
   * (`getDisplayMedia`), publishes the capture as an additional `video`
   * publication with `source === "screen"` — the camera keeps running — plus,
   * when the capture carries audio (Chromium "Share audio" on a tab / full
   * screen), a second `audio` publication with the same source. Resolves once
   * the SFU has acknowledged the video publication. If the user ends the share
   * from the browser UI (`track.onended`), the SDK unpublishes automatically.
   * Calling it while already sharing is a no-op.
   *
   * `constraints` defaults to `{ video: true, audio: true }`; pass
   * `{ video: true }` to never request audio.
   */
  async startScreenShare(constraints: MediaStreamConstraints = { video: true, audio: true }): Promise<void> {
    if (this.screen) return;
    const w = this.wiring();
    let stream: MediaStream;
    try { stream = await w.getDisplayMedia(constraints); }
    catch (e) { throw new PublishError("getDisplayMedia failed", e); }
    const video = stream.getVideoTracks()[0];
    if (!video) { stream.getTracks().forEach((t) => t.stop()); throw new PublishError("no screen video track"); }
    const audio = stream.getAudioTracks()[0]; // undefined for a window share / "Share audio" left off

    const onended = () => { void this.stopScreenShare().catch(() => {}); };
    video.addEventListener("ended", onended);
    const sc: NonNullable<LocalParticipant["screen"]> = { stream, video, audio, onended };
    this.screen = sc;
    // Declare each source BEFORE the track reaches the SFU so the Publications
    // it mints are stamped source=screen.
    w.send({ type: "publish", trackId: video.id, kind: "video", source: "screen" });
    if (audio) w.send({ type: "publish", trackId: audio.id, kind: "audio", source: "screen" });
    try {
      sc.videoSender = w.addLocalTrack(video, stream);
      if (audio) sc.audioSender = w.addLocalTrack(audio, stream);
    } catch (e) {
      this._teardownScreen(false);
      throw new PublishError("failed to add screen track", e);
    }
    const done = new Promise<void>((resolve, reject) => { sc.waiter = { resolve, reject }; });
    this.flushScreenPub(); // an echo may already be waiting
    await done;
  }

  /** Stop the current screen share (unpublish + release the capture). No-op if not sharing. */
  async stopScreenShare(): Promise<void> { this._teardownScreen(true); }

  /** @internal */ _teardownScreen(notify: boolean): void {
    this.pendingScreenPubs = {};
    const sc = this.screen;
    if (!sc) return;
    this.screen = null;
    sc.video.removeEventListener("ended", sc.onended);
    sc.stream.getTracks().forEach((t) => t.stop());
    for (const sender of [sc.videoSender, sc.audioSender]) {
      if (sender && this.w) { try { this.w.removeLocalTrack(sender); } catch { /* pc may be gone */ } }
    }
    for (const id of [sc.videoPubId, sc.audioPubId]) {
      if (!id) continue;
      this.publications.delete(id);
      if (notify && this.w) this.w.send({ type: "unpublish", publicationId: id });
    }
    sc.waiter?.reject(new PublishError("screen share stopped"));
    sc.waiter = undefined;
  }

  private waitFor(kind: "audio" | "video"): Promise<void> {
    return new Promise<void>((resolve, reject) => this.waiters.set(kind, { resolve, reject }));
  }
  private wiring(): LocalWiring {
    if (!this.w) throw new PublishError("not connected");
    return this.w;
  }
}

export class ParticipantStore {
  readonly remote = new Map<string, RemoteParticipant>();
  localIdentity: string | undefined;
  /**
   * Tracks whose `ontrack` fired before we knew the matching publication,
   * keyed by stream id (the publisher's participant id). Matched to a
   * publication by the receiver track id when the browser carries the SFU's
   * msid track id (`pub-…`), otherwise by (streamId, kind) in arrival order.
   */
  private pendingTracks = new Map<string, MediaStreamTrack[]>();

  applyWelcome(m: WelcomeMsg): void { this.localIdentity = m.participantId; }

  /**
   * Reconcile the store against the authoritative membership carried by
   * `room_joined`. `room_joined` is the *full* membership list, both on a fresh
   * connect and on a reconnect (including a node-failure rejoin, where the
   * participant ids change wholesale) — so anything the server did not list is
   * gone and must be reported as a departure rather than silently kept.
   *
   * @returns the participants created by this message (`added`) and the ones
   * dropped because the server no longer lists them (`removed`).
   */
  applyRoomJoined(m: RoomJoinedMsg): { added: RemoteParticipant[]; removed: RemoteParticipant[] } {
    const desired = new Map<string, { name?: string; tracks?: SnapshotTrack[] }>();
    for (const p of m.participants) desired.set(p.participantId, { name: p.name });
    for (const p of m.snapshot?.participants ?? []) {
      const prev = desired.get(p.participantId);
      desired.set(p.participantId, { name: p.name ?? prev?.name, tracks: p.tracks });
    }

    const removed: RemoteParticipant[] = [];
    for (const [id, rp] of [...this.remote]) {
      if (desired.has(id)) continue;
      this.remote.delete(id);
      removed.push(rp);
    }

    const added: RemoteParticipant[] = [];
    for (const [id, info] of desired) {
      const existed = this.remote.has(id);
      const rp = this.ensure(id, info.name);
      if (!existed) added.push(rp);
      if (!info.tracks) continue;
      // The snapshot is authoritative for this participant's publications:
      // anything it omits was unpublished while we were away.
      const keep = new Set(info.tracks.map((t) => t.publicationId));
      for (const pubId of [...rp.publications.keys()]) if (!keep.has(pubId)) rp.publications.delete(pubId);
      for (const t of info.tracks) {
        let pub = rp.publications.get(t.publicationId);
        if (!pub) {
          pub = new RemotePublication(t.publicationId, t.kind, t.source);
          rp.publications.set(t.publicationId, pub);
          pub.track = this.takeBufferedTrack(id, t.kind);
        }
        pub.muted = t.muted;
      }
    }
    return { added, removed };
  }

  /**
   * Drop every remote track handle: they belong to a peer connection that is
   * being closed (reconnect or teardown) and are dead once it goes away. The
   * publications themselves survive — `room_joined`/`publication_added` on the
   * new session re-attach fresh tracks.
   */
  clearTracks(): void {
    for (const rp of this.remote.values()) {
      for (const pub of rp.publications.values()) { pub.track = undefined; pub.subscribed = false; }
    }
    this.pendingTracks.clear();
  }

  /** Forget everything: used when a Room is disconnected and may be reused. */
  clear(): void {
    this.remote.clear();
    this.pendingTracks.clear();
    this.localIdentity = undefined;
  }

  applyParticipantJoined(m: ParticipantJoinedMsg): RemoteParticipant { return this.ensure(m.participantId, m.name); }

  applyParticipantLeft(m: ParticipantLeftMsg): RemoteParticipant | undefined {
    const rp = this.remote.get(m.participantId);
    if (rp) this.remote.delete(m.participantId);
    return rp;
  }

  applyPublicationEvent(m: PublicationEventMsg):
    { participant: RemoteParticipant; publication: RemotePublication; removed: boolean } | undefined {
    if (m.participantId === this.localIdentity) return undefined;
    const rp = this.ensure(m.participantId);
    if (m.type === "publication_removed") {
      const pub = rp.publications.get(m.publicationId);
      if (!pub) return undefined;
      rp.publications.delete(m.publicationId);
      return { participant: rp, publication: pub, removed: true };
    }
    let pub = rp.publications.get(m.publicationId);
    if (!pub) {
      pub = new RemotePublication(m.publicationId, m.kind, m.source);
      rp.publications.set(m.publicationId, pub);
      pub.track = this.takeBufferedTrack(m.participantId, m.kind);
    }
    pub.muted = m.muted;
    return { participant: rp, publication: pub, removed: false };
  }

  applySubscriptionEvent(m: SubscriptionEventMsg): void {
    const pub = this.remote.get(m.participantId)?.publications.get(m.publicationId);
    if (pub) pub.subscribed = m.type === "subscription_added";
  }

  attachTrack(streamId: string, track: MediaStreamTrack, trackId?: string):
    { participant: RemoteParticipant; publication: RemotePublication } | undefined {
    const kind = track.kind === "audio" ? "audio" : "video";
    const rp = this.remote.get(streamId);
    if (!rp) { this.bufferTrack(streamId, track); return undefined; }
    // Exact match when the browser carries the SFU's msid track id; this is
    // what lets camera and screen (both video) attach to the right
    // publication. Fall back to first free publication of the kind.
    let pub = trackId ? rp.publications.get(trackId) : undefined;
    if (pub?.track) pub = undefined;
    if (!pub) pub = this.freePublication(rp, kind);
    if (!pub) { this.bufferTrack(streamId, track); return undefined; }
    pub.track = track;
    return { participant: rp, publication: pub };
  }

  /** First publication of `kind` on `rp` that has no track yet. */
  private freePublication(rp: RemoteParticipant, kind: "audio" | "video"): RemotePublication | undefined {
    for (const p of rp.publications.values()) if (p.kind === kind && !p.track) return p;
    return undefined;
  }

  private bufferTrack(streamId: string, track: MediaStreamTrack): void {
    const list = this.pendingTracks.get(streamId);
    if (list) list.push(track);
    else this.pendingTracks.set(streamId, [track]);
  }

  /** Remove and return a buffered track for (streamId, kind), if any. */
  private takeBufferedTrack(streamId: string, kind: "audio" | "video"): MediaStreamTrack | undefined {
    const list = this.pendingTracks.get(streamId);
    if (!list) return undefined;
    const i = list.findIndex((t) => (t.kind === "audio" ? "audio" : "video") === kind);
    if (i < 0) return undefined;
    const [t] = list.splice(i, 1);
    if (list.length === 0) this.pendingTracks.delete(streamId);
    return t;
  }

  private ensure(identity: string, name?: string): RemoteParticipant {
    let rp = this.remote.get(identity);
    if (!rp) { rp = new RemoteParticipant(identity, name); this.remote.set(identity, rp); }
    else if (name && !rp.name) rp.name = name;
    return rp;
  }
}
