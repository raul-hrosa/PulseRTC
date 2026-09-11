import { LocalParticipant } from "../src/participants.js";
import { PublishError } from "../src/errors.js";

function fakeTrack(kind: "audio" | "video", id?: string): MediaStreamTrack {
  const listeners: Record<string, (() => void)[]> = {};
  return {
    kind, enabled: true, id: id ?? `t-${kind}`, stop() {},
    addEventListener(ev: string, fn: () => void) { (listeners[ev] ??= []).push(fn); },
    removeEventListener(ev: string, fn: () => void) {
      listeners[ev] = (listeners[ev] ?? []).filter((f) => f !== fn);
    },
    dispatch(ev: string) { (listeners[ev] ?? []).forEach((f) => f()); },
  } as unknown as MediaStreamTrack;
}

function fakeDisplayStream(video: MediaStreamTrack, audio?: MediaStreamTrack): MediaStream {
  const tracks = audio ? [video, audio] : [video];
  return {
    getVideoTracks: () => [video],
    getAudioTracks: () => (audio ? [audio] : []),
    getTracks: () => tracks,
  } as unknown as MediaStream;
}

function wiring(overrides: Partial<Parameters<typeof LocalParticipant.prototype._wire>[0]> = {}) {
  const sent: unknown[] = [];
  const calls: string[] = [];
  return {
    sent,
    calls,
    getUserMedia: async () => ({
      getAudioTracks: () => [fakeTrack("audio")],
      getVideoTracks: () => [fakeTrack("video")],
      getTracks: () => [fakeTrack("audio"), fakeTrack("video")],
    }) as unknown as MediaStream,
    getDisplayMedia: async () => fakeDisplayStream(fakeTrack("video", "screen-track")),
    addLocalTrack: () => { calls.push("addLocalTrack"); return {} as RTCRtpSender; },
    removeLocalTrack: () => { calls.push("removeLocalTrack"); },
    send: (m: unknown) => { sent.push(m); calls.push(`send:${(m as { type: string }).type}`); },
    ...overrides,
  };
}

test("enableCameraAndMicrophone publishes two tracks and resolves after both publication_added", async () => {
  const w = wiring();
  const lp = new LocalParticipant("me");
  lp._wire(w);
  const p = lp.enableCameraAndMicrophone();
  lp._onPublicationAdded("audio", "pub-a");
  lp._onPublicationAdded("video", "pub-v");
  await p;
  expect(lp.publications.get("pub-a")?.kind).toBe("audio");
  expect(lp.publications.get("pub-v")?.kind).toBe("video");
});

test("enableCameraAndMicrophone rejects with PublishError when getUserMedia fails", async () => {
  const lp = new LocalParticipant("me");
  lp._wire(wiring({ getUserMedia: async () => { throw new DOMException("denied", "NotAllowedError"); } }));
  await expect(lp.enableCameraAndMicrophone()).rejects.toBeInstanceOf(PublishError);
});

test("setMicrophoneEnabled toggles the track and sends set_mute", async () => {
  const w = wiring();
  const lp = new LocalParticipant("me");
  lp._wire(w);
  const p = lp.enableCameraAndMicrophone();
  lp._onPublicationAdded("audio", "pub-a");
  lp._onPublicationAdded("video", "pub-v");
  await p;
  await lp.setMicrophoneEnabled(false);
  expect(w.sent).toContainEqual({ type: "set_mute", publicationId: "pub-a", muted: true, kind: "audio" });
});

test("_onPublicationMuted flips the flag and _onPublicationRemoved drops the publication", async () => {
  const lp = new LocalParticipant("me");
  lp._wire(wiring());
  const p = lp.enableCameraAndMicrophone();
  lp._onPublicationAdded("audio", "pub-a");
  lp._onPublicationAdded("video", "pub-v");
  await p;

  lp._onPublicationMuted("pub-a", true);
  expect(lp.publications.get("pub-a")?.muted).toBe(true);
  lp._onPublicationMuted("pub-a", false);
  expect(lp.publications.get("pub-a")?.muted).toBe(false);

  lp._onPublicationRemoved("pub-a");
  expect(lp.publications.has("pub-a")).toBe(false);
  expect(lp.publications.has("pub-v")).toBe(true);
  lp._onPublicationRemoved("nope");     // unknown id is a no-op
  expect(lp.publications.size).toBe(1);
});

test("enableCameraAndMicrophone rejects with PublishError when _onPublishDenied fires before publications arrive", async () => {
  const lp = new LocalParticipant("me");
  lp._wire(wiring());
  const p = lp.enableCameraAndMicrophone();
  lp._onPublishDenied();
  await expect(p).rejects.toBeInstanceOf(PublishError);
});

// --- screen share ----------------------------------------------------------

test("startScreenShare declares source, adds the track and resolves on the screen publication_added", async () => {
  const w = wiring();
  const lp = new LocalParticipant("me");
  lp._wire(w);

  const started = lp.startScreenShare();
  await Promise.resolve(); // let getDisplayMedia resolve
  lp._onScreenPublicationAdded("pub-screen", "video");
  await started;

  expect(w.sent).toContainEqual({ type: "publish", trackId: "screen-track", kind: "video", source: "screen" });
  // the publish intent is sent BEFORE the track is added
  expect(w.calls.indexOf("send:publish")).toBeLessThan(w.calls.indexOf("addLocalTrack"));

  const pub = lp.publications.get("pub-screen");
  expect(pub?.kind).toBe("video");
  expect(pub?.source).toBe("screen");
  expect(lp.isScreenSharing).toBe(true);
});

test("camera + screen coexist and a camera mute does not touch the screen publication", async () => {
  const w = wiring();
  const lp = new LocalParticipant("me");
  lp._wire(w);

  const cam = lp.enableCameraAndMicrophone();
  lp._onPublicationAdded("audio", "pub-a");
  lp._onPublicationAdded("video", "pub-cam");
  await cam;

  const scr = lp.startScreenShare();
  lp._onScreenPublicationAdded("pub-screen", "video");
  await scr;

  expect(lp.publications.get("pub-cam")?.source).toBe("camera");
  expect(lp.publications.get("pub-screen")?.source).toBe("screen");

  await lp.setCameraEnabled(false);
  expect(lp.publications.get("pub-cam")?.muted).toBe(true);
  expect(lp.publications.get("pub-screen")?.muted).toBe(false);
  expect(w.sent).not.toContainEqual(
    expect.objectContaining({ type: "set_mute", publicationId: "pub-screen" }),
  );
});

test("stopScreenShare unpublishes, stops the track and clears state", async () => {
  const stopped: string[] = [];
  const track = fakeTrack("video", "screen-track");
  (track as unknown as { stop: () => void }).stop = () => stopped.push("screen-track");
  const w = wiring({ getDisplayMedia: async () => fakeDisplayStream(track) });
  const lp = new LocalParticipant("me");
  lp._wire(w);

  const started = lp.startScreenShare();
  lp._onScreenPublicationAdded("pub-screen", "video");
  await started;

  await lp.stopScreenShare();
  expect(w.sent).toContainEqual({ type: "unpublish", publicationId: "pub-screen" });
  expect(stopped).toContain("screen-track");
  expect(lp.isScreenSharing).toBe(false);
  expect(lp.publications.has("pub-screen")).toBe(false);
});

test("track.onended (browser 'Stop sharing') auto-unpublishes", async () => {
  const track = fakeTrack("video", "screen-track");
  const w = wiring({ getDisplayMedia: async () => fakeDisplayStream(track) });
  const lp = new LocalParticipant("me");
  lp._wire(w);

  const started = lp.startScreenShare();
  lp._onScreenPublicationAdded("pub-screen", "video");
  await started;

  (track as unknown as { dispatch: (e: string) => void }).dispatch("ended");
  await Promise.resolve();

  expect(lp.isScreenSharing).toBe(false);
  expect(w.sent).toContainEqual({ type: "unpublish", publicationId: "pub-screen" });
});

test("startScreenShare rejects with PublishError when getDisplayMedia fails", async () => {
  const lp = new LocalParticipant("me");
  lp._wire(wiring({ getDisplayMedia: async () => { throw new DOMException("denied", "NotAllowedError"); } }));
  await expect(lp.startScreenShare()).rejects.toBeInstanceOf(PublishError);
  expect(lp.isScreenSharing).toBe(false);
});

test("startScreenShare while already sharing is a no-op", async () => {
  const w = wiring();
  const lp = new LocalParticipant("me");
  lp._wire(w);
  const started = lp.startScreenShare();
  lp._onScreenPublicationAdded("pub-screen", "video");
  await started;

  await lp.startScreenShare();
  const publishMsgs = w.sent.filter((m) => (m as { type?: string }).type === "publish");
  expect(publishMsgs).toHaveLength(1);
});

test("_resetForReconnect drops the screen share (not auto-restored)", async () => {
  const w = wiring();
  const lp = new LocalParticipant("me");
  lp._wire(w);
  const started = lp.startScreenShare();
  lp._onScreenPublicationAdded("pub-screen", "video");
  await started;

  lp._resetForReconnect();
  expect(lp.isScreenSharing).toBe(false);
});

test("startScreenShare with capture audio publishes a second audio publication (source=screen)", async () => {
  const vid = fakeTrack("video", "scr-vid");
  const aud = fakeTrack("audio", "scr-aud");
  const w = wiring({ getDisplayMedia: async () => fakeDisplayStream(vid, aud) });
  const lp = new LocalParticipant("me");
  lp._wire(w);

  const started = lp.startScreenShare();
  await Promise.resolve();
  lp._onScreenPublicationAdded("pub-scr-v", "video");
  lp._onScreenPublicationAdded("pub-scr-a", "audio");
  await started;

  expect(w.sent).toContainEqual({ type: "publish", trackId: "scr-vid", kind: "video", source: "screen" });
  expect(w.sent).toContainEqual({ type: "publish", trackId: "scr-aud", kind: "audio", source: "screen" });
  expect(lp.publications.get("pub-scr-v")?.kind).toBe("video");
  expect(lp.publications.get("pub-scr-a")?.kind).toBe("audio");
  expect(lp.publications.get("pub-scr-a")?.source).toBe("screen");

  // mic mute must not touch the screen audio publication
  const cam = lp.enableCameraAndMicrophone();
  lp._onPublicationAdded("audio", "pub-mic");
  lp._onPublicationAdded("video", "pub-cam");
  await cam;
  w.sent.length = 0;
  await lp.setMicrophoneEnabled(false);
  expect(w.sent).toContainEqual({ type: "set_mute", publicationId: "pub-mic", muted: true, kind: "audio" });
  expect(w.sent).not.toContainEqual(expect.objectContaining({ publicationId: "pub-scr-a" }));

  await lp.stopScreenShare();
  expect(w.sent).toContainEqual({ type: "unpublish", publicationId: "pub-scr-v" });
  expect(w.sent).toContainEqual({ type: "unpublish", publicationId: "pub-scr-a" });
});

test("SFU-driven publication_removed for the screen reconciles without a second unpublish", async () => {
  const w = wiring();
  const lp = new LocalParticipant("me");
  lp._wire(w);
  const started = lp.startScreenShare();
  lp._onScreenPublicationAdded("pub-screen", "video");
  await started;
  w.sent.length = 0;

  lp._onPublicationRemoved("pub-screen");
  expect(lp.isScreenSharing).toBe(false);
  expect(w.sent).not.toContainEqual(expect.objectContaining({ type: "unpublish" }));
});
