import { ParticipantStore } from "../src/participants.js";

function store() {
  const s = new ParticipantStore();
  s.applyWelcome({ type: "welcome", participantId: "me.aaaa", userId: "me" });
  return s;
}

test("participant_joined then publication_added builds a remote publication", () => {
  const s = store();
  s.applyParticipantJoined({ type: "participant_joined", participantId: "bob.bbbb", name: "Bob" });
  const r = s.applyPublicationEvent({
    type: "publication_added", participantId: "bob.bbbb", publicationId: "pub-1", kind: "video", muted: false,
  });
  expect(r?.publication.id).toBe("pub-1");
  expect(s.remote.get("bob.bbbb")?.publications.get("pub-1")?.kind).toBe("video");
});

test("own publication echo returns undefined", () => {
  const s = store();
  const r = s.applyPublicationEvent({
    type: "publication_added", participantId: "me.aaaa", publicationId: "pub-x", kind: "audio", muted: false,
  });
  expect(r).toBeUndefined();
});

// A real browser gives RTCRtpReceiver.track.id a fresh UUID, never the SFU's
// publicationId — so tracks match publications by (streamId, kind), not by id.
const browserTrack = (kind: "audio" | "video"): MediaStreamTrack =>
  ({ id: `br-${kind}-${Math.random().toString(36).slice(2)}`, kind }) as MediaStreamTrack;

test("attachTrack matches a publication by (streamId, kind) — track.id is unrelated", () => {
  const s = store();
  s.applyParticipantJoined({ type: "participant_joined", participantId: "bob.bbbb" });
  s.applyPublicationEvent({
    type: "publication_added", participantId: "bob.bbbb", publicationId: "pub-abc", kind: "video", muted: false,
  });
  const track = browserTrack("video");
  const r = s.attachTrack("bob.bbbb", track);
  expect(r?.publication.id).toBe("pub-abc");
  expect(r?.publication.track).toBe(track);
});

test("attachTrack buffers a track that arrives before its publication_added, then drains by kind", () => {
  const s = store();
  s.applyParticipantJoined({ type: "participant_joined", participantId: "bob.bbbb" });
  const track = browserTrack("video");
  expect(s.attachTrack("bob.bbbb", track)).toBeUndefined();     // buffered — no publication yet
  const r = s.applyPublicationEvent({
    type: "publication_added", participantId: "bob.bbbb", publicationId: "pub-9", kind: "video", muted: false,
  });
  expect(r?.publication.track).toBe(track);
});

test("attachTrack keeps audio and video on their own publications", () => {
  const s = store();
  s.applyParticipantJoined({ type: "participant_joined", participantId: "bob.bbbb" });
  s.applyPublicationEvent({ type: "publication_added", participantId: "bob.bbbb", publicationId: "pa", kind: "audio", muted: false });
  s.applyPublicationEvent({ type: "publication_added", participantId: "bob.bbbb", publicationId: "pv", kind: "video", muted: false });
  const a = browserTrack("audio");
  const v = browserTrack("video");
  s.attachTrack("bob.bbbb", v);
  s.attachTrack("bob.bbbb", a);
  expect(s.remote.get("bob.bbbb")?.publications.get("pa")?.track).toBe(a);
  expect(s.remote.get("bob.bbbb")?.publications.get("pv")?.track).toBe(v);
});

test("publication_muted flips the flag", () => {
  const s = store();
  s.applyParticipantJoined({ type: "participant_joined", participantId: "bob.bbbb" });
  s.applyPublicationEvent({ type: "publication_added", participantId: "bob.bbbb", publicationId: "p1", kind: "audio", muted: false });
  const r = s.applyPublicationEvent({ type: "publication_muted", participantId: "bob.bbbb", publicationId: "p1", kind: "audio", muted: true });
  expect(r?.publication.muted).toBe(true);
});

test("room_joined seeds existing participants and snapshot tracks", () => {
  const s = store();
  const r = s.applyRoomJoined({
    type: "room_joined", roomId: "r",
    participants: [{ participantId: "bob.bbbb", name: "Bob" }],
    snapshot: { type: "room.snapshot", roomId: "r", roomGeneration: 1, participants: [
      { participantId: "bob.bbbb", tracks: [{ publicationId: "p1", kind: "video", muted: false }] },
    ] },
  });
  expect(s.remote.get("bob.bbbb")?.publications.get("p1")?.kind).toBe("video");
  expect(r.added.map((p) => p.identity)).toEqual(["bob.bbbb"]);
  expect(r.removed).toEqual([]);
});

test("room_joined reconciles: unlisted participants are removed, known ones are not re-added", () => {
  const s = store();
  s.applyParticipantJoined({ type: "participant_joined", participantId: "bob.bbbb" });
  s.applyParticipantJoined({ type: "participant_joined", participantId: "gone.cccc" });

  const r = s.applyRoomJoined({
    type: "room_joined", roomId: "r",
    participants: [{ participantId: "bob.bbbb" }, { participantId: "new.dddd" }],
  });

  expect(r.added.map((p) => p.identity)).toEqual(["new.dddd"]);      // bob already known
  expect(r.removed.map((p) => p.identity)).toEqual(["gone.cccc"]);
  expect([...s.remote.keys()]).toEqual(["bob.bbbb", "new.dddd"]);
});

test("room_joined drops publications the snapshot no longer lists", () => {
  const s = store();
  s.applyParticipantJoined({ type: "participant_joined", participantId: "bob.bbbb" });
  s.applyPublicationEvent({ type: "publication_added", participantId: "bob.bbbb", publicationId: "old", kind: "video", muted: false });

  s.applyRoomJoined({
    type: "room_joined", roomId: "r",
    participants: [{ participantId: "bob.bbbb" }],
    snapshot: { type: "room.snapshot", roomId: "r", roomGeneration: 2, participants: [
      { participantId: "bob.bbbb", tracks: [{ publicationId: "fresh", kind: "video", muted: true }] },
    ] },
  });

  expect([...(s.remote.get("bob.bbbb")?.publications.keys() ?? [])]).toEqual(["fresh"]);
  expect(s.remote.get("bob.bbbb")?.publications.get("fresh")?.muted).toBe(true);
});

test("clearTracks drops the handles owned by a closed peer connection", () => {
  const s = store();
  s.applyParticipantJoined({ type: "participant_joined", participantId: "bob.bbbb" });
  s.applyPublicationEvent({ type: "publication_added", participantId: "bob.bbbb", publicationId: "p1", kind: "video", muted: false });
  s.attachTrack("bob.bbbb", { id: "p1", kind: "video" } as MediaStreamTrack);
  expect(s.remote.get("bob.bbbb")?.getTrack("video")).toBeDefined();

  s.clearTracks();
  expect(s.remote.get("bob.bbbb")?.publications.get("p1")?.track).toBeUndefined();
  expect(s.remote.get("bob.bbbb")?.publications.get("p1")).toBeDefined();   // publication survives
});

test("clear forgets everything so the store can be reused", () => {
  const s = store();
  s.applyParticipantJoined({ type: "participant_joined", participantId: "bob.bbbb" });
  s.clear();
  expect(s.remote.size).toBe(0);
  expect(s.localIdentity).toBeUndefined();
});
