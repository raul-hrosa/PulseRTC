import { Room } from "../src/room.js";
import { RoomEvent, ConnectionState } from "../src/events.js";
import { ConnectionError, TokenError } from "../src/errors.js";
import { FakeWebSocket, FakeRTCPeerConnection } from "./fakes.js";
import type { RemoteParticipant } from "../src/participants.js";

function makeRoom() {
  let ws!: FakeWebSocket;
  const room = new Room({ qoe: false }, {
    wsFactory: (u, p) => (ws = new FakeWebSocket(u, p)),
    pcFactory: () => new FakeRTCPeerConnection() as unknown as RTCPeerConnection,
    getUserMedia: async () => ({ getTracks: () => [], getAudioTracks: () => [], getVideoTracks: () => [] }) as unknown as MediaStream,
    now: () => 0,
  });
  return { room, getWs: () => ws };
}

/** A Room whose getUserMedia yields a real (fake) audio+video stream. */
function makeMediaRoom() {
  let ws!: FakeWebSocket;
  const track = (kind: "audio" | "video") =>
    ({ kind, enabled: true, id: `t-${kind}`, stop() {} }) as unknown as MediaStreamTrack;
  const room = new Room({ qoe: false }, {
    wsFactory: (u, p) => (ws = new FakeWebSocket(u, p)),
    pcFactory: () => new FakeRTCPeerConnection() as unknown as RTCPeerConnection,
    getUserMedia: async () => ({
      getAudioTracks: () => [track("audio")],
      getVideoTracks: () => [track("video")],
      getTracks: () => [track("audio"), track("video")],
    }) as unknown as MediaStream,
    now: () => 0,
  });
  return { room, getWs: () => ws };
}

test("connect sends join with the roomId, resolves on room_joined, moves to CONNECTED", async () => {
  const { room, getWs } = makeRoom();
  const states: ConnectionState[] = [];
  room.on(RoomEvent.ConnectionStateChanged, (s) => states.push(s));
  const p = room.connect("wss://h/ws", "tok", "r");
  getWs().open();
  expect(getWs().sent[0]).toEqual({ type: "join", roomId: "r" });
  getWs().serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  getWs().serverSend({ type: "room_joined", roomId: "r", participants: [] });
  await p;
  expect(room.state).toBe(ConnectionState.Connected);
  expect(states).toEqual([ConnectionState.Connecting, ConnectionState.Connected]);
});

test("connect rejects with TokenError on an auth error before room_joined", async () => {
  const { room, getWs } = makeRoom();
  const p = room.connect("wss://h/ws", "tok", "r");
  getWs().open();
  getWs().serverSend({ type: "error", code: "EXPIRED_TOKEN", message: "expired" });
  await expect(p).rejects.toBeInstanceOf(TokenError);
});

test("participant_joined then publication_added emits ParticipantConnected + (on track) TrackSubscribed", async () => {
  const { room, getWs } = makeRoom();
  await connectRoom(room, getWs);
  const joined: string[] = [];
  const subs: string[] = [];
  room.on(RoomEvent.ParticipantConnected, (rp) => joined.push(rp.identity));
  room.on(RoomEvent.TrackSubscribed, (_t, pub, rp) => subs.push(`${rp.identity}/${pub.id}`));
  getWs().serverSend({ type: "participant_joined", participantId: "bob.bb", name: "Bob" });
  getWs().serverSend({ type: "publication_added", participantId: "bob.bb", publicationId: "pub-1", kind: "video", muted: false });
  (room.engine.pc as unknown as FakeRTCPeerConnection).emitTrack({ id: "pub-1" } as MediaStreamTrack, [{ id: "bob.bb" } as MediaStream]);
  expect(joined).toEqual(["bob.bb"]);
  expect(subs).toEqual(["bob.bb/pub-1"]);
});

test("off() detaches a listener registered with on()", async () => {
  const { room, getWs } = makeRoom();
  await connectRoom(room, getWs);
  const joined: string[] = [];
  const fn = (rp: RemoteParticipant) => joined.push(rp.identity);
  room.on(RoomEvent.ParticipantConnected, fn);
  room.off(RoomEvent.ParticipantConnected, fn);
  getWs().serverSend({ type: "participant_joined", participantId: "bob.bb", name: "Bob" });
  expect(joined).toEqual([]);
});

test("off() removes only the registration for the event it names", async () => {
  const { room, getWs } = makeRoom();
  await connectRoom(room, getWs);
  const seen: string[] = [];
  const fn = () => seen.push("hit");
  room.on(RoomEvent.ParticipantConnected, fn as never);
  room.on(RoomEvent.ParticipantDisconnected, fn as never);
  room.off(RoomEvent.ParticipantConnected, fn as never);

  getWs().serverSend({ type: "participant_joined", participantId: "bob.bb" });
  expect(seen).toEqual([]);                                  // detached
  getWs().serverSend({ type: "participant_left", participantId: "bob.bb" });
  expect(seen).toEqual(["hit"]);                             // still attached on the other event
});

test("disconnect() rejects a connect() that is still waiting for room_joined", async () => {
  const { room, getWs } = makeRoom();
  const p = room.connect("wss://h/ws", "tok", "r");
  getWs().open();
  getWs().serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  await room.disconnect();
  await expect(p).rejects.toBeInstanceOf(ConnectionError);
  expect(room.state).toBe(ConnectionState.Disconnected);
});

test("a disconnect() -> connect() cycle starts a fresh session", async () => {
  const { room, getWs } = makeRoom();
  const p = room.connect("wss://h/ws", "tok", "r");
  getWs().open();
  getWs().serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  getWs().serverSend({ type: "room_joined", roomId: "r", participants: [{ participantId: "bob.bb" }], sessionId: "s1", generation: 3 });
  await p;
  expect(room.remoteParticipants.size).toBe(1);

  await room.disconnect();
  expect(room.remoteParticipants.size).toBe(0);
  expect(room.localParticipant.identity).toBe("");

  const p2 = room.connect("wss://h/ws", "tok", "r");
  getWs().open();
  expect(getWs().sent[0]).toEqual({ type: "join", roomId: "r" });   // no resume from the old session
  getWs().serverSend({ type: "welcome", participantId: "me.zz", userId: "me", iceServers: [] });
  getWs().serverSend({ type: "room_joined", roomId: "r", participants: [] });
  await p2;
  expect(room.state).toBe(ConnectionState.Connected);
});

test("local publication_muted / publication_removed echoes update the LocalParticipant", async () => {
  const { room, getWs } = makeMediaRoom();
  const p = room.connect("wss://h/ws", "tok", "r");
  getWs().open();
  getWs().serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  getWs().serverSend({ type: "room_joined", roomId: "r", participants: [] });
  await p;

  const pub = room.localParticipant.enableCameraAndMicrophone();
  getWs().serverSend({ type: "publication_added", participantId: "me.aa", publicationId: "pub-a", kind: "audio", muted: false });
  getWs().serverSend({ type: "publication_added", participantId: "me.aa", publicationId: "pub-v", kind: "video", muted: false });
  await pub;
  expect(room.localParticipant.publications.size).toBe(2);
  expect(room.remoteParticipants.has("me.aa")).toBe(false);   // own echoes never reach the remote store

  // muted echo (e.g. a server-side mute we did not initiate) flips the flag
  getWs().serverSend({ type: "publication_muted", participantId: "me.aa", publicationId: "pub-a", kind: "audio", muted: true });
  expect(room.localParticipant.publications.get("pub-a")?.muted).toBe(true);

  // removed echo drops the publication
  getWs().serverSend({ type: "publication_removed", participantId: "me.aa", publicationId: "pub-a", kind: "audio", muted: true });
  expect(room.localParticipant.publications.has("pub-a")).toBe(false);
  expect(room.localParticipant.publications.has("pub-v")).toBe(true);
});

test("an unrecognised error after CONNECTED surfaces as RoomEvent.Error", async () => {
  const { room, getWs } = makeRoom();
  await connectRoom(room, getWs);
  const errs: Error[] = [];
  room.on(RoomEvent.Error, (e) => errs.push(e));
  getWs().serverSend({ type: "error", code: "SOMETHING_NEW", message: "boom" });
  expect(errs).toHaveLength(1);
  expect(errs[0]?.message).toBe("boom");
  expect(room.state).toBe(ConnectionState.Connected);              // not fatal
});

test("room_closed disconnects with reason room_closed", async () => {
  const { room, getWs } = makeRoom();
  await connectRoom(room, getWs);
  const reasons: string[] = [];
  room.on(RoomEvent.Disconnected, (r) => reasons.push(r));
  getWs().serverSend({ type: "room_closed", roomId: "r", reason: "closed_by_api" });
  expect(reasons).toEqual(["room_closed"]);
  expect(room.state).toBe(ConnectionState.Disconnected);
});

async function connectRoom(room: Room, getWs: () => FakeWebSocket) {
  const p = room.connect("wss://h/ws", "tok", "r");
  getWs().open();
  getWs().serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  getWs().serverSend({ type: "room_joined", roomId: "r", participants: [] });
  await p;
}
