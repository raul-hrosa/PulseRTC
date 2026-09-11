import { Room } from "../src/room.js";
import { RoomEvent, ConnectionState } from "../src/events.js";
import { FakeWebSocket, FakeRTCPeerConnection } from "./fakes.js";

// `noUncheckedIndexedAccess` is on: this narrows `sockets[i]` / `pcs[i]` in the
// bodies below without cluttering every line with `!`.
function at<T>(xs: T[], i: number): T {
  const x = xs[i];
  if (x === undefined) throw new Error(`no element at index ${i}`);
  return x;
}

function makeRoom() {
  const sockets: FakeWebSocket[] = [];
  const room = new Room({ qoe: false }, {
    wsFactory: (u, p) => { const w = new FakeWebSocket(u, p); sockets.push(w); return w; },
    pcFactory: () => new FakeRTCPeerConnection() as unknown as RTCPeerConnection,
    getUserMedia: async () => ({ getAudioTracks: () => [], getVideoTracks: () => [], getTracks: () => [] }) as unknown as MediaStream,
    now: () => 0,
  });
  // deterministic, instant reconnect scheduling
  (room as unknown as { reconnector: { arm(fn: () => void): void } }).reconnector.arm = (fn: () => void) => fn();
  return { room, sockets };
}

test("an unclean socket close after CONNECTED drives RECONNECTING then CONNECTED via resume", async () => {
  const { room, sockets } = makeRoom();
  const states: ConnectionState[] = [];
  room.on(RoomEvent.ConnectionStateChanged, (s) => states.push(s));

  const p = room.connect("wss://h/ws", "tok", "r");
  at(sockets, 0).open();
  at(sockets, 0).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  at(sockets, 0).serverSend({ type: "room_joined", roomId: "r", participants: [], sessionId: "s1", generation: 1,
    reconnect: { initialDelayMs: 1, maxDelayMs: 1, jitter: 0 } });
  await p;

  at(sockets, 0).serverClose(1006);          // unclean drop -> arm() fires synchronously -> new socket
  expect(sockets).toHaveLength(2);
  at(sockets, 1).open();
  expect(at(sockets, 1).sent[0]).toEqual({ type: "join", roomId: "r", resume: { sessionId: "s1", generation: 1 } });
  at(sockets, 1).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  at(sockets, 1).serverSend({ type: "room_joined", roomId: "r", participants: [], reconnected: true, sessionId: "s1", generation: 2 });

  expect(states).toEqual([
    ConnectionState.Connecting, ConnectionState.Connected,
    ConnectionState.Reconnecting, ConnectionState.Connected,
  ]);
});

test("session.stale drops the resume so the next join is fresh", async () => {
  const { room, sockets } = makeRoom();
  const p = room.connect("wss://h/ws", "tok", "r");
  at(sockets, 0).open();
  at(sockets, 0).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  at(sockets, 0).serverSend({ type: "room_joined", roomId: "r", participants: [], sessionId: "s1", generation: 1 });
  await p;
  at(sockets, 0).serverSend({ type: "session.stale", reason: "STALE_SESSION" });
  at(sockets, 0).serverClose(1006);
  at(sockets, 1).open();
  expect(at(sockets, 1).sent[0]).toEqual({ type: "join", roomId: "r" });   // no resume
});

test("session.stale answering a join retries a FRESH join on the same live socket", async () => {
  const { room, sockets } = makeRoom();
  const p = room.connect("wss://h/ws", "tok", "r");
  at(sockets, 0).open();
  at(sockets, 0).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  at(sockets, 0).serverSend({ type: "room_joined", roomId: "r", participants: [], sessionId: "s1", generation: 1 });
  await p;

  at(sockets, 0).serverClose(1006);
  at(sockets, 1).open();
  at(sockets, 1).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  expect(at(sockets, 1).sent[0]).toEqual({ type: "join", roomId: "r", resume: { sessionId: "s1", generation: 1 } });

  // The server refuses the resume and ABORTS the join without closing the
  // socket: no room_joined and no onclose would ever follow, so the SDK has to
  // retry by itself, on this very socket.
  at(sockets, 1).serverSend({ type: "session.stale", reason: "STALE_SESSION" });
  expect(sockets).toHaveLength(2);                                     // no new socket
  expect(at(sockets, 1).sent[1]).toEqual({ type: "join", roomId: "r" }); // fresh, no resume

  at(sockets, 1).serverSend({ type: "room_joined", roomId: "r", participants: [], sessionId: "s2", generation: 1 });
  expect(room.state).toBe(ConnectionState.Connected);
});

test("session.stale on a first connect still settles the connect() promise", async () => {
  const { room, sockets } = makeRoom();
  const p = room.connect("wss://h/ws", "tok", "r");
  at(sockets, 0).open();
  at(sockets, 0).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  at(sockets, 0).serverSend({ type: "session.stale", reason: "STALE_SESSION" });
  at(sockets, 0).serverSend({ type: "room_joined", roomId: "r", participants: [] });
  await p;
  expect(room.state).toBe(ConnectionState.Connected);
});

test("session.recovery_failed drops the resume so the next join is fresh", async () => {
  const { room, sockets } = makeRoom();
  const p = room.connect("wss://h/ws", "tok", "r");
  at(sockets, 0).open();
  at(sockets, 0).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  at(sockets, 0).serverSend({ type: "room_joined", roomId: "r", participants: [], sessionId: "s1", generation: 1 });
  await p;
  at(sockets, 0).serverSend({ type: "session.recovery_failed", reason: "RECOVERY_TIMEOUT" });
  at(sockets, 0).serverClose(1006);
  at(sockets, 1).open();
  expect(at(sockets, 1).sent[0]).toEqual({ type: "join", roomId: "r" });
});

test("a reconnect reconciles membership instead of unioning it", async () => {
  const { room, sockets } = makeRoom();
  const joined: string[] = [];
  const left: string[] = [];
  room.on(RoomEvent.ParticipantConnected, (rp) => joined.push(rp.identity));
  room.on(RoomEvent.ParticipantDisconnected, (rp) => left.push(rp.identity));

  const p = room.connect("wss://h/ws", "tok", "r");
  at(sockets, 0).open();
  at(sockets, 0).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  at(sockets, 0).serverSend({ type: "room_joined", roomId: "r", sessionId: "s1", generation: 1,
    participants: [{ participantId: "bob.bb" }, { participantId: "carol.cc" }] });
  await p;
  expect(joined).toEqual(["bob.bb", "carol.cc"]);

  at(sockets, 0).serverClose(1006);
  at(sockets, 1).open();
  at(sockets, 1).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  // carol left while we were away; bob is unchanged; dave is new
  at(sockets, 1).serverSend({ type: "room_joined", roomId: "r", reconnected: true, sessionId: "s1", generation: 2,
    participants: [{ participantId: "bob.bb" }, { participantId: "dave.dd" }] });

  expect(left).toEqual(["carol.cc"]);
  expect(joined).toEqual(["bob.bb", "carol.cc", "dave.dd"]);   // bob NOT re-announced
  expect([...room.remoteParticipants.keys()]).toEqual(["bob.bb", "dave.dd"]);
});

test("closing the pc on a reconnect drops the stale remote track handles", async () => {
  const { room, sockets } = makeRoom();
  const p = room.connect("wss://h/ws", "tok", "r");
  at(sockets, 0).open();
  at(sockets, 0).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  at(sockets, 0).serverSend({ type: "room_joined", roomId: "r", participants: [{ participantId: "bob.bb" }],
    sessionId: "s1", generation: 1 });
  await p;
  at(sockets, 0).serverSend({ type: "publication_added", participantId: "bob.bb", publicationId: "pub-1", kind: "video", muted: false });
  (room.engine.pc as unknown as FakeRTCPeerConnection)
    .emitTrack({ id: "pub-1" } as MediaStreamTrack, [{ id: "bob.bb" } as MediaStream]);
  expect(room.remoteParticipants.get("bob.bb")?.getTrack("video")).toBeDefined();

  at(sockets, 0).serverClose(1006);   // pc is closed -> its tracks are dead
  expect(room.remoteParticipants.get("bob.bb")?.getTrack("video")).toBeUndefined();
});

function makeMediaRoom() {
  const sockets: FakeWebSocket[] = [];
  const pcs: FakeRTCPeerConnection[] = [];
  const track = (kind: "audio" | "video") =>
    ({ kind, enabled: true, id: `t-${kind}`, stop() {} }) as unknown as MediaStreamTrack;
  const room = new Room({ qoe: false }, {
    wsFactory: (u, p) => { const w = new FakeWebSocket(u, p); sockets.push(w); return w; },
    pcFactory: () => { const pc = new FakeRTCPeerConnection(); pcs.push(pc); return pc as unknown as RTCPeerConnection; },
    getUserMedia: async () => ({
      getAudioTracks: () => [track("audio")],
      getVideoTracks: () => [track("video")],
      getTracks: () => [track("audio"), track("video")],
    }) as unknown as MediaStream,
    now: () => 0,
  });
  (room as unknown as { reconnector: { arm(fn: () => void): void } }).reconnector.arm = (fn: () => void) => fn();
  return { room, sockets, pcs };
}

test("reconnected:true re-publishes the local tracks against the fresh pc", async () => {
  const { room, sockets, pcs } = makeMediaRoom();
  const p = room.connect("wss://h/ws", "tok", "r");
  at(sockets, 0).open();
  at(sockets, 0).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  at(sockets, 0).serverSend({ type: "room_joined", roomId: "r", participants: [], sessionId: "s1", generation: 1 });
  await p;

  const pub = room.localParticipant.enableCameraAndMicrophone();
  at(sockets, 0).serverSend({ type: "publication_added", participantId: "me.aa", publicationId: "pub-a", kind: "audio", muted: false });
  at(sockets, 0).serverSend({ type: "publication_added", participantId: "me.aa", publicationId: "pub-v", kind: "video", muted: false });
  await pub;
  expect(room.localParticipant.publications.size).toBe(2);

  at(sockets, 0).serverClose(1006);
  expect(sockets).toHaveLength(2);
  at(sockets, 1).open();
  at(sockets, 1).serverSend({ type: "welcome", participantId: "me.aa", userId: "me", iceServers: [] });
  at(sockets, 1).serverSend({ type: "room_joined", roomId: "r", participants: [], reconnected: true, sessionId: "s1", generation: 2 });
  await Promise.resolve();
  at(sockets, 1).serverSend({ type: "publication_added", participantId: "me.aa", publicationId: "pub-a2", kind: "audio", muted: false });
  at(sockets, 1).serverSend({ type: "publication_added", participantId: "me.aa", publicationId: "pub-v2", kind: "video", muted: false });
  await new Promise((r) => setTimeout(r, 0));

  expect(pcs.reduce((n, pc) => n + pc.senders.length, 0)).toBe(4); // 2 initial + 2 on republish
  expect(room.localParticipant.publications.size).toBe(2);
});
