import { FakeWebSocket, FakeRTCPeerConnection, fakeStatsReport } from "./fakes.js";

test("FakeWebSocket records sent JSON and delivers server messages", () => {
  const ws = new FakeWebSocket("wss://x/ws", ["pulsertc"]);
  const seen: unknown[] = [];
  ws.onmessage = (ev) => seen.push(JSON.parse(ev.data));
  ws.open();
  ws.send(JSON.stringify({ type: "join", roomId: "r" }));
  ws.serverSend({ type: "welcome", participantId: "p1" });
  expect(ws.sent).toEqual([{ type: "join", roomId: "r" }]);
  expect(seen).toEqual([{ type: "welcome", participantId: "p1" }]);
});

test("FakeRTCPeerConnection emits ontrack on command", () => {
  const pc = new FakeRTCPeerConnection();
  const tracks: MediaStreamTrack[] = [];
  pc.ontrack = (ev: RTCTrackEvent) => tracks.push(ev.track);
  const t = { id: "pub-1", kind: "video" } as MediaStreamTrack;
  pc.emitTrack(t, [{ id: "publisher-1" } as MediaStream]);
  expect(tracks[0]?.id).toBe("pub-1");
});

test("fakeStatsReport is forEach-iterable", () => {
  const r = fakeStatsReport([{ type: "outbound-rtp", kind: "video", packetsSent: 10 }]);
  const seen: unknown[] = [];
  r.forEach((s) => seen.push(s));
  expect(seen).toHaveLength(1);
});
