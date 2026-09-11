import { Signaling } from "../src/signaling.js";
import { FakeWebSocket } from "./fakes.js";

function setup() {
  let ws!: FakeWebSocket;
  const s = new Signaling((url, protocols) => (ws = new FakeWebSocket(url, protocols)));
  return { s, getWs: () => ws };
}

test("connect opens the socket with the token subprotocol", () => {
  const { s, getWs } = setup();
  s.connect("wss://h/ws", "JWT123");
  expect(getWs().protocols).toEqual(["pulsertc", "pulsertc.token.JWT123"]);
});

test("send is buffered until open, then flushed", () => {
  const { s, getWs } = setup();
  s.connect("wss://h/ws", "t");
  s.send({ type: "join", roomId: "r" });
  expect(getWs().sent).toEqual([]);          // not open yet
  getWs().open();
  expect(getWs().sent).toEqual([{ type: "join", roomId: "r" }]);
});

test("valid frames are dispatched, junk is ignored", () => {
  const { s, getWs } = setup();
  const seen: string[] = [];
  s.onMessage = (m) => seen.push(m.type);
  s.connect("wss://h/ws", "t");
  getWs().open();
  getWs().serverSend({ type: "welcome", participantId: "p" });
  getWs().onmessage?.({ data: "not json" });
  getWs().onmessage?.({ data: JSON.stringify({ nope: 1 }) });
  expect(seen).toEqual(["welcome"]);
});

test("unexpected close is reported as unclean", () => {
  const { s, getWs } = setup();
  let info: { code: number; clean: boolean } | undefined;
  s.onClose = (i) => (info = i);
  s.connect("wss://h/ws", "t");
  getWs().open();
  getWs().serverClose(1006);
  expect(info).toEqual({ code: 1006, clean: false });
});

test("owner close() is clean and idempotent", () => {
  const { s, getWs } = setup();
  const calls: boolean[] = [];
  s.onClose = (i) => calls.push(i.clean);
  s.connect("wss://h/ws", "t");
  getWs().open();
  s.close();
  s.close();
  expect(calls).toEqual([true]);
});
