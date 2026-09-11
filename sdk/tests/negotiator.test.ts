import { Negotiator } from "../src/negotiator.js";
import { FakeRTCPeerConnection } from "./fakes.js";

function setup() {
  let pc!: FakeRTCPeerConnection;
  const sent: unknown[] = [];
  const n = new Negotiator(() => (pc = new FakeRTCPeerConnection()) as unknown as RTCPeerConnection, (m) => sent.push(m));
  n.create([{ urls: ["stun:x:1"] }]);
  return { n, getPc: () => pc, sent };
}

test("a remote offer is answered", async () => {
  const { n, sent } = setup();
  await n.handleSignal({ type: "sfu_offer", payload: { sdp: "remote-offer" } });
  expect(sent).toContainEqual({ type: "sfu_answer", payload: { sdp: "fake-answer" } });
});

test("glare: an incoming offer while making our own offer is handled by rollback (polite)", async () => {
  const { n, getPc, sent } = setup();
  getPc().signalingState = "have-local-offer";           // we have an outstanding offer
  await n.handleSignal({ type: "sfu_offer", payload: { sdp: "remote-offer" } });
  // polite peer still answers after rolling back
  expect(sent).toContainEqual({ type: "sfu_answer", payload: { sdp: "fake-answer" } });
  expect(getPc().signalingState).toBe("stable");
});

test("ICE candidates before the remote description are buffered then flushed", async () => {
  const { n, getPc } = setup();
  await n.handleSignal({ type: "sfu_ice_candidate", payload: { candidate: "cand:1" } });
  expect(getPc().addedIce).toEqual([]);                  // buffered
  await n.handleSignal({ type: "sfu_offer", payload: { sdp: "remote-offer" } });
  expect(getPc().addedIce).toEqual([{ candidate: "cand:1" }]);
});

test("ontrack forwards the track and its stream id", () => {
  const { n, getPc } = setup();
  const seen: [string, string][] = [];
  n.onTrack = (t, sid) => seen.push([t.id, sid]);
  getPc().emitTrack({ id: "pub-7" } as MediaStreamTrack, [{ id: "publisher-7" } as MediaStream]);
  expect(seen).toEqual([["pub-7", "publisher-7"]]);
});
