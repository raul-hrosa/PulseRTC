import { QoEReporter, parseQualityEvent } from "../src/qoe.js";
import { fakeStatsReport } from "./fakes.js";

test("collectOnce turns outbound/inbound stats into cumulative samples", async () => {
  const report = fakeStatsReport([
    { id: "o1", type: "outbound-rtp", kind: "video", packetsSent: 100, bytesSent: 5000 },
    { id: "i1", type: "inbound-rtp", kind: "audio", packetsReceived: 200, packetsLost: 2, bytesReceived: 3000 },
    { id: "cp", type: "candidate-pair", nominated: true, state: "succeeded", currentRoundTripTime: 0.05 },
  ]);
  const r = new QoEReporter({
    getStats: async () => report,
    send: () => {},
    localPubId: (k) => (k === "video" ? "pub-v" : "pub-a"),
    now: () => 1000,
  });
  const samples = (await r.collectOnce()) as Record<string, unknown>[];
  expect(samples.find((s) => s["direction"] === "outbound")).toMatchObject({ kind: "video", packetsSent: 100, publicationId: "pub-v" });
  expect(samples.find((s) => s["direction"] === "inbound")).toMatchObject({ kind: "audio", packetsReceived: 200 });
  expect(samples.find((s) => s["kind"] === "connection")).toMatchObject({ rttMs: 50 });
});

test("collectOnce ignores a nominated candidate pair that is not succeeded", async () => {
  const r = new QoEReporter({
    getStats: async () => fakeStatsReport([
      { id: "cp", type: "candidate-pair", nominated: true, state: "in-progress", currentRoundTripTime: 0.05 },
    ]),
    send: () => {}, localPubId: () => undefined, now: () => 0,
  });
  const samples = (await r.collectOnce()) as Record<string, unknown>[];
  // only the always-on placeholder, without an rtt read off a half-open pair
  expect(samples).toEqual([{ kind: "connection", tMs: 0 }]);
});

test("collectOnce always emits a connection sample", async () => {
  const r = new QoEReporter({
    getStats: async () => fakeStatsReport([]),
    send: () => {}, localPubId: () => undefined, now: () => 0,
  });
  const samples = (await r.collectOnce()) as Record<string, unknown>[];
  expect(samples.some((s) => s["kind"] === "connection")).toBe(true);
});

test("parseQualityEvent maps the wire shape", () => {
  const q = parseQualityEvent({
    type: "quality_degraded", participantId: "p", direction: "inbound",
    mediaType: "video", from: "GOOD", status: "WARNING", reason: "HIGH_PACKET_LOSS",
  });
  expect(q).toEqual({ status: "WARNING", from: "GOOD", direction: "inbound", mediaType: "video", reason: "HIGH_PACKET_LOSS" });
});
