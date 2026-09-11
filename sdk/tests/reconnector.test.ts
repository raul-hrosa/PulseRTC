import { nextReconnectDelay, Reconnector, decideRejoin } from "../src/reconnector.js";

const hints = { initialDelayMs: 500, maxDelayMs: 10000, jitter: 0.2 };

test("delay doubles and caps, jitter within [0, base*jitter)", () => {
  expect(nextReconnectDelay(0, hints, () => 0)).toBe(500);
  expect(nextReconnectDelay(1, hints, () => 0)).toBe(1000);
  expect(nextReconnectDelay(5, hints, () => 0)).toBe(10000);        // capped
  expect(nextReconnectDelay(0, hints, () => 0.5)).toBe(500 + 0.5 * 500 * 0.2);
});

test("Reconnector arms with an increasing attempt count and can reset", () => {
  const runs: number[] = [];
  let fire: () => void = () => {};
  const r = new Reconnector({ hints, rnd: () => 0, schedule: (fn) => { fire = fn; return () => {}; } });
  r.arm(() => runs.push(r.attempt));
  fire();
  r.arm(() => runs.push(r.attempt));
  fire();
  expect(runs).toEqual([1, 2]);
  r.reset();
  expect(r.attempt).toBe(0);
});

test("decideRejoin resumes when a session is held, fresh otherwise", () => {
  expect(decideRejoin({ sessionId: "s", generation: 2 })).toEqual({ type: "resume", resume: { sessionId: "s", generation: 2 } });
  expect(decideRejoin(null)).toEqual({ type: "fresh" });
});
