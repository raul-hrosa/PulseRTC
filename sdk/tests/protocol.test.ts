import { isServerMessage } from "../src/protocol.js";
import type { ServerMessage, WelcomeMsg } from "../src/protocol.js";

test("isServerMessage accepts an object with a string type", () => {
  expect(isServerMessage({ type: "welcome", participantId: "p1" })).toBe(true);
});

test("isServerMessage rejects non-objects and typeless objects", () => {
  expect(isServerMessage(null)).toBe(false);
  expect(isServerMessage("welcome")).toBe(false);
  expect(isServerMessage({ foo: 1 })).toBe(false);
});

test("WelcomeMsg is assignable from a realistic payload", () => {
  const w: WelcomeMsg = {
    type: "welcome", participantId: "u.ab12", userId: "u", name: "A",
    iceServers: [{ urls: ["stun:x:1"] }],
  };
  const m: ServerMessage = w;
  expect(m.type).toBe("welcome");
});
