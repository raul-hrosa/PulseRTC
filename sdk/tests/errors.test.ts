import { PulseRTCError, TokenError, PublishError, RoomOnOtherNodeError } from "../src/errors.js";

test("TokenError carries a code and is a PulseRTCError", () => {
  const e = new TokenError("expired", "EXPIRED_TOKEN");
  expect(e).toBeInstanceOf(PulseRTCError);
  expect(e.code).toBe("EXPIRED_TOKEN");
  expect(e.name).toBe("TokenError");
});

test("PublishError preserves a cause", () => {
  const dom = new Error("NotAllowedError");
  const e = new PublishError("mic blocked", dom);
  expect(e.cause).toBe(dom);
});

test("RoomOnOtherNodeError carries the target node id", () => {
  expect(new RoomOnOtherNodeError("node-b").nodeId).toBe("node-b");
});
