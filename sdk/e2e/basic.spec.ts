import { test, expect, type Page } from "@playwright/test";
import type { ChildProcess } from "node:child_process";
import {
  startServer,
  mintToken,
  SERVER_URL,
  SDK_URL,
  HEALTH_URL,
} from "./harness.js";

let server: ChildProcess;

test.beforeAll(async () => {
  server = await startServer();
});

test.afterAll(() => {
  server?.kill("SIGKILL");
});

/**
 * Runs inside the browser: dynamic-imports the built SDK, joins the room with a
 * 3-arg `connect`, publishes camera + mic (fake devices), and records the kinds
 * of every `TrackSubscribed`.
 */
async function join(page: Page, token: string, roomId: string): Promise<void> {
  // Navigate to a real http://localhost origin so the dynamic import of the
  // same-origin SDK module is allowed.
  await page.goto(HEALTH_URL);
  await page.evaluate(
    async ({ sdkUrl, serverUrl, token, roomId }) => {
      const mod = await import(/* @vite-ignore */ sdkUrl);
      const { Room, RoomEvent } = mod as typeof import("../src/index.js");
      const room = new Room();
      const w = window as unknown as { __subs: string[]; __room: InstanceType<typeof Room> };
      w.__subs = [];
      room.on(RoomEvent.TrackSubscribed, (_track, pub) => {
        w.__subs.push(pub.kind);
      });
      await room.connect(serverUrl, token, roomId);
      await room.localParticipant.enableCameraAndMicrophone();
      w.__room = room;
    },
    { sdkUrl: SDK_URL, serverUrl: SERVER_URL, token, roomId },
  );
}

function subscribedKinds(page: Page): Promise<string> {
  return page.evaluate(() =>
    [...new Set((window as unknown as { __subs: string[] }).__subs)].sort().join(","),
  );
}

function inboundPackets(page: Page): Promise<number> {
  return page.evaluate(async () => {
    const room = (window as unknown as { __room: { engine: { pc: RTCPeerConnection } } }).__room;
    const stats = await room.engine.pc.getStats();
    let recv = 0;
    stats.forEach((s) => {
      if (s.type === "inbound-rtp") recv += (s as RTCInboundRtpStreamStats).packetsReceived ?? 0;
    });
    return recv;
  });
}

test("two participants exchange audio + video", async ({ browser }) => {
  const roomId = "e2e";
  const ctxA = await browser.newContext();
  const ctxB = await browser.newContext();
  const a = await ctxA.newPage();
  const b = await ctxB.newPage();

  await join(a, mintToken("alice", roomId), roomId);
  await join(b, mintToken("bob", roomId), roomId);

  await expect.poll(() => subscribedKinds(a), { timeout: 20_000 }).toBe("audio,video");
  await expect.poll(() => subscribedKinds(b), { timeout: 20_000 }).toBe("audio,video");

  await expect.poll(() => inboundPackets(a), { timeout: 20_000 }).toBeGreaterThan(0);
  await expect.poll(() => inboundPackets(b), { timeout: 20_000 }).toBeGreaterThan(0);

  await ctxA.close();
  await ctxB.close();
});
