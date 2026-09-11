import { test, expect, type Page } from "@playwright/test";
import type { ChildProcess } from "node:child_process";
import { startServer, mintToken, SERVER_URL } from "./harness.js";

// Drives the real web/example-sdk/ demo page (served by the Go server at
// /example-sdk/, which loads the SDK from /sdk/index.js). The shared harness
// runs the server with auth ON, so each tab pastes a minted JWT into the form.

let server: ChildProcess;
test.beforeAll(async () => { server = await startServer(); });
test.afterAll(() => { server?.kill("SIGKILL"); });

const PAGE_URL = SERVER_URL.replace("ws://", "http://").replace("/ws", "/example-sdk/");

async function joinPage(page: Page, token: string, roomId: string): Promise<void> {
  await page.goto(PAGE_URL);
  await page.fill("#url", SERVER_URL);
  await page.fill("#roomId", roomId);
  await page.fill("#token", token);
  await page.click("#join");
  await expect.poll(() => page.locator("#conn").textContent(), { timeout: 20_000 }).toBe("connected");
}

test("the demo client renders a remote tile, badges and the participant list", async ({ browser }) => {
  const roomId = "demo-e2e";
  const ctxA = await browser.newContext();
  const ctxB = await browser.newContext();
  const a = await ctxA.newPage();
  const b = await ctxB.newPage();

  await joinPage(a, mintToken("alice", roomId), roomId);
  await joinPage(b, mintToken("bob", roomId), roomId);

  // A gets a tile carrying both of B's tracks.
  await expect
    .poll(
      () =>
        a.evaluate(() => {
          const v = document.querySelector<HTMLVideoElement>("#tiles video");
          return (v?.srcObject as MediaStream | null)?.getTracks().length ?? 0;
        }),
      { timeout: 25_000 },
    )
    .toBeGreaterThanOrEqual(2);

  await expect(a.locator("#participantCount")).toHaveText("2");
  await expect(a.locator("#tiles figure")).toHaveCount(1);
  await expect(a.locator("#tiles figcaption")).toContainText("🎤");

  // B mutes its mic → A's badge for B flips to 🔇.
  await b.click("#mic");
  await expect(a.locator("#tiles figcaption")).toContainText("🔇");

  // B leaves → A's tile and count update.
  await b.click("#leave");
  await expect(a.locator("#tiles figure")).toHaveCount(0);
  await expect(a.locator("#participantCount")).toHaveText("1");

  await ctxA.close();
  await ctxB.close();
});
