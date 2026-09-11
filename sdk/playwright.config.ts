import { defineConfig } from "@playwright/test";

// One serial worker: the e2e boots a single real PulseRTC server on fixed
// ports (see e2e/harness.ts) and drives two browser contexts against it.
export default defineConfig({
  testDir: "e2e",
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  timeout: 60_000,
  reporter: "list",
  use: {
    launchOptions: {
      args: [
        "--use-fake-ui-for-media-stream",
        "--use-fake-device-for-media-stream",
      ],
    },
  },
});
