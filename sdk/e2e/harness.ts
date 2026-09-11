import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";

/** Fixed test secret — shared by the server and every minted token. */
export const SECRET = "e2e-test-secret-please-change";

/** Fixed ports so the browser page can hard-code the SDK / WS URLs. */
export const PORT = 8096;
export const SFU_UDP_PORT = 8097;

/** Repo root (two levels up from sdk/e2e/). */
const REPO_ROOT = fileURLToPath(new URL("../..", import.meta.url));

export const SERVER_URL = `ws://localhost:${PORT}/ws`;
export const SDK_URL = `http://localhost:${PORT}/sdk/index.js`;
export const HEALTH_URL = `http://localhost:${PORT}/health`;

/**
 * Boots `go run ./cmd/server` with auth on, waits for /health, and returns the
 * child process. Requires `go` on PATH and a built `sdk/dist/` (served at /sdk/).
 */
export async function startServer(): Promise<ChildProcess> {
  const proc = spawn("go", ["run", "./cmd/server"], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      PORT: String(PORT),
      SFU_UDP_PORT: String(SFU_UDP_PORT),
      SFU_NAT_1TO1_IP: "127.0.0.1",
      PULSERTC_AUTH_ENABLED: "true",
      PULSERTC_JWT_SECRET: SECRET,
      PULSERTC_API_ENABLED: "false",
    },
    stdio: "inherit",
  });

  // First CI run compiles the whole Go project cold via `go run` — give the
  // server up to ~90 s to answer /health.
  for (let i = 0; i < 180; i++) {
    try {
      const r = await fetch(HEALTH_URL);
      if (r.ok) return proc;
    } catch {
      /* not up yet — retry */
    }
    await sleep(500);
  }
  proc.kill("SIGKILL");
  throw new Error(`server did not become healthy at ${HEALTH_URL}`);
}

/** Mints a bare participant JWT via `go run ./cmd/token` (prints token + \n). */
export function mintToken(sub: string, room: string): string {
  return execFileSync("go", ["run", "./cmd/token", "-sub", sub, "-room", room], {
    cwd: REPO_ROOT,
    env: { ...process.env, PULSERTC_JWT_SECRET: SECRET },
  })
    .toString()
    .trim();
}
