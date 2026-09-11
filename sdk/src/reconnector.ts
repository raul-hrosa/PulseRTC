import type { ReconnectHints } from "./protocol.js";

const DEFAULTS: ReconnectHints = { initialDelayMs: 500, maxDelayMs: 10000, jitter: 0.2 };

export function nextReconnectDelay(attempt: number, hints: ReconnectHints, rnd: () => number): number {
  const n = Math.max(0, attempt);
  const base = Math.min(hints.initialDelayMs * 2 ** n, hints.maxDelayMs);
  return base + (hints.jitter > 0 ? rnd() * base * hints.jitter : 0);
}

export type ResumeState = { sessionId: string; generation: number } | null;

export function decideRejoin(resume: ResumeState):
  { type: "resume"; resume: { sessionId: string; generation: number } } | { type: "fresh" } {
  return resume ? { type: "resume", resume } : { type: "fresh" };
}

interface ReconnectorOpts {
  hints?: Partial<ReconnectHints>;
  rnd?: () => number;
  schedule?: (fn: () => void, ms: number) => () => void;
}

export class Reconnector {
  private hints: ReconnectHints;
  private rnd: () => number;
  private schedule: (fn: () => void, ms: number) => () => void;
  private cancelPending: (() => void) | null = null;
  attempt = 0;

  constructor(o: ReconnectorOpts = {}) {
    this.hints = { ...DEFAULTS, ...o.hints };
    this.rnd = o.rnd ?? Math.random;
    this.schedule = o.schedule ?? ((fn, ms) => { const id = setTimeout(fn, ms); return () => clearTimeout(id); });
  }

  setHints(h: Partial<ReconnectHints>): void { this.hints = { ...this.hints, ...h }; }
  get armed(): boolean { return this.cancelPending !== null; }

  arm(run: () => void): void {
    this.cancel();
    const delay = nextReconnectDelay(this.attempt, this.hints, this.rnd);
    this.attempt += 1;
    this.cancelPending = this.schedule(() => { this.cancelPending = null; run(); }, delay);
  }

  cancel(): void { this.cancelPending?.(); this.cancelPending = null; }
  reset(): void { this.cancel(); this.attempt = 0; }
}
