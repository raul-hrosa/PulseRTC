import type { QualityEventMsg } from "./protocol.js";

export interface ParticipantQuality {
  status: string; from?: string;
  direction?: "inbound" | "outbound"; mediaType?: string; reason?: string;
}

const s2ms = (v: unknown): number | undefined => (typeof v === "number" ? v * 1000 : undefined);

export interface QoEOptions {
  getStats(): Promise<RTCStatsReport>;
  send(samples: unknown[]): void;
  localPubId(kind: "audio" | "video"): string | undefined;
  now(): number;
  intervalMs?: number;
}

export class QoEReporter {
  private timer: ReturnType<typeof setInterval> | null = null;
  constructor(private readonly o: QoEOptions) {}

  start(): void {
    if (this.timer) return;
    const tick = () => void this.collectOnce().then((s) => { if (s.length) this.o.send(s); }).catch(() => {});
    tick();
    this.timer = setInterval(tick, this.o.intervalMs ?? 1000);
  }
  stop(): void { if (this.timer) { clearInterval(this.timer); this.timer = null; } }

  async collectOnce(): Promise<unknown[]> {
    const report = await this.o.getStats();
    const byId = new Map<string, Record<string, unknown>>();
    report.forEach((s) => byId.set((s as { id: string }).id, s as Record<string, unknown>));
    const now = Math.round(this.o.now());
    const samples: Record<string, unknown>[] = [];

    report.forEach((raw) => {
      const s = raw as Record<string, unknown>;
      if (s["type"] === "outbound-rtp" && !s["isRemote"]) {
        const remote = s["remoteId"] ? byId.get(s["remoteId"] as string) : undefined;
        samples.push({
          publicationId: this.o.localPubId(s["kind"] as "audio" | "video") ?? "",
          kind: s["kind"], direction: "outbound", tMs: now,
          packetsSent: s["packetsSent"], bytesSent: s["bytesSent"],
          packetsLost: remote ? remote["packetsLost"] : undefined,
          rttMs: remote ? s2ms(remote["roundTripTime"]) : undefined,
          jitterMs: remote ? s2ms(remote["jitter"]) : undefined,
        });
      } else if (s["type"] === "inbound-rtp") {
        samples.push({
          publicationId: s["trackIdentifier"] ?? "", kind: s["kind"], direction: "inbound", tMs: now,
          packetsReceived: s["packetsReceived"], packetsLost: s["packetsLost"], bytesReceived: s["bytesReceived"],
          framesDecoded: s["framesDecoded"], framesDropped: s["framesDropped"],
          jitterMs: s2ms(s["jitter"]), fps: s["framesPerSecond"], width: s["frameWidth"], height: s["frameHeight"],
        });
      } else if (s["type"] === "candidate-pair" && (s["selected"] || (s["nominated"] && s["state"] === "succeeded"))) {
        samples.push({ kind: "connection", tMs: now, rttMs: s2ms(s["currentRoundTripTime"]) });
      }
    });

    if (!samples.some((x) => x["kind"] === "connection")) samples.push({ kind: "connection", tMs: now });
    return samples;
  }
}

export function parseQualityEvent(m: QualityEventMsg): ParticipantQuality {
  return {
    status: m.status, from: m.from,
    direction: m.direction, mediaType: m.mediaType, reason: m.reason,
  };
}
