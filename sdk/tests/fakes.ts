import type { WebSocketLike } from "../src/deps.js";

export class FakeWebSocket implements WebSocketLike {
  readyState = 0;
  readonly sent: object[] = [];
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onclose: ((ev: { code: number; reason: string }) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  constructor(readonly url: string, readonly protocols: string[]) {}
  send(data: string): void { this.sent.push(JSON.parse(data)); }
  close(): void { this.readyState = 3; this.onclose?.({ code: 1000, reason: "" }); }
  open(): void { this.readyState = 1; this.onopen?.({}); }
  serverSend(msg: object): void { this.onmessage?.({ data: JSON.stringify(msg) }); }
  serverClose(code = 1006, reason = ""): void { this.readyState = 3; this.onclose?.({ code, reason }); }
  serverError(): void { this.onerror?.({}); }
}

type Handler = ((ev: unknown) => void) | null;

export class FakeRTCPeerConnection {
  signalingState: RTCSignalingState = "stable";
  localDescription: RTCSessionDescriptionInit | null = null;
  remoteDescription: RTCSessionDescriptionInit | null = null;
  readonly senders: { track: MediaStreamTrack | null }[] = [];
  readonly addedIce: RTCIceCandidateInit[] = [];
  ontrack: ((ev: RTCTrackEvent) => void) | null = null;
  onnegotiationneeded: Handler = null;
  onicecandidate: ((ev: { candidate: RTCIceCandidate | null }) => void) | null = null;
  onconnectionstatechange: Handler = null;
  oniceconnectionstatechange: Handler = null;
  connectionState: RTCPeerConnectionState = "new";
  private stats: RTCStatsReport = new Map() as unknown as RTCStatsReport;

  async setLocalDescription(desc?: RTCSessionDescriptionInit): Promise<void> {
    if (desc?.type === "rollback") { this.signalingState = "stable"; return; }
    this.localDescription = desc ?? { type: "offer", sdp: "fake-offer" };
    this.signalingState = this.localDescription.type === "offer" ? "have-local-offer" : "stable";
  }
  async setRemoteDescription(desc: RTCSessionDescriptionInit): Promise<void> {
    this.remoteDescription = desc;
    this.signalingState = desc.type === "offer" ? "have-remote-offer" : "stable";
  }
  async createOffer(): Promise<RTCSessionDescriptionInit> { return { type: "offer", sdp: "fake-offer" }; }
  async createAnswer(): Promise<RTCSessionDescriptionInit> { return { type: "answer", sdp: "fake-answer" }; }
  addTrack(track: MediaStreamTrack): { track: MediaStreamTrack | null } {
    const s = { track }; this.senders.push(s); return s;
  }
  removeTrack(sender: { track: MediaStreamTrack | null }): void { sender.track = null; }
  async addIceCandidate(c: RTCIceCandidateInit): Promise<void> { this.addedIce.push(c); }
  getSenders(): { track: MediaStreamTrack | null }[] { return this.senders; }
  async getStats(): Promise<RTCStatsReport> { return this.stats; }
  close(): void { this.connectionState = "closed"; }

  emitTrack(track: MediaStreamTrack, streams: MediaStream[]): void {
    this.ontrack?.({ track, streams } as unknown as RTCTrackEvent);
  }
  emitNegotiationNeeded(): void { this.onnegotiationneeded?.({}); }
  emitIceCandidate(candidate: RTCIceCandidate | null): void { this.onicecandidate?.({ candidate }); }
  setStats(report: RTCStatsReport): void { this.stats = report; }
}

export function fakeStatsReport(entries: Record<string, unknown>[]): RTCStatsReport {
  const m = new Map<string, unknown>();
  entries.forEach((e, i) => m.set(String(e["id"] ?? i), e));
  return m as unknown as RTCStatsReport;
}
