import type { RoomDeps } from "./deps.js";
import type { IceServerConfig, SfuSignalMsg } from "./protocol.js";

type Send = (
  m:
    | { type: "sfu_offer" | "sfu_answer"; payload: { sdp: string } }
    | { type: "sfu_ice_candidate"; payload: RTCIceCandidateInit },
) => void;

export class Negotiator {
  onTrack: (track: MediaStreamTrack, streamId: string, trackId: string) => void = () => {};

  private _pc: RTCPeerConnection | null = null;
  private makingOffer = false;
  private ignoreOffer = false;
  private remoteSet = false;
  private pendingIce: RTCIceCandidateInit[] = [];

  constructor(private readonly pcFactory: RoomDeps["pcFactory"], private readonly send: Send) {}

  /** The live peer connection; throws when there is none (internal callers). */
  get pc(): RTCPeerConnection {
    if (!this._pc) throw new Error("Negotiator: create() not called");
    return this._pc;
  }

  /** The live peer connection, or null before `create()` / after `close()`. */
  get pcOrNull(): RTCPeerConnection | null { return this._pc; }

  create(iceServers: IceServerConfig[]): void {
    this._pc?.close();
    this.makingOffer = this.ignoreOffer = this.remoteSet = false;
    this.pendingIce = [];
    const pc = this.pcFactory({ iceServers: iceServers as RTCIceServer[] });
    this._pc = pc;
    pc.onicecandidate = (ev) => { if (ev.candidate) this.send({ type: "sfu_ice_candidate", payload: ev.candidate.toJSON() }); };
    pc.ontrack = (ev) => { const s = ev.streams[0]; if (s) this.onTrack(ev.track, s.id, ev.track.id); };
    pc.onnegotiationneeded = async () => {
      try {
        this.makingOffer = true;
        await pc.setLocalDescription();
        this.send({ type: "sfu_offer", payload: { sdp: pc.localDescription!.sdp } });
      } finally {
        this.makingOffer = false;
      }
    };
  }

  addLocalTrack(track: MediaStreamTrack, stream: MediaStream): RTCRtpSender { return this.pc.addTrack(track, stream); }
  removeLocalTrack(sender: RTCRtpSender): void { this.pc.removeTrack(sender); }

  async handleSignal(m: SfuSignalMsg): Promise<void> {
    const pc = this.pc;
    if (m.type === "sfu_ice_candidate") {
      const init = m.payload as RTCIceCandidateInit;
      if (!this.remoteSet) { this.pendingIce.push(init); return; }
      try { await pc.addIceCandidate(init); } catch { if (!this.ignoreOffer) throw new Error("addIceCandidate failed"); }
      return;
    }
    if (m.type === "sfu_answer") {
      if (pc.signalingState !== "have-local-offer") return;
      await pc.setRemoteDescription({ type: "answer", sdp: m.payload.sdp });
      this.remoteSet = true;
      await this.flushIce();
      return;
    }
    // sfu_offer
    const collision = this.makingOffer || pc.signalingState !== "stable";
    this.ignoreOffer = collision;
    if (collision) {
      await Promise.all([
        pc.setLocalDescription({ type: "rollback" }),
        pc.setRemoteDescription({ type: "offer", sdp: m.payload.sdp }),
      ]);
    } else {
      await pc.setRemoteDescription({ type: "offer", sdp: m.payload.sdp });
    }
    this.remoteSet = true;
    await this.flushIce();
    await pc.setLocalDescription(await pc.createAnswer());
    this.send({ type: "sfu_answer", payload: { sdp: pc.localDescription!.sdp } });
  }

  private async flushIce(): Promise<void> {
    for (const c of this.pendingIce.splice(0)) {
      try { await this.pc.addIceCandidate(c); } catch { /* ignore during rollback */ }
    }
  }

  close(): void { this._pc?.close(); this._pc = null; }
}
