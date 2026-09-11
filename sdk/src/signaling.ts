import type { RoomDeps, WebSocketLike } from "./deps.js";
import { isServerMessage, type ClientMessage, type ServerMessage } from "./protocol.js";

export class Signaling {
  onMessage: (msg: ServerMessage) => void = () => {};
  onOpen: () => void = () => {};
  onClose: (info: { code: number; clean: boolean }) => void = () => {};

  private ws: WebSocketLike | null = null;
  private queue: ClientMessage[] = [];
  private closedByOwner = false;
  private closeReported = false;

  constructor(private readonly wsFactory: RoomDeps["wsFactory"]) {}

  get connected(): boolean { return this.ws?.readyState === 1; }

  connect(url: string, token: string): void {
    this.closedByOwner = false;
    this.closeReported = false;
    const ws = this.wsFactory(url, ["pulsertc", `pulsertc.token.${token}`]);
    this.ws = ws;
    ws.onopen = () => { for (const m of this.queue.splice(0)) ws.send(JSON.stringify(m)); this.onOpen(); };
    ws.onmessage = (ev) => {
      if (this.ws !== ws) return;   // a superseded socket must not feed the new session
      let parsed: unknown;
      try { parsed = JSON.parse(ev.data); } catch { return; }
      if (isServerMessage(parsed)) this.onMessage(parsed);
    };
    ws.onclose = (ev) => { if (this.ws === ws) this.report(ev.code); };
    ws.onerror = () => { /* onclose always follows */ };
  }

  send(msg: ClientMessage): void {
    if (this.ws && this.ws.readyState === 1) this.ws.send(JSON.stringify(msg));
    else this.queue.push(msg);
  }

  close(): void {
    this.closedByOwner = true;
    this.queue = [];
    this.ws?.close(1000, "client");
    this.report(1000);
  }

  private report(code: number): void {
    if (this.closeReported) return;
    this.closeReported = true;
    this.onClose({ code, clean: this.closedByOwner || code === 1000 });
  }
}
