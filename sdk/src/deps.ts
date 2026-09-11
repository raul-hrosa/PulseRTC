export interface WebSocketLike {
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: string }) => void) | null;
  onclose: ((ev: { code: number; reason: string }) => void) | null;
  onerror: ((ev: unknown) => void) | null;
}

export interface RoomDeps {
  wsFactory(url: string, protocols: string[]): WebSocketLike;
  pcFactory(config: RTCConfiguration): RTCPeerConnection;
  getUserMedia(constraints: MediaStreamConstraints): Promise<MediaStream>;
  getDisplayMedia(constraints: MediaStreamConstraints): Promise<MediaStream>;
  now(): number;
}

export const defaultDeps: RoomDeps = {
  wsFactory: (url, protocols) => new WebSocket(url, protocols) as unknown as WebSocketLike,
  pcFactory: (config) => new RTCPeerConnection(config),
  getUserMedia: (c) => navigator.mediaDevices.getUserMedia(c),
  getDisplayMedia: (c) => navigator.mediaDevices.getDisplayMedia(c),
  now: () => performance.now(),
};
