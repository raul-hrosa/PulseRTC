export class PulseRTCError extends Error {
  constructor(message: string) { super(message); this.name = "PulseRTCError"; }
}
export class ConnectionError extends PulseRTCError {
  constructor(message: string) { super(message); this.name = "ConnectionError"; }
}
export class TokenError extends PulseRTCError {
  code: string;
  constructor(message: string, code: string) { super(message); this.name = "TokenError"; this.code = code; }
}
export class PublishError extends PulseRTCError {
  override cause?: unknown;
  constructor(message: string, cause?: unknown) { super(message); this.name = "PublishError"; this.cause = cause; }
}
export class RoomOnOtherNodeError extends PulseRTCError {
  nodeId: string;
  constructor(nodeId: string) { super(`room is owned by node ${nodeId}`); this.name = "RoomOnOtherNodeError"; this.nodeId = nodeId; }
}
