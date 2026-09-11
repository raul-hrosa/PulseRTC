export { Room } from "./room.js";
export type { RoomOptions } from "./room.js";
export { RoomEvent, ConnectionState } from "./events.js";
export type { DisconnectReason, RoomEventMap } from "./events.js";
export {
  PulseRTCError, ConnectionError, TokenError, PublishError, RoomOnOtherNodeError,
} from "./errors.js";
export {
  type Participant, RemoteParticipant, RemotePublication, LocalParticipant, LocalPublication,
} from "./participants.js";
export type { ParticipantQuality } from "./qoe.js";
export type { MediaSource } from "./protocol.js";
/** Advanced/testing seam: the platform objects a Room builds its transport from. */
export type { RoomDeps, WebSocketLike } from "./deps.js";

export const SDK_VERSION = "0.1.0";
