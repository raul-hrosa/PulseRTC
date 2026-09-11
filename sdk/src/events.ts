import type { ParticipantQuality } from "./qoe.js";
import type { Participant, RemoteParticipant, RemotePublication } from "./participants.js";

export enum ConnectionState {
  Connecting = "connecting", Connected = "connected",
  Reconnecting = "reconnecting", Disconnected = "disconnected",
}
export type DisconnectReason = "client" | "room_closed" | "replaced" | "token_expired" | "error";

export enum RoomEvent {
  ConnectionStateChanged = "connectionStateChanged",
  ParticipantConnected = "participantConnected",
  ParticipantDisconnected = "participantDisconnected",
  TrackSubscribed = "trackSubscribed",
  TrackUnsubscribed = "trackUnsubscribed",
  TrackMuted = "trackMuted",
  QualityChanged = "qualityChanged",
  SubscribeDenied = "subscribeDenied",
  /** A non-fatal failure the SDK could not attribute to a pending `connect()`. */
  Error = "error",
  Disconnected = "disconnected",
}

export interface RoomEventMap {
  [RoomEvent.ConnectionStateChanged]: [state: ConnectionState];
  [RoomEvent.ParticipantConnected]: [participant: RemoteParticipant];
  [RoomEvent.ParticipantDisconnected]: [participant: RemoteParticipant];
  [RoomEvent.TrackSubscribed]: [track: MediaStreamTrack, publication: RemotePublication, participant: RemoteParticipant];
  [RoomEvent.TrackUnsubscribed]: [publication: RemotePublication, participant: RemoteParticipant];
  [RoomEvent.TrackMuted]: [publication: RemotePublication, participant: RemoteParticipant];
  [RoomEvent.QualityChanged]: [quality: ParticipantQuality, participant: Participant];
  [RoomEvent.SubscribeDenied]: [info: { publicationId?: string }];
  [RoomEvent.Error]: [error: Error];
  [RoomEvent.Disconnected]: [reason: DisconnectReason];
}
