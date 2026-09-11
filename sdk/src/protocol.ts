export type IceServerConfig = { urls: string[]; username?: string; credential?: string };
export type ReconnectHints = { initialDelayMs: number; maxDelayMs: number; jitter: number };

export type MediaSource = "camera" | "microphone" | "screen";

export interface SnapshotTrack { publicationId: string; kind: "audio" | "video"; source?: MediaSource; muted: boolean }
export interface SnapshotParticipant { participantId: string; name?: string; tracks: SnapshotTrack[] }
export interface RoomSnapshot {
  type: "room.snapshot"; roomId: string; roomGeneration: number; participants: SnapshotParticipant[];
}

export interface WelcomeMsg {
  type: "welcome"; participantId: string; userId: string; name?: string;
  iceServers?: IceServerConfig[];
}
export interface RoomJoinedMsg {
  type: "room_joined"; roomId: string;
  participants: { participantId: string; name?: string }[];
  quality?: { participantId: string; status: string; reason?: string }[];
  sessionId?: string; generation?: number; roomGeneration?: number; ownerNodeId?: string;
  reconnected?: boolean; reconnect?: ReconnectHints; snapshot?: RoomSnapshot;
}
export interface ParticipantJoinedMsg { type: "participant_joined"; participantId: string; name?: string }
export interface ParticipantLeftMsg { type: "participant_left"; participantId: string; name?: string }

export interface SfuSignalMsg {
  type: "sfu_offer" | "sfu_answer" | "sfu_ice_candidate";
  payload: { sdp?: string; candidate?: string; sdpMid?: string | null; sdpMLineIndex?: number | null } & Record<string, unknown>;
}

export interface PublicationEventMsg {
  type: "publication_added" | "publication_removed" | "publication_muted";
  publicationId: string; participantId: string; kind: "audio" | "video"; source?: MediaSource; muted: boolean;
}
export interface SubscriptionEventMsg {
  type: "subscription_added" | "subscription_removed";
  publicationId: string; participantId: string; kind: "audio" | "video"; source?: MediaSource;
}
export interface QualityEventMsg {
  type: "quality_degraded" | "quality_recovered" | "quality_changed";
  participantId: string; direction?: "inbound" | "outbound"; mediaType?: string;
  trackId?: string; from: string; status: string; reason?: string; at?: string;
}
export interface SessionEventMsg {
  type: "session.recovery" | "session.reconnected" | "session.stale" | "session.replaced" | "session.recovery_failed";
  reason?: string; roomId?: string; sessionId?: string;
}
export interface RoomClosedMsg { type: "room_closed"; roomId: string; reason: string }
export interface ErrorMsg { type: "error"; message?: string; code?: string; nodeId?: string; roomId?: string }
export interface PublishDeniedMsg { type: "publish_denied"; publicationId?: string }
export interface SubscribeDeniedMsg { type: "subscribe_denied"; publicationId?: string }

export type ServerMessage =
  | WelcomeMsg | RoomJoinedMsg | ParticipantJoinedMsg | ParticipantLeftMsg
  | SfuSignalMsg | PublicationEventMsg | SubscriptionEventMsg | QualityEventMsg
  | SessionEventMsg | RoomClosedMsg | ErrorMsg | PublishDeniedMsg | SubscribeDeniedMsg
  | RoomSnapshot;

export type ClientMessage =
  | { type: "join"; roomId: string; resume?: { sessionId: string; generation: number } }
  | { type: "sfu_offer" | "sfu_answer"; payload: { sdp: string } }
  | { type: "sfu_ice_candidate"; payload: RTCIceCandidateInit }
  | { type: "subscribe" | "unsubscribe" | "unpublish"; publicationId: string }
  | { type: "publish"; trackId: string; kind: "audio" | "video"; source: MediaSource }
  | { type: "set_mute"; publicationId: string; muted: boolean; kind: "audio" | "video" }
  | { type: "quality_report"; samples: unknown[] }
  | { type: "session.resume"; resume: { sessionId: string; generation: number } };

export function isServerMessage(v: unknown): v is ServerMessage {
  return typeof v === "object" && v !== null && typeof (v as { type?: unknown }).type === "string";
}
