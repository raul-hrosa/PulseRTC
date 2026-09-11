# @pulsertc/client

Browser SDK for PulseRTC — realtime audio/video rooms over WebRTC.

## Installation

```bash
npm install @pulsertc/client
```

## Quickstart

```javascript
import { Room, RoomEvent, ConnectionState } from "@pulsertc/client";

// Create a room instance
const room = new Room({ qoe: true }); // Enable QoE monitoring (optional)

// Listen for remote participant tracks
room.on(RoomEvent.TrackSubscribed, (track, publication, participant) => {
  const video = document.createElement("video");
  video.autoplay = true;
  video.muted = false;
  video.srcObject = new MediaStream([track]);
  document.body.appendChild(video);
});

// Connect to the server
const serverUrl = "wss://your-server.example.com/ws";
const token = "..."; // Obtained from your backend
const roomId = "my-room";

await room.connect(serverUrl, token, roomId);

// Enable local camera and microphone
await room.localParticipant.enableCameraAndMicrophone();

// Listen for connection state changes
room.on(RoomEvent.ConnectionStateChanged, (state) => {
  console.log("Connection state:", state);
});

// Disconnect when done
await room.disconnect();
```

## Token Acquisition

Your backend must issue a token by calling:

```
POST /v1/rooms/{roomId}/tokens
```

See [integration guide](../docs/api/integration-guide.md) for details.

## Events

| Event | Arguments | Description |
|-------|-----------|-------------|
| `ConnectionStateChanged` | `(state: ConnectionState)` | Connection state changed (connecting, connected, reconnecting, disconnected) |
| `ParticipantConnected` | `(participant: RemoteParticipant)` | Remote participant joined the room |
| `ParticipantDisconnected` | `(participant: RemoteParticipant)` | Remote participant left the room |
| `TrackSubscribed` | `(track, publication, participant)` | Subscribed to a remote participant's track. `publication.source` is `"camera"` / `"microphone"` / `"screen"` — check it to render a shared screen separately. |
| `TrackUnsubscribed` | `(publication, participant)` | Unsubscribed from a remote participant's track (fires for a screen when the sharer stops) |
| `TrackMuted` | `(publication, participant)` | Remote participant muted a track |
| `QualityChanged` | `(quality, participant)` | Quality metrics changed (QoE monitoring) |
| `SubscribeDenied` | `(info)` | Subscription to a track was denied |
| `Disconnected` | `(reason: DisconnectReason)` | Disconnected from the room |

## API

- `new Room(options?)` — Create a room instance
  - `options.qoe?: boolean` — Enable QoE monitoring (default: true)

- `await room.connect(serverUrl, token, roomId)` — Connect to a PulseRTC server
  - `serverUrl: string` — WebSocket server URL (e.g., `wss://...`)
  - `token: string` — JWT token from backend
  - `roomId: string` — Room identifier

- `await room.disconnect()` — Disconnect from the room

- `room.state: ConnectionState` — Current connection state

- `room.localParticipant: LocalParticipant`
  - `await enableCameraAndMicrophone()` — Enable both camera and microphone
  - `setMicrophoneEnabled(bool)` — Control microphone
  - `setCameraEnabled(bool)` — Control camera
  - `await startScreenShare(constraints?)` — Prompt `getDisplayMedia` and publish the capture as an extra `video` publication with `source === "screen"` (the camera keeps running). When the capture carries audio (Chromium "Share audio"), it is published too as a second `audio` publication with the same source. `constraints` defaults to `{ video: true, audio: true }`; pass `{ video: true }` to never request audio. Resolves once the server acknowledges the video publication.
  - `await stopScreenShare()` — Unpublish (both tracks) and release the capture. Also called automatically when the user ends the share from the browser UI (`track.onended`).
  - `isScreenSharing: boolean`
  - Screen share is **not** auto-restored after a reconnect — the browser almost never lets a display-capture track survive a new PeerConnection, so `isScreenSharing` goes back to `false` and the user re-initiates.

- `room.remoteParticipants: ReadonlyMap<string, RemoteParticipant>` — Connected remote participants

- `room.engine.pc: RTCPeerConnection` — Underlying WebRTC peer connection (advanced)

## Error Handling

The SDK exports error classes for handling failures:

```javascript
import { ConnectionError, TokenError, PublishError } from "@pulsertc/client";

try {
  await room.connect(serverUrl, token, roomId);
} catch (err) {
  if (err instanceof TokenError) {
    console.error("Invalid or expired token");
  } else if (err instanceof ConnectionError) {
    console.error("Connection failed:", err.message);
  }
}
```

## Documentation

- [Integration Guide](../docs/api/integration-guide.md) — Backend API and token issuance
- [Browser SDK Design Spec](../docs/superpowers/specs/2026-09-02-browser-sdk-design.md) — Architecture and detailed API reference
