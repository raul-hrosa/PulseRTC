package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// NodeTransport is how a node talks to a peer. ships an
// HTTP implementation against the peer's /internal API; the interface keeps the
// door open for something else later. It is deliberately NOT a general RPC
// layer — only the few queries the foundation needs.
type NodeTransport interface {
	// RoomOwner asks a peer whether it owns roomID.
	RoomOwner(ctx context.Context, peerBaseURL, roomID string) (owner string, ok bool, err error)
	// Participant asks a peer for a participant's location.
	Participant(ctx context.Context, peerBaseURL, participantID string) (ParticipantLocation, bool, error)
	// NodeInfo fetches a peer's self-description.
	NodeInfo(ctx context.Context, peerBaseURL string) (NodeInfo, error)
}

// HTTPTransport calls the peer's /internal endpoints with the cluster bearer
// credential.
type HTTPTransport struct {
	secret string
	client *http.Client
	onReq  func()
	onFail func()
}

// NewHTTPTransport builds a transport. A zero timeout defaults to 3s — these
// calls are on the signaling path (join), never on the media path.
func NewHTTPTransport(secret string, timeout time.Duration) *HTTPTransport {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &HTTPTransport{secret: secret, client: &http.Client{Timeout: timeout}}
}

func (t *HTTPTransport) get(ctx context.Context, url string, out any) (int, error) {
	if t.onReq != nil {
		t.onReq()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", internalToken(t.secret))
	resp, err := t.client.Do(req)
	if err != nil {
		if t.onFail != nil {
			t.onFail()
		}
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return resp.StatusCode, nil
	}
	if resp.StatusCode >= 400 {
		if t.onFail != nil {
			t.onFail()
		}
		return resp.StatusCode, fmt.Errorf("peer returned %s", resp.Status)
	}
	if out != nil {
		return resp.StatusCode, json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode, nil
}

func (t *HTTPTransport) RoomOwner(ctx context.Context, peer, roomID string) (string, bool, error) {
	var body struct {
		RoomID string `json:"roomId"`
		NodeID string `json:"nodeId"`
	}
	status, err := t.get(ctx, peer+"/internal/rooms/"+roomID, &body)
	if err != nil {
		return "", false, err
	}
	if status == http.StatusNotFound || body.NodeID == "" {
		return "", false, nil
	}
	return body.NodeID, true, nil
}

func (t *HTTPTransport) Participant(ctx context.Context, peer, participantID string) (ParticipantLocation, bool, error) {
	var loc ParticipantLocation
	status, err := t.get(ctx, peer+"/internal/participants/"+participantID, &loc)
	if err != nil {
		return ParticipantLocation{}, false, err
	}
	if status == http.StatusNotFound || loc.ParticipantID == "" {
		return ParticipantLocation{}, false, nil
	}
	return loc, true, nil
}

func (t *HTTPTransport) NodeInfo(ctx context.Context, peer string) (NodeInfo, error) {
	var n NodeInfo
	if _, err := t.get(ctx, peer+"/internal/node", &n); err != nil {
		return NodeInfo{}, err
	}
	return n, nil
}
