package cluster

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// The /internal API is node-to-node only and must never be reachable by a
// client. It is authenticated with a dedicated cluster
// credential — NEVER the user's JWT: different trust contexts.
//
// The credential is a static shared secret (PULSERTC_CLUSTER_SECRET) presented
// as a bearer token. Constant-time compared. A future version can rotate it or
// move to mTLS without changing callers.

const internalAuthHeader = "Authorization"

// internalToken is what a node sends to a peer.
func internalToken(secret string) string { return "Bearer " + secret }

// checkInternalAuth validates an inbound /internal request.
func checkInternalAuth(r *http.Request, secret string) bool {
	if secret == "" {
		return false // never allow unauthenticated internal calls
	}
	h := r.Header.Get(internalAuthHeader)
	if !strings.HasPrefix(h, "Bearer ") {
		return false
	}
	got := strings.TrimSpace(h[len("Bearer "):])
	return subtle.ConstantTimeCompare([]byte(got), []byte(secret)) == 1
}

// InternalAuthMiddleware wraps an /internal handler so it rejects any request
// without the cluster credential.
func (c *Cluster) InternalAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !checkInternalAuth(r, c.cfg.Secret) {
			c.logger.Warn("internal_request_unauthorized", "path", r.URL.Path, "remote", r.RemoteAddr)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"` + CodeClusterUnauthorized + `"}`))
			return
		}
		next(w, r)
	}
}
