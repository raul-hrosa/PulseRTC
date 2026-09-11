package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"hash"
	"strings"
)

// Supported signing algorithms. HMAC only in — the same shared secret
// signs and verifies, which is enough for a single instance and a trusted token
// issuer. RS/ES can be added later without touching callers.
const (
	AlgHS256 = "HS256"
	AlgHS384 = "HS384"
	AlgHS512 = "HS512"
)

var hmacHashes = map[string]func() hash.Hash{
	AlgHS256: sha256.New,
	AlgHS384: sha512.New384,
	AlgHS512: sha512.New,
}

type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

var b64 = base64.RawURLEncoding

// Sign builds a compact JWS (header.payload.signature) for the claims using the
// given HMAC algorithm and secret. Used by tests and by any token minting
// service embedded in the future; the server itself only verifies.
func Sign(claims Claims, alg, secret string) (string, error) {
	newHash, ok := hmacHashes[alg]
	if !ok {
		return "", errBadAlg
	}
	hb, err := json.Marshal(jwtHeader{Alg: alg, Typ: "JWT"})
	if err != nil {
		return "", err
	}
	pb, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signing := b64.EncodeToString(hb) + "." + b64.EncodeToString(pb)
	mac := hmac.New(newHash, []byte(secret))
	mac.Write([]byte(signing))
	return signing + "." + b64.EncodeToString(mac.Sum(nil)), nil
}

// parse decodes and signature-checks a token WITHOUT applying time / issuer /
// audience policy — that is Validator.Validate's job. allowedAlgs restricts
// which "alg" header values are honoured (never trust the token's own choice
// blindly — that is the classic JWT downgrade attack).
func parse(token, secret string, allowedAlgs map[string]bool) (*Claims, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return nil, errMalformed
	}

	hb, err := b64.DecodeString(parts[0])
	if err != nil {
		return nil, errMalformed
	}
	var hdr jwtHeader
	if err := json.Unmarshal(hb, &hdr); err != nil {
		return nil, errMalformed
	}
	if !allowedAlgs[hdr.Alg] {
		return nil, errBadAlg
	}
	newHash, ok := hmacHashes[hdr.Alg]
	if !ok {
		return nil, errBadAlg
	}

	sig, err := b64.DecodeString(parts[2])
	if err != nil {
		return nil, errMalformed
	}
	mac := hmac.New(newHash, []byte(secret))
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if subtle.ConstantTimeCompare(sig, mac.Sum(nil)) != 1 {
		return nil, errBadSig
	}

	pb, err := b64.DecodeString(parts[1])
	if err != nil {
		return nil, errMalformed
	}
	var claims Claims
	dec := json.NewDecoder(strings.NewReader(string(pb)))
	if err := dec.Decode(&claims); err != nil {
		return nil, errMalformed
	}
	return &claims, nil
}
