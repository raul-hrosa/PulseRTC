// Command token mints a PulseRTC access token for manual testing and demos.
//
//	go run ./cmd/token -sub user-123 -room demo -ttl 1h
//
// The signing secret comes from PULSERTC_JWT_SECRET (or -secret). This is a
// developer convenience, not a production token service.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/raulhrosa/pulsertc/internal/auth"
)

func main() {
	var (
		sub      = flag.String("sub", "user-"+fmt.Sprint(time.Now().Unix()), "subject (account id)")
		name     = flag.String("name", "", "display name")
		room     = flag.String("room", "", "room claim (empty = any room)")
		ttl      = flag.Duration("ttl", time.Hour, "token lifetime")
		issuer   = flag.String("iss", envOr("PULSERTC_JWT_ISSUER", "pulsertc"), "issuer")
		audience = flag.String("aud", envOr("PULSERTC_JWT_AUDIENCE", "pulsertc"), "audience")
		secret   = flag.String("secret", os.Getenv("PULSERTC_JWT_SECRET"), "HS256 signing secret")
		join     = flag.Bool("join", true, "grant JOIN")
		publish  = flag.Bool("publish", true, "grant PUBLISH")
		sub2     = flag.Bool("subscribe", true, "grant SUBSCRIBE")
		control  = flag.Bool("control", false, "grant CONTROL")
	)
	flag.Parse()

	if *secret == "" {
		fmt.Fprintln(os.Stderr, "error: set PULSERTC_JWT_SECRET or pass -secret")
		os.Exit(1)
	}

	now := time.Now()
	tok, err := auth.Sign(auth.Claims{
		Subject:     *sub,
		Name:        *name,
		Room:        *room,
		Permissions: auth.Permissions{Join: *join, Publish: *publish, Subscribe: *sub2, Control: *control},
		IssuedAt:    now.Unix(),
		ExpiresAt:   now.Add(*ttl).Unix(),
		Issuer:      *issuer,
		Audience:    *audience,
	}, auth.AlgHS256, *secret)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(tok)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
