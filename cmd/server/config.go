package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

type serverConfig struct {
	Port       int
	Addr       string
	WebDir     string
	SDKDistDir string
}

func loadServerConfig() (serverConfig, error) {
	c := serverConfig{
		Port:       8090,
		Addr:       os.Getenv("ADDR"),
		WebDir:     envOr("WEB_DIR", "web"),
		SDKDistDir: envOr("SDK_DIST_DIR", "sdk/dist"),
	}
	var errs []error
	if v := os.Getenv("PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p < 1 || p > 65535 {
			errs = append(errs, fmt.Errorf("PORT %q is not a valid TCP port", v))
		} else {
			c.Port = p
		}
	}
	if info, err := os.Stat(c.WebDir); err != nil || !info.IsDir() {
		errs = append(errs, fmt.Errorf("WEB_DIR %q is not a readable directory", c.WebDir))
	}
	return c, errors.Join(errs...)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
