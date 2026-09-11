package main

import (
	"strings"
	"testing"
)

func TestLoadServerConfigDefaults(t *testing.T) {
	t.Setenv("WEB_DIR", ".") // "." always exists
	c, err := loadServerConfig()
	if err != nil {
		t.Fatalf("defaults rejected: %v", err)
	}
	if c.Port != 8090 || c.SDKDistDir != "sdk/dist" {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestLoadServerConfigRejectsBadPortAndMissingWebDir(t *testing.T) {
	t.Setenv("PORT", "0")
	t.Setenv("WEB_DIR", "/no/such/dir/x92")
	_, err := loadServerConfig()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "PORT") || !strings.Contains(err.Error(), "WEB_DIR") {
		t.Fatalf("error should name both, got: %v", err)
	}
}
