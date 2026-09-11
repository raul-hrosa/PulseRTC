package cluster

import (
	"strings"
	"testing"
	"time"
)

func TestNodeConfigValidate(t *testing.T) {
	ok := Config{Enabled: false, HeartbeatInterval: 5 * time.Second, StaleAfter: 15 * time.Second}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	okEnabled := Config{Enabled: true, Secret: "s", HeartbeatInterval: 5 * time.Second, StaleAfter: 15 * time.Second}
	if err := okEnabled.Validate(); err != nil {
		t.Fatalf("valid enabled config rejected: %v", err)
	}
	bad := Config{Enabled: true, Secret: "", HeartbeatInterval: 0, StaleAfter: 0}
	err := bad.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"SECRET", "HeartbeatInterval", "StaleAfter"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}
