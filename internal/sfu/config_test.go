package sfu

import (
	"strings"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	base := Config{NegotiationConcurrency: 4, NackBufferSize: 256, SubQueueAudio: 64, SubQueueVideo: 512}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	bad := base
	bad.NegotiationConcurrency = 0
	bad.SubQueueVideo = -1
	err := bad.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "NegotiationConcurrency") || !strings.Contains(err.Error(), "SubQueueVideo") {
		t.Fatalf("error should name both bad fields, got: %v", err)
	}
}

func TestConfigFromEnvReadsNegotiationConcurrency(t *testing.T) {
	t.Setenv("SFU_NEGOTIATION_CONCURRENCY", "7")
	c := configFromEnv(testLogger())
	if c.NegotiationConcurrency != 7 {
		t.Fatalf("NegotiationConcurrency = %d, want 7", c.NegotiationConcurrency)
	}
}
