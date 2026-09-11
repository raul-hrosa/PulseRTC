package loadtest

import "testing"

func TestSummarizeCalculatesPercentilesAndRates(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Participants = 2
	cfg.Publishers = 1
	cfg.Scenario = "unit"

	server := []ServerMetrics{
		{UptimeSec: 1},
		{UptimeSec: 2},
		{UptimeSec: 3},
	}
	server[0].Runtime.Goroutines = 10
	server[1].Runtime.Goroutines = 20
	server[2].Runtime.Goroutines = 30
	server[0].Runtime.HeapAllocBytes = 100 * 1024 * 1024
	server[1].Runtime.HeapAllocBytes = 200 * 1024 * 1024
	server[2].Runtime.HeapAllocBytes = 300 * 1024 * 1024
	server[0].Runtime.GoCPUSeconds = 1
	server[1].Runtime.GoCPUSeconds = 1.5
	server[2].Runtime.GoCPUSeconds = 2
	server[0].SFU.RTP.BytesSent = 0
	server[1].SFU.RTP.BytesSent = 1_000_000
	server[2].SFU.RTP.BytesSent = 3_000_000

	clients := []ClientSample{
		{PacketsReceived: 100, PacketsLost: 1, RTTMs: 10, JitterMs: 2, BitrateInBps: 1_000_000, ConnectionTimeMs: 100},
		{PacketsReceived: 100, PacketsLost: 3, RTTMs: 30, JitterMs: 6, BitrateInBps: 3_000_000, ConnectionTimeMs: 300},
	}

	result := Summarize(cfg, server, clients)
	if result.Summary.Goroutines.P50 != 20 {
		t.Fatalf("goroutine p50 = %v, want 20", result.Summary.Goroutines.P50)
	}
	if result.Summary.HeapAllocMB.Max != 300 {
		t.Fatalf("heap max = %v, want 300", result.Summary.HeapAllocMB.Max)
	}
	if result.Summary.CPUPercent.Average != 50 {
		t.Fatalf("cpu avg = %v, want 50", result.Summary.CPUPercent.Average)
	}
	if result.Summary.PacketLossPct <= 0 {
		t.Fatal("expected packet loss percentage")
	}
	if result.Summary.BitrateInMbps.P50 != 2 {
		t.Fatalf("client bitrate p50 = %v, want 2", result.Summary.BitrateInMbps.P50)
	}
}

func TestScenarioPresetBaseline(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Scenario = "baseline"
	preset, ok := ScenarioPreset(cfg.Scenario)
	if !ok {
		t.Fatal("baseline preset not found")
	}
	preset.apply(&cfg)
	if err := cfg.Normalize(); err != nil {
		t.Fatal(err)
	}
	if cfg.Participants != 2 || cfg.Publishers != 1 {
		t.Fatalf("baseline participants/publishers = %d/%d", cfg.Participants, cfg.Publishers)
	}
	if !cfg.PublishAudio || !cfg.PublishVideo || !cfg.SubscribeAll {
		t.Fatal("baseline media flags should be enabled")
	}
}
