package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/raulhrosa/pulsertc/internal/loadtest"
)

func main() {
	cfg, err := loadtest.ParseFlags(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	result, path, err := loadtest.NewRunner(cfg).Run(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	s := result.Summary
	fmt.Println("Benchmark")
	fmt.Println("-----------------------------")
	fmt.Printf("Scenario: %s\n", result.Scenario)
	fmt.Printf("Participants: %d\n", s.Participants)
	fmt.Printf("Duration: %s\n", cfg.Duration)
	if cfg.Auth.Enabled {
		fmt.Printf("Auth: enabled (invalid ratio %.0f%%, failed connections %d)\n",
			cfg.Auth.InvalidRatio*100, s.FailedConnections)
	}
	fmt.Println()
	fmt.Printf("CPU avg: %.2f%%\n", s.CPUPercent.Average)
	fmt.Printf("CPU max: %.2f%%\n", s.CPUPercent.Max)
	if result.ServerStart != nil && result.ServerEnd != nil {
		fmt.Printf("Heap start/end/max: %.2f / %.2f / %.2f MB\n",
			bytesToMB(result.ServerStart.Runtime.HeapAllocBytes),
			bytesToMB(result.ServerEnd.Runtime.HeapAllocBytes),
			s.HeapAllocMB.Max,
		)
	}
	fmt.Printf("Goroutines p95: %.0f\n\n", s.Goroutines.P95)
	fmt.Printf("PeerConnections active/created/closed: %d/%d/%d\n", s.PeerConnections.Active, s.PeerConnections.Created, s.PeerConnections.Closed)
	fmt.Printf("Tracks active/created/removed: %d/%d/%d\n", s.Tracks.Active, s.Tracks.Created, s.Tracks.Removed)
	fmt.Printf("Subscriptions active/created/removed: %d/%d/%d\n", s.Subscriptions.Active, s.Subscriptions.Created, s.Subscriptions.Removed)
	fmt.Printf("Negotiations started/completed/coalesced/failed: %d/%d/%d/%d\n\n",
		s.Negotiations.Started, s.Negotiations.Completed, s.Negotiations.Coalesced, s.Negotiations.Failed)
	fmt.Printf("Network In p95: %.2f Mbps\n", s.NetworkInMbps.P95)
	fmt.Printf("Network Out p95: %.2f Mbps\n", s.NetworkOutMbps.P95)
	fmt.Printf("Packet Loss: %.2f%%\n", s.PacketLossPct)
	fmt.Printf("Jitter p95: %.2f ms\n", s.JitterMs.P95)
	fmt.Printf("RTT p95: %.2f ms\n\n", s.RTTMs.P95)
	fmt.Printf("Connection Time p95: %.2f ms\n", s.ConnectionTimeMs.P95)
	fmt.Printf("Media Start Time p95: %.2f ms\n\n", s.MediaStartTimeMs.P95)
	fmt.Printf("Quality: audio=%s video=%s connection=%s overall=%s\n",
		s.AudioQuality, s.VideoQuality, s.ConnectionQuality, s.OverallQuality)
	fmt.Printf("Likely bottleneck: %s\n", s.LikelyBottleneck)
	fmt.Printf("Result: %s\n", path)
}

func bytesToMB(v uint64) float64 { return float64(v) / 1024 / 1024 }
