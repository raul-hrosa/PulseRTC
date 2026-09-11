package signaling

import (
	"encoding/json"
	"net/http"
	"runtime"
	rtmetrics "runtime/metrics"
	"time"
)

type benchmarkMetrics struct {
	Timestamp string         `json:"timestamp"`
	UptimeSec float64        `json:"uptimeSec"`
	Host      hostMetrics    `json:"host"`
	Runtime   runtimeMetrics `json:"runtime"`
	SFU       any            `json:"sfu"`
	Auth      any            `json:"auth"`
	Cluster   any            `json:"cluster"`
	Session   any            `json:"session"`
	History   any            `json:"history"`
}

type hostMetrics struct {
	OS     string `json:"os"`
	Arch   string `json:"arch"`
	CPUs   int    `json:"cpus"`
	Go     string `json:"go"`
	Commit string `json:"commit"`
}

type runtimeMetrics struct {
	Goroutines      int     `json:"goroutines"`
	HeapAllocBytes  uint64  `json:"heapAllocBytes"`
	HeapSysBytes    uint64  `json:"heapSysBytes"`
	AllocBytes      uint64  `json:"allocBytes"`
	TotalAllocBytes uint64  `json:"totalAllocBytes"`
	SysBytes        uint64  `json:"sysBytes"`
	NumGC           uint32  `json:"numGC"`
	GoCPUSeconds    float64 `json:"goCpuSeconds,omitempty"`
}

// HandleMetrics serves the Prometheus text exposition format at /metrics.
func (s *Server) HandleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writePrometheus(w, s)
}

// HandleMetricsJSON returns process/runtime and aggregate SFU metrics for local
// benchmarking. It is deliberately JSON-only and dependency-free.
func (s *Server) HandleMetricsJSON(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	now := time.Now()
	_ = json.NewEncoder(w).Encode(benchmarkMetrics{
		Timestamp: now.UTC().Format(time.RFC3339Nano),
		UptimeSec: now.Sub(s.started).Seconds(),
		Host: hostMetrics{
			OS:     runtime.GOOS,
			Arch:   runtime.GOARCH,
			CPUs:   runtime.NumCPU(),
			Go:     runtime.Version(),
			Commit: "unavailable",
		},
		Runtime: readRuntimeMetrics(),
		SFU:     s.sfu.Metrics(),
		Auth:    s.auth.MetricsSnapshot(),
		Cluster: s.cluster.MetricsSnapshot(),
		Session: s.sessionMetricsSnapshot(),
		History: s.history.MetricsSnapshot(),
	})
}

func readRuntimeMetrics() runtimeMetrics {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	out := runtimeMetrics{
		Goroutines:      runtime.NumGoroutine(),
		HeapAllocBytes:  ms.HeapAlloc,
		HeapSysBytes:    ms.HeapSys,
		AllocBytes:      ms.Alloc,
		TotalAllocBytes: ms.TotalAlloc,
		SysBytes:        ms.Sys,
		NumGC:           ms.NumGC,
	}

	samples := []rtmetrics.Sample{
		{Name: "/cpu/classes/user:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/scavenge/total:cpu-seconds"},
	}
	rtmetrics.Read(samples)
	for _, sample := range samples {
		if sample.Value.Kind() == rtmetrics.KindFloat64 {
			out.GoCPUSeconds += sample.Value.Float64()
		}
	}
	return out
}
