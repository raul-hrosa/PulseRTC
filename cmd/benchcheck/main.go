// Command benchcheck compares a fresh load-test run against the committed
// baseline and exits non-zero when a tracked metric regresses beyond a
// tolerance. It is the gate wired into CI by Task E3.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/raulhrosa/pulsertc/internal/loadtest"
)

// regressed reports whether cur is worse than base by more than tol (a
// fractional tolerance). A non-positive base is treated as "no data" so an
// empty baseline metric never fails the gate.
func regressed(base, cur, tol float64) bool {
	return base > 0 && cur > base*(1+tol)
}

// tsSuffix matches the "-<scenario>-<RFC3339-ish timestamp>.json" suffix that
// loadtest.WriteResult appends, e.g. "-20260903T185936Z".
var tsSuffix = regexp.MustCompile(`-\d{8}T\d{6}Z$`)

// scenarioKey derives the comparison key for a result: prefer the explicit
// Result.Scenario field; otherwise fall back to the filename with any
// timestamp suffix and the .json extension stripped.
func scenarioKey(scenario, filename string) string {
	if s := strings.TrimSpace(scenario); s != "" {
		return s
	}
	base := strings.TrimSuffix(filepath.Base(filename), ".json")
	base = tsSuffix.ReplaceAllString(base, "")
	return base
}

func loadDir(dir string) (map[string]loadtest.Result, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	out := make(map[string]loadtest.Result, len(matches))
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var r loadtest.Result
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		out[scenarioKey(r.Scenario, path)] = r
	}
	return out, nil
}

type metric struct {
	name string
	get  func(loadtest.ResultSummary) float64
}

var metrics = []metric{
	{"mediaStartMs.p95", func(s loadtest.ResultSummary) float64 { return s.MediaStartTimeMs.P95 }},
	{"cpuPercent.p95", func(s loadtest.ResultSummary) float64 { return s.CPUPercent.P95 }},
	{"heapAllocMb.p95", func(s loadtest.ResultSummary) float64 { return s.HeapAllocMB.P95 }},
}

func main() {
	baselineDir := flag.String("baseline", "benchmarks/baseline", "directory of baseline result JSON files")
	currentDir := flag.String("current", "", "directory of fresh result JSON files (required)")
	tolerance := flag.Float64("tolerance", 0.25, "fractional regression tolerance")
	flag.Parse()

	if *currentDir == "" {
		fmt.Fprintln(os.Stderr, "benchcheck: -current is required")
		os.Exit(2)
	}

	base, err := loadDir(*baselineDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchcheck: baseline: %v\n", err)
		os.Exit(2)
	}
	cur, err := loadDir(*currentDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchcheck: current: %v\n", err)
		os.Exit(2)
	}

	var scenarios []string
	for name := range base {
		if _, ok := cur[name]; ok {
			scenarios = append(scenarios, name)
		}
	}
	sort.Strings(scenarios)

	if len(scenarios) == 0 {
		fmt.Printf("benchcheck: no overlapping scenarios between %s and %s — nothing to compare\n", *baselineDir, *currentDir)
		os.Exit(0)
	}

	fmt.Printf("%-18s %-18s %12s %12s %10s  %s\n", "SCENARIO", "METRIC", "BASE", "CUR", "DELTA%", "STATUS")
	regressions := 0
	for _, name := range scenarios {
		bs := base[name].Summary
		cs := cur[name].Summary
		for _, m := range metrics {
			b := m.get(bs)
			c := m.get(cs)
			delta := 0.0
			if b > 0 {
				delta = (c - b) / b * 100
			}
			status := "OK"
			if regressed(b, c, *tolerance) {
				status = "REGRESSED"
				regressions++
			}
			fmt.Printf("%-18s %-18s %12.2f %12.2f %9.1f%%  %s\n", name, m.name, b, c, delta, status)
		}
	}

	fmt.Println()
	if regressions > 0 {
		fmt.Printf("benchcheck: %d metric(s) regressed past %.0f%% tolerance across %d scenario(s)\n", regressions, *tolerance*100, len(scenarios))
		os.Exit(1)
	}
	fmt.Printf("benchcheck: OK — %d scenario(s) within %.0f%% tolerance\n", len(scenarios), *tolerance*100)
}
