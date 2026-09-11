package main

import "testing"

func TestRegressed(t *testing.T) {
	if regressed(100, 120, 0.25) {
		t.Fatal("120 vs 100 within 25% tolerance should pass")
	}
	if !regressed(100, 130, 0.25) {
		t.Fatal("130 vs 100 exceeds 25% tolerance")
	}
	if regressed(0, 0, 0.25) {
		t.Fatal("0 vs 0 is not a regression")
	}
}

func TestScenarioKey(t *testing.T) {
	cases := []struct {
		name     string
		scenario string
		filename string
		want     string
	}{
		{"from scenario field", "baseline", "baseline-20260903T185936Z.json", "baseline"},
		{"empty scenario, timestamped file", "", "5pub-5sub-20260903T185936Z.json", "5pub-5sub"},
		{"empty scenario, plain file", "", "20-participants.json", "20-participants"},
		{"empty scenario, plain file without timestamp", "", "custom.json", "custom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scenarioKey(tc.scenario, tc.filename)
			if got != tc.want {
				t.Fatalf("scenarioKey(%q, %q) = %q, want %q", tc.scenario, tc.filename, got, tc.want)
			}
		})
	}
}
