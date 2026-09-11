package quality

import "testing"

func TestHysteresisNoFlapOnSmallOscillation(t *testing.T) {
	s := newStabilizer(Config{DegradeSamples: 3, RecoverSamples: 5})
	// acquire GOOD
	for i := 0; i < 5; i++ {
		s.observe(Good)
	}
	if s.stable != Good {
		t.Fatalf("expected stable GOOD, got %s", s.stable)
	}
	// GOOD, WARNING, GOOD, WARNING, GOOD — must stay GOOD
	for _, st := range []Status{Warning, Good, Warning, Good, Warning, Good} {
		got, changed := s.observe(st)
		if got != Good || changed {
			t.Fatalf("oscillation moved stable status to %s (changed=%v)", got, changed)
		}
	}
}

func TestHysteresisDegradesOnConsistentDrop(t *testing.T) {
	s := newStabilizer(Config{DegradeSamples: 3, RecoverSamples: 5})
	for i := 0; i < 5; i++ {
		s.observe(Good)
	}
	// two WARNING: still GOOD
	s.observe(Warning)
	if got, _ := s.observe(Warning); got != Good {
		t.Fatalf("2 consecutive WARNING should not flip yet, got %s", got)
	}
	// third WARNING: flip
	got, changed := s.observe(Warning)
	if got != Warning || !changed {
		t.Fatalf("3rd consecutive WARNING should flip to WARNING, got %s changed=%v", got, changed)
	}
}

func TestHysteresisRecoversSlower(t *testing.T) {
	s := newStabilizer(Config{DegradeSamples: 3, RecoverSamples: 5})
	for i := 0; i < 5; i++ {
		s.observe(Poor)
	}
	if s.stable != Poor {
		t.Fatalf("expected stable POOR, got %s", s.stable)
	}
	// 4 GOOD: still POOR (recover needs 5)
	for i := 0; i < 4; i++ {
		if got, _ := s.observe(Good); got != Poor {
			t.Fatalf("recovery too fast: flipped after %d samples", i+1)
		}
	}
	got, changed := s.observe(Good)
	if got != Good || !changed {
		t.Fatalf("5th GOOD should recover, got %s changed=%v", got, changed)
	}
}
