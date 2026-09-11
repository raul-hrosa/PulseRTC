package quality

// stabilizer smooths the per-window verdict into a stable status.
//
// A new verdict must repeat for DegradeSamples consecutive windows before the
// stable status gets worse, and RecoverSamples before it gets better. This
// stops GOOD⇄WARNING flapping on small oscillations while still reacting to a
// sustained change.
type stabilizer struct {
	stable    Status
	candidate Status
	streak    int
	degrade   int
	recover   int
}

func newStabilizer(cfg Config) *stabilizer {
	return &stabilizer{
		stable:  Unknown,
		degrade: cfg.DegradeSamples,
		recover: cfg.RecoverSamples,
	}
}

// observe feeds one windowed verdict. It returns the (possibly unchanged)
// stable status and whether it just changed.
func (s *stabilizer) observe(raw Status) (Status, bool) {
	if raw == s.stable {
		s.candidate = s.stable
		s.streak = 0
		return s.stable, false
	}

	if raw == s.candidate {
		s.streak++
	} else {
		s.candidate = raw
		s.streak = 1
	}

	// Default (degrade count): getting worse, first acquisition from Unknown,
	// or dropping to Unknown (track disabled / data gone) — all should react
	// reasonably fast. Only a strict improvement between two known statuses
	// uses the slower recover count.
	need := s.degrade
	if rank(s.stable) >= 0 && rank(raw) >= 0 && rank(raw) < rank(s.stable) {
		need = s.recover
	}

	if s.streak >= need {
		s.stable = raw
		s.candidate = raw
		s.streak = 0
		return s.stable, true
	}
	return s.stable, false
}
