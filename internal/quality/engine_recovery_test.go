package quality

import "testing"

// A participant mid session-recovery is reported as WARNING with
// reason SESSION_RECOVERY, not a POOR network verdict from the torn-down
// connection's last samples.
func TestSnapshotRecoveringOverridesPoor(t *testing.T) {
	e := New(DefaultConfig())

	// Drive the connection stream to POOR with sustained loss + RTT.
	for i := 0; i < 12; i++ {
		e.Ingest(Sample{Key: StreamKey{Participant: "alice", Kind: Connection}})
	}
	base := e.Snapshot("alice")

	e.SetRecovering("alice", true)
	rec := e.Snapshot("alice")
	if rec.Reason != "SESSION_RECOVERY" {
		t.Fatalf("reason = %q, want SESSION_RECOVERY", rec.Reason)
	}
	if rec.Overall == Poor {
		t.Fatalf("overall still POOR during recovery (base was %v)", base.Overall)
	}

	e.SetRecovering("alice", false)
	if got := e.Snapshot("alice"); got.Reason == "SESSION_RECOVERY" {
		t.Fatal("recovery reason not cleared")
	}

	// Forget also clears the flag.
	e.SetRecovering("alice", true)
	e.Forget("alice")
	if got := e.Snapshot("alice"); got.Reason == "SESSION_RECOVERY" {
		t.Fatal("Forget did not clear recovering flag")
	}
}
