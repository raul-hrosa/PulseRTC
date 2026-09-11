package cluster

import (
	"encoding/json"
	"testing"
	"time"
)

func FuzzClusterMessageValidate(f *testing.F) {
	f.Add([]byte(`{"type":"participant.message","requestId":"1","sourceNodeId":"a","targetNodeId":"b","timestamp":"2026-01-01T00:00:00Z"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"type":"x"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		var m ClusterMessage
		if json.Unmarshal(data, &m) != nil {
			return
		}
		_ = m.Validate(time.Now(), 30*time.Second, 65536) // must never panic
	})
}
