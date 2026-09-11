package quality

import "testing"

// BenchmarkEngineIngest guards the per-sample QoE hot path (engine),
// which runs once per second per tracked stream. It must stay allocation-light.
//
//	go test ./internal/quality/ -run x -bench . -benchmem
func BenchmarkEngineIngest(b *testing.B) {
	e := New(DefaultConfig())
	key := StreamKey{Participant: "p", Direction: Inbound, Kind: Video, TrackID: "pub-1"}
	recv, lost, byts := 0.0, 0.0, 0.0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		recv += 1000
		lost += 5
		byts += 60000
		e.Ingest(Sample{
			Key: key, AtMillis: int64(i) * 1000,
			PacketsReceived: ptr(recv), PacketsLost: ptr(lost), BytesReceived: ptr(byts),
			JitterMs: ptr(12.0), FPS: ptr(28.0), Enabled: ptr(true),
		})
	}
}

func BenchmarkEngineSnapshot(b *testing.B) {
	e := New(DefaultConfig())
	for _, kind := range []Kind{Audio, Video} {
		for _, dir := range []Direction{Inbound, Outbound} {
			k := StreamKey{Participant: "p", Direction: dir, Kind: kind, TrackID: string(kind) + string(dir)}
			for i := 0; i < 10; i++ {
				e.Ingest(Sample{Key: k, AtMillis: int64(i) * 1000,
					PacketsReceived: ptr(float64(i * 1000)), PacketsLost: ptr(0.0),
					BytesReceived: ptr(float64(i * 50000)), JitterMs: ptr(8.0), Enabled: ptr(true)})
			}
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = e.Snapshot("p")
	}
}
