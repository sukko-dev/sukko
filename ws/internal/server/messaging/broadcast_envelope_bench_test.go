package messaging

import "testing"

// BenchmarkEnvelopeAppendTo measures per-call cost of AppendTo with buffer reuse.
// Expected: 0 allocs/op in steady-state (buffer pre-warmed before loop).
func BenchmarkEnvelopeAppendTo(b *testing.B) {
	env := newTestEnvelope(b)
	var dst []byte
	dst = env.AppendTo(nil, 0)
	if len(dst) == 0 {
		b.Fatal("pre-warm returned empty bytes")
	}
	b.ResetTimer()
	for b.Loop() {
		dst = env.AppendTo(dst[:0], 42)
	}
}
