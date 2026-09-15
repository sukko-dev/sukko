package server

import (
	"testing"

	"github.com/sukko-dev/sukko/internal/server/messaging"
)

// BenchmarkOutgoingMsgAppendTo measures per-call cost of AppendTo with buffer reuse.
// Expected: 0 allocs/op in steady-state (buffer pre-warmed before loop).
func BenchmarkOutgoingMsgAppendTo(b *testing.B) {
	env, err := messaging.NewBroadcastEnvelope("BTC.trade", 1708903200000, []byte(`{"price":45000}`), "", "")
	if err != nil {
		b.Fatalf("NewBroadcastEnvelope: %v", err)
	}
	msg := OutgoingMsg{envelope: env, seq: 42}
	var dst []byte
	dst = msg.AppendTo(nil)
	if len(dst) == 0 {
		b.Fatal("pre-warm returned empty bytes")
	}
	b.ResetTimer()
	for b.Loop() {
		dst = msg.AppendTo(dst[:0])
	}
}
