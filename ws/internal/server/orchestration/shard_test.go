package orchestration

import (
	"testing"

	"github.com/sukko-dev/sukko/internal/server/broadcast"
)

// =============================================================================
// Shard Configuration Tests
// =============================================================================

func TestShard_GetMaxConnections(t *testing.T) {
	t.Parallel()
	tests := []int{1, 10, 100, 1000, 10000}

	for _, max := range tests {
		shard := &Shard{maxConnections: max}
		if got := shard.GetMaxConnections(); got != max {
			t.Errorf("GetMaxConnections() = %d, want %d", got, max)
		}
	}
}

// =============================================================================
// Broadcast Message Tests
// =============================================================================

func TestBroadcastMessage_Fields(t *testing.T) {
	t.Parallel()
	msg := &broadcast.Message{
		Subject: "BTC.trade",
		Payload: []byte(`{"price":"100.50"}`),
	}

	if msg.Subject != "BTC.trade" {
		t.Errorf("Subject: got %s, want BTC.trade", msg.Subject)
	}
	if string(msg.Payload) != `{"price":"100.50"}` {
		t.Errorf("Payload: got %s, want {\"price\":\"100.50\"}", msg.Payload)
	}
}
