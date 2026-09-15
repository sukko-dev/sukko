package runner

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// probeUntilLive is the transport-agnostic core of the delivery-liveness warmup. These tests pin
// its retry / timeout / reset contract without a live WebSocket (publish/received/clear injected).

func TestProbeUntilLive_RepublishesUntilReceived(t *testing.T) {
	t.Parallel()
	// received flips true only after the 3rd publish — simulating the Kafka cold-start window where
	// the first probes are produced before the consumer joins (AtEnd) and are dropped.
	var publishCount, resetCount int
	publish := func() error { publishCount++; return nil }
	received := func() bool { return publishCount >= 3 }
	reset := func() { resetCount++ }

	if err := probeUntilLive(context.Background(), time.Second, time.Millisecond, publish, received, reset, zerolog.Nop()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if publishCount < 3 {
		t.Errorf("want >=3 publishes (republish until live), got %d", publishCount)
	}
	if resetCount != 1 {
		t.Errorf("want tracker reset exactly once on success, got %d", resetCount)
	}
}

func TestProbeUntilLive_TimesOut(t *testing.T) {
	t.Parallel()
	publish := func() error { return nil }
	received := func() bool { return false } // never live
	reset := func() { t.Fatal("reset must NOT be called when the loop never becomes live") }

	err := probeUntilLive(context.Background(), 20*time.Millisecond, time.Millisecond, publish, received, reset, zerolog.Nop())
	if err == nil {
		t.Fatal("want a timeout error when no probe is ever received, got nil (would ship a false green)")
	}
}

func TestProbeUntilLive_PublishErrorFailsFast(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("boom")
	publish := func() error { return wantErr }

	err := probeUntilLive(context.Background(), time.Second, time.Millisecond, publish,
		func() bool { return false }, func() { t.Fatal("reset must not be called") }, zerolog.Nop())
	if !errors.Is(err, wantErr) {
		t.Fatalf("want wrapped publish error, got %v", err)
	}
}

func TestProbeUntilLive_ContextCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Long interval/timeout so only the canceled ctx can fire (deterministic).
	err := probeUntilLive(ctx, time.Hour, time.Hour, func() error { return nil },
		func() bool { return false }, func() { t.Fatal("reset must not be called") }, zerolog.Nop())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

// measuredIDs must isolate the measured sequence (order-*) from delivery-warmup probe stragglers so
// a late warmup receipt cannot inflate the ordering count or corrupt the FIFO check (FR-W3).
func TestMeasuredIDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{"filters warmup stragglers", []string{"warmup-x", "order-0000", "warmup-y", "order-0001"}, []string{"order-0000", "order-0001"}},
		{"empty input", nil, []string{}},
		{"all warmup — nothing measured", []string{"warmup-a", "warmup-b"}, []string{}},
		{"preserves arrival order", []string{"order-0002", "order-0000", "order-0001"}, []string{"order-0002", "order-0000", "order-0001"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := measuredIDs(tt.in); !slices.Equal(got, tt.want) {
				t.Errorf("measuredIDs(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
