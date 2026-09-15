package registry

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// newTestWriter creates a Writer for unit tests. The Valkey client is nil because
// the tests here never call Run() or any method that touches the client.
// A fresh prometheus.Registry is used per test to avoid duplicate-registration panics.
func newTestWriter(t *testing.T, bufSize int) *Writer {
	t.Helper()
	cfg := &platform.ServerConfig{}
	cfg.Environment = "test"
	cfg.ConnectionsRegistryBuffer = bufSize
	cfg.ConnectionsRegistryFlushInterval = 100 * time.Millisecond
	cfg.ConnectionsRegistryHeartbeatInterval = 1 * time.Second
	cfg.ConnectionsRegistryRestartInitialBackoff = 10 * time.Millisecond
	cfg.ConnectionsRegistryRestartMaxBackoff = 1 * time.Second
	cfg.ConnectionsRegistryTTL = 120 * time.Second
	cfg.ConnectionsRegistryShutdownDrainTimeout = 0

	hw := NewHealthWriter(1)
	reg := prometheus.NewRegistry()
	return NewWriter(context.Background(), cfg, nil, hw, zerolog.Nop(), reg)
}

// TestWriter_PushConnect_Enqueues verifies that PushConnect places a kindConnect event
// on workChan with the correct fields.
func TestWriter_PushConnect_Enqueues(t *testing.T) {
	t.Parallel()

	w := newTestWriter(t, 8)
	now := time.Now()

	w.PushConnect("conn-1", "acme", "key-abc", "user-1", "pod-1", 0, "1.2.3.4", "ws", now)

	if len(w.workChan) != 1 {
		t.Fatalf("workChan length = %d, want 1", len(w.workChan))
	}
	ev := <-w.workChan
	if ev.Kind != kindConnect {
		t.Errorf("Kind = %v, want kindConnect", ev.Kind)
	}
	if ev.ConnID != "conn-1" {
		t.Errorf("ConnID = %q, want %q", ev.ConnID, "conn-1")
	}
	if ev.TenantID != "acme" {
		t.Errorf("TenantID = %q, want %q", ev.TenantID, "acme")
	}
	if ev.APIKeyID != "key-abc" {
		t.Errorf("APIKeyID = %q, want %q", ev.APIKeyID, "key-abc")
	}
	if ev.Transport != "ws" {
		t.Errorf("Transport = %q, want %q", ev.Transport, "ws")
	}
	if !ev.ConnectedAt.Equal(now) {
		t.Errorf("ConnectedAt = %v, want %v", ev.ConnectedAt, now)
	}
}

// TestWriter_PushDisconnect_Enqueues verifies that PushDisconnect places a kindDisconnect
// event with the correct identity fields.
func TestWriter_PushDisconnect_Enqueues(t *testing.T) {
	t.Parallel()

	w := newTestWriter(t, 8)

	w.PushDisconnect("conn-2", "tenant-x", "apikey-y")

	if len(w.workChan) != 1 {
		t.Fatalf("workChan length = %d, want 1", len(w.workChan))
	}
	ev := <-w.workChan
	if ev.Kind != kindDisconnect {
		t.Errorf("Kind = %v, want kindDisconnect", ev.Kind)
	}
	if ev.ConnID != "conn-2" {
		t.Errorf("ConnID = %q, want %q", ev.ConnID, "conn-2")
	}
	if ev.TenantID != "tenant-x" {
		t.Errorf("TenantID = %q, want %q", ev.TenantID, "tenant-x")
	}
	if ev.APIKeyID != "apikey-y" {
		t.Errorf("APIKeyID = %q, want %q", ev.APIKeyID, "apikey-y")
	}
}

// TestWriter_Push_DropsWhenFull verifies that push is non-blocking when workChan is full:
// excess events are dropped and pendingDrops is incremented for each dropped event.
func TestWriter_Push_DropsWhenFull(t *testing.T) {
	t.Parallel()

	// Buffer of 1: first push succeeds, second is dropped.
	w := newTestWriter(t, 1)

	w.PushConnect("conn-1", "t", "", "", "pod", 0, "1.1.1.1", "ws", time.Now())
	w.PushConnect("conn-2", "t", "", "", "pod", 0, "1.1.1.1", "ws", time.Now())

	drops := w.pendingDrops.Load()
	if drops != 1 {
		t.Errorf("pendingDrops = %d, want 1", drops)
	}
	if len(w.workChan) != 1 {
		t.Errorf("workChan length = %d, want 1 (first event stays)", len(w.workChan))
	}
}

// TestWriter_Push_MultipleDropsAccumulate verifies that each dropped event increments
// pendingDrops independently (not a one-time flag).
func TestWriter_Push_MultipleDropsAccumulate(t *testing.T) {
	t.Parallel()

	w := newTestWriter(t, 1)

	// Fill the channel.
	w.PushConnect("conn-0", "t", "", "", "pod", 0, "1.1.1.1", "ws", time.Now())

	// Three more drops.
	w.PushDisconnect("conn-1", "t", "")
	w.PushDisconnect("conn-2", "t", "")
	w.PushDisconnect("conn-3", "t", "")

	drops := w.pendingDrops.Load()
	if drops != 3 {
		t.Errorf("pendingDrops = %d, want 3", drops)
	}
}

// TestWriter_PushSubscribe_EventFields verifies that PushSubscribe enqueues a kindSubscribe
// event with the provided channels slice and capped flag intact.
func TestWriter_PushSubscribe_EventFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		connID   string
		channels []string
		capped   bool
	}{
		{
			name:     "uncapped channels",
			connID:   "conn-sub",
			channels: []string{"acme.prices", "acme.trades"},
			capped:   false,
		},
		{
			name:     "capped channels flag preserved",
			connID:   "conn-capped",
			channels: []string{"ch1", "ch2", "ch3"},
			capped:   true,
		},
		{
			name:     "empty channels list",
			connID:   "conn-empty",
			channels: []string{},
			capped:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := newTestWriter(t, 8)
			w.PushSubscribe(tt.connID, tt.channels, tt.capped)

			if len(w.workChan) != 1 {
				t.Fatalf("workChan length = %d, want 1", len(w.workChan))
			}
			ev := <-w.workChan
			if ev.Kind != kindSubscribe {
				t.Errorf("Kind = %v, want kindSubscribe", ev.Kind)
			}
			if ev.ConnID != tt.connID {
				t.Errorf("ConnID = %q, want %q", ev.ConnID, tt.connID)
			}
			if ev.ChannelsCapped != tt.capped {
				t.Errorf("ChannelsCapped = %v, want %v", ev.ChannelsCapped, tt.capped)
			}
			if len(ev.Channels) != len(tt.channels) {
				t.Errorf("len(Channels) = %d, want %d", len(ev.Channels), len(tt.channels))
			}
		})
	}
}

// TestWriter_PushUnsubscribe_EventFields verifies that PushUnsubscribe enqueues a
// kindUnsubscribe event with the correct fields.
func TestWriter_PushUnsubscribe_EventFields(t *testing.T) {
	t.Parallel()

	w := newTestWriter(t, 8)
	channels := []string{"acme.prices"}

	w.PushUnsubscribe("conn-unsub", channels, false)

	if len(w.workChan) != 1 {
		t.Fatalf("workChan length = %d, want 1", len(w.workChan))
	}
	ev := <-w.workChan
	if ev.Kind != kindUnsubscribe {
		t.Errorf("Kind = %v, want kindUnsubscribe", ev.Kind)
	}
	if ev.ConnID != "conn-unsub" {
		t.Errorf("ConnID = %q, want %q", ev.ConnID, "conn-unsub")
	}
	if len(ev.Channels) != 1 || ev.Channels[0] != "acme.prices" {
		t.Errorf("Channels = %v, want [acme.prices]", ev.Channels)
	}
}

// TestWriter_PendingDrops_SwapToZero verifies that reading pendingDrops with Swap(0) is
// idiomatic: after a drop the counter is non-zero, and after Swap it resets.
// This mirrors the two-phase drain: Writer.pendingDrops.Swap(0) → HealthWriter.AddDrops.
func TestWriter_PendingDrops_SwapToZero(t *testing.T) {
	t.Parallel()

	w := newTestWriter(t, 1)

	// Fill channel, then force a drop.
	w.PushConnect("conn-0", "t", "", "", "pod", 0, "1.1.1.1", "ws", time.Now())
	w.PushConnect("conn-1", "t", "", "", "pod", 0, "1.1.1.1", "ws", time.Now())

	before := w.pendingDrops.Swap(0)
	if before != 1 {
		t.Fatalf("pendingDrops before swap = %d, want 1", before)
	}
	after := w.pendingDrops.Load()
	if after != 0 {
		t.Errorf("pendingDrops after swap = %d, want 0", after)
	}
}

// TestWriter_WorkChan_Capacity verifies that the writer allocates a workChan with
// exactly the configured capacity.
func TestWriter_WorkChan_Capacity(t *testing.T) {
	t.Parallel()

	for _, bufSize := range []int{1, 4, 16, 64} {
		w := newTestWriter(t, bufSize)
		if cap(w.workChan) != bufSize {
			t.Errorf("cap(workChan) = %d, want %d", cap(w.workChan), bufSize)
		}
	}
}
