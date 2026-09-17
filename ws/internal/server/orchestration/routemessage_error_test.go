package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/broadcast"
)

// fakeAnalyticsCollector counts IncrementMessages calls so tests can prove the
// tenant-visible analytics increment fires exactly once per DELIVERED message,
// not once per publish attempt.
type fakeAnalyticsCollector struct {
	messageEvents atomic.Int64
}

func (f *fakeAnalyticsCollector) IncrementConnections(_, _ string, _ int64) {}
func (f *fakeAnalyticsCollector) IncrementMessages(_, _ string, _, _, _, _ int64) {
	f.messageEvents.Add(1)
}
func (f *fakeAnalyticsCollector) RecordPushDelivery(_, _ string, _, _, _, _, _ int64) {}
func (f *fakeAnalyticsCollector) Start(_ context.Context, _ *sync.WaitGroup) error    { return nil }
func (f *fakeAnalyticsCollector) Flush(_ context.Context) error                       { return nil }

// newRouteMessagePool builds a minimal pool wired to the given mock bus with
// the registry map resolving sukko.tenant1.market → tenant1.
func newRouteMessagePool(t *testing.T, bus *mockBroadcastBus, collector *fakeAnalyticsCollector) *MultiTenantConsumerPool {
	t.Helper()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:            []string{"localhost:9092"},
		Namespace:          "prod",
		Registry:           &mockTenantRegistry{},
		BroadcastBus:       bus,
		ResourceGuard:      &mockResourceGuard{},
		Logger:             logger,
		AnalyticsCollector: collector,
	})
	if err != nil {
		t.Fatalf("NewMultiTenantConsumerPool: %v", err)
	}
	pool.topicTenants.Store(map[string]string{"sukko.tenant1.market": "tenant1"})
	return pool
}

// TestRouteMessage_PublishErrorClasses pins routeMessage's BroadcastFunc return
// contract: a transient bus failure (broadcast.ErrPublishUnavailable) MUST be
// propagated so the Kafka consumer retries the same record before marking its
// offset, while permanent publish rejects MUST be swallowed (logged + counted
// as dropped) so an undeliverable record cannot wedge the partition.
func TestRouteMessage_PublishErrorClasses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		publishErr  error
		wantErr     bool
		wantDropped uint64
	}{
		{
			name:        "transient backend-down error propagates for retry",
			publishErr:  fmt.Errorf("%w: publish to %q: connection refused", broadcast.ErrPublishUnavailable, "ws.broadcast:tenant1"),
			wantErr:     true,
			wantDropped: 0,
		},
		{
			name:        "permanent reject is swallowed and counted as dropped",
			publishErr:  broadcast.ErrEmptyTenantID,
			wantErr:     false,
			wantDropped: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockBroadcastBus{publishErr: tt.publishErr}
			pool := newRouteMessagePool(t, bus, &fakeAnalyticsCollector{})

			err := pool.routeMessage("tenant1.BTC.trade", []byte(`{}`), "sukko.tenant1.market", 0, 100)

			if tt.wantErr {
				if !errors.Is(err, broadcast.ErrPublishUnavailable) {
					t.Errorf("routeMessage error: got %v, want ErrPublishUnavailable (consumer must retry the record)", err)
				}
			} else if err != nil {
				t.Errorf("routeMessage error: got %v, want nil (permanent reject must not wedge the partition)", err)
			}

			if got := pool.GetMetrics().MessagesDropped; got != tt.wantDropped {
				t.Errorf("MessagesDropped: got %d, want %d", got, tt.wantDropped)
			}
		})
	}
}

// TestRouteMessage_CountersFireOncePerDelivery pins the attempt-vs-message
// accounting: the consumer re-enters routeMessage on every retry of the same
// record, so MessagesRouted, the Prometheus routing callback, and — tenant
// visible, potentially billing-relevant — the analytics IncrementMessages call
// MUST fire only after a successful publish, never per failed attempt. Three
// failed attempts followed by one success must count exactly ONE message.
func TestRouteMessage_CountersFireOncePerDelivery(t *testing.T) {
	t.Parallel()
	transientErr := fmt.Errorf("%w: publish: connection refused", broadcast.ErrPublishUnavailable)
	bus := &mockBroadcastBus{publishErr: transientErr}
	collector := &fakeAnalyticsCollector{}
	pool := newRouteMessagePool(t, bus, collector)

	// Three failed attempts (the consumer's retry loop re-invoking BroadcastFunc).
	for range 3 {
		if err := pool.routeMessage("tenant1.BTC.trade", []byte(`{}`), "sukko.tenant1.market", 0, 100); !errors.Is(err, broadcast.ErrPublishUnavailable) {
			t.Fatalf("routeMessage during outage: got %v, want ErrPublishUnavailable", err)
		}
	}
	if got := pool.GetMetrics().MessagesRouted; got != 0 {
		t.Errorf("MessagesRouted after 3 failed attempts: got %d, want 0 (attempts are not messages)", got)
	}
	if got := collector.messageEvents.Load(); got != 0 {
		t.Errorf("analytics IncrementMessages after 3 failed attempts: got %d, want 0 (phantom tenant-visible messages)", got)
	}

	// Bus recovers; the retried record is delivered once.
	bus.mu.Lock()
	bus.publishErr = nil
	bus.mu.Unlock()
	if err := pool.routeMessage("tenant1.BTC.trade", []byte(`{}`), "sukko.tenant1.market", 0, 100); err != nil {
		t.Fatalf("routeMessage after recovery: got %v, want nil", err)
	}

	if got := pool.GetMetrics().MessagesRouted; got != 1 {
		t.Errorf("MessagesRouted after delivery: got %d, want 1", got)
	}
	if got := collector.messageEvents.Load(); got != 1 {
		t.Errorf("analytics IncrementMessages after delivery: got %d, want 1", got)
	}
}

// TestRouteMessage_PublishSuccessReturnsNil pins the happy path: nil error.
func TestRouteMessage_PublishSuccessReturnsNil(t *testing.T) {
	t.Parallel()
	bus := &mockBroadcastBus{}
	pool := newRouteMessagePool(t, bus, &fakeAnalyticsCollector{})

	if err := pool.routeMessage("tenant1.BTC.trade", []byte(`{}`), "sukko.tenant1.market", 0, 100); err != nil {
		t.Errorf("routeMessage: got %v, want nil", err)
	}
	if got := bus.getPublishCount(); got != 1 {
		t.Errorf("publishCount: got %d, want 1", got)
	}
}
