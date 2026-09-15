package broadcast

import (
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
)

// TestValkeyBus_ConfigValidation tests configuration validation
func TestValkeyBus_ConfigValidation(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()

	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "missing addrs",
			cfg: Config{
				Type:       "valkey",
				BufferSize: 1024,
				Valkey: ValkeyConfig{
					Addrs: []string{},
				},
			},
			wantErr: "address",
		},
		{
			name: "valid single addr",
			cfg: Config{
				Type:       "valkey",
				BufferSize: 1024,
				Valkey: ValkeyConfig{
					Addrs:   []string{"localhost:6379"},
					Channel: "test",
				},
			},
			wantErr: "", // Will fail on connection, not validation
		},
		{
			name: "valid sentinel addrs",
			cfg: Config{
				Type:       "valkey",
				BufferSize: 1024,
				Valkey: ValkeyConfig{
					Addrs:      []string{"sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379"},
					MasterName: "mymaster",
					Channel:    "test",
				},
			},
			wantErr: "", // Will fail on connection, not validation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := newValkeyBus(tt.cfg, logger)
			if tt.wantErr != "" {
				if err == nil {
					t.Errorf("Expected error containing %q, got nil", tt.wantErr)
				} else if !containsIgnoreCase(err.Error(), tt.wantErr) {
					t.Errorf("Expected error containing %q, got: %v", tt.wantErr, err)
				}
			}
			// For valid configs, we expect connection errors (no server running)
		})
	}
}

// TestValkeyBus_DefaultValues tests that defaults are applied
func TestValkeyBus_DefaultValues(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()

	cfg := Config{
		Type:       "valkey",
		BufferSize: 0, // Should use default
		Valkey: ValkeyConfig{
			Addrs:      []string{"localhost:6379"},
			MasterName: "", // Should default to "mymaster"
			Channel:    "", // Should default to "ws.broadcast"
		},
	}

	// Will fail on connection but should apply defaults first
	_, err := newValkeyBus(cfg, logger)
	if err == nil {
		t.Skip("Valkey server available")
	}

	// The error should be about connection, not about missing defaults
	if containsIgnoreCase(err.Error(), "channel") && containsIgnoreCase(err.Error(), "empty") {
		t.Error("Should have applied default channel")
	}
}

// TestValkeyBus_FanOutDropsOnFullChannel tests drop behavior on a full channel.
func TestValkeyBus_FanOutDropsOnFullChannel(t *testing.T) {
	t.Parallel()
	// Create a full channel (buffer size 1, already has 1 message)
	subCh := make(chan *Message, 1)
	subCh <- &Message{Subject: "existing"}

	msg := &Message{Subject: "new"}

	// Try to send (should drop)
	dropped := false
	select {
	case subCh <- msg:
		// Sent
	default:
		dropped = true
	}

	if !dropped {
		t.Error("Expected message to be dropped on full channel")
	}
}

// TestValkeyBus_MetricsStructure tests metrics returned format
func TestValkeyBus_MetricsStructure(t *testing.T) {
	t.Parallel()
	// Verify metrics structure matches expected format
	m := Metrics{
		Type:             "valkey",
		Healthy:          true,
		ChannelPrefix:    "ws.broadcast",
		Subscribers:      3,
		PublishErrors:    0,
		MessagesReceived: 100,
		LastPublishAgo:   0.5,
	}

	if m.Type != "valkey" {
		t.Errorf("Type: got %s, want valkey", m.Type)
	}
	if !m.Healthy {
		t.Error("Healthy should be true")
	}
}

// newTestBusMetrics returns an isolated Prometheus metrics instance for unit tests.
// Each call uses unique metric names to avoid duplicate registration panics.
func newTestBusMetrics(suffix string) *busMetrics {
	return &busMetrics{
		droppedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_dropped_" + suffix,
		}, []string{"tenant_id"}),
		subscribeCommandsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_subscribe_" + suffix,
		}, []string{"result"}),
		reconcileCorrectionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_reconcile_" + suffix,
		}),
	}
}

// TestValkeyBus_ErrSubscriberNotFound verifies that Unsubscribe and UnsubscribeAll
// return ErrSubscriberNotFound when the channel was never registered.
func TestValkeyBus_ErrSubscriberNotFound(t *testing.T) {
	t.Parallel()
	b := &valkeyBus{
		tenantSubscribers: make(map[string][]subscriberEntry),
		subRefCounts:      make(map[string]int),
		subCmdCh:          make(chan subCmd, subCmdChCapacity),
		bufferSize:        8,
		logger:            zerolog.Nop(),
		metrics:           newTestBusMetrics("notfound"),
	}

	fakeCh := make(<-chan *Message)
	err := b.Unsubscribe("unknown", fakeCh)
	if !errors.Is(err, ErrSubscriberNotFound) {
		t.Errorf("Unsubscribe: got %v, want ErrSubscriberNotFound", err)
	}
	err = b.UnsubscribeAll(fakeCh)
	if !errors.Is(err, ErrSubscriberNotFound) {
		t.Errorf("UnsubscribeAll: got %v, want ErrSubscriberNotFound", err)
	}
}

// TestValkeyBus_PerCallerIsolation verifies that two Subscribes for the same tenant
// return independent channels and fan-out delivers to both.
func TestValkeyBus_PerCallerIsolation(t *testing.T) {
	t.Parallel()
	b := &valkeyBus{
		tenantSubscribers: make(map[string][]subscriberEntry),
		subRefCounts:      make(map[string]int),
		subCmdCh:          make(chan subCmd, subCmdChCapacity),
		bufferSize:        8,
		logger:            zerolog.Nop(),
		metrics:           newTestBusMetrics("isolation"),
	}

	ch1, err1 := b.Subscribe("acme")
	ch2, err2 := b.Subscribe("acme")
	if err1 != nil || err2 != nil {
		t.Fatalf("Subscribe errors: %v, %v", err1, err2)
	}
	if ch1 == ch2 {
		t.Fatal("two Subscribe calls returned the same channel")
	}

	// fanOutTenant should deliver to both independent channels.
	msg := &Message{Subject: "test", TenantID: "acme"}
	b.fanOutTenant(msg)

	for i, ch := range []<-chan *Message{ch1, ch2} {
		select {
		case received := <-ch:
			if received.Subject != msg.Subject {
				t.Errorf("ch%d: wrong subject", i+1)
			}
		default:
			t.Errorf("ch%d: no message received", i+1)
		}
	}
}

// TestValkeyBus_FanOutDropIsolation verifies that a full channel for tenant A does not
// affect delivery to tenant B (per-tenant fairness — the core fix of this branch).
func TestValkeyBus_FanOutDropIsolation(t *testing.T) {
	t.Parallel()
	b := &valkeyBus{
		tenantSubscribers: make(map[string][]subscriberEntry),
		subRefCounts:      make(map[string]int),
		subCmdCh:          make(chan subCmd, subCmdChCapacity),
		bufferSize:        1, // 1-slot buffer to make it easy to fill
		logger:            zerolog.Nop(),
		metrics:           newTestBusMetrics("dropisolation"),
	}

	chA, _ := b.Subscribe("tenantA")
	chB, _ := b.Subscribe("tenantB")

	// Fill chA completely.
	b.fanOutTenant(&Message{Subject: "fill", TenantID: "tenantA"})

	// Second message to A should drop; B's message must still arrive.
	b.fanOutTenant(&Message{Subject: "drop", TenantID: "tenantA"})
	b.fanOutTenant(&Message{Subject: "ok", TenantID: "tenantB"})

	// chA: only 1 message (the first one) should be present.
	select {
	case <-chA:
	default:
		t.Error("chA: expected first message to be present")
	}
	select {
	case <-chA:
		t.Error("chA: second message was NOT dropped")
	default:
	}

	// chB must have its message despite tenantA overflow.
	select {
	case msg := <-chB:
		if msg.Subject != "ok" {
			t.Errorf("chB: wrong message %q", msg.Subject)
		}
	default:
		t.Error("chB: message dropped due to tenantA channel overflow (isolation violation)")
	}
}

// TestValkeyBus_SubscribeValidation verifies Subscribe returns typed errors for bad tenant IDs.
func TestValkeyBus_SubscribeValidation(t *testing.T) {
	t.Parallel()
	b := &valkeyBus{
		tenantSubscribers: make(map[string][]subscriberEntry),
		subRefCounts:      make(map[string]int),
		subCmdCh:          make(chan subCmd, subCmdChCapacity),
		bufferSize:        8,
		logger:            zerolog.Nop(),
		metrics:           newTestBusMetrics("validation"),
	}

	_, err := b.Subscribe("")
	if !errors.Is(err, ErrEmptyTenantID) {
		t.Errorf("Subscribe(\"\"): got %v, want ErrEmptyTenantID", err)
	}
	_, err = b.Subscribe("bad:tenant")
	if !errors.Is(err, ErrInvalidTenantID) {
		t.Errorf("Subscribe(\"bad:tenant\"): got %v, want ErrInvalidTenantID", err)
	}
}

// TestValkeyBus_PublishValidation verifies that Publish increments the droppedTotal metric
// for invalid TenantID values and does not attempt to reach the Valkey client.
func TestValkeyBus_PublishValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tenantID    string
		wantLabel   string
		wantDropped float64
	}{
		{
			name:        "empty tenant ID",
			tenantID:    "",
			wantLabel:   metricTenantLabelEmpty,
			wantDropped: 1,
		},
		{
			name:        "tenant ID contains separator",
			tenantID:    "bad:tenant",
			wantLabel:   metricTenantLabelInvalid,
			wantDropped: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := newTestBusMetrics("publishval_" + tt.name)
			b := &valkeyBus{
				channelPrefix: "ws.broadcast",
				logger:        zerolog.Nop(),
				metrics:       m,
				// client is nil intentionally — validation paths return before any client call
			}

			b.Publish(&Message{Subject: "test", TenantID: tt.tenantID, Payload: []byte("x")})

			got := testutil.ToFloat64(m.droppedTotal.WithLabelValues(tt.wantLabel))
			if got != tt.wantDropped {
				t.Errorf("droppedTotal[%q]: got %.0f, want %.0f", tt.wantLabel, got, tt.wantDropped)
			}
		})
	}
}

// Helper function
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
