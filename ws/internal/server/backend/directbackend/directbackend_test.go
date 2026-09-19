package directbackend

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/broadcast"
)

// Compile-time interface check.
var _ backend.MessageBackend = (*DirectBackend)(nil)

// mockBus implements broadcast.Bus and records Publish calls for assertions.
type mockBus struct {
	mu         sync.Mutex
	messages   []*broadcast.Message
	publishErr error // returned by Publish when non-nil (message not recorded)
}

func (m *mockBus) Publish(msg *broadcast.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishErr != nil {
		return m.publishErr
	}
	m.messages = append(m.messages, msg)
	return nil
}

func (m *mockBus) Subscribe(_ string) (<-chan *broadcast.Message, error) {
	return make(chan *broadcast.Message), nil
}
func (m *mockBus) SubscribeAll() (<-chan *broadcast.Message, error) {
	return make(chan *broadcast.Message), nil
}
func (m *mockBus) Unsubscribe(_ string, _ <-chan *broadcast.Message) error { return nil }
func (m *mockBus) UnsubscribeAll(_ <-chan *broadcast.Message) error        { return nil }
func (m *mockBus) Run()                                                    {}
func (m *mockBus) Shutdown()                                               {}
func (m *mockBus) ShutdownWithContext(context.Context)                     {}
func (m *mockBus) IsHealthy() bool                                         { return true }
func (m *mockBus) GetMetrics() broadcast.Metrics                           { return broadcast.Metrics{} }

func (m *mockBus) published() []*broadcast.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*broadcast.Message, len(m.messages))
	copy(out, m.messages)
	return out
}

func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bus     broadcast.Bus
		wantErr bool
	}{
		{
			name:    "nil bus returns error",
			bus:     nil,
			wantErr: true,
		},
		{
			name:    "valid bus succeeds",
			bus:     &mockBus{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, err := New(tt.bus, zerolog.Nop())
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if db != nil {
					t.Fatal("expected nil DirectBackend when error is returned")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if db == nil {
				t.Fatal("expected non-nil DirectBackend")
			}
		})
	}
}

func TestStart(t *testing.T) {
	t.Parallel()

	db, err := New(&mockBus{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := db.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}
}

func TestPublish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		clientID int64
		channel  string
		data     []byte
	}{
		{
			name:     "simple message",
			clientID: 1,
			channel:  "BTC.trade",
			data:     []byte(`{"price":"50000"}`),
		},
		{
			name:     "empty payload",
			clientID: 2,
			channel:  "ETH.ticker",
			data:     []byte{},
		},
		{
			name:     "nil payload",
			clientID: 3,
			channel:  "XRP.depth",
			data:     nil,
		},
		{
			name:     "large client ID",
			clientID: 9999999,
			channel:  "DOGE.trade",
			data:     []byte(`{"amount":"1000000"}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bus := &mockBus{}
			db, err := New(bus, zerolog.Nop())
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			if _, err := db.Publish(context.Background(), tt.clientID, "test-tenant", tt.channel, tt.data); err != nil {
				t.Fatalf("Publish returned unexpected error: %v", err)
			}

			msgs := bus.published()
			if len(msgs) != 1 {
				t.Fatalf("expected 1 published message, got %d", len(msgs))
			}

			msg := msgs[0]
			if msg.Subject != tt.channel {
				t.Errorf("Subject: got %q, want %q", msg.Subject, tt.channel)
			}
			if !bytes.Equal(msg.Payload, tt.data) {
				t.Errorf("Payload: got %q, want %q", msg.Payload, tt.data)
			}
			if msg.TenantID != "test-tenant" {
				t.Errorf("TenantID: got %q, want %q", msg.TenantID, "test-tenant")
			}
		})
	}
}

func TestPublish_EmptyChannel(t *testing.T) {
	t.Parallel()

	db, err := New(&mockBus{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = db.Publish(context.Background(), 1, "test-tenant", "", []byte("data"))
	if err == nil {
		t.Fatal("expected error for empty channel, got nil")
	}
	if !errors.Is(err, backend.ErrPublishFailed) {
		t.Errorf("error = %v, want wrapping %v", err, backend.ErrPublishFailed)
	}
}

func TestPublish_EmptyTenantID(t *testing.T) {
	t.Parallel()

	bus := &mockBus{}
	db, err := New(bus, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = db.Publish(context.Background(), 1, "", "some.channel", []byte("data"))
	if err == nil {
		t.Fatal("expected error for empty tenantID, got nil")
	}
	if !errors.Is(err, backend.ErrPublishFailed) {
		t.Errorf("error = %v, want wrapping %v", err, backend.ErrPublishFailed)
	}
	// Verify the bus was NOT called — message must not be delivered with empty tenantID.
	if msgs := bus.published(); len(msgs) != 0 {
		t.Errorf("bus received %d messages, want 0 (message must be rejected before bus call)", len(msgs))
	}
}

func TestReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  backend.ReplayRequest
	}{
		{
			name: "empty request",
			req:  backend.ReplayRequest{},
		},
		{
			name: "request with positions",
			req: backend.ReplayRequest{
				Positions:     map[string]map[int32]int64{"topic-a": {0: 42}},
				MaxMessages:   100,
				Subscriptions: []string{"BTC.trade"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, err := New(&mockBus{}, zerolog.Nop())
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			msgs, err := db.Replay(context.Background(), tt.req)
			if err != nil {
				t.Fatalf("Replay returned unexpected error: %v", err)
			}
			if msgs != nil {
				t.Fatalf("expected nil messages, got %v", msgs)
			}
		})
	}
}

func TestIsHealthy(t *testing.T) {
	t.Parallel()

	db, err := New(&mockBus{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if !db.IsHealthy() {
		t.Fatal("IsHealthy: expected true, got false")
	}
}

func TestShutdown(t *testing.T) {
	t.Parallel()

	db, err := New(&mockBus{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := db.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown returned unexpected error: %v", err)
	}
}

// Publish must mint a stable message identity (mid): returned to the caller
// (for the publish ack) AND carried on the bus message (so the live and
// history copies share it). Direct mode has no replay, so bus-carriage is the
// entire cross-copy equality obligation here.
func TestPublish_MintsMid(t *testing.T) {
	t.Parallel()

	bus := &mockBus{}
	db, err := New(bus, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	mid, err := db.Publish(context.Background(), 1, "test-tenant", "BTC.trade", []byte(`{"p":1}`))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if mid == "" {
		t.Fatal("Publish returned empty mid — direct backend must mint one")
	}
	if len(mid) > 64 {
		t.Errorf("mid length = %d, want ≤ 64 (documented client contract)", len(mid))
	}

	msgs := bus.published()
	if len(msgs) != 1 {
		t.Fatalf("bus messages = %d, want 1", len(msgs))
	}
	if msgs[0].Mid != mid {
		t.Errorf("bus message mid = %q, returned mid = %q — must be identical (cross-copy identity)", msgs[0].Mid, mid)
	}
}

// Two publishes must never share a mid.
func TestPublish_MidsAreUnique(t *testing.T) {
	t.Parallel()

	bus := &mockBus{}
	db, err := New(bus, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	a, err := db.Publish(context.Background(), 1, "t", "BTC.trade", []byte(`{}`))
	if err != nil {
		t.Fatalf("Publish a: %v", err)
	}
	b, err := db.Publish(context.Background(), 1, "t", "BTC.trade", []byte(`{}`))
	if err != nil {
		t.Fatalf("Publish b: %v", err)
	}
	if a == b {
		t.Errorf("two publishes produced the same mid %q", a)
	}
}

// backendPublishErrorsValue reads the current value of
// ws_backend_publish_errors_total{backend="direct"} from the default gatherer
// (the metric lives behind unexported package globals in server/metrics, so the
// public Gather API is the only sanctioned way to observe it). Returns 0 when
// the series does not exist yet.
func backendPublishErrorsValue(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "ws_backend_publish_errors_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "backend" && lp.GetValue() == "direct" {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestPublish_BusFailureReturnsError pins the fail-fast contract on the
// client-publish path: when the broadcast bus rejects the message (e.g. Valkey
// down), Publish MUST return an error wrapping backend.ErrPublishFailed and no
// mid — acknowledging a message that never reached the bus would be silent
// data loss toward the publishing client. The failure must also be recorded on
// ws_backend_publish_errors_total{backend="direct"} (backends own their publish
// metrics — same contract as kafkabackend).
func TestPublish_BusFailureReturnsError(t *testing.T) {
	t.Parallel()

	bus := &mockBus{publishErr: broadcast.ErrPublishUnavailable}
	db, err := New(bus, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	errorsBefore := backendPublishErrorsValue(t)
	mid, err := db.Publish(context.Background(), 1, "test-tenant", "BTC.trade", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error when the bus rejects the publish, got nil")
	}
	if !errors.Is(err, backend.ErrPublishFailed) {
		t.Errorf("error = %v, want wrapping %v", err, backend.ErrPublishFailed)
	}
	if !errors.Is(err, broadcast.ErrPublishUnavailable) {
		t.Errorf("error = %v, want wrapping the bus cause %v", err, broadcast.ErrPublishUnavailable)
	}
	if mid != "" {
		t.Errorf("mid = %q, want empty (no ack identity for an undelivered message)", mid)
	}
	if got := backendPublishErrorsValue(t) - errorsBefore; got != 1 {
		t.Errorf("ws_backend_publish_errors_total{backend=%q} delta: got %.0f, want 1", "direct", got)
	}
}
