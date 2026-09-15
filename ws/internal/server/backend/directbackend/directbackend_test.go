package directbackend

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/broadcast"
)

// Compile-time interface check.
var _ backend.MessageBackend = (*DirectBackend)(nil)

// mockBus implements broadcast.Bus and records Publish calls for assertions.
type mockBus struct {
	mu       sync.Mutex
	messages []*broadcast.Message
}

func (m *mockBus) Publish(msg *broadcast.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
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
