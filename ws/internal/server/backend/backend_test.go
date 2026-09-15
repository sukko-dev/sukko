package backend_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sukko-dev/sukko/internal/server/backend"
)

// mockBackend is a minimal MessageBackend implementation for interface contract tests.
type mockBackend struct {
	startErr    error
	publishErr  error
	replayMsgs  []backend.ReplayMessage
	replayErr   error
	healthy     bool
	ready       bool
	shutdownErr error
	shutdowns   int
}

func (m *mockBackend) Start(_ context.Context) error {
	return m.startErr
}

func (m *mockBackend) Publish(_ context.Context, _ int64, _, _ string, _ []byte) (string, error) {
	return "mock-mid", m.publishErr
}

func (m *mockBackend) Replay(_ context.Context, _ backend.ReplayRequest) ([]backend.ReplayMessage, error) {
	return m.replayMsgs, m.replayErr
}

func (m *mockBackend) IsHealthy() bool {
	return m.healthy
}

func (m *mockBackend) Ready() bool {
	return m.ready
}

func (m *mockBackend) Shutdown(_ context.Context) error {
	m.shutdowns++
	return m.shutdownErr
}

func (m *mockBackend) ChannelTopic(_ string) (string, bool) { return "", false }

// Compile-time interface check.
var _ backend.MessageBackend = (*mockBackend)(nil)

func TestMockBackend_Start(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		wantErr bool
	}{
		{
			name:    "returns nil on success",
			err:     nil,
			wantErr: false,
		},
		{
			name:    "returns error on failure",
			err:     errors.New("start failed"),
			wantErr: true,
		},
		{
			name:    "returns sentinel ErrBackendUnavailable",
			err:     backend.ErrBackendUnavailable,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mb := &mockBackend{startErr: tt.err}
			var iface backend.MessageBackend = mb

			err := iface.Start(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMockBackend_Replay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		req       backend.ReplayRequest
		msgs      []backend.ReplayMessage
		err       error
		wantErr   bool
		wantCount int
	}{
		{
			name: "nil positions returns without error",
			req: backend.ReplayRequest{
				Positions:     nil,
				MaxMessages:   10,
				Subscriptions: []string{"ch1"},
			},
			msgs:      nil,
			err:       nil,
			wantErr:   false,
			wantCount: 0,
		},
		{
			name: "empty positions returns without error",
			req: backend.ReplayRequest{
				Positions:     map[string]map[int32]int64{},
				MaxMessages:   10,
				Subscriptions: []string{"ch1"},
			},
			msgs:      nil,
			err:       nil,
			wantErr:   false,
			wantCount: 0,
		},
		{
			name: "empty subscriptions returns without error",
			req: backend.ReplayRequest{
				Positions:   map[string]map[int32]int64{"topic1": {0: 0}},
				MaxMessages: 10,
			},
			msgs:      nil,
			err:       nil,
			wantErr:   false,
			wantCount: 0,
		},
		{
			name: "returns messages on success",
			req: backend.ReplayRequest{
				Positions:     map[string]map[int32]int64{"topic1": {0: 5}},
				MaxMessages:   100,
				Subscriptions: []string{"ch1"},
			},
			msgs: []backend.ReplayMessage{
				{Subject: "ch1", Data: []byte("msg1")},
				{Subject: "ch1", Data: []byte("msg2")},
			},
			err:       nil,
			wantErr:   false,
			wantCount: 2,
		},
		{
			name: "returns ErrReplayNotSupported",
			req: backend.ReplayRequest{
				Positions:     map[string]map[int32]int64{"topic1": {0: 0}},
				MaxMessages:   10,
				Subscriptions: []string{"ch1"},
			},
			msgs:      nil,
			err:       backend.ErrReplayNotSupported,
			wantErr:   true,
			wantCount: 0,
		},
		{
			name:      "zero-value request returns without error",
			req:       backend.ReplayRequest{},
			msgs:      nil,
			err:       nil,
			wantErr:   false,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mb := &mockBackend{replayMsgs: tt.msgs, replayErr: tt.err}
			var iface backend.MessageBackend = mb

			msgs, err := iface.Replay(context.Background(), tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Replay() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(msgs) != tt.wantCount {
				t.Errorf("Replay() returned %d messages, want %d", len(msgs), tt.wantCount)
			}
		})
	}
}

func TestMockBackend_IsHealthy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		val  bool
		want bool
	}{
		{
			name: "returns true when healthy",
			val:  true,
			want: true,
		},
		{
			name: "returns false when unhealthy",
			val:  false,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mb := &mockBackend{healthy: tt.val}
			var iface backend.MessageBackend = mb

			got := iface.IsHealthy()
			if got != tt.want {
				t.Errorf("IsHealthy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMockBackend_Shutdown_Idempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		wantErr bool
	}{
		{
			name:    "shutdown succeeds and is idempotent",
			err:     nil,
			wantErr: false,
		},
		{
			name:    "shutdown with error is idempotent",
			err:     errors.New("shutdown failed"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mb := &mockBackend{shutdownErr: tt.err}
			var iface backend.MessageBackend = mb
			ctx := context.Background()

			// First call — must not panic.
			err1 := iface.Shutdown(ctx)
			if (err1 != nil) != tt.wantErr {
				t.Errorf("Shutdown() first call error = %v, wantErr %v", err1, tt.wantErr)
			}

			// Second call — must not panic (idempotent).
			err2 := iface.Shutdown(ctx)
			if (err2 != nil) != tt.wantErr {
				t.Errorf("Shutdown() second call error = %v, wantErr %v", err2, tt.wantErr)
			}

			if mb.shutdowns != 2 {
				t.Errorf("expected Shutdown called 2 times, got %d", mb.shutdowns)
			}
		})
	}
}

func TestMockBackend_Publish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		wantErr bool
	}{
		{
			name:    "publish succeeds",
			err:     nil,
			wantErr: false,
		},
		{
			name:    "publish returns error",
			err:     backend.ErrPublishFailed,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mb := &mockBackend{publishErr: tt.err}
			var iface backend.MessageBackend = mb

			_, err := iface.Publish(context.Background(), 42, "test-tenant", "test.channel", []byte("payload"))
			if (err != nil) != tt.wantErr {
				t.Errorf("Publish() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
