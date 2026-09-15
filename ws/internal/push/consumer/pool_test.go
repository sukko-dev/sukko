package consumer

import (
	"testing"

	"github.com/rs/zerolog"
)

// validPoolConfig returns a PoolConfig with all required fields for testing.
func validPoolConfig() PoolConfig {
	return PoolConfig{
		Brokers:       []string{"localhost:9092"},
		Namespace:     "local",
		ConsumerGroup: "push-service",
		Logger:        zerolog.Nop(),
	}
}

func TestNewPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modify  func(*PoolConfig)
		wantErr string
	}{
		{
			name:    "valid config",
			modify:  func(_ *PoolConfig) {},
			wantErr: "",
		},
		{
			name: "empty brokers",
			modify: func(c *PoolConfig) {
				c.Brokers = nil
			},
			wantErr: "at least one broker is required",
		},
		{
			name: "empty namespace",
			modify: func(c *PoolConfig) {
				c.Namespace = ""
			},
			wantErr: "namespace is required",
		},
		{
			name: "empty consumer group",
			modify: func(c *PoolConfig) {
				c.ConsumerGroup = ""
			},
			wantErr: "consumer group is required",
		},
		{
			name: "multiple brokers accepted",
			modify: func(c *PoolConfig) {
				c.Brokers = []string{"broker1:9092", "broker2:9092", "broker3:9092"}
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validPoolConfig()
			tt.modify(&cfg)
			pool, err := NewPool(cfg)

			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				if pool == nil {
					t.Fatal("expected non-nil pool")
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if pool != nil {
				t.Error("expected nil pool on error")
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}

func TestExtractTenantFromTopic(t *testing.T) {
	t.Parallel()

	pool := &Pool{
		config: PoolConfig{
			Namespace: "prod",
		},
	}

	tests := []struct {
		name    string
		topic   string
		wantTID string
	}{
		{
			name:    "valid three-segment topic",
			topic:   "prod.tenant-abc.messages",
			wantTID: "tenant-abc",
		},
		{
			name:    "valid with UUID tenant",
			topic:   "prod.550e8400-e29b-41d4-a716-446655440000.events",
			wantTID: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:    "wrong namespace prefix",
			topic:   "dev.tenant-abc.messages",
			wantTID: "",
		},
		{
			name:    "no suffix segment after tenant",
			topic:   "prod.tenant-abc",
			wantTID: "",
		},
		{
			name:    "empty topic",
			topic:   "",
			wantTID: "",
		},
		{
			name:    "only namespace prefix",
			topic:   "prod.",
			wantTID: "",
		},
		{
			name:    "namespace without dot separator",
			topic:   "prodtenant.messages",
			wantTID: "",
		},
		{
			name:    "extra segments are ignored",
			topic:   "prod.tenant-x.events.sub",
			wantTID: "tenant-x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := pool.extractTenantFromTopic(tt.topic)
			if got != tt.wantTID {
				t.Errorf("extractTenantFromTopic(%q) = %q, want %q", tt.topic, got, tt.wantTID)
			}
		})
	}
}

func TestUpdateTopicsBeforeStart(t *testing.T) {
	t.Parallel()

	pool, err := NewPool(validPoolConfig())
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	// UpdateTopics before Start should just update the internal topic set without panicking.
	pool.UpdateTopics([]string{"local.tenant1.messages", "local.tenant2.events"})

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if len(pool.topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(pool.topics))
	}
	if !pool.topics["local.tenant1.messages"] {
		t.Error("expected topic local.tenant1.messages to be tracked")
	}
	if !pool.topics["local.tenant2.events"] {
		t.Error("expected topic local.tenant2.events to be tracked")
	}

	// Updating again should replace the set
	pool.mu.Unlock()
	pool.UpdateTopics([]string{"local.tenant3.messages"})
	pool.mu.Lock()

	if len(pool.topics) != 1 {
		t.Fatalf("expected 1 topic after update, got %d", len(pool.topics))
	}
	if !pool.topics["local.tenant3.messages"] {
		t.Error("expected topic local.tenant3.messages to be tracked")
	}
}

// TODO: integration tests with Kafka broker for Start, consumeLoop, and UpdateTopics
// with a live client. The consumer pool's core loop requires a real Kafka connection;
// these tests cover the parseable/validatable parts only.

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
