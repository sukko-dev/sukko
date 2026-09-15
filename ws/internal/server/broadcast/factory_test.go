package broadcast

import (
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestNewBus_InvalidType(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()

	cfg := Config{
		Type: "invalid",
	}

	_, err := NewBus(cfg, logger)
	if err == nil {
		t.Error("Expected error for invalid type, got nil")
	}
	if !errors.Is(err, ErrUnknownBroadcastType) {
		t.Errorf("Expected ErrUnknownBroadcastType, got: %v", err)
	}
}

func TestNewBus_NATSRejected(t *testing.T) {
	t.Parallel()
	cfg := Config{Type: "nats"}
	_, err := NewBus(cfg, zerolog.Nop())
	if err == nil {
		t.Fatal("Expected error for nats type, got nil")
	}
	if !errors.Is(err, ErrUnknownBroadcastType) {
		t.Errorf("Expected ErrUnknownBroadcastType, got: %v", err)
	}
}

func TestNewBus_RedisAliasRemoved(t *testing.T) {
	t.Parallel()
	cfg := Config{Type: "redis"}
	_, err := NewBus(cfg, zerolog.Nop())
	if err == nil {
		t.Fatal("Expected error for redis alias, got nil")
	}
	if !errors.Is(err, ErrUnknownBroadcastType) {
		t.Errorf("Expected ErrUnknownBroadcastType, got: %v", err)
	}
}

func TestNewBus_DefaultsApplied(t *testing.T) {
	t.Parallel()
	// Test that defaults are applied when values are zero
	cfg := Config{
		Type:            "valkey",
		BufferSize:      0, // Should default to 1024
		ShutdownTimeout: 0, // Should default to 5s
		Valkey: ValkeyConfig{
			Addrs: []string{"localhost:6379"},
		},
	}

	// We can't actually create the bus without a real Valkey connection,
	// but we can verify the factory accepts the config
	logger := zerolog.Nop()
	_, err := NewBus(cfg, logger)

	// Will fail because no Valkey server, but error should be connection-related
	if err == nil {
		t.Skip("Valkey server available, skipping connection error test")
	}

	// Error should be about connection, not about missing buffer size
	if strings.Contains(err.Error(), "buffer") {
		t.Errorf("Should not error about buffer size: %v", err)
	}
}

func TestNewBus_ValkeyType(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Type:       "valkey",
		BufferSize: 1024,
		Valkey: ValkeyConfig{
			Addrs:   []string{"localhost:6379"},
			Channel: "test",
		},
	}
	_, err := NewBus(cfg, zerolog.Nop())
	// Will fail due to no connection, but should attempt Valkey (not return "unsupported")
	if err != nil && strings.Contains(err.Error(), "unsupported") {
		t.Errorf("Type %q should be supported: %v", "valkey", err)
	}
}

func TestNewBus_ValkeyMissingAddrs(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()

	cfg := Config{
		Type:       "valkey",
		BufferSize: 1024,
		Valkey: ValkeyConfig{
			Addrs: []string{}, // Empty
		},
	}

	_, err := NewBus(cfg, logger)
	if err == nil {
		t.Error("Expected error for missing Valkey addrs")
	}
	if !strings.Contains(err.Error(), "address") {
		t.Errorf("Error should mention address requirement: %v", err)
	}
}
