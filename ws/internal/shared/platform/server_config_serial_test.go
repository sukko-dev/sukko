// Serial tests — uses os.Unsetenv which modifies global process state. Do NOT add t.Parallel() to any test in this file.
package platform

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
)

// TestServerConfig_ValkeyDefaultValid verifies that when VALKEY_ADDRS is unset,
// env.Parse fills it from the envDefault "localhost:6379" and Validate passes.
func TestServerConfig_ValkeyDefaultValid(t *testing.T) {
	os.Unsetenv("VALKEY_ADDRS")
	os.Unsetenv("BROADCAST_TYPE")
	os.Unsetenv("MESSAGE_BACKEND")
	os.Unsetenv("WS_HISTORY_ENABLED")

	var cfg ServerConfig
	if err := env.Parse(&cfg); err != nil {
		t.Fatalf("env.Parse failed: %v", err)
	}
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate should pass with default ValkeyAddrs, got: %v", err)
	}
	if len(cfg.ValkeyAddrs) != 1 || cfg.ValkeyAddrs[0] != "localhost:6379" {
		t.Errorf("ValkeyAddrs default = %v, want [localhost:6379]", cfg.ValkeyAddrs)
	}
}

// TestServerConfig_ValkeyAddrGuard verifies the compound ValkeyAddrs validation guard.
func TestServerConfig_ValkeyAddrGuard(t *testing.T) {
	os.Unsetenv("VALKEY_ADDRS")
	os.Unsetenv("BROADCAST_TYPE")
	os.Unsetenv("MESSAGE_BACKEND")
	os.Unsetenv("WS_HISTORY_ENABLED")

	var base ServerConfig
	if err := env.Parse(&base); err != nil {
		t.Fatalf("env.Parse failed: %v", err)
	}
	base.Normalize()

	t.Run("nil addrs rejected", func(t *testing.T) {
		cfg := base
		cfg.ValkeyAddrs = nil
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for nil ValkeyAddrs")
		}
	})

	t.Run("empty slice rejected", func(t *testing.T) {
		cfg := base
		cfg.ValkeyAddrs = []string{}
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty ValkeyAddrs slice")
		}
	})

	t.Run("empty string rejected", func(t *testing.T) {
		cfg := base
		cfg.ValkeyAddrs = []string{""}
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for ValkeyAddrs with empty string")
		}
	})

	t.Run("whitespace-only rejected", func(t *testing.T) {
		cfg := base
		cfg.ValkeyAddrs = []string{"  "}
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for ValkeyAddrs with whitespace-only entry")
		}
	})

	t.Run("mixed valid and empty accepted", func(t *testing.T) {
		cfg := base
		cfg.ValkeyAddrs = []string{"valkey:6379", ""}
		if err := cfg.Validate(); err != nil {
			t.Errorf("mixed valid/empty should pass, got: %v", err)
		}
	})
}

// TestServerConfig_Validate_NATSJetStreamBoundsNotValidatedOnDirectMode verifies
// that the previously removed JetStream bounds validation blocks are gone.
func TestServerConfig_Validate_NATSJetStreamBoundsNotValidatedOnDirectMode(t *testing.T) {
	cfg := newValidServerConfig()
	cfg.MessageBackend = MessageBackendDirect
	if err := cfg.Validate(); err != nil {
		t.Errorf("direct backend should pass with no JetStream fields, got: %v", err)
	}
}
