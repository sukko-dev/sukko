package main

import (
	"testing"

	"github.com/caarlos0/env/v11"

	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// TestBuildLoggerConfig_SetsServiceIdentity pins DEFECT 1: the logger must be built with the
// service identity. Omitting it panics in logging.NewLogger, which is why the worker never booted.
func TestBuildLoggerConfig_SetsServiceIdentity(t *testing.T) {
	t.Parallel()

	cfg := &platform.WebhookWorkerConfig{}
	got := buildLoggerConfig(cfg)
	if got.ServiceName != serviceName {
		t.Errorf("buildLoggerConfig ServiceName = %q, want %q (empty would panic in NewLogger)", got.ServiceName, serviceName)
	}
	if got.ServiceName != "webhook-worker" {
		t.Errorf("service identity = %q, want %q", got.ServiceName, "webhook-worker")
	}
}

// TestBuildBroadcastConfig_DefaultsBootable pins DEFECTS 2 & 3: with the config's production
// defaults (via env.Parse — no env set), the assembled broadcast config must have every
// bus-consumed field non-zero and the channel prefix equal to ws-server's default. A zero
// StartupPingTimeout/HealthCheckInterval or an empty Channel is exactly what kept the worker dead.
func TestBuildBroadcastConfig_DefaultsBootable(t *testing.T) {
	t.Parallel()

	cfg := &platform.WebhookWorkerConfig{}
	if err := env.Parse(cfg); err != nil { // applies every envDefault; no env vars required
		t.Fatalf("env.Parse: %v", err)
	}

	bc := buildBroadcastConfig(cfg)

	// DEFECT 3 (most dangerous): channel prefix must equal ws-server's default so the worker
	// receives the events ws-server publishes. Empty would subscribe to a dead ":*" namespace.
	if bc.Valkey.Channel != "ws.broadcast" {
		t.Errorf("Valkey.Channel = %q, want %q (empty → subscribes to a dead namespace)", bc.Valkey.Channel, "ws.broadcast")
	}

	// DEFECT 2 (fatal at zero): startup ping timeout zero → instant deadline; health-check
	// interval zero → NewTicker(0) panics → bus reports healthy forever.
	if bc.Valkey.StartupPingTimeout <= 0 {
		t.Error("Valkey.StartupPingTimeout must be > 0 (zero → instant 'context deadline exceeded')")
	}
	if bc.Valkey.HealthCheckInterval <= 0 {
		t.Error("Valkey.HealthCheckInterval must be > 0 (zero → NewTicker(0) panic → bus healthy forever)")
	}

	// Parity guards (bus tolerates zero at runtime, but must match ws-server, §XVIII).
	if bc.Valkey.WriteTimeout <= 0 {
		t.Error("Valkey.WriteTimeout must be > 0")
	}
	if bc.Valkey.HealthCheckTimeout <= 0 {
		t.Error("Valkey.HealthCheckTimeout must be > 0")
	}
	if bc.ShutdownTimeout <= 0 {
		t.Error("ShutdownTimeout must be > 0")
	}
}

// TestBuildBroadcastConfig_TLSPropagated pins the worker's TLS settings must reach the
// bus config (they were previously silently dropped).
func TestBuildBroadcastConfig_TLSPropagated(t *testing.T) {
	t.Parallel()

	cfg := &platform.WebhookWorkerConfig{}
	cfg.ValkeyConfig.TLSEnabled = true
	cfg.ValkeyConfig.TLSInsecure = true
	cfg.ValkeyConfig.TLSCAPath = "/etc/ssl/valkey-ca.pem"

	bc := buildBroadcastConfig(cfg)
	if !bc.Valkey.TLSEnabled || !bc.Valkey.TLSInsecure || bc.Valkey.TLSCAPath != "/etc/ssl/valkey-ca.pem" {
		t.Errorf("TLS settings not propagated to bus config: %+v", bc.Valkey)
	}
}
