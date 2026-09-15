package push

import (
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// validConfig returns a Config with all valid defaults for testing.
// Each test case mutates one field to test a specific validation rule.
func validConfig() Config {
	return Config{
		BaseConfig: platform.BaseConfig{
			LogLevel:    "info",
			LogFormat:   "json",
			Environment: "local",
		},
		ProvisioningClientConfig: platform.ProvisioningClientConfig{
			ProvisioningGRPCAddr: "localhost:9090",
			GRPCReconnectConfig:  platform.GRPCReconnectConfig{GRPCReconnectDelay: 1 * time.Second, GRPCReconnectMaxDelay: 30 * time.Second},
		},
		MessageBackendConfig: platform.MessageBackendConfig{
			MessageBackend: "kafka",
			KafkaConnectionConfig: platform.KafkaConnectionConfig{
				KafkaBrokers: "localhost:19092",
			},
		},
		KafkaNamespaceConfig: platform.KafkaNamespaceConfig{
			ValidNamespaces:     "local,dev,stag,prod",
			KafkaTopicNamespace: "local",
		},
		DatabaseConfig: platform.DatabaseConfig{
			DatabaseURL: "postgres://localhost:5432/push",
		},
		WorkerPoolSize: 200,
		JobQueueSize:   10000,
		GRPCPort:       3008,
		HTTPPort:       3009,
		DefaultTTL:     2419200,
		DefaultUrgency: "normal",
		MaxRetries:     3,
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string // Substring expected in error message; empty = no error
	}{
		{
			name:    "valid config with all defaults",
			modify:  func(_ *Config) {},
			wantErr: "",
		},
		{
			name: "direct message backend rejected",
			modify: func(c *Config) {
				c.MessageBackend = "direct"
			},
			wantErr: "push notifications require MESSAGE_BACKEND=kafka",
		},
		{
			name: "empty topic namespace rejected (push always requires it)",
			modify: func(c *Config) {
				c.KafkaTopicNamespace = ""
			},
			wantErr: "KAFKA_TOPIC_NAMESPACE is required",
		},
		{
			name: "empty database URL",
			modify: func(c *Config) {
				c.DatabaseURL = ""
			},
			wantErr: "DATABASE_URL is required",
		},
		{
			name: "worker pool size zero",
			modify: func(c *Config) {
				c.WorkerPoolSize = 0
			},
			wantErr: "PUSH_WORKER_POOL_SIZE must be >= 1",
		},
		{
			name: "job queue size zero",
			modify: func(c *Config) {
				c.JobQueueSize = 0
			},
			wantErr: "PUSH_JOB_QUEUE_SIZE must be >= 1",
		},
		{
			name: "gRPC port zero",
			modify: func(c *Config) {
				c.GRPCPort = 0
			},
			wantErr: "PUSH_GRPC_PORT must be > 0",
		},
		{
			name: "HTTP port zero",
			modify: func(c *Config) {
				c.HTTPPort = 0
			},
			wantErr: "PUSH_HTTP_PORT must be > 0",
		},
		{
			name: "invalid default urgency",
			modify: func(c *Config) {
				c.DefaultUrgency = "critical"
			},
			wantErr: "PUSH_DEFAULT_URGENCY",
		},
		{
			name: "negative max retries",
			modify: func(c *Config) {
				c.MaxRetries = -1
			},
			wantErr: "PUSH_MAX_RETRIES must be >= 0",
		},
		{
			name: "nats message backend is rejected",
			modify: func(c *Config) {
				c.MessageBackend = "nats"
			},
			wantErr: "MESSAGE_BACKEND",
		},
		{
			name: "max retries zero is valid",
			modify: func(c *Config) {
				c.MaxRetries = 0
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validConfig()
			tt.modify(&cfg)
			err := cfg.Validate()

			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}

// TestLoadConfig_CommunityStart asserts that LoadConfig with no license key
// fails with the edition gate error — push-service requires Pro or higher (WebPush, ADR-0009).
func TestLoadConfig_CommunityStart(t *testing.T) {
	// Set required env vars so env.Parse succeeds.
	t.Setenv("PROVISIONING_GRPC_ADDR", "localhost:9090")
	t.Setenv("MESSAGE_BACKEND", "kafka")
	t.Setenv("KAFKA_BROKERS", "localhost:19092")
	t.Setenv("KAFKA_TOPIC_NAMESPACE", "local")
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/push")
	t.Setenv("SUKKO_LICENSE_KEY", "") // explicitly empty — no key

	_, err := LoadConfig(zerolog.Nop())
	if err == nil {
		t.Fatal("LoadConfig with empty license key must return an error — push-service requires Pro or higher (WebPush)")
	}
	if !strings.Contains(err.Error(), "push-service requires a Pro license or higher") {
		t.Errorf("expected edition gate error, got: %v", err)
	}
}

// enterpriseGateEnvVars sets the minimum required env vars for LoadConfig to reach the edition gate.
func enterpriseGateEnvVars(t *testing.T) {
	t.Helper()
	t.Setenv("PROVISIONING_GRPC_ADDR", "localhost:9090")
	t.Setenv("MESSAGE_BACKEND", "kafka")
	t.Setenv("KAFKA_BROKERS", "localhost:19092")
	t.Setenv("KAFKA_TOPIC_NAMESPACE", "local")
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/push")
}

// TestLoadConfig_EnterpriseGate_EmptyKey verifies that LoadConfig returns an error
// when no license key is set — push-service requires Pro or higher (WebPush, ADR-0009).
func TestLoadConfig_EnterpriseGate_EmptyKey(t *testing.T) {
	enterpriseGateEnvVars(t)
	t.Setenv("SUKKO_LICENSE_KEY", "")

	_, err := LoadConfig(zerolog.Nop())
	if err == nil {
		t.Fatal("LoadConfig with empty key must return the edition gate error")
	}
	if !strings.Contains(err.Error(), "push-service requires a Pro license or higher") {
		t.Errorf("expected edition gate error, got: %v", err)
	}
}

// TestLoadConfig_EnterpriseGate_CommunityKey verifies that a Community key is rejected (Pro+ passes).
// MUST NOT use t.Parallel() — SetPublicKeyForTesting mutates package-level state.
func TestLoadConfig_EnterpriseGate_CommunityKey(t *testing.T) {
	priv, pub := license.GenerateTestKeyPair()
	license.SetPublicKeyForTesting(pub)

	communityKey := license.SignTestLicense(license.Claims{
		Edition: license.Community,
		Org:     "CommunityOrg",
		Exp:     1<<62 - 1, // far future
		Iat:     1,
	}, priv)

	enterpriseGateEnvVars(t)
	t.Setenv("SUKKO_LICENSE_KEY", communityKey)

	_, err := LoadConfig(zerolog.Nop())
	if err == nil {
		t.Fatal("LoadConfig with Community key must return the edition gate error")
	}
	if !strings.Contains(err.Error(), "push-service requires a Pro license or higher") {
		t.Errorf("expected edition gate error, got: %v", err)
	}
}

// TestLoadConfig_EnterpriseGate_EnterpriseKey verifies that a valid Enterprise key passes the gate.
// MUST NOT use t.Parallel() — SetPublicKeyForTesting mutates package-level state.
func TestLoadConfig_EnterpriseGate_EnterpriseKey(t *testing.T) {
	priv, pub := license.GenerateTestKeyPair()
	license.SetPublicKeyForTesting(pub)

	enterpriseKey := license.SignTestLicense(license.Claims{
		Edition: license.Enterprise,
		Org:     "EnterpriseOrg",
		Exp:     1<<62 - 1, // far future
		Iat:     1,
	}, priv)

	enterpriseGateEnvVars(t)
	t.Setenv("SUKKO_LICENSE_KEY", enterpriseKey)

	cfg, err := LoadConfig(zerolog.Nop())
	if err != nil {
		t.Fatalf("LoadConfig with Enterprise key must not return an error, got: %v", err)
	}
	if cfg.EditionManager().Edition() != license.Enterprise {
		t.Errorf("expected Enterprise edition, got: %s", cfg.EditionManager().Edition())
	}
}
