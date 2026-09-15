package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	provauth "github.com/sukko-dev/sukko/internal/provisioning/auth"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

func TestTesterConfig_Validate(t *testing.T) {
	t.Parallel()

	validConfig := func() TesterConfig {
		return TesterConfig{
			BaseConfig: platform.BaseConfig{
				LogLevel:    "info",
				LogFormat:   "json",
				Environment: "test",
			},
			KafkaNamespaceConfig:   platform.KafkaNamespaceConfig{ValidNamespaces: "local,dev,stag,prod"},
			Port:                   8090,
			GatewayURL:             "ws://localhost:3000",
			ProvisioningURL:        "http://localhost:8080",
			AdminKeyID:             provauth.BootstrapAdminKeyID, // mirrors envDefault:"bootstrap-0"
			JWTLifetime:            15 * time.Minute,
			JWTRefreshBefore:       2 * time.Minute,
			KeyExpiry:              24 * time.Hour,
			AuthMode:               "jwt",
			AuthMixRatio:           0.5,
			AuthUpgradeTimeout:     10 * time.Second,
			GatewayMetricsInterval: 30 * time.Second,
			WebhookDeliveryTimeout: 15 * time.Second,
			WebhookRetryTimeout:    30 * time.Second,
		}
	}

	tests := []struct {
		name    string
		modify  func(*TesterConfig)
		wantErr string
	}{
		{
			name:   "valid config",
			modify: func(_ *TesterConfig) {},
		},
		{
			name:    "port zero",
			modify:  func(c *TesterConfig) { c.Port = 0 },
			wantErr: "TESTER_PORT must be between 1 and 65535",
		},
		{
			name:    "port too high",
			modify:  func(c *TesterConfig) { c.Port = 70000 },
			wantErr: "TESTER_PORT must be between 1 and 65535",
		},
		{
			name:    "empty gateway URL",
			modify:  func(c *TesterConfig) { c.GatewayURL = "" },
			wantErr: "GATEWAY_URL is required",
		},
		{
			name:    "empty provisioning URL",
			modify:  func(c *TesterConfig) { c.ProvisioningURL = "" },
			wantErr: "PROVISIONING_URL is required",
		},
		{
			name: "sasl enabled without username",
			modify: func(c *TesterConfig) {
				c.KafkaSASLEnabled = true
				c.KafkaSASLMechanism = "scram-sha-256"
				c.KafkaSASLUsername = ""
				c.KafkaSASLPassword = "pass"
			},
			wantErr: "KAFKA_SASL_USERNAME",
		},
		{
			name: "sasl enabled with plain mechanism valid",
			modify: func(c *TesterConfig) {
				c.KafkaSASLEnabled = true
				c.KafkaSASLMechanism = "plain"
				c.KafkaSASLUsername = "key"
				c.KafkaSASLPassword = "secret"
			},
		},
		{
			name: "empty brokers valid (kafka-ingest skips)",
			modify: func(c *TesterConfig) {
				c.KafkaBrokers = ""
			},
		},
		{
			name: "brokers set + namespace missing rejected",
			modify: func(c *TesterConfig) {
				c.KafkaBrokers = "localhost:19092"
				c.KafkaTopicNamespace = ""
			},
			wantErr: "KAFKA_TOPIC_NAMESPACE is required",
		},
		{
			name: "brokers set + valid namespace ok",
			modify: func(c *TesterConfig) {
				c.KafkaBrokers = "localhost:19092"
				c.KafkaTopicNamespace = "dev"
			},
		},
		{
			name: "brokers set + namespace not in valid set",
			modify: func(c *TesterConfig) {
				c.KafkaBrokers = "localhost:19092"
				c.KafkaTopicNamespace = "staging"
			},
			wantErr: "KAFKA_TOPIC_NAMESPACE",
		},
		{
			name:    "JWT lifetime zero",
			modify:  func(c *TesterConfig) { c.JWTLifetime = 0 },
			wantErr: "TESTER_JWT_LIFETIME must be positive",
		},
		{
			name:    "JWT refresh before >= lifetime",
			modify:  func(c *TesterConfig) { c.JWTRefreshBefore = 15 * time.Minute },
			wantErr: "TESTER_JWT_REFRESH_BEFORE must be positive and less than",
		},
		{
			name:    "JWT refresh before zero",
			modify:  func(c *TesterConfig) { c.JWTRefreshBefore = 0 },
			wantErr: "TESTER_JWT_REFRESH_BEFORE must be positive and less than",
		},
		{
			name:    "key expiry less than JWT lifetime",
			modify:  func(c *TesterConfig) { c.KeyExpiry = 1 * time.Minute },
			wantErr: "TESTER_KEY_EXPIRY",
		},
		{
			name:   "admin key file not set",
			modify: func(_ *TesterConfig) {},
			// AdminKeyFile="" is valid — local dev mode
		},
		{
			name: "admin key file valid",
			modify: func(c *TesterConfig) {
				_, priv, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					return
				}
				path := filepath.Join(os.TempDir(), "test-admin-valid.key")
				_ = os.WriteFile(path, []byte(priv), 0o600)
				c.AdminKeyFile = path
			},
		},
		{
			name: "admin key file missing path",
			modify: func(c *TesterConfig) {
				c.AdminKeyFile = "/nonexistent/path/to/key.bin"
			},
			wantErr: "/nonexistent/path/to/key.bin",
		},
		{
			name: "admin key file invalid bytes",
			modify: func(c *TesterConfig) {
				path := filepath.Join(os.TempDir(), "test-admin-short.key")
				_ = os.WriteFile(path, make([]byte, 32), 0o600) // 32 bytes, should be 64
				c.AdminKeyFile = path
			},
			wantErr: "must be 64 bytes",
		},
		{
			name: "admin key file set but key id empty",
			modify: func(c *TesterConfig) {
				_, priv, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					return
				}
				path := filepath.Join(os.TempDir(), "test-admin-empty-kid.key")
				_ = os.WriteFile(path, []byte(priv), 0o600)
				c.AdminKeyFile = path
				c.AdminKeyID = ""
			},
			wantErr: "TESTER_ADMIN_KEY_ID must not be empty",
		},
		{
			name: "admin key ID too long",
			modify: func(c *TesterConfig) {
				_, priv, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					return
				}
				path := filepath.Join(os.TempDir(), "test-admin-long-kid.key")
				_ = os.WriteFile(path, []byte(priv), 0o600)
				c.AdminKeyFile = path
				c.AdminKeyID = strings.Repeat("a", 64) // 64 chars, max is 63
			},
			wantErr: "TESTER_ADMIN_KEY_ID",
		},
		{
			name: "admin key ID starts with digit",
			modify: func(c *TesterConfig) {
				_, priv, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					return
				}
				path := filepath.Join(os.TempDir(), "test-admin-digit-kid.key")
				_ = os.WriteFile(path, []byte(priv), 0o600)
				c.AdminKeyFile = path
				c.AdminKeyID = "1-invalid"
			},
			wantErr: "TESTER_ADMIN_KEY_ID",
		},
		{
			name: "admin key ID uppercase rejected",
			modify: func(c *TesterConfig) {
				_, priv, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					return
				}
				path := filepath.Join(os.TempDir(), "test-admin-upper-kid.key")
				_ = os.WriteFile(path, []byte(priv), 0o600)
				c.AdminKeyFile = path
				c.AdminKeyID = "Bootstrap-0" // uppercase B rejected
			},
			wantErr: "TESTER_ADMIN_KEY_ID",
		},
		// Auth mode validation cases
		{
			name:    "invalid auth_mode",
			modify:  func(c *TesterConfig) { c.AuthMode = "oauth" },
			wantErr: "TESTER_AUTH_MODE must be jwt|api-key|upgrade|mixed",
		},
		{
			name:   "valid auth_mode default (jwt)",
			modify: func(_ *TesterConfig) {}, // validConfig already sets AuthMode="jwt"
		},
		{
			name:    "auth_mix_ratio negative",
			modify:  func(c *TesterConfig) { c.AuthMixRatio = -0.1 },
			wantErr: "TESTER_AUTH_MIX_RATIO must be in [0.0, 1.0]",
		},
		{
			name:    "auth_mix_ratio too high",
			modify:  func(c *TesterConfig) { c.AuthMixRatio = 1.1 },
			wantErr: "TESTER_AUTH_MIX_RATIO must be in [0.0, 1.0]",
		},
		{
			name:   "auth_mix_ratio 0.0 valid",
			modify: func(c *TesterConfig) { c.AuthMixRatio = 0.0 },
		},
		{
			name:   "auth_mix_ratio 1.0 valid",
			modify: func(c *TesterConfig) { c.AuthMixRatio = 1.0 },
		},
		{
			name:   "auth_mix_ratio 0.5 valid",
			modify: func(c *TesterConfig) { c.AuthMixRatio = 0.5 },
		},
		{
			name: "api-key mode missing key",
			modify: func(c *TesterConfig) {
				c.AuthMode = "api-key"
				c.APIKey = "" // ensure no key is set
			},
			wantErr: `TESTER_API_KEY is required when TESTER_AUTH_MODE is "api-key"`,
		},
		{
			name: "upgrade mode missing key",
			modify: func(c *TesterConfig) {
				c.AuthMode = "upgrade"
				c.APIKey = ""
			},
			wantErr: `TESTER_API_KEY is required when TESTER_AUTH_MODE is "upgrade"`,
		},
		{
			name: "api-key mode with key",
			modify: func(c *TesterConfig) {
				c.AuthMode = "api-key"
				c.APIKey = "pk_live_test123"
			},
		},
		{
			name:    "auth_upgrade_timeout zero",
			modify:  func(c *TesterConfig) { c.AuthUpgradeTimeout = 0 },
			wantErr: "TESTER_AUTH_UPGRADE_TIMEOUT must be > 0 and",
		},
		{
			name:    "auth_upgrade_timeout negative",
			modify:  func(c *TesterConfig) { c.AuthUpgradeTimeout = -1 * time.Second },
			wantErr: "TESTER_AUTH_UPGRADE_TIMEOUT must be > 0 and",
		},
		{
			name:    "auth_upgrade_timeout too large",
			modify:  func(c *TesterConfig) { c.AuthUpgradeTimeout = 61 * time.Second },
			wantErr: "TESTER_AUTH_UPGRADE_TIMEOUT must be > 0 and",
		},
		{
			name:   "auth_upgrade_timeout valid",
			modify: func(c *TesterConfig) { c.AuthUpgradeTimeout = 10 * time.Second },
		},
		// GatewayMetricsInterval validation
		{
			name:    "gateway_metrics_interval zero",
			modify:  func(c *TesterConfig) { c.GatewayMetricsInterval = 0 },
			wantErr: "TESTER_GATEWAY_METRICS_INTERVAL must be positive",
		},
		{
			name:    "gateway_metrics_interval negative",
			modify:  func(c *TesterConfig) { c.GatewayMetricsInterval = -1 * time.Second },
			wantErr: "TESTER_GATEWAY_METRICS_INTERVAL must be positive",
		},
		{
			name:   "gateway_metrics_interval valid",
			modify: func(c *TesterConfig) { c.GatewayMetricsInterval = 30 * time.Second },
		},
		// GatewayMetricsURL validation
		{
			name:    "gateway_metrics_url invalid scheme",
			modify:  func(c *TesterConfig) { c.GatewayMetricsURL = "ftp://invalid:9999" },
			wantErr: "TESTER_GATEWAY_METRICS_URL must start with http:// or https://",
		},
		{
			name:    "gateway_metrics_url no scheme",
			modify:  func(c *TesterConfig) { c.GatewayMetricsURL = "localhost:9090" },
			wantErr: "TESTER_GATEWAY_METRICS_URL must start with http:// or https://",
		},
		{
			name:   "gateway_metrics_url valid http",
			modify: func(c *TesterConfig) { c.GatewayMetricsURL = "http://gateway:9090" },
		},
		{
			name:   "gateway_metrics_url valid https",
			modify: func(c *TesterConfig) { c.GatewayMetricsURL = "https://gateway:9090" },
		},
		{
			name:   "gateway_metrics_url empty (disabled)",
			modify: func(c *TesterConfig) { c.GatewayMetricsURL = "" },
		},
		// WebhookBaseURL validation
		{
			name:    "webhook_base_url invalid scheme",
			modify:  func(c *TesterConfig) { c.WebhookBaseURL = "ftp://tester:8085" },
			wantErr: "TESTER_WEBHOOK_BASE_URL",
		},
		{
			name:    "webhook_base_url no scheme",
			modify:  func(c *TesterConfig) { c.WebhookBaseURL = "tester.sukko-ci.svc:8085" },
			wantErr: "TESTER_WEBHOOK_BASE_URL",
		},
		{
			name:   "webhook_base_url http ok",
			modify: func(c *TesterConfig) { c.WebhookBaseURL = "http://tester.sukko-ci.svc.cluster.local:8085" },
		},
		{
			name:   "webhook_base_url https ok",
			modify: func(c *TesterConfig) { c.WebhookBaseURL = "https://tester.sukko-ci:8085" },
		},
		{
			name:   "webhook_base_url empty (webhooks suite skipped)",
			modify: func(c *TesterConfig) { c.WebhookBaseURL = "" },
		},
		// WebhookDeliveryTimeout validation
		{
			name:    "webhook_delivery_timeout zero",
			modify:  func(c *TesterConfig) { c.WebhookDeliveryTimeout = 0 },
			wantErr: "TESTER_WEBHOOK_DELIVERY_TIMEOUT",
		},
		{
			name:    "webhook_delivery_timeout exceeds max",
			modify:  func(c *TesterConfig) { c.WebhookDeliveryTimeout = 6 * time.Minute },
			wantErr: "TESTER_WEBHOOK_DELIVERY_TIMEOUT",
		},
		{
			name:   "webhook_delivery_timeout valid",
			modify: func(c *TesterConfig) { c.WebhookDeliveryTimeout = 15 * time.Second },
		},
		// WebhookRetryTimeout validation
		{
			name:    "webhook_retry_timeout zero",
			modify:  func(c *TesterConfig) { c.WebhookRetryTimeout = 0 },
			wantErr: "TESTER_WEBHOOK_RETRY_TIMEOUT",
		},
		{
			name:    "webhook_retry_timeout exceeds max",
			modify:  func(c *TesterConfig) { c.WebhookRetryTimeout = 31 * time.Minute },
			wantErr: "TESTER_WEBHOOK_RETRY_TIMEOUT",
		},
		{
			name:   "webhook_retry_timeout valid",
			modify: func(c *TesterConfig) { c.WebhookRetryTimeout = 30 * time.Second },
		},
		{
			name:    "webhook_retry_timeout equals delivery_timeout",
			modify:  func(c *TesterConfig) { c.WebhookRetryTimeout = c.WebhookDeliveryTimeout },
			wantErr: "TESTER_WEBHOOK_RETRY_TIMEOUT",
		},
		{
			name:    "webhook_retry_timeout less than delivery_timeout",
			modify:  func(c *TesterConfig) { c.WebhookRetryTimeout = c.WebhookDeliveryTimeout / 2 },
			wantErr: "TESTER_WEBHOOK_RETRY_TIMEOUT",
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
					t.Errorf("expected no error, got %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.wantErr)
				} else if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("expected error containing %q, got %q", tt.wantErr, err.Error())
				}
			}
		})
	}
}

// TestTesterConfig_DefaultAdminKeyID_EqualsBootstrapConstant verifies that the
// TESTER_ADMIN_KEY_ID env default matches the canonical BootstrapAdminKeyID constant.
// This prevents the tester from silently using a wrong default in remote mode.
func TestTesterConfig_DefaultAdminKeyID_EqualsBootstrapConstant(t *testing.T) {
	t.Parallel()

	// A zero-value TesterConfig uses Go's zero values, not env defaults.
	// We compare the constant values directly to ensure they stay in sync.
	const envDefault = "bootstrap-0"
	if provauth.BootstrapAdminKeyID != envDefault {
		t.Errorf("BootstrapAdminKeyID = %q, want %q (must match TESTER_ADMIN_KEY_ID envDefault)",
			provauth.BootstrapAdminKeyID, envDefault)
	}
}
