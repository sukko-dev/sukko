package platform

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// setProvisioningEditionManager creates a test license and assigns the manager to cfg.
// MUST NOT use t.Parallel() in tests that call this — it shares license.SetPublicKeyForTesting.
func setProvisioningEditionManager(t *testing.T, cfg *ProvisioningConfig, edition license.Edition) {
	t.Helper()
	priv, pub := license.GenerateTestKeyPair()
	license.SetPublicKeyForTesting(pub)
	claims := license.Claims{
		Edition: edition,
		Org:     "test",
		Exp:     time.Now().Add(time.Hour).Unix(),
	}
	key := license.SignTestLicense(claims, priv)
	mgr, err := license.NewManager(key, zerolog.Nop())
	if err != nil {
		t.Fatalf("create test license manager: %v", err)
	}
	cfg.editionManager = mgr
}

func newValidProvisioningConfig() *ProvisioningConfig {
	return &ProvisioningConfig{
		BaseConfig: BaseConfig{
			LogLevel:    "info",
			LogFormat:   "json",
			Environment: "local",
		},
		AuthConfig: AuthConfig{
			AuthMode: "required",
		},
		DatabaseConfig: DatabaseConfig{
			DatabaseURL: "postgres://test:test@localhost:5432/test",
		},
		KafkaNamespaceConfig: KafkaNamespaceConfig{
			ValidNamespaces:     "local,dev,stag,prod",
			KafkaTopicNamespace: "local",
		},
		HTTPTimeoutConfig: HTTPTimeoutConfig{
			HTTPReadTimeout:  15 * time.Second,
			HTTPWriteTimeout: 15 * time.Second,
			HTTPIdleTimeout:  60 * time.Second,
		},
		Addr:                              ":8080",
		GRPCPort:                          9090,
		WebhookWorkerGRPCPort:             9091,
		DefaultPartitions:                 3,
		DefaultRetentionMs:                604800000,
		MaxTopicsPerTenant:                50,
		MaxPartitionsPerTenant:            200,
		MaxStorageBytes:                   10737418240,
		ProducerByteRate:                  10485760,
		ConsumerByteRate:                  52428800,
		MaxRoutingRulesPerTenant:          100,
		MaxTopicsPerRule:                  10,
		DeadLetterTopicPartitions:         1,
		DeadLetterTopicRetentionMs:        604800000,
		InfraTopicReplicationFactor:       1,
		DeprovisionGraceDays:              30,
		LifecycleCheckInterval:            time.Hour,
		LifecycleManagerEnabled:           true,
		APIRateLimitPerMinute:             60,
		KeyRegistryRefreshInterval:        time.Minute,
		KeyRegistryQueryTimeout:           5 * time.Second,
		ShutdownTimeout:                   30 * time.Second,
		CORSAllowedOrigins:                []string{"http://localhost:3000"},
		CORSMaxAge:                        3600,
		MaxTenantsFetchLimit:              10000,
		DeletionTimeout:                   5 * time.Minute,
		SlugRenameTopicHoldPeriod:         7 * 24 * time.Hour, // 168h default
		TokenRevocationMaxLifetime:        time.Hour,          // 1h default
		TokenRevocationRateLimitPerMinute: 30,                 // 30/min default
		TokenRevocationRateLimitBurst:     5,                  // burst 5 default
		CredentialsConfig: CredentialsConfig{
			CredentialsEncryptionKey: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", // 64-char hex = 32 bytes
		},
		ValkeyConfig: ValkeyClientConfig{
			Addrs: []string{"localhost:6379"},
		},
		BulkDisconnectConcurrency:    10,
		MaxWebhooksPerTenant:         10,
		WebhookHTTPConfig:            WebhookHTTPConfig{WebhookAllowHTTP: false},
		WebhookDowngradePollInterval: 5 * time.Minute,
	}
}

func TestProvisioningConfig_Validate_Valid(t *testing.T) {
	t.Parallel()
	cfg := newValidProvisioningConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("Valid config should not error: %v", err)
	}
}

func TestProvisioningConfig_Validate_DatabaseURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		dbURL       string
		shouldError bool
	}{
		{"valid url", "postgres://localhost/db", false},
		{"empty url", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.DatabaseURL = tt.dbURL
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_AdminBootstrapKey(t *testing.T) {
	t.Parallel()

	// Generate a valid 32-byte Ed25519 public key for testing
	validKey := make([]byte, 32)
	for i := range validKey {
		validKey[i] = byte(i)
	}
	validBase64 := base64.StdEncoding.EncodeToString(validKey)

	tests := []struct {
		name           string
		bootstrapKey   string
		shouldError    bool
		errorSubstring string
	}{
		{"empty (no bootstrap)", "", false, ""},
		{"valid base64 padded", validBase64, false, ""},
		{"valid base64 unpadded", base64.RawStdEncoding.EncodeToString(validKey), false, ""},
		{"invalid base64", "not-valid-base64!!!", true, "valid base64"},
		{"wrong key size (16 bytes)", base64.StdEncoding.EncodeToString(make([]byte, 16)), true, "32 bytes"},
		{"wrong key size (64 bytes)", base64.StdEncoding.EncodeToString(make([]byte, 64)), true, "32 bytes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.AdminBootstrapKey = tt.bootstrapKey
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorSubstring != "" && !strings.Contains(err.Error(), tt.errorSubstring) {
					t.Errorf("Error should contain %q, got: %v", tt.errorSubstring, err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_GRPCPort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		port        int
		shouldError bool
	}{
		{"valid default", 9090, false},
		{"valid min", 1, false},
		{"valid max", 65535, false},
		{"zero", 0, true},
		{"negative", -1, true},
		{"too large", 65536, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.GRPCPort = tt.port
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_MaxRoutingRules(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		value       int
		shouldError bool
	}{
		{"valid default", 100, false},
		{"valid min", 1, false},
		{"valid large", 1000, false},
		{"zero", 0, true},
		{"negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.MaxRoutingRulesPerTenant = tt.value
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_NamespaceRequired(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		namespace   string
		shouldError bool
	}{
		{"valid namespace", "dev", false},
		{"empty is required (provisioning always builds topic names)", "", true},
		{"not in valid set", "staging", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.KafkaTopicNamespace = tt.namespace
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error on missing/invalid namespace")
				} else if !strings.Contains(err.Error(), "KAFKA_TOPIC_NAMESPACE") {
					t.Errorf("Error should mention KAFKA_TOPIC_NAMESPACE: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
			}
		})
	}
}

func TestProvisioningConfig_Validate_RetentionMs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		value       int64
		shouldError bool
	}{
		{"valid default 7 days", 604800000, false},
		{"valid exactly 60000", 60000, false},
		{"below minimum", 59999, true},
		{"zero", 0, true},
		{"negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.DefaultRetentionMs = tt.value
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_Quotas(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		maxStorage     int64
		producerRate   int64
		consumerRate   int64
		shouldError    bool
		errorSubstring string
	}{
		{"valid defaults", 10737418240, 10485760, 52428800, false, ""},
		{"storage exactly 1MB", 1048576, 10485760, 52428800, false, ""},
		{"storage below 1MB", 1048575, 10485760, 52428800, true, "MAX_STORAGE_BYTES"},
		{"storage zero", 0, 10485760, 52428800, true, "MAX_STORAGE_BYTES"},
		{"producer rate exactly 1024", 10737418240, 1024, 52428800, false, ""},
		{"producer rate below 1024", 10737418240, 1023, 52428800, true, "PRODUCER_BYTE_RATE"},
		{"producer rate zero", 10737418240, 0, 52428800, true, "PRODUCER_BYTE_RATE"},
		{"consumer rate exactly 1024", 10737418240, 10485760, 1024, false, ""},
		{"consumer rate below 1024", 10737418240, 10485760, 1023, true, "CONSUMER_BYTE_RATE"},
		{"consumer rate zero", 10737418240, 10485760, 0, true, "CONSUMER_BYTE_RATE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.MaxStorageBytes = tt.maxStorage
			cfg.ProducerByteRate = tt.producerRate
			cfg.ConsumerByteRate = tt.consumerRate
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorSubstring != "" && !strings.Contains(err.Error(), tt.errorSubstring) {
					t.Errorf("Error should contain %q, got: %v", tt.errorSubstring, err)
				}
			} else {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
			}
		})
	}
}

func TestProvisioningConfig_Validate_HTTPTimeouts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		readTimeout    time.Duration
		writeTimeout   time.Duration
		idleTimeout    time.Duration
		shouldError    bool
		errorSubstring string
	}{
		{"valid defaults", 15 * time.Second, 15 * time.Second, 60 * time.Second, false, ""},
		{"read exactly 1s", time.Second, 15 * time.Second, 60 * time.Second, false, ""},
		{"read exactly 120s", 120 * time.Second, 15 * time.Second, 60 * time.Second, false, ""},
		{"read below 1s", 500 * time.Millisecond, 15 * time.Second, 60 * time.Second, true, "HTTP_READ_TIMEOUT"},
		{"read above 120s", 121 * time.Second, 15 * time.Second, 60 * time.Second, true, "HTTP_READ_TIMEOUT"},
		{"read zero", 0, 15 * time.Second, 60 * time.Second, true, "HTTP_READ_TIMEOUT"},
		{"write exactly 1s", 15 * time.Second, time.Second, 60 * time.Second, false, ""},
		{"write exactly 120s", 15 * time.Second, 120 * time.Second, 60 * time.Second, false, ""},
		{"write below 1s", 15 * time.Second, 500 * time.Millisecond, 60 * time.Second, true, "HTTP_WRITE_TIMEOUT"},
		{"write above 120s", 15 * time.Second, 121 * time.Second, 60 * time.Second, true, "HTTP_WRITE_TIMEOUT"},
		{"idle exactly 1s", 15 * time.Second, 15 * time.Second, time.Second, false, ""},
		{"idle exactly 300s", 15 * time.Second, 15 * time.Second, 300 * time.Second, false, ""},
		{"idle below 1s", 15 * time.Second, 15 * time.Second, 500 * time.Millisecond, true, "HTTP_IDLE_TIMEOUT"},
		{"idle above 300s", 15 * time.Second, 15 * time.Second, 301 * time.Second, true, "HTTP_IDLE_TIMEOUT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.HTTPReadTimeout = tt.readTimeout
			cfg.HTTPWriteTimeout = tt.writeTimeout
			cfg.HTTPIdleTimeout = tt.idleTimeout
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorSubstring != "" && !strings.Contains(err.Error(), tt.errorSubstring) {
					t.Errorf("Error should contain %q, got: %v", tt.errorSubstring, err)
				}
			} else {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
			}
		})
	}
}

func TestProvisioningConfig_Validate_KeyRegistry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		refresh        time.Duration
		queryTimeout   time.Duration
		shouldError    bool
		errorSubstring string
	}{
		{"valid defaults", time.Minute, 5 * time.Second, false, ""},
		{"refresh exactly 1s", time.Second, 5 * time.Second, false, ""},
		{"refresh below 1s", 500 * time.Millisecond, 5 * time.Second, true, "KEY_REGISTRY_REFRESH_INTERVAL"},
		{"refresh zero", 0, 5 * time.Second, true, "KEY_REGISTRY_REFRESH_INTERVAL"},
		{"query timeout exactly 1s", time.Minute, time.Second, false, ""},
		{"query timeout below 1s", time.Minute, 500 * time.Millisecond, true, "KEY_REGISTRY_QUERY_TIMEOUT"},
		{"query timeout zero", time.Minute, 0, true, "KEY_REGISTRY_QUERY_TIMEOUT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.KeyRegistryRefreshInterval = tt.refresh
			cfg.KeyRegistryQueryTimeout = tt.queryTimeout
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorSubstring != "" && !strings.Contains(err.Error(), tt.errorSubstring) {
					t.Errorf("Error should contain %q, got: %v", tt.errorSubstring, err)
				}
			} else {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
			}
		})
	}
}

func TestProvisioningConfig_Validate_ShutdownTimeout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		timeout     time.Duration
		shouldError bool
	}{
		{"valid default", 30 * time.Second, false},
		{"exactly 1s", time.Second, false},
		{"large value", 5 * time.Minute, false},
		{"below 1s", 500 * time.Millisecond, true},
		{"zero", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.ShutdownTimeout = tt.timeout
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_LifecycleCheckInterval(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		enabled     bool
		interval    time.Duration
		shouldError bool
	}{
		{"enabled valid default", true, time.Hour, false},
		{"enabled exactly 1m", true, time.Minute, false},
		{"enabled below 1m", true, 30 * time.Second, true},
		{"enabled zero", true, 0, true},
		{"disabled ignores low value", false, 0, false},
		{"disabled ignores below 1m", false, 30 * time.Second, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.LifecycleManagerEnabled = tt.enabled
			cfg.LifecycleCheckInterval = tt.interval
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_DLQTopicRetentionMs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		value       int64
		shouldError bool
	}{
		{"valid default 7 days", 604800000, false},
		{"valid exactly 60000", 60000, false},
		{"below minimum 59999", 59999, true},
		{"zero", 0, true},
		{"negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.DeadLetterTopicRetentionMs = tt.value
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_InfraTopicReplicationFactor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		value       int
		shouldError bool
	}{
		{"valid default 1", 1, false},
		{"valid 3", 3, false},
		{"valid max", 32767, false},
		{"zero", 0, true},
		{"negative", -1, true},
		{"overflow int16", 32768, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.InfraTopicReplicationFactor = tt.value
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_MaxTenantsFetchLimit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		limit       int
		shouldError bool
	}{
		{"valid default", 100, false},
		{"valid large", 10000, false},
		{"valid min", 1, false},
		{"zero", 0, true},
		{"negative", -1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.MaxTenantsFetchLimit = tt.limit
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if !strings.Contains(err.Error(), "PROVISIONING_MAX_TENANTS_FETCH_LIMIT") {
					t.Errorf("Error should mention PROVISIONING_MAX_TENANTS_FETCH_LIMIT: %v", err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_DeletionTimeout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		timeout     time.Duration
		shouldError bool
	}{
		{"valid default", 30 * time.Second, false},
		{"valid large", 5 * time.Minute, false},
		{"zero", 0, true},
		{"negative", -1 * time.Second, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.DeletionTimeout = tt.timeout
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if !strings.Contains(err.Error(), "PROVISIONING_DELETION_TIMEOUT") {
					t.Errorf("Error should mention PROVISIONING_DELETION_TIMEOUT: %v", err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_SlugRenameTopicHoldPeriod(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		period      time.Duration
		shouldError bool
	}{
		{"zero", 0, true},
		{"below minimum 12h", 12 * time.Hour, true},
		{"just below minimum 23h59m59s", 23*time.Hour + 59*time.Minute + 59*time.Second, true},
		{"exact minimum 24h", 24 * time.Hour, false},
		{"default 168h", 168 * time.Hour, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.SlugRenameTopicHoldPeriod = tt.period
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if !strings.Contains(err.Error(), "SLUG_RENAME_TOPIC_HOLD_PERIOD") {
					t.Errorf("Error should mention SLUG_RENAME_TOPIC_HOLD_PERIOD: %v", err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_TokenRevocationMaxLifetime(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		duration    time.Duration
		shouldError bool
	}{
		{"negative", -1 * time.Second, true},
		{"zero", 0, true},
		{"positive 1h", time.Hour, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.TokenRevocationMaxLifetime = tt.duration
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if !strings.Contains(err.Error(), "PROVISIONING_TOKEN_REVOCATION_MAX_LIFETIME") {
					t.Errorf("Error should mention PROVISIONING_TOKEN_REVOCATION_MAX_LIFETIME: %v", err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_TokenRevocationRateLimit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		perMin, burst int
		wantErrSubstr string
	}{
		{"valid defaults", 30, 5, ""},
		{"perMin zero", 0, 5, "PROVISIONING_TOKEN_REVOCATION_RATE_LIMIT_PER_MIN"},
		{"perMin negative", -1, 5, "PROVISIONING_TOKEN_REVOCATION_RATE_LIMIT_PER_MIN"},
		{"burst zero", 30, 0, "PROVISIONING_TOKEN_REVOCATION_RATE_LIMIT_BURST"},
		{"burst negative", 30, -1, "PROVISIONING_TOKEN_REVOCATION_RATE_LIMIT_BURST"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.TokenRevocationRateLimitPerMinute = tt.perMin
			cfg.TokenRevocationRateLimitBurst = tt.burst
			err := cfg.Validate()
			if tt.wantErrSubstr == "" {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
				return
			}
			if err == nil {
				t.Error("Should error")
			} else if !strings.Contains(err.Error(), tt.wantErrSubstr) {
				t.Errorf("Error should mention %s: %v", tt.wantErrSubstr, err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_ValkeyAddrs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		addrs   []string
		wantErr bool
	}{
		{"valid single addr", []string{"localhost:6379"}, false},
		{"valid multiple addrs", []string{"a:6379", "b:6379"}, false},
		{"empty slice", []string{}, true},
		{"nil slice", nil, true},
		{"whitespace only entry", []string{"   "}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.ValkeyConfig.Addrs = tt.addrs
			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), "PROVISIONING_VALKEY_ADDRS") {
					t.Errorf("error should mention PROVISIONING_VALKEY_ADDRS: %v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_BulkDisconnectConcurrency(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		value   int
		wantErr bool
	}{
		{"at min", MinBulkDisconnectConcurrency, false},
		{"at max", MaxBulkDisconnectConcurrency, false},
		{"below min (0)", 0, true},
		{"above max", MaxBulkDisconnectConcurrency + 1, true},
		{"valid mid-range", 10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.BulkDisconnectConcurrency = tt.value
			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), "PROVISIONING_BULK_DISCONNECT_CONCURRENCY") {
					t.Errorf("error should mention PROVISIONING_BULK_DISCONNECT_CONCURRENCY: %v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_MaxWebhooksPerTenant(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		value   int
		wantErr bool
	}{
		{"zero", 0, true},
		{"min valid", 1, false},
		{"max valid", 100, false},
		{"above max", 101, true},
		{"negative", -1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.MaxWebhooksPerTenant = tt.value
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_WebhookAllowHTTP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		allowHTTP bool
		wantErr   bool
	}{
		{"false — ok", false, false},
		{"true — ok (warning only, not a validation error)", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			cfg.WebhookAllowHTTP = tt.allowHTTP
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

//nolint:paralleltest // shares license.SetPublicKeyForTesting via setProvisioningEditionManager
func TestProvisioningConfig_Validate_WebhookInternalToken(t *testing.T) {
	t.Run("empty token on Community edition — ok", func(t *testing.T) {
		cfg := newValidProvisioningConfig()
		cfg.WebhookInternalToken = ""
		if err := cfg.Validate(); err != nil {
			t.Errorf("unexpected error on Community edition: %v", err)
		}
	})
	t.Run("empty token on Pro edition — error", func(t *testing.T) {
		cfg := newValidProvisioningConfig()
		setProvisioningEditionManager(t, cfg, license.Pro)
		cfg.WebhookInternalToken = ""
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty WEBHOOK_INTERNAL_TOKEN on Pro edition")
		}
	})
	t.Run("set token on Pro edition — ok", func(t *testing.T) {
		cfg := newValidProvisioningConfig()
		setProvisioningEditionManager(t, cfg, license.Pro)
		cfg.WebhookInternalToken = "some-secret-token-that-is-at-least-32-chars"
		cfg.WebhookWorkerGRPCAddr = "webhook-worker:9095"
		cfg.WebhookDowngradePollInterval = 5 * time.Minute
		if err := cfg.Validate(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
	t.Run("short token on Pro edition — error", func(t *testing.T) {
		cfg := newValidProvisioningConfig()
		setProvisioningEditionManager(t, cfg, license.Pro)
		cfg.WebhookInternalToken = "too-short"
		cfg.WebhookWorkerGRPCAddr = "webhook-worker:9095"
		cfg.WebhookDowngradePollInterval = 5 * time.Minute
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for short WEBHOOK_INTERNAL_TOKEN on Pro edition")
		}
	})
}

func TestCredentialsConfig_Validate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"empty", "", true},
		{"valid hex 64 chars", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", false},
		{"valid base64", base64.StdEncoding.EncodeToString(make([]byte, 32)), false},
		{"too short hex", "0123456789abcdef", true},
		{"garbage string", "not-a-key", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := CredentialsConfig{CredentialsEncryptionKey: tt.key}
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestProvisioningConfig_Validate_AdminUI(t *testing.T) {
	t.Parallel()
	validToken := strings.Repeat("x", 32)
	tests := []struct {
		name    string
		cfg     func(*ProvisioningConfig)
		wantErr bool
		errMsg  string
	}{
		{
			name:    "disabled with empty token — no error",
			cfg:     func(c *ProvisioningConfig) { c.AdminUIEnabled = false; c.AdminToken = "" },
			wantErr: false,
		},
		{
			name:    "disabled with short token — no error",
			cfg:     func(c *ProvisioningConfig) { c.AdminUIEnabled = false; c.AdminToken = "short" },
			wantErr: false,
		},
		{
			name:    "enabled with empty token — error",
			cfg:     func(c *ProvisioningConfig) { c.AdminUIEnabled = true; c.AdminToken = "" },
			wantErr: true,
			errMsg:  "ADMIN_TOKEN",
		},
		{
			name:    "enabled with token < 32 chars — error",
			cfg:     func(c *ProvisioningConfig) { c.AdminUIEnabled = true; c.AdminToken = "tooshort" },
			wantErr: true,
			errMsg:  "ADMIN_TOKEN",
		},
		{
			name: "enabled with OIDC issuer set — error",
			cfg: func(c *ProvisioningConfig) {
				c.AdminUIEnabled = true
				c.AdminToken = validToken
				c.AdminSessionTTL = 8 * time.Hour
				c.AdminOIDCIssuer = "https://accounts.google.com"
			},
			wantErr: true,
			errMsg:  "ADMIN_OIDC_ISSUER",
		},
		{
			name: "enabled with valid token — no error",
			cfg: func(c *ProvisioningConfig) {
				c.AdminUIEnabled = true
				c.AdminToken = validToken
				c.AdminSessionTTL = 8 * time.Hour
			},
			wantErr: false,
		},
		{
			name: "enabled with TTL below minimum — error",
			cfg: func(c *ProvisioningConfig) {
				c.AdminUIEnabled = true
				c.AdminToken = validToken
				c.AdminSessionTTL = 30 * time.Second // below 1m
			},
			wantErr: true,
			errMsg:  "ADMIN_SESSION_TTL",
		},
		{
			name: "enabled with TTL above maximum — error",
			cfg: func(c *ProvisioningConfig) {
				c.AdminUIEnabled = true
				c.AdminToken = validToken
				c.AdminSessionTTL = 31 * 24 * time.Hour // above 30d
			},
			wantErr: true,
			errMsg:  "ADMIN_SESSION_TTL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidProvisioningConfig()
			tt.cfg(cfg)
			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("error should contain %q, got: %v", tt.errMsg, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

//nolint:paralleltest // shares license.SetPublicKeyForTesting via setProvisioningEditionManager
func TestProvisioningConfig_Validate_WebhookDowngradePollInterval(t *testing.T) {
	tests := []struct {
		name    string
		d       time.Duration
		wantErr bool
	}{
		{"below min (30s)", 30 * time.Second, true},
		{"min valid (1m)", time.Minute, false},
		{"max valid (1h)", time.Hour, false},
		{"above max (61m)", 61 * time.Minute, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newValidProvisioningConfig()
			setProvisioningEditionManager(t, cfg, license.Pro)
			cfg.WebhookInternalToken = "token-that-meets-the-32-char-minimum-requirement"
			cfg.WebhookWorkerGRPCAddr = "webhook-worker:9095"
			cfg.WebhookDowngradePollInterval = tt.d
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
