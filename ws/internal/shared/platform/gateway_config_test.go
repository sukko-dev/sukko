package platform

import (
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// newValidGatewayConfig returns a gateway config with all valid defaults for testing.
func newValidGatewayConfig() *GatewayConfig {
	return &GatewayConfig{
		BaseConfig: BaseConfig{
			LogLevel:    "info",
			LogFormat:   "json",
			Environment: "test",
		},
		AuthConfig: AuthConfig{
			AuthMode: "required",
		},
		ProvisioningClientConfig: ProvisioningClientConfig{
			ProvisioningGRPCAddr: "localhost:9090",
			GRPCReconnectConfig:  GRPCReconnectConfig{GRPCReconnectDelay: 1 * time.Second, GRPCReconnectMaxDelay: 30 * time.Second},
		},
		Port:                         3000,
		ReadTimeout:                  15 * time.Second,
		WriteTimeout:                 15 * time.Second,
		IdleTimeout:                  60 * time.Second,
		BackendURL:                   "ws://localhost:3005/ws",
		DialTimeout:                  10 * time.Second,
		MessageTimeout:               60 * time.Second,
		RequireTenantID:              true,
		MaxFrameSize:                 1048576,
		PublishRateLimit:             10.0,
		PublishBurst:                 100,
		MaxPublishSize:               65536,
		TenantConnectionLimitEnabled: true,
		DefaultTenantConnectionLimit: 1000,
		AuthRefreshRateInterval:      30 * time.Second,
		AuthValidationTimeout:        5 * time.Second,
		ShutdownTimeout:              30 * time.Second,
		ChannelRulesCacheTTL:         1 * time.Minute,
		RegistryQueryTimeout:         5 * time.Second,
		ServerGRPCAddr:               "localhost:3006",
		PushGRPCAddr:                 "localhost:3008",
		SSEKeepAliveInterval:         45 * time.Second,
		CORSAllowedOrigins:           []string{"*"},
	}
}

func TestGatewayConfig_Validate_Valid(t *testing.T) {
	t.Parallel()
	cfg := newValidGatewayConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("Valid config should not error: %v", err)
	}
}

func TestGatewayConfig_Validate_AuthDisabledRejected(t *testing.T) {
	t.Parallel()
	cfg := newValidGatewayConfig()
	cfg.AuthMode = "disabled"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("AUTH_MODE=disabled should be rejected")
	}
	if !strings.Contains(err.Error(), "has been removed") {
		t.Errorf("Error should mention removal: %v", err)
	}
}

func TestGatewayConfig_Validate_AuthMode_RequiresGRPC(t *testing.T) {
	t.Parallel()
	cfg := newValidGatewayConfig()
	cfg.AuthMode = "required"
	cfg.ProvisioningGRPCAddr = "" // Missing gRPC addr

	err := cfg.Validate()
	if err == nil {
		t.Error("Should error when auth enabled without gRPC addr")
	}
	if !strings.Contains(err.Error(), "PROVISIONING_GRPC_ADDR") {
		t.Errorf("Error should mention PROVISIONING_GRPC_ADDR: %v", err)
	}
}

func TestGatewayConfig_Validate_AuthMode_WithGRPC(t *testing.T) {
	t.Parallel()
	cfg := newValidGatewayConfig()
	cfg.AuthMode = "required"
	cfg.ProvisioningGRPCAddr = "localhost:9090"

	if err := cfg.Validate(); err != nil {
		t.Errorf("Auth enabled with gRPC addr should not error: %v", err)
	}
}

func TestGatewayConfig_Validate_Port(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		port        int
		shouldError bool
	}{
		{"valid min", 1, false},
		{"valid max", 65535, false},
		{"valid common", 3000, false},
		{"zero", 0, true},
		{"negative", -1, true},
		{"too large", 65536, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.Port = tt.port
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

func TestGatewayConfig_Validate_GRPCReconnectSettings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		delay         time.Duration
		maxDelay      time.Duration
		shouldError   bool
		errorContains string
	}{
		{"valid defaults", 1 * time.Second, 30 * time.Second, false, ""},
		{"delay too small", 50 * time.Millisecond, 30 * time.Second, true, "PROVISIONING_GRPC_RECONNECT_DELAY"},
		{"max delay < delay", 5 * time.Second, 1 * time.Second, true, "PROVISIONING_GRPC_RECONNECT_MAX_DELAY"},
		{"equal values", 5 * time.Second, 5 * time.Second, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.AuthMode = "required" // GRPC validation runs when auth is enabled
			cfg.GRPCReconnectDelay = tt.delay
			cfg.GRPCReconnectMaxDelay = tt.maxDelay
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Error should contain %q: %v", tt.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
			}
		})
	}
}

func TestGatewayConfig_Validate_BackendURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		url         string
		shouldError bool
	}{
		{"valid ws", "ws://localhost:3005/ws", false},
		{"valid wss", "wss://example.com/ws", false},
		{"valid with port", "ws://127.0.0.1:8080/websocket", false},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.BackendURL = tt.url
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

func TestGatewayConfig_Validate_LogLevel(t *testing.T) {
	t.Parallel()
	validLevels := []string{"debug", "info", "warn", "error"}
	invalidLevels := []string{"DEBUG", "INFO", "invalid", "", "trace"}

	for _, level := range validLevels {
		t.Run("valid_"+level, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.LogLevel = level
			if err := cfg.Validate(); err != nil {
				t.Errorf("%s should be valid: %v", level, err)
			}
		})
	}

	for _, level := range invalidLevels {
		t.Run("invalid_"+level, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.LogLevel = level
			err := cfg.Validate()
			if err == nil {
				t.Errorf("%s should be invalid", level)
			}
			if !strings.Contains(err.Error(), "LOG_LEVEL") {
				t.Errorf("Error should mention LOG_LEVEL: %v", err)
			}
		})
	}
}

func TestGatewayConfig_Validate_LogFormat(t *testing.T) {
	t.Parallel()
	validFormats := []string{"json", "pretty"}
	invalidFormats := []string{"JSON", "xml", "text", "", "console"}

	for _, format := range validFormats {
		t.Run("valid_"+format, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.LogFormat = format
			if err := cfg.Validate(); err != nil {
				t.Errorf("%s should be valid: %v", format, err)
			}
		})
	}

	for _, format := range invalidFormats {
		t.Run("invalid_"+format, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.LogFormat = format
			err := cfg.Validate()
			if err == nil {
				t.Errorf("%s should be invalid", format)
			}
		})
	}
}

func TestGatewayConfig_Validate_MaxFrameSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		size        int
		shouldError bool
	}{
		{"valid default 1MB", 1048576, false},
		{"valid min 1KB", 1024, false},
		{"valid max 10MB", 10 * 1024 * 1024, false},
		{"zero is too small", 0, true},
		{"too small", 512, true},
		{"too large", 10*1024*1024 + 1, true},
		{"negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.MaxFrameSize = tt.size
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

func TestGatewayConfig_Validate_HTTPTimeouts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		field         string
		value         time.Duration
		shouldError   bool
		errorContains string
	}{
		{"read_valid", "read", 15 * time.Second, false, ""},
		{"read_zero", "read", 0, true, "GATEWAY_READ_TIMEOUT"},
		{"read_too_large", "read", 121 * time.Second, true, "GATEWAY_READ_TIMEOUT"},
		{"write_valid", "write", 15 * time.Second, false, ""},
		{"write_zero", "write", 0, true, "GATEWAY_WRITE_TIMEOUT"},
		{"write_too_large", "write", 121 * time.Second, true, "GATEWAY_WRITE_TIMEOUT"},
		{"idle_valid", "idle", 60 * time.Second, false, ""},
		{"idle_zero", "idle", 0, true, "GATEWAY_IDLE_TIMEOUT"},
		{"idle_too_large", "idle", 301 * time.Second, true, "GATEWAY_IDLE_TIMEOUT"},
		{"idle_max", "idle", 300 * time.Second, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			switch tt.field {
			case "read":
				cfg.ReadTimeout = tt.value
			case "write":
				cfg.WriteTimeout = tt.value
			case "idle":
				cfg.IdleTimeout = tt.value
			}
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Error should contain %q: %v", tt.errorContains, err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestGatewayConfig_Validate_DialTimeout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		value       time.Duration
		shouldError bool
	}{
		{"valid", 10 * time.Second, false},
		{"valid min", 1 * time.Second, false},
		{"valid max", 60 * time.Second, false},
		{"zero", 0, true},
		{"too large", 61 * time.Second, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.DialTimeout = tt.value
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

func TestGatewayConfig_Validate_MessageTimeout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		value       time.Duration
		shouldError bool
	}{
		{"valid default", 60 * time.Second, false},
		{"valid min", 1 * time.Second, false},
		{"valid max", 300 * time.Second, false},
		{"zero", 0, true},
		{"negative", -1 * time.Second, true},
		{"too large", 301 * time.Second, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.MessageTimeout = tt.value
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

func TestGatewayConfig_Validate_PublishSettings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		rateLimit     float64
		burst         int
		maxSize       int
		shouldError   bool
		errorContains string
	}{
		{"valid defaults", 10.0, 100, 65536, false, ""},
		{"burst zero rejected", 10.0, 0, 65536, true, "GATEWAY_PUBLISH_BURST"},
		{"rate zero rejected", 0, 100, 65536, true, "GATEWAY_PUBLISH_RATE_LIMIT"},
		{"rate negative", -1.0, 100, 65536, true, "GATEWAY_PUBLISH_RATE_LIMIT"},
		{"size too small", 10.0, 100, 512, true, "GATEWAY_MAX_PUBLISH_SIZE"},
		{"size too large", 10.0, 100, 10*1024*1024 + 1, true, "GATEWAY_MAX_PUBLISH_SIZE"},
		{"size valid min", 10.0, 100, 1024, false, ""},
		{"size valid max", 10.0, 100, 10 * 1024 * 1024, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.PublishRateLimit = tt.rateLimit
			cfg.PublishBurst = tt.burst
			cfg.MaxPublishSize = tt.maxSize
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Error should contain %q: %v", tt.errorContains, err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestGatewayConfig_Validate_TenantConnectionLimit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		enabled     bool
		limit       int
		shouldError bool
	}{
		{"enabled valid", true, 1000, false},
		{"enabled min valid", true, 1, false},
		{"enabled zero", true, 0, true},
		{"enabled negative", true, -1, true},
		{"disabled zero ok", false, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.TenantConnectionLimitEnabled = tt.enabled
			cfg.DefaultTenantConnectionLimit = tt.limit
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

func TestGatewayConfig_Validate_AuthValidationTimeout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		timeout     time.Duration
		shouldError bool
	}{
		{"valid", 5 * time.Second, false},
		{"zero", 0, true},
		{"negative", -1 * time.Second, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.AuthValidationTimeout = tt.timeout
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			} else if tt.shouldError && !strings.Contains(err.Error(), "GATEWAY_AUTH_VALIDATION_TIMEOUT") {
				t.Errorf("Error should mention GATEWAY_AUTH_VALIDATION_TIMEOUT: %v", err)
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestGatewayConfig_Validate_ShutdownTimeout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		timeout     time.Duration
		shouldError bool
	}{
		{"valid", 30 * time.Second, false},
		{"zero", 0, true},
		{"negative", -1 * time.Second, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			cfg.ShutdownTimeout = tt.timeout
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			} else if tt.shouldError && !strings.Contains(err.Error(), "GATEWAY_SHUTDOWN_TIMEOUT") {
				t.Errorf("Error should mention GATEWAY_SHUTDOWN_TIMEOUT: %v", err)
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestGatewayConfig_Validate_CacheTTLAndRegistryTimeout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		setField      func(*GatewayConfig)
		shouldError   bool
		errorContains string
	}{
		{"channel rules cache TTL valid", func(c *GatewayConfig) { c.ChannelRulesCacheTTL = 1 * time.Minute }, false, ""},
		{"channel rules cache TTL zero", func(c *GatewayConfig) { c.ChannelRulesCacheTTL = 0 }, true, "GATEWAY_CHANNEL_RULES_CACHE_TTL"},
		{"registry query timeout valid", func(c *GatewayConfig) { c.RegistryQueryTimeout = 5 * time.Second }, false, ""},
		{"registry query timeout zero", func(c *GatewayConfig) { c.RegistryQueryTimeout = 0 }, true, "GATEWAY_REGISTRY_QUERY_TIMEOUT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidGatewayConfig()
			tt.setField(cfg)
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Error should contain %q: %v", tt.errorContains, err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

// --- Edition Gate Tests ---
// MUST NOT use t.Parallel() — tests share license.SetPublicKeyForTesting.

func setGatewayEditionManager(t *testing.T, cfg *GatewayConfig, edition license.Edition) {
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

//nolint:paralleltest // shares license.SetPublicKeyForTesting via setEditionManager helper
func TestGatewayConfig_Validate_EditionGates_Community(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*GatewayConfig)
		errSub string
	}{
		{
			name:   "community rejects tenant connection limits",
			modify: func(c *GatewayConfig) { c.TenantConnectionLimitEnabled = true },
			errSub: "TENANT_CONNECTION_LIMIT_ENABLED",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newValidGatewayConfig()
			cfg.TenantConnectionLimitEnabled = false
			setGatewayEditionManager(t, cfg, license.Community)
			tt.modify(cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.errSub) {
				t.Errorf("error %q should contain %q", err.Error(), tt.errSub)
			}
		})
	}
}

//nolint:paralleltest // shares license.SetPublicKeyForTesting via setEditionManager helper
func TestGatewayConfig_Validate_EditionGates_ProAccepts(t *testing.T) {
	cfg := newValidGatewayConfig()
	setGatewayEditionManager(t, cfg, license.Pro)
	cfg.TenantConnectionLimitEnabled = true

	if err := cfg.Validate(); err != nil {
		t.Errorf("Pro should accept per-tenant features: %v", err)
	}
}

// GATEWAY_PUSH_ENABLED is an explicit deployment mode (§XV — no inference from
// PUSH_GRPC_ADDR presence): default off; when off, the push address is not
// required; when on, it is.
func TestGatewayConfig_PushEnabled_DefaultOff(t *testing.T) {
	t.Parallel()
	cfg := newValidGatewayConfig()
	if cfg.PushEnabled {
		t.Error("PushEnabled default = true, want false (push is deploy-optional, envDefault false)")
	}
}

func TestGatewayConfig_Validate_PushDisabled_AddrNotRequired(t *testing.T) {
	t.Parallel()
	cfg := newValidGatewayConfig()
	cfg.PushEnabled = false
	cfg.PushGRPCAddr = ""
	if err := cfg.Validate(); err != nil {
		t.Errorf("push disabled must not require PUSH_GRPC_ADDR: %v", err)
	}
}

func TestGatewayConfig_Validate_PushEnabled_AddrRequired(t *testing.T) {
	t.Parallel()
	cfg := newValidGatewayConfig()
	cfg.PushEnabled = true
	cfg.PushGRPCAddr = ""
	err := cfg.Validate()
	if err == nil {
		t.Fatal("push enabled with empty PUSH_GRPC_ADDR must fail validation")
	}
	if !strings.Contains(err.Error(), "PUSH_GRPC_ADDR") {
		t.Errorf("error %q should name PUSH_GRPC_ADDR", err.Error())
	}
}
