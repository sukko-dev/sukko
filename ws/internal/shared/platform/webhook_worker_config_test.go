package platform

import (
	"strings"
	"testing"
	"time"
)

func newValidWebhookWorkerConfig() *WebhookWorkerConfig {
	return &WebhookWorkerConfig{
		BaseConfig: BaseConfig{
			LogLevel:    "info",
			LogFormat:   "json",
			Environment: "local",
		},
		CredentialsConfig: CredentialsConfig{
			CredentialsEncryptionKey: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
		GRPCReconnectConfig: GRPCReconnectConfig{
			GRPCReconnectDelay:    time.Second,
			GRPCReconnectMaxDelay: 30 * time.Second,
		},
		WebhookInternalTokenConfig: WebhookInternalTokenConfig{
			WebhookInternalToken: "test-internal-token-that-is-at-least-32-chars",
		},
		WebhookHTTPConfig:     WebhookHTTPConfig{WebhookAllowHTTP: false},
		Port:                  8083,
		InternalGRPCPort:      9095,
		WebhookWorkerGRPCAddr: "localhost:9091",
		WorkerConcurrency:     50,
		RetryQueueSize:        10000,
		DeliveryTimeout:       10 * time.Second,
		CacheTTL:              30 * time.Second,
		ValkeyConfig:          ValkeyClientConfig{Addrs: []string{"localhost:6379"}},

		ValkeyChannel:             "ws.broadcast",
		ValkeyStartupPingTimeout:  5 * time.Second,
		ValkeyWriteTimeout:        3 * time.Second,
		ValkeyHealthCheckInterval: 10 * time.Second,
		ValkeyHealthCheckTimeout:  5 * time.Second,
		BroadcastShutdownTimeout:  5 * time.Second,
	}
}

func TestWebhookWorkerConfig_Validate_Valid(t *testing.T) {
	t.Parallel()
	cfg := newValidWebhookWorkerConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid config should not error: %v", err)
	}
}

func TestWebhookWorkerConfig_Validate_WorkerConcurrency(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		concurrency int
		wantErr     bool
	}{
		{"zero", 0, true},
		{"min valid", 1, false},
		{"max valid", 500, false},
		{"above max", 501, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidWebhookWorkerConfig()
			cfg.WorkerConcurrency = tt.concurrency
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

func TestWebhookWorkerConfig_Validate_RetryQueueSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		size    int
		wantErr bool
		errMsg  string
	}{
		{"below min", 99, true, "WEBHOOK_WORKER_RETRY_QUEUE_SIZE"},
		{"min valid", 100, false, ""},
		{"max valid", 100_000, false, ""},
		{"above max", 100_001, true, "WEBHOOK_WORKER_RETRY_QUEUE_SIZE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidWebhookWorkerConfig()
			cfg.RetryQueueSize = tt.size
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
				return
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.errMsg != "" && err != nil && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("error %q should contain %q", err.Error(), tt.errMsg)
			}
		})
	}
}

func TestWebhookWorkerConfig_Validate_DeliveryTimeout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		timeout time.Duration
		wantErr bool
	}{
		{"zero", 0, true},
		{"below min", 999 * time.Millisecond, true},
		{"min valid", time.Second, false},
		{"max valid", 30 * time.Second, false},
		{"above max", 31 * time.Second, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidWebhookWorkerConfig()
			cfg.DeliveryTimeout = tt.timeout
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

func TestWebhookWorkerConfig_Validate_CacheTTL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		ttl     time.Duration
		wantErr bool
	}{
		{"below min", 4 * time.Second, true},
		{"min valid", 5 * time.Second, false},
		{"max valid", 5 * time.Minute, false},
		{"above max", 5*time.Minute + time.Second, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidWebhookWorkerConfig()
			cfg.CacheTTL = tt.ttl
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

func TestWebhookWorkerConfig_Validate_Port(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		port    int
		wantErr bool
	}{
		{"zero", 0, true},
		{"min valid", 1, false},
		{"max valid", MaxPort, false},
		{"above max", MaxPort + 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidWebhookWorkerConfig()
			cfg.Port = tt.port
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

func TestWebhookWorkerConfig_Validate_InternalGRPCPort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		httpPort  int
		grpcPort  int
		wantErr   bool
		errSubstr string
	}{
		{"valid distinct ports", 8083, 9095, false, ""},
		{"zero grpc port", 8083, 0, true, "WEBHOOK_WORKER_INTERNAL_GRPC_PORT"},
		{"grpc port above max", 8083, MaxPort + 1, true, "WEBHOOK_WORKER_INTERNAL_GRPC_PORT"},
		{"cross-port conflict", 8083, 8083, true, "must differ"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidWebhookWorkerConfig()
			cfg.Port = tt.httpPort
			cfg.InternalGRPCPort = tt.grpcPort
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
				return
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.errSubstr != "" && err != nil && !strings.Contains(err.Error(), tt.errSubstr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.errSubstr)
			}
		})
	}
}

func TestWebhookWorkerConfig_Validate_WebhookAllowHTTP(t *testing.T) {
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
			cfg := newValidWebhookWorkerConfig()
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

func TestWebhookWorkerConfig_Validate_WebhookAllowPrivateIPs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		allowPrivateIPs bool
		wantErr         bool
	}{
		{"false — ok", false, false},
		{"true — ok in any environment (warning only, not a validation error)", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidWebhookWorkerConfig()
			cfg.WebhookAllowPrivateIPs = tt.allowPrivateIPs
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

func TestWebhookWorkerConfig_Validate_CredentialsEncryptionKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"empty", "", true},
		{"valid hex", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", false},
		{"too short hex", "0123456789abcdef", true},
		{"garbage", "not-a-key", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidWebhookWorkerConfig()
			cfg.CredentialsEncryptionKey = tt.key
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

func TestWebhookWorkerConfig_Validate_InternalToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		token   string
		wantErr bool
	}{
		{"empty", "", true},
		{"too short (31 chars)", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true},
		{"minimum valid (32 chars)", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"long token", "test-internal-token-that-is-at-least-32-chars", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidWebhookWorkerConfig()
			cfg.WebhookInternalToken = tt.token
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWebhookWorkerConfig_Validate_ValkeyAddrs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		addrs   []string
		wantErr bool
	}{
		{"nil addrs", nil, true},
		{"empty slice", []string{}, true},
		{"whitespace only", []string{"   "}, true},
		{"valid addr", []string{"localhost:6379"}, false},
		{"multiple addrs", []string{"host1:6379", "host2:6379"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidWebhookWorkerConfig()
			cfg.ValkeyConfig.Addrs = tt.addrs
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

func TestWebhookWorkerConfig_Validate_WebhookWorkerGRPCAddr(t *testing.T) {
	t.Parallel()
	cfg := newValidWebhookWorkerConfig()
	cfg.WebhookWorkerGRPCAddr = ""
	if err := cfg.Validate(); err == nil {
		t.Error("empty WEBHOOK_WORKER_PROVISIONING_GRPC_ADDR should error")
	}
}

func TestWebhookWorkerConfig_Validate_BroadcastBusTuning(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*WebhookWorkerConfig)
		errMsg  string // required substring when wantErr
		wantErr bool
	}{
		{"valid defaults ok", func(*WebhookWorkerConfig) {}, "", false},
		{"empty channel", func(c *WebhookWorkerConfig) { c.ValkeyChannel = "" }, "WEBHOOK_WORKER_VALKEY_CHANNEL", true},
		{"whitespace channel", func(c *WebhookWorkerConfig) { c.ValkeyChannel = "  " }, "WEBHOOK_WORKER_VALKEY_CHANNEL", true},
		{"zero startup ping", func(c *WebhookWorkerConfig) { c.ValkeyStartupPingTimeout = 0 }, "WEBHOOK_WORKER_VALKEY_STARTUP_PING_TIMEOUT", true},
		{"negative write timeout", func(c *WebhookWorkerConfig) { c.ValkeyWriteTimeout = -1 }, "WEBHOOK_WORKER_VALKEY_WRITE_TIMEOUT", true},
		{"zero health interval", func(c *WebhookWorkerConfig) { c.ValkeyHealthCheckInterval = 0 }, "WEBHOOK_WORKER_VALKEY_HEALTH_CHECK_INTERVAL", true},
		{"zero health timeout", func(c *WebhookWorkerConfig) { c.ValkeyHealthCheckTimeout = 0 }, "WEBHOOK_WORKER_VALKEY_HEALTH_CHECK_TIMEOUT", true},
		{"zero shutdown timeout", func(c *WebhookWorkerConfig) { c.BroadcastShutdownTimeout = 0 }, "WEBHOOK_WORKER_BROADCAST_SHUTDOWN_TIMEOUT", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidWebhookWorkerConfig()
			tt.mutate(cfg)
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.errMsg != "" && err != nil && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("error %q should name %q", err.Error(), tt.errMsg)
			}
		})
	}
}

func TestWebhookWorkerConfig_Validate_GRPCReconnectMaxDelayAboveCeiling(t *testing.T) {
	t.Parallel()
	cfg := newValidWebhookWorkerConfig()
	cfg.GRPCReconnectMaxDelay = 6 * time.Minute
	err := cfg.Validate()
	if err == nil {
		t.Error("GRPCReconnectMaxDelay above 5m ceiling should error")
	}
	if err != nil && !strings.Contains(err.Error(), "PROVISIONING_GRPC_RECONNECT_MAX_DELAY must be <= 5m") {
		t.Errorf("unexpected error message: %v", err)
	}
}
