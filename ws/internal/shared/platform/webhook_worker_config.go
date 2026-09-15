package platform

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// WebhookWorkerConfig holds all configuration for the webhook-worker service.
// It embeds BaseConfig (shared logging/env/license/OTEL/pprof fields),
// CredentialsConfig (shared AES-256 encryption key), and three shared
// webhook-specific config structs (§X: no duplicate fields across configs).
type WebhookWorkerConfig struct {
	BaseConfig
	CredentialsConfig
	GRPCReconnectConfig        // embeds PROVISIONING_GRPC_RECONNECT_* env vars
	WebhookInternalTokenConfig // embeds WEBHOOK_INTERNAL_TOKEN env var
	WebhookHTTPConfig          // embeds WEBHOOK_ALLOW_HTTP env var

	// WebhookAllowPrivateIPs bypasses SSRF dialer for private-IP delivery targets.
	// Intended for CI/testing environments where the tester pod runs in the same cluster.
	// Protection: safe default (false) + startup warning in main.go. No environment-name
	// guard — ENVIRONMENT is operator-defined free text; string comparison provides false
	// safety. Do NOT add a c.Environment == "prod" check here.
	WebhookAllowPrivateIPs bool `env:"WEBHOOK_ALLOW_PRIVATE_IPS" envDefault:"false"` // Allow webhook delivery to private-IP endpoints (disables SSRF protection). Safe default: false; enable only in trusted environments via explicit opt-in.

	// Port is the HTTP server port for health and metrics endpoints.
	Port int `env:"WEBHOOK_HTTP_PORT" envDefault:"8083"` // HTTP port for the webhook worker's management API.

	// InternalGRPCPort is the port the worker's internal gRPC server listens on
	// for inbound TestDeliver calls from the provisioning service.
	// Distinct from WEBHOOK_WORKER_GRPC_PORT (provisioning side; default 9091).
	InternalGRPCPort int `env:"WEBHOOK_WORKER_INTERNAL_GRPC_PORT" envDefault:"9095"` // gRPC port for internal communication between provisioning service and webhook worker.

	// WebhookWorkerGRPCAddr is the gRPC address of the WebhookWorkerService endpoint
	// on the provisioning server (WEBHOOK_WORKER_GRPC_PORT, default :9091).
	// Distinct from PROVISIONING_GRPC_ADDR (:9090) used by ws-server and ws-gateway.
	// No envDefault — required at startup (§XV: cannot be both defaulted and required).
	WebhookWorkerGRPCAddr string `env:"WEBHOOK_WORKER_PROVISIONING_GRPC_ADDR"` // gRPC address of the provisioning service for webhook configuration queries.

	// Worker pool
	WorkerConcurrency int           `env:"WEBHOOK_WORKER_CONCURRENCY"      envDefault:"50"`    // Maximum number of concurrent webhook delivery goroutines.
	RetryQueueSize    int           `env:"WEBHOOK_WORKER_RETRY_QUEUE_SIZE" envDefault:"10000"` // In-memory retry queue capacity. Webhooks failing initial delivery are queued here.
	DeliveryTimeout   time.Duration `env:"WEBHOOK_DELIVERY_TIMEOUT"        envDefault:"10s"`   // HTTP timeout for a single webhook delivery attempt.
	CacheTTL          time.Duration `env:"WEBHOOK_CACHE_TTL"               envDefault:"30s"`   // TTL for cached webhook endpoint configurations fetched from provisioning.

	// ValkeyConfig is the Valkey connection for this service.
	// envPrefix:"WEBHOOK_WORKER_" produces WEBHOOK_WORKER_VALKEY_ADDRS etc.
	ValkeyConfig ValkeyClientConfig `envPrefix:"WEBHOOK_WORKER_"`

	// Broadcast bus tuning. These fields are webhook-worker-specific (the shared
	// ValkeyClientConfig above is also embedded by provisioning, which does not use a
	// broadcast bus — keeping them here avoids dead knobs on provisioning). Defaults
	// mirror ws-server's production-intended values so the two services stay in sync (§XVIII).
	ValkeyChannel             string        `env:"WEBHOOK_WORKER_VALKEY_CHANNEL" envDefault:"ws.broadcast"`      // Valkey pub/sub channel prefix for broadcast events. MUST match ws-server's VALKEY_CHANNEL or the worker receives no events (the full per-tenant channel is {prefix}:{tenantID}).
	ValkeyStartupPingTimeout  time.Duration `env:"WEBHOOK_WORKER_VALKEY_STARTUP_PING_TIMEOUT" envDefault:"5s"`   // Timeout for the Valkey connectivity check during webhook-worker startup. Must be > 0 or the check fails instantly.
	ValkeyWriteTimeout        time.Duration `env:"WEBHOOK_WORKER_VALKEY_WRITE_TIMEOUT" envDefault:"3s"`          // Timeout for Valkey write operations (SUBSCRIBE/PSUBSCRIBE) from the broadcast bus.
	ValkeyHealthCheckInterval time.Duration `env:"WEBHOOK_WORKER_VALKEY_HEALTH_CHECK_INTERVAL" envDefault:"10s"` // Interval for periodic Valkey health checks. Must be > 0 or the health monitor is disabled.
	ValkeyHealthCheckTimeout  time.Duration `env:"WEBHOOK_WORKER_VALKEY_HEALTH_CHECK_TIMEOUT" envDefault:"5s"`   // Timeout for each Valkey health check PING.
	BroadcastShutdownTimeout  time.Duration `env:"WEBHOOK_WORKER_BROADCAST_SHUTDOWN_TIMEOUT" envDefault:"5s"`    // Timeout for draining the broadcast bus during graceful shutdown.
}

// Validate checks all WebhookWorkerConfig bounds and required fields.
func (c *WebhookWorkerConfig) Validate() error {
	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}
	if err := c.CredentialsConfig.Validate(); err != nil {
		return err
	}
	if err := c.GRPCReconnectConfig.Validate(); err != nil {
		return err
	}
	if err := c.WebhookInternalTokenConfig.Validate(); err != nil {
		return err
	}
	if c.WorkerConcurrency < 1 || c.WorkerConcurrency > 500 {
		return fmt.Errorf("WEBHOOK_WORKER_CONCURRENCY must be between 1 and 500, got %d", c.WorkerConcurrency)
	}
	if c.RetryQueueSize < 100 || c.RetryQueueSize > 100_000 {
		return fmt.Errorf("WEBHOOK_WORKER_RETRY_QUEUE_SIZE must be between 100 and 100000, got %d", c.RetryQueueSize)
	}
	if c.DeliveryTimeout < time.Second || c.DeliveryTimeout > 30*time.Second {
		return fmt.Errorf("WEBHOOK_DELIVERY_TIMEOUT must be between 1s and 30s, got %s", c.DeliveryTimeout)
	}
	if c.CacheTTL < 5*time.Second || c.CacheTTL > 5*time.Minute {
		return fmt.Errorf("WEBHOOK_CACHE_TTL must be between 5s and 5m, got %s", c.CacheTTL)
	}
	if c.Port < 1 || c.Port > MaxPort {
		return fmt.Errorf("WEBHOOK_HTTP_PORT must be between 1 and %d, got %d", MaxPort, c.Port)
	}
	if c.InternalGRPCPort < 1 || c.InternalGRPCPort > MaxPort {
		return fmt.Errorf("WEBHOOK_WORKER_INTERNAL_GRPC_PORT must be between 1 and %d, got %d", MaxPort, c.InternalGRPCPort)
	}
	if c.InternalGRPCPort == c.Port {
		return fmt.Errorf("WEBHOOK_WORKER_INTERNAL_GRPC_PORT (%d) must differ from WEBHOOK_HTTP_PORT (%d)",
			c.InternalGRPCPort, c.Port)
	}
	if !slices.ContainsFunc(c.ValkeyConfig.Addrs, func(s string) bool { return strings.TrimSpace(s) != "" }) {
		return errors.New("WEBHOOK_WORKER_VALKEY_ADDRS is required and must contain at least one non-empty address")
	}
	if strings.TrimSpace(c.ValkeyChannel) == "" {
		return errors.New("WEBHOOK_WORKER_VALKEY_CHANNEL cannot be empty")
	}
	if c.ValkeyStartupPingTimeout <= 0 {
		return fmt.Errorf("WEBHOOK_WORKER_VALKEY_STARTUP_PING_TIMEOUT must be > 0, got %v", c.ValkeyStartupPingTimeout)
	}
	if c.ValkeyWriteTimeout <= 0 {
		return fmt.Errorf("WEBHOOK_WORKER_VALKEY_WRITE_TIMEOUT must be > 0, got %v", c.ValkeyWriteTimeout)
	}
	if c.ValkeyHealthCheckInterval <= 0 {
		return fmt.Errorf("WEBHOOK_WORKER_VALKEY_HEALTH_CHECK_INTERVAL must be > 0, got %v", c.ValkeyHealthCheckInterval)
	}
	if c.ValkeyHealthCheckTimeout <= 0 {
		return fmt.Errorf("WEBHOOK_WORKER_VALKEY_HEALTH_CHECK_TIMEOUT must be > 0, got %v", c.ValkeyHealthCheckTimeout)
	}
	if c.BroadcastShutdownTimeout <= 0 {
		return fmt.Errorf("WEBHOOK_WORKER_BROADCAST_SHUTDOWN_TIMEOUT must be > 0, got %v", c.BroadcastShutdownTimeout)
	}
	if c.WebhookWorkerGRPCAddr == "" {
		return errors.New("WEBHOOK_WORKER_PROVISIONING_GRPC_ADDR is required")
	}
	return nil
}

// LoadWebhookWorkerConfig loads configuration from environment and .env file.
// Follows the same pattern as LoadProvisioningConfig, LoadServerConfig, LoadGatewayConfig:
// godotenv.Load() first (no-op if no .env file), then env.Parse, then Validate.
func LoadWebhookWorkerConfig() (*WebhookWorkerConfig, error) {
	_ = godotenv.Load()
	cfg := &WebhookWorkerConfig{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}
