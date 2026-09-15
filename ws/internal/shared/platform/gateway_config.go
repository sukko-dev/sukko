// Package platform provides configuration loading for all components.
// Config loading happens here (at entry point), not in library packages.
package platform

import (
	"errors"
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

// GatewayConfig holds gateway configuration loaded from environment variables.
type GatewayConfig struct {
	BaseConfig
	AuthConfig
	ProvisioningClientConfig
	AnalyticsConfig
	PodIdentityConfig

	// Server settings
	Port         int           `env:"GATEWAY_PORT" envDefault:"3000"`         // HTTP listen port for the WebSocket gateway.
	ReadTimeout  time.Duration `env:"GATEWAY_READ_TIMEOUT" envDefault:"15s"`  // HTTP server read timeout. Applies to the WebSocket upgrade handshake and HTTP request reads.
	WriteTimeout time.Duration `env:"GATEWAY_WRITE_TIMEOUT" envDefault:"15s"` // HTTP server write timeout.
	IdleTimeout  time.Duration `env:"GATEWAY_IDLE_TIMEOUT" envDefault:"60s"`  // HTTP keep-alive idle timeout. Connections idle longer than this are closed.

	// Backend ws-server connection
	BackendURL     string        `env:"GATEWAY_BACKEND_URL" envDefault:"ws://localhost:3005/ws"` // WebSocket URL of the ws-server backend the gateway proxies connections to. Must include the /ws path.
	DialTimeout    time.Duration `env:"GATEWAY_DIAL_TIMEOUT" envDefault:"10s"`                   // Timeout for dialing the ws-server backend when establishing a new proxied connection.
	MessageTimeout time.Duration `env:"GATEWAY_MESSAGE_TIMEOUT" envDefault:"60s"`                // Timeout for forwarding a single message between a client and the ws-server backend.

	// MaxFrameSize is the maximum allowed WebSocket frame size in bytes.
	// Frames exceeding this are rejected before payload allocation to prevent OOM.
	MaxFrameSize int `env:"GATEWAY_MAX_FRAME_SIZE" envDefault:"1048576"` // Maximum WebSocket frame size in bytes. Frames larger than this are rejected before allocation. Default: 1MB.

	// Multi-tenant settings
	RequireTenantID              bool `env:"REQUIRE_TENANT_ID" envDefault:"true"`                // When true, all WebSocket connections must identify a tenant via their JWT or API key. Disable only in single-tenant deployments where the tenant is implicit.
	DefaultTenantConnectionLimit int  `env:"DEFAULT_TENANT_CONNECTION_LIMIT" envDefault:"1000"`  // Default maximum simultaneous WebSocket connections allowed per tenant when TENANT_CONNECTION_LIMIT_ENABLED is true.
	TenantConnectionLimitEnabled bool `env:"TENANT_CONNECTION_LIMIT_ENABLED" envDefault:"false"` // When true, the gateway enforces per-tenant connection limits using DEFAULT_TENANT_CONNECTION_LIMIT as the default cap.

	// Channel authorization is provisioning-only: per-tenant rules streamed
	// over gRPC (PROVISIONING_GRPC_ADDR) are the sole source — there is no
	// static pattern configuration and no fallback rule set.

	// Publish-specific settings
	PublishRateLimit float64 `env:"GATEWAY_PUBLISH_RATE_LIMIT" envDefault:"10.0"` // Sustained rate limit for REST and WebSocket publish requests per authenticated principal, in messages per second.
	PublishBurst     int     `env:"GATEWAY_PUBLISH_BURST" envDefault:"100"`       // Burst allowance for the REST and WebSocket publish rate limiter. Allows short spikes above GATEWAY_PUBLISH_RATE_LIMIT before throttling kicks in.
	MaxPublishSize   int     `env:"GATEWAY_MAX_PUBLISH_SIZE" envDefault:"65536"`  // Maximum REST publish payload size in bytes. Payloads exceeding this are rejected. Default: 64KB.

	// Auth refresh settings
	AuthRefreshRateInterval time.Duration `env:"GATEWAY_AUTH_REFRESH_RATE_INTERVAL" envDefault:"30s"` // Minimum interval between JWT refresh operations per connection. Prevents refresh storms under load.

	// Auth validation timeout for intercepting auth refresh operations
	AuthValidationTimeout time.Duration `env:"GATEWAY_AUTH_VALIDATION_TIMEOUT" envDefault:"5s"` // Timeout for validating auth refresh operations per connection.

	// Token revocation
	RevocationMaxLifetime time.Duration `env:"REVOCATION_MAX_LIFETIME" envDefault:"24h"` // Max time a revocation entry lives when exp is not provided

	// Graceful shutdown timeout
	ShutdownTimeout time.Duration `env:"GATEWAY_SHUTDOWN_TIMEOUT" envDefault:"30s"` // Maximum time the gateway waits for active connections to drain before forcing shutdown.

	// InternalSecret is forwarded verbatim as X-Sukko-Internal-Secret to ws-server.
	// No envDefault — empty string is valid when ws-server WS_INTERNAL_SECRET_ENABLED=false.
	// redact:"true" prevents the /config endpoint from exposing this value.
	InternalSecret string `env:"WS_INTERNAL_SECRET" redact:"true"` // Shared secret forwarded as X-Sukko-Internal-Secret to ws-server. Set WS_INTERNAL_SECRET_ENABLED=true on ws-server to enforce.

	// SSE + REST Publish (Pro edition)
	ServerGRPCAddr string `env:"SERVER_GRPC_ADDR" envDefault:"localhost:3006"` // ws-server gRPC address for RealtimeService
	// Push service gRPC address for PushService (RegisterDevice, UnregisterDevice, GetVAPIDKey)
	PushEnabled          bool          `env:"GATEWAY_PUSH_ENABLED" envDefault:"false"`              // Enables the gateway's push endpoints and push-service client; when false the /api/v1/push/* routes are not registered (404) and no push connection is made. Explicit deployment mode — the push service is deploy-optional.
	PushGRPCAddr         string        `env:"PUSH_GRPC_ADDR" envDefault:"localhost:3008"`           // Push service gRPC address for device registration and VAPID key endpoints; required (and used) only when GATEWAY_PUSH_ENABLED=true.
	SSEKeepAliveInterval time.Duration `env:"SSE_KEEPALIVE_INTERVAL" envDefault:"45s"`              // Interval for SSE keepalive messages sent to prevent proxy timeouts on long-lived connections.
	CORSAllowedOrigins   []string      `env:"GATEWAY_CORS_ORIGINS" envDefault:"*" envSeparator:","` // CORS allowed origins (* = all, production: restrict)

	// Channel rules provider cache TTLs
	ChannelRulesCacheTTL time.Duration `env:"GATEWAY_CHANNEL_RULES_CACHE_TTL" envDefault:"1m"` // How long per-tenant channel rules are cached locally before re-fetching from the provisioning service.
	RegistryQueryTimeout time.Duration `env:"GATEWAY_REGISTRY_QUERY_TIMEOUT" envDefault:"5s"`  // Timeout for querying the connections registry when enforcing per-tenant connection limits.

	// editionManager holds the license-resolved edition and limits.
	// Set by LoadGatewayConfig() before Validate(). Not an env var — derived from SUKKO_LICENSE_KEY.
	editionManager *license.Manager
}

// EditionManager returns the license manager for this config.
func (c *GatewayConfig) EditionManager() *license.Manager {
	return c.editionManager
}

// LoadGatewayConfig reads gateway configuration from environment variables.
// Optionally loads from .env file if present.
func LoadGatewayConfig(logger zerolog.Logger) (*GatewayConfig, error) {
	// Load .env file (optional)
	if err := godotenv.Load(); err != nil {
		logger.Debug().Msg("No .env file found (using environment variables only)")
	}

	cfg := &GatewayConfig{}

	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse gateway config: %w", err)
	}

	// Create license manager before validation — Validate() uses edition gates.
	mgr, err := license.NewManager(cfg.LicenseKey, logger)
	if err != nil {
		return nil, fmt.Errorf("license: %w", err)
	}
	cfg.editionManager = mgr

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("gateway config validation failed: %w", err)
	}

	// Defense-in-depth: warn when secret is empty so operators can catch misconfigured
	// deployments before they reach production traffic.
	if cfg.InternalSecret == "" {
		logger.Warn().
			Str("environment", cfg.Environment).
			Msg("WS_INTERNAL_SECRET is empty — gateway will forward empty X-Sukko-Internal-Secret header; ensure ws-server WS_INTERNAL_SECRET_ENABLED=false in this environment")
	}

	return cfg, nil
}

// Validate checks gateway configuration for errors.
func (c *GatewayConfig) Validate() error {
	// Auth mode validation (must be first — other validations depend on it)
	if err := c.AuthConfig.Validate(); err != nil {
		return err
	}

	if c.Port < 1 || c.Port > MaxPort {
		return fmt.Errorf("GATEWAY_PORT must be between 1 and %d, got %d", MaxPort, c.Port)
	}

	// MaxFrameSize validation (1KB-10MB, default already applied above)
	if c.MaxFrameSize < MinFrameSize || c.MaxFrameSize > MaxFrameSizeLimit {
		return fmt.Errorf("GATEWAY_MAX_FRAME_SIZE must be between %d and %d, got %d", MinFrameSize, MaxFrameSizeLimit, c.MaxFrameSize)
	}

	// HTTP timeout validation
	if c.ReadTimeout < MinTimeout || c.ReadTimeout > MaxReadWriteTimeout {
		return fmt.Errorf("GATEWAY_READ_TIMEOUT must be %v-%v, got %v", MinTimeout, MaxReadWriteTimeout, c.ReadTimeout)
	}
	if c.WriteTimeout < MinTimeout || c.WriteTimeout > MaxReadWriteTimeout {
		return fmt.Errorf("GATEWAY_WRITE_TIMEOUT must be %v-%v, got %v", MinTimeout, MaxReadWriteTimeout, c.WriteTimeout)
	}
	if c.IdleTimeout < MinTimeout || c.IdleTimeout > MaxIdleTimeout {
		return fmt.Errorf("GATEWAY_IDLE_TIMEOUT must be %v-%v, got %v", MinTimeout, MaxIdleTimeout, c.IdleTimeout)
	}

	// Backend connection validation
	if c.DialTimeout < MinTimeout || c.DialTimeout > MaxDialTimeout {
		return fmt.Errorf("GATEWAY_DIAL_TIMEOUT must be %v-%v, got %v", MinTimeout, MaxDialTimeout, c.DialTimeout)
	}
	if c.MessageTimeout < MinTimeout || c.MessageTimeout > MaxMessageTimeout {
		return fmt.Errorf("GATEWAY_MESSAGE_TIMEOUT must be %v-%v, got %v", MinTimeout, MaxMessageTimeout, c.MessageTimeout)
	}

	// Provisioning client config is always required (auth is always on).
	if err := c.ProvisioningClientConfig.Validate(); err != nil {
		return err
	}

	if c.AuthRefreshRateInterval < time.Second {
		return fmt.Errorf("GATEWAY_AUTH_REFRESH_RATE_INTERVAL must be >= 1s, got %v", c.AuthRefreshRateInterval)
	}

	if c.BackendURL == "" {
		return errors.New("GATEWAY_BACKEND_URL is required")
	}

	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}
	if err := c.AnalyticsConfig.Validate(); err != nil {
		return err
	}

	// Publish settings validation
	if c.PublishBurst < 1 {
		return fmt.Errorf("GATEWAY_PUBLISH_BURST must be >= 1, got %d", c.PublishBurst)
	}
	if c.PublishRateLimit <= 0 {
		return fmt.Errorf("GATEWAY_PUBLISH_RATE_LIMIT must be > 0, got %f", c.PublishRateLimit)
	}
	if c.MaxPublishSize < MinFrameSize || c.MaxPublishSize > MaxFrameSizeLimit {
		return fmt.Errorf("GATEWAY_MAX_PUBLISH_SIZE must be %d-%d, got %d", MinFrameSize, MaxFrameSizeLimit, c.MaxPublishSize)
	}

	// Tenant connection limit (when enabled)
	if c.TenantConnectionLimitEnabled && c.DefaultTenantConnectionLimit < 1 {
		return fmt.Errorf("DEFAULT_TENANT_CONNECTION_LIMIT must be >= 1, got %d", c.DefaultTenantConnectionLimit)
	}

	// New externalized duration fields (reject non-positive)
	if c.AuthValidationTimeout <= 0 {
		return fmt.Errorf("GATEWAY_AUTH_VALIDATION_TIMEOUT must be > 0, got %v", c.AuthValidationTimeout)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("GATEWAY_SHUTDOWN_TIMEOUT must be > 0, got %v", c.ShutdownTimeout)
	}
	if c.ChannelRulesCacheTTL <= 0 {
		return fmt.Errorf("GATEWAY_CHANNEL_RULES_CACHE_TTL must be > 0, got %v", c.ChannelRulesCacheTTL)
	}
	if c.RegistryQueryTimeout <= 0 {
		return fmt.Errorf("GATEWAY_REGISTRY_QUERY_TIMEOUT must be > 0, got %v", c.RegistryQueryTimeout)
	}

	// SSE + REST Publish config validation
	if c.ServerGRPCAddr == "" {
		return errors.New("SERVER_GRPC_ADDR must not be empty")
	}
	// Push address is required only when the push surface is enabled — the
	// enablement is the explicit GATEWAY_PUSH_ENABLED mode, never inferred
	// from the address being set (§XV: no dual-purpose values).
	if c.PushEnabled && c.PushGRPCAddr == "" {
		return errors.New("PUSH_GRPC_ADDR must not be empty when GATEWAY_PUSH_ENABLED=true")
	}
	if c.SSEKeepAliveInterval <= 0 {
		return fmt.Errorf("SSE_KEEPALIVE_INTERVAL must be > 0, got %v", c.SSEKeepAliveInterval)
	}
	if len(c.CORSAllowedOrigins) == 0 {
		return errors.New("GATEWAY_CORS_ORIGINS must have at least one entry")
	}

	// Edition gates — uses startup-resolved Edition(), not expiry-aware CurrentEdition().
	if c.editionManager != nil {
		edition := c.editionManager.Edition()

		if c.TenantConnectionLimitEnabled && !license.EditionHasFeature(edition, license.PerTenantConnectionLimits) {
			return license.NewFeatureError(license.PerTenantConnectionLimits, edition)
		}
		if c.OTELTracingEnabled && !license.EditionHasFeature(edition, license.ConnectionTracing) {
			return license.NewFeatureError(license.ConnectionTracing, edition)
		}
	}

	return nil
}

// LogConfig logs gateway configuration using structured logging.
func (c *GatewayConfig) LogConfig(logger zerolog.Logger) {
	edition := "community"
	if c.editionManager != nil {
		edition = c.editionManager.Edition().String()
	}

	event := logger.Info().
		Str("edition", edition).
		Str("environment", c.Environment).
		Int("port", c.Port).
		Str("auth_mode", c.AuthMode).
		Dur("read_timeout", c.ReadTimeout).
		Dur("write_timeout", c.WriteTimeout).
		Dur("idle_timeout", c.IdleTimeout).
		Str("backend_url", c.BackendURL).
		Dur("dial_timeout", c.DialTimeout).
		Int("max_frame_size", c.MaxFrameSize).
		Dur("auth_refresh_rate_interval", c.AuthRefreshRateInterval).
		Str("log_level", c.LogLevel).
		Str("log_format", c.LogFormat)

	// Add routing fields when auth is disabled
	// Auth fields (always on). Channel authorization is provisioning-only:
	// per-tenant rules stream over the same gRPC connection.
	event = event.
		Bool("require_tenant_id", c.RequireTenantID).
		Str("provisioning_grpc_addr", c.ProvisioningGRPCAddr).
		Dur("grpc_reconnect_delay", c.GRPCReconnectDelay).
		Dur("grpc_reconnect_max_delay", c.GRPCReconnectMaxDelay)

	event.Msg("Gateway configuration loaded")
}
