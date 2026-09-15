package platform

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

// minTopicRetentionMs is the minimum allowed Kafka topic retention in milliseconds (1 minute).
// Applied to both DEFAULT_RETENTION_MS and ROUTING_DLQ_TOPIC_RETENTION_MS.
const minTopicRetentionMs int64 = 60000

// MinSlugRenameHoldPeriod is the minimum allowed hold period after a tenant slug rename.
// During this period the old slug is still accepted by the JWT grace-period check.
// Exported from the platform package so the provisioning service can reference it without
// a cross-package import inversion.
const MinSlugRenameHoldPeriod = 24 * time.Hour

// maxInfraTopicReplicationFactor is the maximum allowed replication factor for infrastructure topics.
// int16 max is 32767; values >= 32768 would overflow silently when cast to int16 at CreateTopic call sites.
const maxInfraTopicReplicationFactor = 32767

// adminUIMinTokenLen is the minimum required ADMIN_TOKEN length when the Admin UI is enabled.
// Intentionally duplicated (not imported from adminui) to avoid a platform→adminui import cycle.
const adminUIMinTokenLen = 32

// adminMinSessionTTL and adminMaxSessionTTL bound ADMIN_SESSION_TTL validation.
const (
	adminMinSessionTTL = time.Minute
	adminMaxSessionTTL = 30 * 24 * time.Hour
)

// ProvisioningConfig holds all provisioning service configuration.
// Tags:
//
//	env: Environment variable name
//	envDefault: Default value if not set
//	required: Must be provided (no default)
type ProvisioningConfig struct {
	BaseConfig
	AuthConfig
	DatabaseConfig
	KafkaNamespaceConfig
	HTTPTimeoutConfig
	AnalyticsConfig
	PodIdentityConfig

	// Analytics — provisioning-only fields
	PartmanInterval time.Duration `env:"ANALYTICS_PARTMAN_INTERVAL" envDefault:"15m"` // Interval for running pg_partman maintenance on analytics partition tables.
	SSEMaxConns     int           `env:"ANALYTICS_SSE_MAX_CONNS"    envDefault:"10"`  // Maximum concurrent SSE connections for the analytics streaming endpoint.

	// Kafka brokers — used by the provisioning service for topic management (create/delete).
	// Uses a provisioning-specific env var to avoid collision with the ws-server's KAFKA_BROKERS
	// (defined in MessageBackendConfig). In-cluster default: <release>-redpanda:9092.
	KafkaBrokers string `env:"PROVISIONING_KAFKA_BROKERS" envDefault:"localhost:19092"` // Kafka broker addresses for provisioning-originated admin operations (topic creation, etc.). Comma-separated.

	// Server
	Addr string `env:"PROVISIONING_ADDR" envDefault:":8080"` // HTTP listen address for the provisioning API.

	// gRPC — internal service-to-service communication port
	GRPCPort int `env:"GRPC_PORT" envDefault:"9090"` // gRPC server port for internal service-to-service communication.

	// Admin Authentication — bootstrap key for first admin registration (base64-encoded Ed25519 public key)
	AdminBootstrapKey string `env:"ADMIN_BOOTSTRAP_KEY"` // Bootstrap admin API key for provisioning service. Required; used for initial tenant setup.

	// Topic Defaults
	DefaultPartitions  int   `env:"DEFAULT_PARTITIONS" envDefault:"3"`           // Default Kafka partition count assigned to new tenant topics.
	DefaultRetentionMs int64 `env:"DEFAULT_RETENTION_MS" envDefault:"604800000"` // Default Kafka topic retention in milliseconds. Default: 604,800,000ms (7 days).

	// Quotas (defaults per tenant)
	MaxTopicsPerTenant     int   `env:"MAX_TOPICS_PER_TENANT" envDefault:"50"`      // Default maximum number of Kafka topics per tenant.
	MaxPartitionsPerTenant int   `env:"MAX_PARTITIONS_PER_TENANT" envDefault:"200"` // Default maximum total partitions across all topics per tenant.
	MaxStorageBytes        int64 `env:"MAX_STORAGE_BYTES" envDefault:"10737418240"` // Default maximum Kafka storage quota per tenant in bytes. Default: 10,737,418,240 (10GB).
	ProducerByteRate       int64 `env:"PRODUCER_BYTE_RATE" envDefault:"10485760"`   // Default Kafka producer byte-rate quota per tenant in bytes/sec. Default: 10485760 (10MB/s).
	ConsumerByteRate       int64 `env:"CONSUMER_BYTE_RATE" envDefault:"52428800"`   // Default Kafka consumer byte-rate quota per tenant in bytes/sec. Default: 52428800 (50MB/s).

	// Tenant Lifecycle
	DeprovisionGraceDays    int           `env:"DEPROVISION_GRACE_DAYS" envDefault:"30"`      // Days to retain a deprovisioned tenant's Kafka topics and ACLs before permanent deletion.
	LifecycleCheckInterval  time.Duration `env:"LIFECYCLE_CHECK_INTERVAL" envDefault:"1h"`    // How often the lifecycle manager scans for tenants due for deprovisioning or cleanup.
	LifecycleManagerEnabled bool          `env:"LIFECYCLE_MANAGER_ENABLED" envDefault:"true"` // Enables the background tenant lifecycle manager (deprovisioning, retention enforcement).

	// Routing Rules
	MaxRoutingRulesPerTenant    int   `env:"MAX_ROUTING_RULES_PER_TENANT" envDefault:"100"`         // Max routing rules per tenant
	MaxTopicsPerRule            int   `env:"MAX_TOPICS_PER_RULE" envDefault:"10"`                   // Max topics per routing rule
	DeadLetterTopicPartitions   int   `env:"ROUTING_DLQ_TOPIC_PARTITIONS" envDefault:"1"`           // Partitions for dead-letter topics
	DeadLetterTopicRetentionMs  int64 `env:"ROUTING_DLQ_TOPIC_RETENTION_MS" envDefault:"604800000"` // Retention for dead-letter topics (7 days)
	InfraTopicReplicationFactor int   `env:"INFRA_TOPIC_REPLICATION_FACTOR" envDefault:"1"`         // Replication factor for all infrastructure topics (DLQ + default); use int — caarlos0/env v11 does not handle int16; cast to int16 at CreateTopic call sites

	// Rate Limiting
	APIRateLimitPerMinute int `env:"API_RATE_LIMIT_PER_MIN" envDefault:"60"` // Maximum provisioning API requests per minute per IP before HTTP 429 is returned.

	// Key Registry (for JWT validation in API mode with auth enabled)
	KeyRegistryRefreshInterval time.Duration `env:"KEY_REGISTRY_REFRESH_INTERVAL" envDefault:"1m"` // How often the key registry polls for updated tenant JWT signing keys.
	KeyRegistryQueryTimeout    time.Duration `env:"KEY_REGISTRY_QUERY_TIMEOUT" envDefault:"5s"`    // Timeout for each key registry lookup query.

	// Graceful shutdown timeout
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"30s"` // Maximum time to wait for in-flight requests during graceful shutdown.

	// CORS settings
	CORSAllowedOrigins []string `env:"CORS_ALLOWED_ORIGINS" envSeparator:"," envDefault:"http://localhost:3000"` // Allowed origins for CORS preflight responses. Comma-separated; no wildcard in production.
	CORSMaxAge         int      `env:"CORS_MAX_AGE" envDefault:"3600"`                                           // Maximum seconds a CORS preflight response may be cached by the browser.

	// CredentialsConfig embeds the shared AES-256 encryption key.
	// Constitution §I: shared fields defined once in a shared struct, never duplicated.
	CredentialsConfig

	// AdminUIConfig embeds Admin UI session-cookie configuration.
	// All fields are promoted: use c.AdminUIEnabled, c.AdminToken, etc.
	AdminUIConfig

	// Provisioning-specific externalized constants
	MaxTenantsFetchLimit int           `env:"PROVISIONING_MAX_TENANTS_FETCH_LIMIT" envDefault:"10000"` // Maximum number of tenants returned by internal bulk-fetch operations.
	DeletionTimeout      time.Duration `env:"PROVISIONING_DELETION_TIMEOUT" envDefault:"5m"`           // Timeout for a single tenant deletion operation (Kafka topic removal, ACL cleanup).

	// SlugRenameTopicHoldPeriod is how long old Kafka topics and ACLs are retained after a slug rename.
	// During this window the old slug is still accepted by the JWT grace-period check.
	// Must be >= MinSlugRenameHoldPeriod (24h). Default: 168h (7 days).
	SlugRenameTopicHoldPeriod time.Duration `env:"SLUG_RENAME_TOPIC_HOLD_PERIOD" envDefault:"168h"` // How long a tenant's previous slug is honored after a rename: retention window for old Kafka topics/ACLs, and the grace window during which old-slug JWTs are still accepted on both the provisioning API and the WebSocket gateway. Minimum 24h.

	// TokenRevocationMaxLifetime is the maximum duration a revoked token entry is retained
	// before the background prune loop removes it. Must be > 0. Default: 1h.
	TokenRevocationMaxLifetime time.Duration `env:"PROVISIONING_TOKEN_REVOCATION_MAX_LIFETIME" envDefault:"1h"` // Maximum duration to retain a revoked token record before the prune loop removes it.

	// TokenRevocationRateLimitPerMinute is the per-IP sustained rate for the token-revocation
	// endpoint (POST /tokens/revoke), independent of API_RATE_LIMIT_PER_MIN. Must be > 0.
	// Default 30 preserves the historical rate.Every(2s). Raise for high-volume single-IP callers.
	TokenRevocationRateLimitPerMinute int `env:"PROVISIONING_TOKEN_REVOCATION_RATE_LIMIT_PER_MIN" envDefault:"30"` // Maximum token-revocation requests per minute per IP before HTTP 429; independent of API_RATE_LIMIT_PER_MIN.

	// TokenRevocationRateLimitBurst is the per-IP burst for the token-revocation endpoint. Must be > 0.
	// Default 5 preserves the historical burst.
	TokenRevocationRateLimitBurst int `env:"PROVISIONING_TOKEN_REVOCATION_RATE_LIMIT_BURST" envDefault:"5"` // Burst allowance for token-revocation requests per IP before HTTP 429 is returned.

	// ValkeyConfig is the dedicated Valkey client config for the connections registry reader.
	// The envPrefix:"PROVISIONING_" tag makes effective env var names PROVISIONING_VALKEY_ADDRS, etc.
	// PROVISIONING_VALKEY_ADDRS has no envDefault — required runtime value (K8s service discovery);
	// Helm injects it unconditionally (not a §I violation: Helm is not duplicating a Go default).
	ValkeyConfig ValkeyClientConfig `envPrefix:"PROVISIONING_"`

	// BulkDisconnectConcurrency controls the maximum number of concurrent pub/sub publishes
	// during a bulk DELETE /connections request.
	BulkDisconnectConcurrency int `env:"PROVISIONING_BULK_DISCONNECT_CONCURRENCY" envDefault:"10"` // Maximum concurrent Valkey publishes during a bulk connection disconnect request.

	// Webhook delivery (Pro edition)
	WebhookInternalTokenConfig                 // embeds WEBHOOK_INTERNAL_TOKEN (redact:"true"); Validate() called inside EditionHasFeature(Webhooks) guard
	WebhookHTTPConfig                          // embeds WEBHOOK_ALLOW_HTTP (§X: eliminates duplicate field from WebhookWorkerConfig)
	MaxWebhooksPerTenant         int           `env:"MAX_WEBHOOKS_PER_TENANT"         envDefault:"10"` // Maximum number of webhook endpoint registrations per tenant.
	WebhookDowngradePollInterval time.Duration `env:"WEBHOOK_DOWNGRADE_POLL_INTERVAL" envDefault:"5m"` // Polling interval for checking whether a tenant's webhook delivery should pause due to edition downgrade.
	// WebhookWorkerGRPCPort is the dedicated port for the webhook-worker gRPC service.
	// Separate from GRPC_PORT (ProvisioningInternalService) so auth is enforced at the
	// listener level rather than via FullMethod filtering inside a shared interceptor.
	// Direction: webhook-worker → provisioning (WebhookWorkerService).
	WebhookWorkerGRPCPort int `env:"WEBHOOK_WORKER_GRPC_PORT" envDefault:"9091"` // gRPC listen port for the webhook-worker internal service. Must differ from GRPC_PORT.
	// WebhookWorkerGRPCAddr is the address provisioning dials to call the worker's
	// WebhookWorkerInternalService.TestDeliver RPC (reverse direction: provisioning → worker).
	// No envDefault — required only when EditionHasFeature(Webhooks); validated conditionally.
	WebhookWorkerGRPCAddr string `env:"WEBHOOK_WORKER_GRPC_ADDR"` // gRPC address provisioning dials to reach the webhook-worker service. Required for Pro/Enterprise editions.

	// editionManager holds the license-resolved edition and limits.
	// Set by LoadProvisioningConfig() before Validate(). Not an env var — derived from SUKKO_LICENSE_KEY.
	editionManager *license.Manager
}

// EditionManager returns the license manager for this config.
func (c *ProvisioningConfig) EditionManager() *license.Manager {
	return c.editionManager
}

// LoadProvisioningConfig reads provisioning service configuration from .env file
// and environment variables.
// Priority: ENV vars > .env file > defaults
func LoadProvisioningConfig(logger zerolog.Logger) (*ProvisioningConfig, error) {
	// Load .env file (optional)
	if err := godotenv.Load(); err != nil {
		logger.Info().Msg("No .env file found (using environment variables only)")
	} else {
		logger.Info().Msg("Loaded configuration from .env file")
	}

	cfg := &ProvisioningConfig{}

	// Parse environment variables into struct
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Create license manager before validation — Validate() uses edition gates.
	mgr, err := license.NewManager(cfg.LicenseKey, logger)
	if err != nil {
		return nil, fmt.Errorf("license: %w", err)
	}
	cfg.editionManager = mgr

	// Canonicalize the topic namespace (trim + lowercase) before Validate() so the allowlist
	// check and BuildTopicName reads agree.
	cfg.Normalize()

	// Validation (now edition-aware)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	logger.Info().Msg("Configuration loaded and validated successfully")

	return cfg, nil
}

// Validate checks provisioning configuration for errors.
func (c *ProvisioningConfig) Validate() error {
	// Auth mode validation
	if err := c.AuthConfig.Validate(); err != nil {
		return err
	}
	if err := c.AnalyticsConfig.Validate(); err != nil {
		return err
	}
	if c.Enabled {
		if c.PartmanInterval < analyticsPartmanIntervalFloor || c.PartmanInterval > analyticsPartmanIntervalCeiling {
			return fmt.Errorf("ANALYTICS_PARTMAN_INTERVAL must be %s–%s, got %s",
				analyticsPartmanIntervalFloor, analyticsPartmanIntervalCeiling, c.PartmanInterval)
		}
		if c.SSEMaxConns < analyticsSSEMaxConnsMin || c.SSEMaxConns > analyticsSSEMaxConnsMax {
			return fmt.Errorf("ANALYTICS_SSE_MAX_CONNS must be %d–%d, got %d",
				analyticsSSEMaxConnsMin, analyticsSSEMaxConnsMax, c.SSEMaxConns)
		}
	}

	// Required fields
	if c.Addr == "" {
		return errors.New("PROVISIONING_ADDR is required")
	}

	// Database — PostgreSQL is the only supported backend.
	// Pool tuning (max conns, idle conns, lifetime) is configured via pgxpool URL params:
	//   ?pool_max_conns=25&pool_min_conns=5&pool_max_conn_lifetime=5m
	if err := c.DatabaseConfig.Validate(); err != nil {
		return err
	}

	// gRPC port validation
	if c.GRPCPort < 1 || c.GRPCPort > MaxPort {
		return fmt.Errorf("GRPC_PORT must be between 1 and %d, got %d", MaxPort, c.GRPCPort)
	}

	// Credentials encryption key — required for license persistence and push credentials
	if err := c.CredentialsConfig.Validate(); err != nil {
		return err
	}

	// Admin bootstrap key validation (if set, must be valid base64 → 32 bytes for Ed25519)
	// Accept both standard (padded) and raw (unpadded) base64 for operator convenience.
	if c.AdminBootstrapKey != "" {
		decoded, err := base64.StdEncoding.DecodeString(c.AdminBootstrapKey)
		if err != nil {
			// Retry with raw (no padding) — some base64 tools omit padding
			decoded, err = base64.RawStdEncoding.DecodeString(c.AdminBootstrapKey)
			if err != nil {
				return fmt.Errorf("ADMIN_BOOTSTRAP_KEY must be valid base64: %w", err)
			}
		}
		if len(decoded) != 32 {
			return fmt.Errorf("ADMIN_BOOTSTRAP_KEY must decode to 32 bytes (Ed25519 public key), got %d", len(decoded))
		}
	}

	// Range checks
	if c.DefaultPartitions < 1 || c.DefaultPartitions > 100 {
		return fmt.Errorf("DEFAULT_PARTITIONS must be 1-100, got %d", c.DefaultPartitions)
	}
	if c.MaxTopicsPerTenant < 1 {
		return fmt.Errorf("MAX_TOPICS_PER_TENANT must be > 0, got %d", c.MaxTopicsPerTenant)
	}
	if c.MaxPartitionsPerTenant < 1 {
		return fmt.Errorf("MAX_PARTITIONS_PER_TENANT must be > 0, got %d", c.MaxPartitionsPerTenant)
	}
	if c.DeprovisionGraceDays < 0 {
		return fmt.Errorf("DEPROVISION_GRACE_DAYS must be >= 0, got %d", c.DeprovisionGraceDays)
	}
	if c.MaxRoutingRulesPerTenant < 1 {
		return fmt.Errorf("MAX_ROUTING_RULES_PER_TENANT must be > 0, got %d", c.MaxRoutingRulesPerTenant)
	}
	if c.MaxTopicsPerRule < 1 {
		return fmt.Errorf("MAX_TOPICS_PER_RULE must be > 0, got %d", c.MaxTopicsPerRule)
	}
	if c.DeadLetterTopicPartitions < 1 {
		return fmt.Errorf("ROUTING_DLQ_TOPIC_PARTITIONS must be > 0, got %d", c.DeadLetterTopicPartitions)
	}
	if c.DeadLetterTopicRetentionMs < minTopicRetentionMs {
		return fmt.Errorf("ROUTING_DLQ_TOPIC_RETENTION_MS must be >= %d (1 minute), got %d", minTopicRetentionMs, c.DeadLetterTopicRetentionMs)
	}
	if c.InfraTopicReplicationFactor < 1 || c.InfraTopicReplicationFactor > maxInfraTopicReplicationFactor {
		return fmt.Errorf("INFRA_TOPIC_REPLICATION_FACTOR must be 1–%d, got %d", maxInfraTopicReplicationFactor, c.InfraTopicReplicationFactor)
	}
	if c.APIRateLimitPerMinute < 1 {
		return fmt.Errorf("API_RATE_LIMIT_PER_MIN must be > 0, got %d", c.APIRateLimitPerMinute)
	}
	if c.TokenRevocationRateLimitPerMinute < 1 {
		return fmt.Errorf("PROVISIONING_TOKEN_REVOCATION_RATE_LIMIT_PER_MIN must be > 0, got %d", c.TokenRevocationRateLimitPerMinute)
	}
	if c.TokenRevocationRateLimitBurst < 1 {
		return fmt.Errorf("PROVISIONING_TOKEN_REVOCATION_RATE_LIMIT_BURST must be > 0, got %d", c.TokenRevocationRateLimitBurst)
	}

	// Shared field validation (LogLevel, LogFormat, Environment)
	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}

	// Topic namespace validation (allowlist boundary)
	if err := c.KafkaNamespaceConfig.Validate(); err != nil {
		return err
	}
	// Provisioning always builds namespaced topic-name strings (BuildTopicName) for API/ACL output,
	// so the namespace is unconditionally required — there is no direct mode here.
	if c.KafkaTopicNamespace == "" {
		return errors.New("KAFKA_TOPIC_NAMESPACE is required")
	}

	// Validate CORS settings
	if c.CORSMaxAge < 0 {
		return fmt.Errorf("CORS_MAX_AGE must be >= 0, got %d", c.CORSMaxAge)
	}

	// Topic retention
	if c.DefaultRetentionMs < minTopicRetentionMs {
		return fmt.Errorf("DEFAULT_RETENTION_MS must be >= %d (1 minute), got %d", minTopicRetentionMs, c.DefaultRetentionMs)
	}

	// Quota minimums
	if c.MaxStorageBytes < MinStorageBytes {
		return fmt.Errorf("MAX_STORAGE_BYTES must be >= %d (1MB), got %d", MinStorageBytes, c.MaxStorageBytes)
	}
	if c.ProducerByteRate < MinByteRate {
		return fmt.Errorf("PRODUCER_BYTE_RATE must be >= %d, got %d", MinByteRate, c.ProducerByteRate)
	}
	if c.ConsumerByteRate < MinByteRate {
		return fmt.Errorf("CONSUMER_BYTE_RATE must be >= %d, got %d", MinByteRate, c.ConsumerByteRate)
	}

	// HTTP timeouts
	if err := c.HTTPTimeoutConfig.Validate(); err != nil {
		return err
	}

	// Key registry
	if c.KeyRegistryRefreshInterval < time.Second {
		return fmt.Errorf("KEY_REGISTRY_REFRESH_INTERVAL must be >= 1s, got %s", c.KeyRegistryRefreshInterval)
	}
	if c.KeyRegistryQueryTimeout < time.Second {
		return fmt.Errorf("KEY_REGISTRY_QUERY_TIMEOUT must be >= 1s, got %s", c.KeyRegistryQueryTimeout)
	}

	// Graceful shutdown
	if c.ShutdownTimeout < time.Second {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be >= 1s, got %s", c.ShutdownTimeout)
	}

	// Lifecycle check interval (only when lifecycle manager is enabled)
	if c.LifecycleManagerEnabled && c.LifecycleCheckInterval < time.Minute {
		return fmt.Errorf("LIFECYCLE_CHECK_INTERVAL must be >= 1m when LIFECYCLE_MANAGER_ENABLED=true, got %s", c.LifecycleCheckInterval)
	}

	// Provisioning-specific externalized fields
	if c.MaxTenantsFetchLimit < 1 {
		return fmt.Errorf("PROVISIONING_MAX_TENANTS_FETCH_LIMIT must be > 0, got %d", c.MaxTenantsFetchLimit)
	}
	if c.DeletionTimeout <= 0 {
		return fmt.Errorf("PROVISIONING_DELETION_TIMEOUT must be > 0, got %v", c.DeletionTimeout)
	}

	// Slug rename hold period — must be >= 24h to give old JWTs time to expire.
	if c.SlugRenameTopicHoldPeriod < MinSlugRenameHoldPeriod {
		return fmt.Errorf("SLUG_RENAME_TOPIC_HOLD_PERIOD must be >= %s, got %s", MinSlugRenameHoldPeriod, c.SlugRenameTopicHoldPeriod)
	}

	// Token revocation max lifetime — must be positive to avoid infinite retention or no-op prune.
	if c.TokenRevocationMaxLifetime <= 0 {
		return fmt.Errorf("PROVISIONING_TOKEN_REVOCATION_MAX_LIFETIME must be > 0, got %v", c.TokenRevocationMaxLifetime)
	}

	// Connections registry Valkey client — required only when ConnectionsAPI is licensed (Pro+).
	// PROVISIONING_VALKEY_ADDRS has no Go default; it must be injected by Helm for licensed editions.
	// When editionManager is nil (e.g. test contexts), default to requiring the address.
	if c.editionManager == nil || license.EditionHasFeature(c.editionManager.Edition(), license.ConnectionsAPI) {
		if !slices.ContainsFunc(c.ValkeyConfig.Addrs, func(s string) bool { return strings.TrimSpace(s) != "" }) {
			return errors.New("PROVISIONING_VALKEY_ADDRS is required and must contain at least one non-empty address")
		}
	}

	// Bulk disconnect concurrency bounds.
	if c.BulkDisconnectConcurrency < MinBulkDisconnectConcurrency || c.BulkDisconnectConcurrency > MaxBulkDisconnectConcurrency {
		return fmt.Errorf("PROVISIONING_BULK_DISCONNECT_CONCURRENCY must be %d-%d, got %d", MinBulkDisconnectConcurrency, MaxBulkDisconnectConcurrency, c.BulkDisconnectConcurrency)
	}

	// Webhook fields — basic bounds validated unconditionally.
	if c.WebhookWorkerGRPCPort < 1 || c.WebhookWorkerGRPCPort > MaxPort {
		return fmt.Errorf("WEBHOOK_WORKER_GRPC_PORT must be between 1 and %d, got %d", MaxPort, c.WebhookWorkerGRPCPort)
	}
	if c.WebhookWorkerGRPCPort == c.GRPCPort {
		return fmt.Errorf("WEBHOOK_WORKER_GRPC_PORT (%d) must differ from GRPC_PORT (%d)", c.WebhookWorkerGRPCPort, c.GRPCPort)
	}
	if _, portStr, err := net.SplitHostPort(c.Addr); err == nil {
		if httpPort, err := strconv.Atoi(portStr); err == nil && httpPort > 0 {
			if c.GRPCPort == httpPort {
				return fmt.Errorf("GRPC_PORT (%d) must differ from HTTP port in PROVISIONING_ADDR (%s)", c.GRPCPort, c.Addr)
			}
			if c.WebhookWorkerGRPCPort == httpPort {
				return fmt.Errorf("WEBHOOK_WORKER_GRPC_PORT (%d) must differ from HTTP port in PROVISIONING_ADDR (%s)", c.WebhookWorkerGRPCPort, c.Addr)
			}
		}
	}
	if c.MaxWebhooksPerTenant < 1 || c.MaxWebhooksPerTenant > 100 {
		return fmt.Errorf("MAX_WEBHOOKS_PER_TENANT must be between 1 and 100, got %d", c.MaxWebhooksPerTenant)
	}
	// WebhookInternalToken, WebhookWorkerGRPCAddr, and WebhookDowngradePollInterval are only
	// required for Pro/Enterprise. Community deployments have no webhook-worker.
	// Inverted from the Valkey pattern (nil||hasFeature): gate keeps Community/test configs
	// from being forced to set webhook fields they never use.
	if c.editionManager != nil && license.EditionHasFeature(c.editionManager.Edition(), license.Webhooks) {
		if err := c.WebhookInternalTokenConfig.Validate(); err != nil {
			return fmt.Errorf("Pro/Enterprise edition: %w", err)
		}
		if c.WebhookWorkerGRPCAddr == "" {
			return errors.New("WEBHOOK_WORKER_GRPC_ADDR is required for Pro/Enterprise editions")
		}
		if c.WebhookDowngradePollInterval < time.Minute || c.WebhookDowngradePollInterval > time.Hour {
			return fmt.Errorf("WEBHOOK_DOWNGRADE_POLL_INTERVAL must be between 1m and 1h, got %s", c.WebhookDowngradePollInterval)
		}
	}

	// Admin UI — validated even when disabled=false; all constraints are opt-in.
	// NOTE: EditionHasFeature(AdminUI) is intentionally NOT checked at startup — Community
	// operators who enable the UI receive a runtime upgrade notice via AdminUIGate middleware.
	// Failing at startup would prevent the upgrade page from rendering.
	if c.AdminUIEnabled {
		if c.AdminOIDCIssuer != "" {
			return errors.New("ADMIN_OIDC_ISSUER is reserved: OIDC SSO is not yet implemented (see feat/admin-ui-oidc)")
		}
		if len(c.AdminToken) < adminUIMinTokenLen {
			return fmt.Errorf("ADMIN_TOKEN must be at least %d characters when ADMIN_UI_ENABLED=true", adminUIMinTokenLen)
		}
		if c.AdminSessionTTL < adminMinSessionTTL || c.AdminSessionTTL > adminMaxSessionTTL {
			return fmt.Errorf("ADMIN_SESSION_TTL must be between %v and %v, got %v", adminMinSessionTTL, adminMaxSessionTTL, c.AdminSessionTTL)
		}
	}

	return nil
}

// Print logs provisioning configuration for debugging (human-readable format).
// Uses fmt.Fprint* to os.Stdout for startup display before zerolog is initialized.
// fmt.Fprint* errors are non-actionable: writing to os.Stdout cannot be retried or reported.
func (c *ProvisioningConfig) Print() {
	w := os.Stdout
	edition := "community"
	if c.editionManager != nil {
		edition = c.editionManager.Edition().String()
	}

	_, _ = fmt.Fprintln(w, "=== Provisioning Service Configuration ===")
	_, _ = fmt.Fprintf(w, "Edition:            %s\n", edition)
	_, _ = fmt.Fprintf(w, "Environment:        %s\n", c.Environment)
	_, _ = fmt.Fprintf(w, "Address:            %s\n", c.Addr)
	_, _ = fmt.Fprintf(w, "gRPC Port:          %d\n", c.GRPCPort)
	if c.AdminBootstrapKey != "" {
		_, _ = fmt.Fprintf(w, "Bootstrap Key:      [SET]\n")
	}
	_, _ = fmt.Fprintln(w, "\n=== Database ===")
	_, _ = fmt.Fprintf(w, "Database URL:       %s\n", maskDatabaseURL(c.DatabaseURL))
	_, _ = fmt.Fprintln(w, "\n=== Topic Defaults ===")
	if c.KafkaTopicNamespace != "" {
		_, _ = fmt.Fprintf(w, "Topic Namespace:    %s\n", c.KafkaTopicNamespace)
	}
	_, _ = fmt.Fprintf(w, "Partitions:         %d\n", c.DefaultPartitions)
	_, _ = fmt.Fprintf(w, "Retention:          %d ms (%d days)\n", c.DefaultRetentionMs, c.DefaultRetentionMs/86400000)
	_, _ = fmt.Fprintln(w, "\n=== Routing Rules ===")
	_, _ = fmt.Fprintf(w, "Max Rules/Tenant:   %d\n", c.MaxRoutingRulesPerTenant)
	_, _ = fmt.Fprintf(w, "Max Topics/Rule:    %d\n", c.MaxTopicsPerRule)
	_, _ = fmt.Fprintf(w, "DLQ Partitions:     %d\n", c.DeadLetterTopicPartitions)
	_, _ = fmt.Fprintf(w, "DLQ Retention:      %d ms (%d days)\n", c.DeadLetterTopicRetentionMs, c.DeadLetterTopicRetentionMs/86400000)
	_, _ = fmt.Fprintf(w, "Infra Replication:  %d\n", c.InfraTopicReplicationFactor)
	_, _ = fmt.Fprintln(w, "\n=== Tenant Quotas (Defaults) ===")
	_, _ = fmt.Fprintf(w, "Max Topics:         %d\n", c.MaxTopicsPerTenant)
	_, _ = fmt.Fprintf(w, "Max Partitions:     %d\n", c.MaxPartitionsPerTenant)
	_, _ = fmt.Fprintf(w, "Max Storage:        %d MB\n", c.MaxStorageBytes/(1024*1024))
	_, _ = fmt.Fprintf(w, "Producer Rate:      %d MB/s\n", c.ProducerByteRate/(1024*1024))
	_, _ = fmt.Fprintf(w, "Consumer Rate:      %d MB/s\n", c.ConsumerByteRate/(1024*1024))
	_, _ = fmt.Fprintln(w, "\n=== Tenant Lifecycle ===")
	_, _ = fmt.Fprintf(w, "Grace Period:       %d days\n", c.DeprovisionGraceDays)
	_, _ = fmt.Fprintf(w, "Slug Rename Hold:   %s\n", c.SlugRenameTopicHoldPeriod)
	_, _ = fmt.Fprintf(w, "Token Revoc TTL:    %s\n", c.TokenRevocationMaxLifetime)
	_, _ = fmt.Fprintln(w, "\n=== Rate Limiting ===")
	_, _ = fmt.Fprintf(w, "API Rate Limit:     %d req/min\n", c.APIRateLimitPerMinute)
	_, _ = fmt.Fprintf(w, "Revoke Rate Limit:  %d req/min (burst %d)\n", c.TokenRevocationRateLimitPerMinute, c.TokenRevocationRateLimitBurst)
	_, _ = fmt.Fprintln(w, "\n=== HTTP Server ===")
	_, _ = fmt.Fprintf(w, "Read Timeout:       %s\n", c.HTTPReadTimeout)
	_, _ = fmt.Fprintf(w, "Write Timeout:      %s\n", c.HTTPWriteTimeout)
	_, _ = fmt.Fprintf(w, "Idle Timeout:       %s\n", c.HTTPIdleTimeout)
	_, _ = fmt.Fprintln(w, "\n=== Logging ===")
	_, _ = fmt.Fprintf(w, "Level:              %s\n", c.LogLevel)
	_, _ = fmt.Fprintf(w, "Format:             %s\n", c.LogFormat)
	_, _ = fmt.Fprintln(w, "==========================================")
}

// LogConfig logs provisioning configuration using structured logging.
func (c *ProvisioningConfig) LogConfig(logger zerolog.Logger) {
	edition := "community"
	if c.editionManager != nil {
		edition = c.editionManager.Edition().String()
	}

	event := logger.Info().
		Str("edition", edition).
		Str("environment", c.Environment).
		Str("addr", c.Addr).
		Int("grpc_port", c.GRPCPort).
		Str("kafka_topic_namespace", c.KafkaTopicNamespace).
		Int("default_partitions", c.DefaultPartitions).
		Int64("default_retention_ms", c.DefaultRetentionMs).
		Int("max_routing_rules_per_tenant", c.MaxRoutingRulesPerTenant).
		Int("max_topics_per_rule", c.MaxTopicsPerRule).
		Int("dlq_topic_partitions", c.DeadLetterTopicPartitions).
		Int64("dlq_topic_retention_ms", c.DeadLetterTopicRetentionMs).
		Int("infra_topic_replication_factor", c.InfraTopicReplicationFactor).
		Int("max_topics_per_tenant", c.MaxTopicsPerTenant).
		Int("max_partitions_per_tenant", c.MaxPartitionsPerTenant).
		Int("deprovision_grace_days", c.DeprovisionGraceDays).
		Dur("slug_rename_topic_hold_period", c.SlugRenameTopicHoldPeriod).
		Dur("token_revocation_max_lifetime", c.TokenRevocationMaxLifetime).
		Int("api_rate_limit_per_min", c.APIRateLimitPerMinute).
		Int("token_revocation_rate_limit_per_min", c.TokenRevocationRateLimitPerMinute).
		Int("token_revocation_rate_limit_burst", c.TokenRevocationRateLimitBurst).
		Dur("http_read_timeout", c.HTTPReadTimeout).
		Dur("http_write_timeout", c.HTTPWriteTimeout).
		Dur("http_idle_timeout", c.HTTPIdleTimeout).
		Str("auth_mode", c.AuthMode).
		Strs("cors_allowed_origins", c.CORSAllowedOrigins).
		Int("cors_max_age", c.CORSMaxAge).
		Str("log_level", c.LogLevel).
		Str("log_format", c.LogFormat)

	// Bootstrap key — log presence only
	if c.AdminBootstrapKey != "" {
		event = event.Bool("admin_bootstrap_key_set", true)
	}

	event.Msg("Provisioning service configuration loaded")
}

// ParsedValidNamespaces returns the ValidNamespaces string as a set.
func (c *ProvisioningConfig) ParsedValidNamespaces() map[string]bool {
	return parseNamespaces(c.ValidNamespaces)
}

// parseNamespaces converts a comma-separated namespace string into a set.
func parseNamespaces(raw string) map[string]bool {
	ns := map[string]bool{}
	for s := range strings.SplitSeq(raw, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			ns[s] = true
		}
	}
	return ns
}

// maskDatabaseURL masks the password in a database URL for logging.
func maskDatabaseURL(url string) string {
	// Simple masking - just indicate it's set
	if url == "" {
		return "(not set)"
	}
	return "(set, password masked)"
}
