package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	testerapi "github.com/sukko-dev/sukko/cmd/tester/api"
	testerauth "github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/runner"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

const (
	maxWebhookDeliveryTimeout = 5 * time.Minute
	maxWebhookRetryTimeout    = 30 * time.Minute
)

// TesterConfig holds configuration for the sukko-tester service.
type TesterConfig struct {
	platform.BaseConfig
	platform.KafkaConnectionConfig        // Kafka brokers + SASL/TLS for the kafka-ingest suite's direct-to-Kafka publisher (no MESSAGE_BACKEND — the tester has no backend selector).
	platform.KafkaNamespaceConfig         // Explicit KAFKA_TOPIC_NAMESPACE, MUST match the server-under-test.
	Port                           int    `env:"TESTER_PORT" envDefault:"8090"`                       // HTTP port the tester service listens on for test run management and SSE metric streaming.
	AuthToken                      string `env:"TESTER_AUTH_TOKEN"`                                   // Bearer token required for all tester management API endpoints. Must be set explicitly — the tester rejects requests when unset.
	GatewayURL                     string `env:"GATEWAY_URL" envDefault:"ws://localhost:3000"`        // WebSocket URL of the gateway that test connections will target.
	ProvisioningURL                string `env:"PROVISIONING_URL" envDefault:"http://localhost:8080"` // HTTP base URL of the provisioning service used to create test tenants and API keys.

	// License reload suite — PEM PKCS#8 ECDSA P-256 private key for signing test license keys
	SigningKeyFile string `env:"TESTER_SIGNING_KEY_FILE" envDefault:""` // Path to PEM PKCS#8 ECDSA P-256 private key file for signing license tokens in the license-reload test suite. Leave empty when not running license tests.

	// Admin keypair for remote mode (required when targeting a deployed provisioning service).
	// When unset, the tester generates an ephemeral keypair per test run (local dev mode only).
	AdminKeyFile string `env:"TESTER_ADMIN_KEY_FILE" envDefault:""` // Path to Ed25519 private key file for admin JWT signing. When unset, an ephemeral keypair is generated per test run (local dev only).
	// AdminKeyID is the kid embedded in admin JWTs; must match BootstrapAdminKeyID unless a
	// custom key was registered with provisioning under a different ID.
	// envDefault must stay in sync with provauth.BootstrapAdminKeyID; enforced by TestTesterConfig_DefaultAdminKeyID_EqualsBootstrapConstant.
	AdminKeyID string `env:"TESTER_ADMIN_KEY_ID" envDefault:"bootstrap-0"` // Key ID embedded in admin JWTs; must match the kid registered with the provisioning service.

	// Push delivery verification (validate push suite). When the host is unset
	// the delivery check skips — set it (e.g. to the tester's compose service
	// name) so the push service can deliver to the tester's mock receiver.
	PushReceiverHost string `env:"TESTER_PUSH_RECEIVER_HOST" envDefault:""`     // Hostname the push service uses to reach the tester's mock WebPush receiver for delivery verification. Empty skips the delivery check.
	PushReceiverPort int    `env:"TESTER_PUSH_RECEIVER_PORT" envDefault:"8095"` // Port the tester's mock WebPush receiver listens on for push delivery verification.

	// JWT auth configuration
	JWTLifetime      time.Duration `env:"TESTER_JWT_LIFETIME" envDefault:"15m"`      // Lifetime of JWTs issued by the tester. Must be positive and greater than TESTER_JWT_REFRESH_BEFORE.
	JWTRefreshBefore time.Duration `env:"TESTER_JWT_REFRESH_BEFORE" envDefault:"2m"` // How early before JWT expiry the tester proactively refreshes tokens to avoid auth gaps during long-running tests.
	KeyExpiry        time.Duration `env:"TESTER_KEY_EXPIRY" envDefault:"24h"`        // Lifetime of API keys created by the tester for test runs. Must be >= TESTER_JWT_LIFETIME.

	// Auth mode configuration
	// AuthMode controls the credential mode for connections: jwt, api-key, upgrade, or mixed.
	AuthMode runner.AuthMode `env:"TESTER_AUTH_MODE" envDefault:"jwt"` // Credential mode for WebSocket connections: jwt, api-key, upgrade (JWT → API key), or mixed (ratio-split).
	// APIKey is a static pre-provisioned API key for api-key, upgrade, and mixed modes.
	// Tagged redact:"true" so the /config endpoint never echoes it (§IX).
	APIKey             string        `env:"TESTER_API_KEY" envDefault:"" redact:"true"`   // Pre-provisioned static API key for api-key, upgrade, and mixed auth modes. Required when TESTER_AUTH_MODE is api-key or upgrade.
	AuthMixRatio       float64       `env:"TESTER_AUTH_MIX_RATIO" envDefault:"0.5"`       // Fraction of connections using JWT in mixed mode (0.0 = all API key, 1.0 = all JWT).
	AuthUpgradeTimeout time.Duration `env:"TESTER_AUTH_UPGRADE_TIMEOUT" envDefault:"10s"` // Timeout for the JWT-to-API-key upgrade handshake in upgrade mode.

	// Gateway metrics scraping for revocation load suites.
	// GatewayMetricsURL is the Prometheus metrics endpoint of the gateway pod.
	// When empty, metric-drift checks are skipped (not an error — recorded as skipped_checks).
	GatewayMetricsURL      string        `env:"TESTER_GATEWAY_METRICS_URL" envDefault:""`         // Prometheus metrics endpoint of the gateway pod. When empty, metric-drift checks in revocation suites are skipped (recorded as skipped_checks, not failures).
	GatewayMetricsInterval time.Duration `env:"TESTER_GATEWAY_METRICS_INTERVAL" envDefault:"30s"` // Interval for polling gateway Prometheus metrics during revocation load tests.

	// Webhook suite configuration.
	// WebhookBaseURL is the externally-reachable base URL the webhook-worker uses to deliver
	// to this tester pod (e.g. http://tester.sukko-ci.svc.cluster.local:8085).
	// When empty, the webhooks suite is skipped.
	WebhookBaseURL         string        `env:"TESTER_WEBHOOK_BASE_URL" envDefault:""`            // Base URL the webhook-worker uses to deliver webhooks to this tester pod (e.g. http://tester.sukko-ci.svc.cluster.local:8085). When empty, the webhooks suite is skipped.
	WebhookDeliveryTimeout time.Duration `env:"TESTER_WEBHOOK_DELIVERY_TIMEOUT" envDefault:"15s"` // Maximum time to wait for the initial webhook delivery after triggering a test delivery.
	WebhookRetryTimeout    time.Duration `env:"TESTER_WEBHOOK_RETRY_TIMEOUT" envDefault:"30s"`    // Maximum time to wait for the final retry to arrive after triggering a retry test.
}

func (c *TesterConfig) Validate() error {
	if err := c.BaseConfig.Validate(); err != nil {
		return fmt.Errorf("base config: %w", err)
	}
	if err := c.KafkaConnectionConfig.Validate(); err != nil {
		return err //nolint:wrapcheck // pass-through: KafkaConnectionConfig.Validate() returns env-var-named errors with full context; wrapping adds depth without callsite value
	}
	if err := c.KafkaNamespaceConfig.Validate(); err != nil {
		return err //nolint:wrapcheck // pass-through: KafkaNamespaceConfig.Validate() returns env-var-named errors with full context
	}
	// The kafka-ingest suite runs only when brokers are set; the namespace is required then so the
	// tester publishes to the same topics the server-under-test consumes ("empty brokers ⇒ skip").
	if c.KafkaBrokers != "" && c.KafkaTopicNamespace == "" {
		return errors.New("KAFKA_TOPIC_NAMESPACE is required when KAFKA_BROKERS is set")
	}
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("TESTER_PORT must be between 1 and 65535, got %d", c.Port)
	}
	if c.GatewayURL == "" {
		return errors.New("GATEWAY_URL is required")
	}
	if c.ProvisioningURL == "" {
		return errors.New("PROVISIONING_URL is required")
	}
	if c.AdminKeyFile != "" {
		if _, err := testerauth.LoadEd25519PrivateKey(c.AdminKeyFile); err != nil {
			return fmt.Errorf("TESTER_ADMIN_KEY_FILE: %w", err)
		}
		if c.AdminKeyID == "" {
			return errors.New("TESTER_ADMIN_KEY_ID must not be empty when TESTER_ADMIN_KEY_FILE is set")
		}
		if err := testerapi.ValidateAdminKeyID(c.AdminKeyID); err != nil {
			return fmt.Errorf("TESTER_ADMIN_KEY_ID: %w", err)
		}
	}
	if c.JWTLifetime <= 0 {
		return errors.New("TESTER_JWT_LIFETIME must be positive")
	}
	if c.JWTRefreshBefore <= 0 || c.JWTRefreshBefore >= c.JWTLifetime {
		return fmt.Errorf("TESTER_JWT_REFRESH_BEFORE must be positive and less than TESTER_JWT_LIFETIME (%v), got %v", c.JWTLifetime, c.JWTRefreshBefore)
	}
	if c.KeyExpiry < c.JWTLifetime {
		return fmt.Errorf("TESTER_KEY_EXPIRY (%v) must be >= TESTER_JWT_LIFETIME (%v)", c.KeyExpiry, c.JWTLifetime)
	}
	// Auth mode validation
	switch c.AuthMode {
	case runner.AuthModeJWT, runner.AuthModeAPIKey, runner.AuthModeUpgrade, runner.AuthModeMixed:
		// valid
	default:
		return fmt.Errorf("TESTER_AUTH_MODE must be jwt|api-key|upgrade|mixed, got %q", c.AuthMode)
	}
	if c.AuthMixRatio < runner.AuthMixRatioMin || c.AuthMixRatio > runner.AuthMixRatioMax {
		return fmt.Errorf("TESTER_AUTH_MIX_RATIO must be in [%.1f, %.1f], got %v", runner.AuthMixRatioMin, runner.AuthMixRatioMax, c.AuthMixRatio)
	}
	if c.AuthUpgradeTimeout <= 0 || c.AuthUpgradeTimeout > 60*time.Second {
		return fmt.Errorf("TESTER_AUTH_UPGRADE_TIMEOUT must be > 0 and ≤ 60s, got %v", c.AuthUpgradeTimeout)
	}
	if (c.AuthMode == runner.AuthModeAPIKey || c.AuthMode == runner.AuthModeUpgrade) && c.APIKey == "" {
		return fmt.Errorf("TESTER_API_KEY is required when TESTER_AUTH_MODE is %q", c.AuthMode)
	}
	// GatewayMetricsInterval must be positive unconditionally — time.NewTicker(0) panics.
	if c.GatewayMetricsInterval <= 0 {
		return fmt.Errorf("TESTER_GATEWAY_METRICS_INTERVAL must be positive, got %v", c.GatewayMetricsInterval)
	}
	if c.GatewayMetricsURL != "" {
		if !strings.HasPrefix(c.GatewayMetricsURL, "http://") && !strings.HasPrefix(c.GatewayMetricsURL, "https://") {
			return fmt.Errorf("TESTER_GATEWAY_METRICS_URL must start with http:// or https://, got %q", c.GatewayMetricsURL)
		}
	}
	if c.WebhookBaseURL != "" {
		if !strings.HasPrefix(c.WebhookBaseURL, "http://") && !strings.HasPrefix(c.WebhookBaseURL, "https://") {
			return fmt.Errorf("TESTER_WEBHOOK_BASE_URL must start with http:// or https://, got %q", c.WebhookBaseURL)
		}
	}
	if c.WebhookDeliveryTimeout <= 0 || c.WebhookDeliveryTimeout > maxWebhookDeliveryTimeout {
		return fmt.Errorf("TESTER_WEBHOOK_DELIVERY_TIMEOUT must be between 1ns and %v, got %v", maxWebhookDeliveryTimeout, c.WebhookDeliveryTimeout)
	}
	if c.WebhookRetryTimeout <= 0 || c.WebhookRetryTimeout > maxWebhookRetryTimeout {
		return fmt.Errorf("TESTER_WEBHOOK_RETRY_TIMEOUT must be between 1ns and %v, got %v", maxWebhookRetryTimeout, c.WebhookRetryTimeout)
	}
	if c.WebhookRetryTimeout <= c.WebhookDeliveryTimeout {
		return fmt.Errorf("TESTER_WEBHOOK_RETRY_TIMEOUT (%v) must exceed TESTER_WEBHOOK_DELIVERY_TIMEOUT (%v)",
			c.WebhookRetryTimeout, c.WebhookDeliveryTimeout)
	}
	return nil
}
