package platform

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// BaseConfig contains configuration fields shared across ALL services.
type BaseConfig struct {
	LogLevel  string `env:"LOG_LEVEL" envDefault:"info"`  // Logging level (debug, info, warn, error). info is recommended for production.
	LogFormat string `env:"LOG_FORMAT" envDefault:"json"` // Log output format: json for machine parsing (production), text for human-readable (development).

	// Environment — deployment identity label, used for Kafka topic namespace, consumer
	// group naming, and safety guards. Free-form: any string works as deployment identity.
	// Sukko uses: local | dev | stg | prod by convention.
	Environment string `env:"ENVIRONMENT" envDefault:"local"` // Deployment environment name (e.g. local, dev, stag, prod). Used for Kafka topic namespace resolution.

	// LicenseKey is the signed Ed25519 license token that determines the edition
	// (Community/Pro/Enterprise). Empty = Community. Parsed by license.Manager at startup.
	// The editionManager field lives on each service config (ServerConfig, GatewayConfig,
	// ProvisioningConfig) — not here — to avoid BaseConfig depending on the license package.
	LicenseKey string `env:"SUKKO_LICENSE_KEY" redact:"true"` // Sukko license token (JWT). Community edition has no key; Pro/Enterprise obtain via Sukko account. Leave empty for Community.

	// Tracing (OpenTelemetry) — cold-path only, disabled by default.
	OTELTracingEnabled   bool   `env:"OTEL_TRACING_ENABLED" envDefault:"false"`            // Enable distributed tracing via OpenTelemetry. Disabled by default; configure OTEL_EXPORTER_TYPE and OTEL_EXPORTER_ENDPOINT when enabled.
	OTELExporterType     string `env:"OTEL_EXPORTER_TYPE" envDefault:"otlp-grpc"`          // OpenTelemetry trace exporter type: otlp-grpc, otlp-http, or stdout. Required when OTEL_TRACING_ENABLED=true.
	OTELExporterEndpoint string `env:"OTEL_EXPORTER_ENDPOINT" envDefault:"localhost:4317"` // OpenTelemetry collector endpoint (e.g. otelcol:4317). Required when OTEL_EXPORTER_TYPE is otlp-grpc or otlp-http.

	// Profiling — pprof endpoints and Pyroscope continuous profiling, disabled by default.
	PprofEnabled     bool   `env:"PPROF_ENABLED" envDefault:"false"`                  // Enable Go pprof profiling endpoints (/debug/pprof/). Disabled by default; enable only in controlled environments.
	PyroscopeEnabled bool   `env:"PYROSCOPE_ENABLED" envDefault:"false"`              // Enable continuous profiling via Pyroscope. Disabled by default.
	PyroscopeAddr    string `env:"PYROSCOPE_ADDR" envDefault:"http://localhost:4040"` // Pyroscope server address (e.g. http://pyroscope:4040). Required when PYROSCOPE_ENABLED=true.
}

// Validate checks that all BaseConfig fields have valid values.
func (c *BaseConfig) Validate() error {
	if err := validateLogLevel(c.LogLevel); err != nil {
		return err
	}
	if err := validateLogFormat(c.LogFormat); err != nil {
		return err
	}
	if strings.TrimSpace(c.Environment) == "" {
		return errors.New("ENVIRONMENT must not be empty")
	}
	if c.OTELTracingEnabled {
		switch c.OTELExporterType {
		case "otlp-grpc", "otlp-http", "stdout":
			// valid
		default:
			return fmt.Errorf("OTEL_EXPORTER_TYPE must be one of: otlp-grpc, otlp-http, stdout (got: %s)", c.OTELExporterType)
		}
		if c.OTELExporterType != "stdout" && c.OTELExporterEndpoint == "" {
			return errors.New("OTEL_EXPORTER_ENDPOINT is required when OTEL_TRACING_ENABLED=true and exporter is not stdout")
		}
	}
	return nil
}

// AuthConfig holds the authentication mode.
// Embedded by gateway and provisioning (not server — server relies on network-level security).
type AuthConfig struct {
	// AuthMode is the authentication model. Only "required" is supported.
	// Kept temporarily for backward compatibility with deployments that set AUTH_MODE=required.
	// Extensible to "public-read" in the future (anonymous subscribe for public channels).
	AuthMode string `env:"AUTH_MODE" envDefault:"required"` // Authentication enforcement mode. Only "required" is supported; tokens without valid signatures are rejected.
}

// Validate checks that AuthMode is valid. Only "required" is accepted.
// AUTH_MODE=disabled was removed — see docs/migration/auth-mode-removal.md.
func (c *AuthConfig) Validate() error {
	if c.AuthMode == "disabled" {
		return errors.New("AUTH_MODE=disabled has been removed. " +
			"See docs/migration/auth-mode-removal.md. " +
			"Set up admin + tenant JWT auth and remove AUTH_MODE/DEFAULT_TENANT_ID env vars")
	}
	if c.AuthMode != "required" {
		return fmt.Errorf("AUTH_MODE must be \"required\", got %q", c.AuthMode)
	}
	return nil
}

// ProvisioningClientConfig holds gRPC client settings for connecting to the provisioning service.
// Embedded by gateway and server (not provisioning — it IS the gRPC server).
// GRPCReconnectConfig is embedded to eliminate the duplicate inline fields
// that previously appeared here, in WebhookWorkerConfig, and in ProvisioningConfig (§X).
type ProvisioningClientConfig struct {
	GRPCReconnectConfig         // embeds PROVISIONING_GRPC_RECONNECT_* env vars; Validate() enforces bounds
	ProvisioningGRPCAddr string `env:"PROVISIONING_GRPC_ADDR" envDefault:"localhost:9090"` // gRPC address of the provisioning service for internal communication (e.g. channel rule streaming).
}

// Validate checks provisioning client config for errors.
func (c *ProvisioningClientConfig) Validate() error {
	if c.ProvisioningGRPCAddr == "" {
		return errors.New("PROVISIONING_GRPC_ADDR is required")
	}
	return c.GRPCReconnectConfig.Validate()
}

// HTTPTimeoutConfig holds HTTP server timeout settings.
// Embedded by server and provisioning (gateway uses GATEWAY_*_TIMEOUT env var names).
type HTTPTimeoutConfig struct {
	HTTPReadTimeout  time.Duration `env:"HTTP_READ_TIMEOUT" envDefault:"15s"`  // Maximum duration for reading a complete HTTP request. Protects against slow-loris attacks.
	HTTPWriteTimeout time.Duration `env:"HTTP_WRITE_TIMEOUT" envDefault:"15s"` // Maximum duration for writing the HTTP response.
	HTTPIdleTimeout  time.Duration `env:"HTTP_IDLE_TIMEOUT" envDefault:"60s"`  // Maximum idle time on a keep-alive connection before it is closed.
}

// Validate checks HTTP timeout config for errors.
func (c *HTTPTimeoutConfig) Validate() error {
	if c.HTTPReadTimeout < MinTimeout || c.HTTPReadTimeout > MaxReadWriteTimeout {
		return fmt.Errorf("HTTP_READ_TIMEOUT must be between %v and %v, got %v", MinTimeout, MaxReadWriteTimeout, c.HTTPReadTimeout)
	}
	if c.HTTPWriteTimeout < MinTimeout || c.HTTPWriteTimeout > MaxReadWriteTimeout {
		return fmt.Errorf("HTTP_WRITE_TIMEOUT must be between %v and %v, got %v", MinTimeout, MaxReadWriteTimeout, c.HTTPWriteTimeout)
	}
	if c.HTTPIdleTimeout < MinTimeout || c.HTTPIdleTimeout > MaxIdleTimeout {
		return fmt.Errorf("HTTP_IDLE_TIMEOUT must be between %v and %v, got %v", MinTimeout, MaxIdleTimeout, c.HTTPIdleTimeout)
	}
	return nil
}

// KafkaNamespaceConfig holds Kafka topic namespace settings.
// Embedded by server, provisioning, push, and tester.
type KafkaNamespaceConfig struct {
	// KafkaTopicNamespace is the explicit topic-namespace prefix in {namespace}.{tenant}.{suffix}
	// topic names — the single source of truth, with no inference from ENVIRONMENT. It is required
	// wherever a service builds or parses namespaced topics (enforced per service, since this struct
	// cannot see MESSAGE_BACKEND) and validated against ValidNamespaces. Normalize() canonicalizes it
	// (trim + lowercase) once at load so validation and topic-building agree.
	KafkaTopicNamespace string `env:"KAFKA_TOPIC_NAMESPACE"` // Explicit Kafka topic namespace prefix ({namespace}.{tenant}.{suffix}); required when the service builds or parses namespaced topics; validated against VALID_NAMESPACES.

	// ValidNamespaces is a comma-separated list of allowed topic namespace prefixes.
	ValidNamespaces string `env:"VALID_NAMESPACES" envDefault:"local,dev,stag,prod"` // Comma-separated list of allowed Kafka topic namespaces. Used to validate KAFKA_TOPIC_NAMESPACE.
}

// Normalize canonicalizes KafkaTopicNamespace (trim + lowercase). It MUST be called once at
// config load, before Validate(), so the allowlist check and every BuildTopicName consumer read
// the same canonical value. Replaces the normalization the deleted kafka.ResolveNamespace did.
func (c *KafkaNamespaceConfig) Normalize() {
	c.KafkaTopicNamespace = strings.ToLower(strings.TrimSpace(c.KafkaTopicNamespace))
}

// MessageBackend constants for MESSAGE_BACKEND env var.
const (
	MessageBackendDirect = "direct"
	MessageBackendKafka  = "kafka"
)

// KafkaConnectionConfig holds Kafka/Redpanda broker addresses plus SASL/TLS
// connection options. Embedded by MessageBackendConfig (ws-server, push) and
// directly by TesterConfig — the tester has no MESSAGE_BACKEND selector and uses
// these only for the kafka-ingest suite's direct-to-Kafka publisher.
//
// Supported SASL mechanisms:
//   - plain: SASL/PLAIN (required by Confluent Cloud; API key/secret)
//   - scram-sha-256: SCRAM-SHA-256 (Redpanda Cloud, Aiven)
//   - scram-sha-512: SCRAM-SHA-512 (AWS MSK, stronger)
//
// KafkaBrokers has no envDefault: a localhost default is never production-intended
// and would defeat the tester's "empty brokers ⇒ skip kafka-ingest" contract.
type KafkaConnectionConfig struct {
	KafkaBrokers string `env:"KAFKA_BROKERS"` // Comma-separated Kafka/Redpanda broker addresses for ws-server, push, and the tester's kafka-ingest suite (provisioning uses PROVISIONING_KAFKA_BROKERS instead). Required when MESSAGE_BACKEND=kafka; no default.

	KafkaSASLEnabled   bool   `env:"KAFKA_SASL_ENABLED" envDefault:"false"` // Enable SASL authentication for Kafka connections. Required for most managed Kafka/Redpanda services.
	KafkaSASLMechanism string `env:"KAFKA_SASL_MECHANISM"`                  // SASL mechanism: plain, scram-sha-256, or scram-sha-512. Required when KAFKA_SASL_ENABLED=true.
	KafkaSASLUsername  string `env:"KAFKA_SASL_USERNAME"`                   // SASL username for Kafka authentication. Required when KAFKA_SASL_ENABLED=true.
	KafkaSASLPassword  string `env:"KAFKA_SASL_PASSWORD" redact:"true"`     // SASL password for Kafka authentication. Required when KAFKA_SASL_ENABLED=true.

	KafkaTLSEnabled  bool   `env:"KAFKA_TLS_ENABLED" envDefault:"false"`  // Enable TLS encryption for Kafka connections. Required for most managed Kafka/Redpanda services.
	KafkaTLSInsecure bool   `env:"KAFKA_TLS_INSECURE" envDefault:"false"` // Skip TLS certificate verification. For development only — never use in production.
	KafkaTLSCAPath   string `env:"KAFKA_TLS_CA_PATH"`                     // Path to CA certificate file for verifying the Kafka broker's TLS certificate.
}

// Validate checks SASL/TLS connection options when they are enabled. It does NOT
// require KafkaBrokers — an empty value is valid (for the tester it means "skip
// the kafka-ingest suite"; MessageBackendConfig enforces brokers in kafka mode).
func (c *KafkaConnectionConfig) Validate() error {
	if c.KafkaSASLEnabled {
		if err := validateKafkaSASLMechanism(c.KafkaSASLMechanism); err != nil {
			return err
		}
		if c.KafkaSASLUsername == "" {
			return errors.New("KAFKA_SASL_USERNAME is required when KAFKA_SASL_ENABLED=true")
		}
		if c.KafkaSASLPassword == "" {
			return errors.New("KAFKA_SASL_PASSWORD is required when KAFKA_SASL_ENABLED=true")
		}
	}
	if c.KafkaTLSEnabled && c.KafkaTLSCAPath != "" {
		if _, err := os.Stat(c.KafkaTLSCAPath); err != nil {
			return fmt.Errorf("KAFKA_TLS_CA_PATH %q: %w", c.KafkaTLSCAPath, err)
		}
	}
	return nil
}

// MessageBackendConfig holds message ingestion/persistence configuration.
// Embedded by ServerConfig (ws-server) and push.Config (push service).
//
// Controls which message ingestion/persistence layer the service uses:
//   - "direct": Messages flow directly to broadcast bus. No persistence, no replay.
//     Zero external dependencies beyond the broadcast bus. Default for lowest friction.
//   - "kafka": Full Kafka/Redpanda integration. Persistence, offset-based replay,
//     multi-tenant consumer isolation. Requires Kafka infrastructure.
type MessageBackendConfig struct {
	MessageBackend string `env:"MESSAGE_BACKEND" envDefault:"direct"` // Message ingestion backend: direct (no persistence, no Kafka dependency) or kafka (full Kafka/Redpanda integration with replay).

	KafkaConnectionConfig
}

// Validate checks MessageBackendConfig for errors.
func (c *MessageBackendConfig) Validate() error {
	// Backend type validation
	validBackends := map[string]bool{MessageBackendDirect: true, MessageBackendKafka: true}
	if !validBackends[c.MessageBackend] {
		return fmt.Errorf("[CONFIG ERROR] MESSAGE_BACKEND=%q is invalid (valid: %s, %s)", c.MessageBackend, MessageBackendDirect, MessageBackendKafka)
	}

	// Kafka-specific validation (when MESSAGE_BACKEND=kafka)
	if c.MessageBackend == MessageBackendKafka {
		if c.KafkaBrokers == "" {
			return fmt.Errorf("KAFKA_BROKERS is required when MESSAGE_BACKEND=%s", MessageBackendKafka)
		}
		if err := c.KafkaConnectionConfig.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// Validate checks Kafka namespace config for errors.
//
// This is the value/allowlist boundary ONLY — it deliberately does NOT enforce non-empty, because
// KafkaNamespaceConfig cannot see MESSAGE_BACKEND. Each embedding service's own Validate() enforces
// the non-empty requirement per its topic-activation trigger (server: kafka mode; push and
// provisioning: always; tester: when brokers are set).
//
// No environment-name guard: ENVIRONMENT no longer participates in topic naming, and the
// VALID_NAMESPACES allowlist is the value-based enforcement boundary. Do NOT add an env-name guard.
func (c *KafkaNamespaceConfig) Validate() error {
	validNS := parseNamespaces(c.ValidNamespaces)
	if len(validNS) == 0 {
		return errors.New("VALID_NAMESPACES must contain at least one namespace")
	}
	if c.KafkaTopicNamespace != "" && !validNS[c.KafkaTopicNamespace] {
		return fmt.Errorf("KAFKA_TOPIC_NAMESPACE must be one of: %s (got: %s)",
			c.ValidNamespaces, c.KafkaTopicNamespace)
	}
	return nil
}

// DatabaseConfig holds PostgreSQL connection settings.
// Embedded by services that use a database (provisioning, push).
// Gateway and ws-server are stateless — they don't embed this.
type DatabaseConfig struct {
	DatabaseURL string `env:"DATABASE_URL" redact:"true"` // PostgreSQL connection URL (postgres://user:pass@host:5432/db). Required for services that use the database (provisioning, push).
}

// Validate checks that DATABASE_URL is configured.
func (c *DatabaseConfig) Validate() error {
	if c.DatabaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	return nil
}
