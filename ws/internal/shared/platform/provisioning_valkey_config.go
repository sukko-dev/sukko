package platform

// MinBulkDisconnectConcurrency and MaxBulkDisconnectConcurrency bound the
// PROVISIONING_BULK_DISCONNECT_CONCURRENCY field in ProvisioningConfig.
// Defined in provisioning_config.go (not server_config.go) because they bound
// a provisioning-only field, not a server field.
const (
	MinBulkDisconnectConcurrency = 1
	MaxBulkDisconnectConcurrency = 100
)

// ValkeyClientConfig holds Valkey connection parameters for services that need
// their own dedicated Valkey client (e.g., provisioning's connections registry reader).
// Use envPrefix on the embedding field to namespace env var names (e.g., "PROVISIONING_").
type ValkeyClientConfig struct {
	// Addrs is the list of Valkey node addresses. Required — no envDefault.
	// In cluster/sentinel mode, multiple addresses are comma-separated.
	Addrs []string `env:"VALKEY_ADDRS" envSeparator:","` // Valkey server addresses (host:port), comma-separated. For Sentinel mode, list all sentinels.

	// Password for Valkey AUTH. Empty = no auth.
	Password string `env:"VALKEY_PASSWORD" redact:"true"` // Valkey authentication password. Leave empty for unauthenticated local deployments.

	// MasterName is the sentinel master name. Non-empty enables sentinel mode.
	MasterName string `env:"VALKEY_MASTER_NAME"` // Sentinel master name. Required when using Valkey Sentinel for high availability.

	// TLSEnabled controls whether the Valkey connection uses TLS.
	TLSEnabled bool `env:"VALKEY_TLS_ENABLED" envDefault:"false"` // Enable TLS for Valkey connections. Required for managed Valkey services.

	// TLSInsecure disables certificate verification (dev/testing only).
	TLSInsecure bool `env:"VALKEY_TLS_INSECURE" envDefault:"false"` // Skip Valkey TLS certificate verification. For development only.

	// TLSCAPath is the path to a PEM-encoded CA certificate for private PKI.
	TLSCAPath string `env:"VALKEY_TLS_CA_PATH"` // Path to CA certificate file for verifying the Valkey server's TLS certificate.
}
