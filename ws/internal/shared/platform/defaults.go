package platform

// Shared default value constants for envDefault tags that appear in multiple service configs.
//
// These constants are NOT used at runtime. They exist solely as a single source of truth
// for test verification. A reflection-based test in defaults_test.go extracts envDefault
// tags from GatewayConfig, ServerConfig, and ProvisioningConfig and asserts they match
// these constants. This catches drift when someone updates a default in one config but
// forgets the others.
//
// Structurally guaranteed defaults (NOT tracked here):
//   - LogLevel, LogFormat, Environment — via BaseConfig embedding
//   - AuthEnabled — via AuthConfig embedding (gateway + provisioning)
//   - ProvisioningGRPCAddr, GRPCReconnectDelay, GRPCReconnectMaxDelay — via ProvisioningClientConfig embedding (gateway + server)
//   - ValidNamespaces — via KafkaNamespaceConfig embedding (server + provisioning + push + tester)
//   - HTTPReadTimeout, HTTPWriteTimeout, HTTPIdleTimeout — via HTTPTimeoutConfig embedding (server + provisioning)
//
// Helm overrides are unaffected — services read env vars via caarlos0/env struct tags
// as before. These constants are never referenced in production code paths.

// HTTP timeout defaults for semantic consistency verification.
// Gateway uses GATEWAY_*_TIMEOUT env vars; server/provisioning use HTTP_*_TIMEOUT
// (from HTTPTimeoutConfig). Different env var names but same intended defaults.
const (
	DefaultHTTPReadTimeout  = "15s"
	DefaultHTTPWriteTimeout = "15s"
	DefaultHTTPIdleTimeout  = "60s"
)
