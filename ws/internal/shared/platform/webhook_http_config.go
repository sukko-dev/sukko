package platform

// WebhookHTTPConfig holds the WEBHOOK_ALLOW_HTTP setting shared by both
// WebhookWorkerConfig and ProvisioningConfig.
// Embed in both to eliminate the duplicate inline field (§X).
//
// Validate() is zero-argument — a bool field has no invalid state.
// A startup warning is emitted by each service's main() when this is true.
type WebhookHTTPConfig struct {
	// WebhookAllowHTTP allows http:// webhook URLs for local dev/testing.
	WebhookAllowHTTP bool `env:"WEBHOOK_ALLOW_HTTP" envDefault:"false"` // Allow webhook delivery to plain HTTP endpoints. Disabled by default; enable only in trusted network environments.
}

// Validate satisfies the shared config struct convention.
//
// No environment-name guard (e.g. c.Environment != "local"): ENVIRONMENT is operator-defined
// free text — "production", "live", "prod-us-east" are all common and none equal "local".
// A string-comparison guard would silently miss every such name, providing false safety.
// Protection is the safe default (false) + an explicit startup warning in each service's main.go.
// Do NOT re-add an environment-name guard here.
func (c WebhookHTTPConfig) Validate() error {
	return nil
}
