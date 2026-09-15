package platform

import "time"

// AdminUIConfig holds Admin UI session-cookie configuration.
// Embedded anonymously in ProvisioningConfig — fields are promoted:
// use cfg.AdminUIEnabled, cfg.AdminToken, etc.
type AdminUIConfig struct {
	AdminUIEnabled  bool          `env:"ADMIN_UI_ENABLED" envDefault:"false"` // Enable the admin UI operator dashboard. When false, all /admin/* endpoints return 404.
	AdminToken      string        `env:"ADMIN_TOKEN"      redact:"true"`      // Shared secret for admin UI authentication. Required when ADMIN_UI_ENABLED=true; min 32 characters.
	AdminSessionTTL time.Duration `env:"ADMIN_SESSION_TTL" envDefault:"8h"`   // Duration of admin UI login sessions. Operators are re-authenticated when this expires.
	// AdminOIDCIssuer is reserved; activating it returns a startup error.
	AdminOIDCIssuer string `env:"ADMIN_OIDC_ISSUER"` // Reserved — not yet implemented. Setting this value causes a startup error.
	// WSServerHealthURL and GatewayHealthURL are fanned out to by GET /admin/system/health.
	// Empty → "unconfigured" in health response; no envDefault.
	WSServerHealthURL string `env:"WS_SERVER_HEALTH_URL"` // HTTP health endpoint URL of the ws-server pod, used by the admin dashboard for status checks.
	GatewayHealthURL  string `env:"GATEWAY_HEALTH_URL"`   // HTTP health endpoint URL of the gateway pod, used by the admin dashboard for status checks.
}
