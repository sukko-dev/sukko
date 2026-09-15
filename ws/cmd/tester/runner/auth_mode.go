package runner

// AuthMode identifies the authentication mode for a test run.
type AuthMode string

// AuthMode constants enumerate the supported authentication modes.
const (
	AuthModeJWT     AuthMode = "jwt"
	AuthModeAPIKey  AuthMode = "api-key"
	AuthModeUpgrade AuthMode = "upgrade"
	AuthModeMixed   AuthMode = "mixed"
)

// AuthMixRatioMin and AuthMixRatioMax define the valid range for TESTER_AUTH_MIX_RATIO.
// Exported because api/handlers.go (separate package) uses them for request validation.
const (
	AuthMixRatioMin = 0.0
	AuthMixRatioMax = 1.0
)

// defaultAuthMixRatio is the default fraction of API-key connections in mixed mode.
const defaultAuthMixRatio = 0.5

// Validate suite name constants — used in auth-mode/suite combination guards across
// runner.go, api/handlers.go, and validate.go to prevent magic string drift.
const (
	SuiteAPIKey      = "api-key"
	SuiteUpgrade     = "upgrade"
	SuiteRestPublish = "rest-publish"
	SuiteRevocation  = "revocation"
	SuiteWebhooks    = "webhooks"
)

// privateChannelSuffix is the channel name suffix used in the api-key validation
// suite to test that API keys are denied access to private channels.
const privateChannelSuffix = ".private"

// Run-scoped API key name prefixes — used when the runner self-provisions a key at
// test start (named constants per §I; the run ID is appended for a per-run name).
const (
	mixedKeyNamePrefix     = "tester-mixed-"   // mixed-mode self-provisioned API key name prefix
	apiKeyRunKeyNamePrefix = "tester-apikey-"  // api-key-mode self-provisioned API key name prefix
	upgradeKeyNamePrefix   = "tester-upgrade-" // upgrade-mode self-provisioned API key name prefix
)
