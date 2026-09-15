package runner

import (
	"cmp"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// Sentinel errors for expected conditions.
var (
	ErrTestNotFound      = errors.New("test not found")
	ErrTestAlreadyExists = errors.New("test already exists")
	ErrInvalidConfig     = errors.New("invalid config")
)

// TestType identifies the kind of test to run.
type TestType string

// TestSmoke and related constants enumerate the supported test types.
const (
	TestSmoke    TestType = "smoke"
	TestLoad     TestType = "load"
	TestStress   TestType = "stress"
	TestSoak     TestType = "soak"
	TestValidate TestType = "validate"
)

// Test channel suffixes for each test type. Qualified with the run's tenant
// via tenantChannel() at use — the gateway requires every subscribe/publish
// channel to carry the authenticated tenant's prefix, and each run's tenant
// is dynamic (throwaway tester-xxxx tenants), so a hardcoded prefix like the
// old "sukko." never matched and every canary subscribe was filtered out.
const (
	smokeTestChannel  = "smoke.test"
	loadTestChannel   = "load.test"
	stressTestChannel = "stress.test"
	soakTestChannel   = "soak.test"
)

// tenantChannel qualifies a channel suffix with the tenant prefix the gateway
// mandates on every subscribe and publish channel (ValidateChannelTenant).
func tenantChannel(tenantID, suffix string) string {
	return tenantID + "." + suffix
}

// defaultRampRate is the fallback connections-per-second rate when not configured.
const defaultRampRate = 50

// resolveRunNamespace picks the per-run Kafka topic namespace: an explicit per-request value
// overrides the tester's own KAFKA_TOPIC_NAMESPACE, then the result is normalized (trim+lowercase)
// because the per-request value bypasses config load. It MUST match the server-under-test.
func resolveRunNamespace(perRequest, testerDefault string) string {
	// Trim BEFORE cmp.Or so a whitespace-only per-request value counts as absent and does not
	// wipe a valid tester default.
	return strings.ToLower(cmp.Or(strings.TrimSpace(perRequest), strings.TrimSpace(testerDefault)))
}

// TestContext holds deployment context passed from the CLI.
// When present, all core fields are required (all-or-nothing).
type TestContext struct {
	GatewayURL      string `json:"gateway_url"`
	ProvisioningURL string `json:"provisioning_url"`
	Environment     string `json:"environment"`
}

// TestConfig holds the parameters for a test run.
type TestConfig struct {
	Type                   TestType      `json:"type"`
	GatewayURL             string        `json:"gateway_url"`
	ProvisioningURL        string        `json:"provisioning_url,omitempty"`
	APIKey                 string        `json:"-"` // per-request API key; tagged json:"-" — credentials must never appear in API responses (§IX)
	AuthMode               AuthMode      `json:"auth_mode,omitempty"`
	AuthMixRatio           float64       `json:"auth_mix_ratio,omitzero"`
	KafkaBrokers           string        `json:"kafka_brokers,omitempty"`
	KafkaTopicNamespace    string        `json:"kafka_topic_namespace,omitempty"` // per-run override of the tester's KAFKA_TOPIC_NAMESPACE; MUST match the server-under-test
	Connections            int           `json:"connections,omitzero"`
	Duration               string        `json:"duration,omitempty"`
	PublishRate            int           `json:"publish_rate,omitzero"`
	RampRate               int           `json:"ramp_rate,omitzero"`
	Suite                  string        `json:"suite,omitempty"`       // for validate type
	ChannelMode            bool          `json:"channel_mode,omitzero"` // for load: distribute across public/user/group channels
	TenantID               string        `json:"tenant_id,omitempty"`
	GatewayMetricsURL      string        `json:"gateway_metrics_url,omitempty"`
	GatewayMetricsInterval time.Duration `json:"-"` // never serialized — config-level only
	RevocationsPerCycle    int           `json:"revocations_per_cycle,omitzero"`
	SigningKeyFile         string        `json:"signing_key_file,omitempty"` // PEM PKCS#8 ECDSA P-256 private key path (env var fallback)
	SigningKeyBytes        []byte        `json:"-"`                          // PEM PKCS#8 ECDSA P-256 private key bytes (API passthrough, never serialized)
	AdminKeyBytes          []byte        `json:"-"`                          // Ed25519 private key bytes from per-request admin_key (never serialized)
	AdminKeyID             string        `json:"-"`                          // effective kid for per-request key (overridden by admin_key_id body field)
	Context                *TestContext  `json:"context,omitzero"`
}

// TestStatus represents the current state of a test run.
type TestStatus string

// StatusPending and related constants enumerate test run states.
const (
	StatusPending  TestStatus = "pending"
	StatusRunning  TestStatus = "running"
	StatusComplete TestStatus = "complete"
	StatusFailed   TestStatus = "failed"
	StatusStopped  TestStatus = "stopped"
)

// TestRun tracks the state and results of an individual test execution.
type TestRun struct {
	ID               string             `json:"id"`
	Config           TestConfig         `json:"config"`
	Status           TestStatus         `json:"status"`
	Collector        *metrics.Collector `json:"-"`
	Report           *metrics.Report    `json:"report,omitempty"`
	mu               sync.RWMutex       `json:"-"`
	cancel           context.CancelFunc
	authResult       *auth.SetupResult `json:"-"`
	jwtLifetime      time.Duration     `json:"-"`
	jwtRefreshBefore time.Duration     `json:"-"`
	// Auth mode fields set by execute() before dispatching to test functions.
	apiKey             string        `json:"-"` // effective API key for this run
	authUpgradeTimeout time.Duration `json:"-"` // timeout for auth_ack in upgrade flow (upgrade suite only)
	// Authorization deny-wait bounds, seeded unconditionally by execute() from the
	// defaultDenyWait* consts (no env var — internal robustness bounds); tests inject small
	// values. Serve every deny check across all suites. Authoritative: consumers have no <= 0 fallback.
	denyWaitDeadline      time.Duration `json:"-"` // overall deadline for any authorization deny wait (subscribe/publish, all suites)
	denyWaitRetryInterval time.Duration `json:"-"` // re-issue interval within a deny wait
	// Webhook suite fields set by execute().
	webhookBaseURL         string        `json:"-"`
	webhookDeliveryTimeout time.Duration `json:"-"`
	webhookRetryTimeout    time.Duration `json:"-"`
	webhookStore           *WebhookStore `json:"-"`
	// kafka-ingest suite fields set by execute() (credentials never serialized — §IX).
	kafkaSASL        *kafkashared.SASLConfig `json:"-"`
	kafkaTLS         *kafkashared.TLSConfig  `json:"-"`
	kafkaBrokers     string                  `json:"-"`
	pushReceiverHost string                  `json:"-"`
	pushReceiverPort int                     `json:"-"`
	kafkaNamespace   string                  `json:"-"`
}

// StatusSnapshot returns the current Status and Report under a read lock.
func (t *TestRun) StatusSnapshot() (TestStatus, *metrics.Report) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Status, t.Report
}

// ConfigSnapshot returns a copy of Config under a read lock, safe for concurrent access.
func (t *TestRun) ConfigSnapshot() TestConfig {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Config
}

// Runner manages concurrent test executions.
type Runner struct {
	mu           sync.RWMutex
	tests        map[string]*TestRun
	wg           sync.WaitGroup
	logger       zerolog.Logger
	cfg          Config
	webhookStore *WebhookStore
}

// Config holds default settings applied to all test runs.
type Config struct {
	GatewayURL          string
	ProvisioningURL     string
	Environment         string                  // deployment environment label; part of the API context contract, no longer used for topic naming (namespace is explicit — see KafkaTopicNamespace)
	KafkaTopicNamespace string                  // explicit Kafka topic namespace (kafka-ingest); MUST match the server-under-test
	KafkaBrokers        string                  // empty ⇒ kafka-ingest suite skips
	PushReceiverHost    string                  // empty ⇒ push delivery check skips
	PushReceiverPort    int                     // mock WebPush receiver listen port
	KafkaSASL           *kafkashared.SASLConfig // nil = no SASL auth (read by the kafka-ingest suite)
	KafkaTLS            *kafkashared.TLSConfig  // nil = plaintext (read by the kafka-ingest suite)
	JWTLifetime         time.Duration
	JWTRefreshBefore    time.Duration
	KeyExpiry           time.Duration
	SigningKeyFile      string // Ed25519 private key file for license-reload suite signing
	AdminKeyFile        string // path to raw Ed25519 private key file (remote mode; empty = local dev mode)
	AdminKeyID          string // kid embedded in admin JWTs; defaults to BootstrapAdminKeyID
	// Auth mode fields (TESTER_AUTH_MODE and related)
	AuthMode           AuthMode
	APIKey             string // TESTER_API_KEY; stored here only, never in TestConfig (§IX)
	AuthMixRatio       float64
	AuthUpgradeTimeout time.Duration
	// Gateway metrics scraping for revocation load suites.
	GatewayMetricsURL      string
	GatewayMetricsInterval time.Duration
	// Webhook suite configuration.
	WebhookBaseURL         string
	WebhookDeliveryTimeout time.Duration
	WebhookRetryTimeout    time.Duration
}

// New creates a Runner with the given configuration and logger.
func New(cfg Config, logger zerolog.Logger) *Runner {
	return &Runner{
		tests:        make(map[string]*TestRun),
		logger:       logger,
		cfg:          cfg,
		webhookStore: newWebhookStore(),
	}
}

// Start launches a new test run with the given ID and configuration.
func (r *Runner) Start(id string, cfg TestConfig) (*TestRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tests[id]; exists {
		return nil, fmt.Errorf("test %s: %w", id, ErrTestAlreadyExists)
	}

	// Fill in defaults from runner config
	if cfg.GatewayURL == "" {
		cfg.GatewayURL = r.cfg.GatewayURL
	}
	if cfg.ProvisioningURL == "" {
		cfg.ProvisioningURL = r.cfg.ProvisioningURL
	}
	if cfg.KafkaBrokers == "" {
		cfg.KafkaBrokers = r.cfg.KafkaBrokers
	}
	if cfg.SigningKeyFile == "" {
		cfg.SigningKeyFile = r.cfg.SigningKeyFile
	}
	if cfg.AuthMixRatio == 0 && r.cfg.AuthMixRatio != 0 {
		cfg.AuthMixRatio = r.cfg.AuthMixRatio
	}
	// GatewayMetricsURL: per-run override takes precedence; fall back to service-level.
	if cfg.GatewayMetricsURL == "" {
		cfg.GatewayMetricsURL = r.cfg.GatewayMetricsURL
	}
	// GatewayMetricsInterval: always use service-level value — no per-run override.
	cfg.GatewayMetricsInterval = r.cfg.GatewayMetricsInterval

	// §II defense-in-depth: validate auth mode and mode/type combinations.
	// The handler also validates, but the runner validates again for programmatic callers.
	// Auth-mode inference (§XV): suite=api-key and suite=upgrade each have exactly one legal
	// mode, so an absent auth_mode for a validate run of those suites resolves to that mode —
	// this completes a mandatory 1:1 mapping, it is not multi-meaning mode detection. Kept in
	// lockstep with the handler boundary (§II). Every other absent-mode request defaults to jwt.
	if cfg.AuthMode == "" && cfg.Type == TestValidate && cfg.Suite == SuiteAPIKey {
		cfg.AuthMode = AuthModeAPIKey
	}
	if cfg.AuthMode == "" && cfg.Type == TestValidate && cfg.Suite == SuiteUpgrade {
		cfg.AuthMode = AuthModeUpgrade
	}
	if cfg.AuthMode == "" {
		cfg.AuthMode = AuthModeJWT // backward compat: missing auth_mode defaults to jwt
	}
	switch cfg.AuthMode {
	case AuthModeJWT, AuthModeAPIKey, AuthModeUpgrade, AuthModeMixed:
		// valid
	default:
		return nil, fmt.Errorf("unknown auth_mode %q: %w", cfg.AuthMode, ErrInvalidConfig)
	}
	if cfg.AuthMixRatio < AuthMixRatioMin || cfg.AuthMixRatio > AuthMixRatioMax {
		return nil, fmt.Errorf("auth_mix_ratio %.2f out of range [%.1f, %.1f]: %w", cfg.AuthMixRatio, AuthMixRatioMin, AuthMixRatioMax, ErrInvalidConfig)
	}
	switch cfg.AuthMode {
	case AuthModeJWT:
		// no restrictions
	case AuthModeMixed:
		switch cfg.Type {
		case TestLoad, TestSoak:
			// valid for mixed mode
		case TestValidate, TestSmoke, TestStress:
			return nil, fmt.Errorf("auth_mode=mixed is not valid for type=%s: %w", cfg.Type, ErrInvalidConfig)
		}
	case AuthModeAPIKey:
		if cfg.Type != TestValidate {
			return nil, fmt.Errorf("auth_mode=api-key is only valid for type=validate, got type=%s: %w", cfg.Type, ErrInvalidConfig)
		}
		if cfg.Suite != "" && cfg.Suite != SuiteAPIKey && cfg.Suite != SuiteRestPublish {
			return nil, fmt.Errorf("auth_mode=api-key only supports suite=%s or suite=%s, got suite=%s: %w", SuiteAPIKey, SuiteRestPublish, cfg.Suite, ErrInvalidConfig)
		}
		// tenant_id is optional in api-key mode: empty/absent/null → the runner
		// self-provisions a throwaway tenant; a supplied tenant_id is used as-is. Guard removed
		// in lockstep with the handler boundary (§II).
	case AuthModeUpgrade:
		if cfg.Type != TestValidate {
			return nil, fmt.Errorf("auth_mode=upgrade is only valid for type=validate, got type=%s: %w", cfg.Type, ErrInvalidConfig)
		}
		if cfg.Suite != "" && cfg.Suite != SuiteUpgrade {
			return nil, fmt.Errorf("auth_mode=upgrade only supports suite=%s, got suite=%s: %w", SuiteUpgrade, cfg.Suite, ErrInvalidConfig)
		}
		// tenant_id is optional in upgrade mode: empty/absent/null → auth.Setup
		// self-provisions a throwaway tenant; a supplied tenant_id is used as-is. Guard removed
		// in lockstep with the handler boundary (§II).
	}

	// suite=revocation is only valid for stress and soak test types.
	if cfg.Suite == SuiteRevocation && cfg.Type != TestStress && cfg.Type != TestSoak {
		return nil, fmt.Errorf("suite=%s is only valid for type=stress or type=soak, got type=%s: %w", SuiteRevocation, cfg.Type, ErrInvalidConfig)
	}

	ctx, cancel := context.WithCancel(context.Background())

	run := &TestRun{
		ID:        id,
		Config:    cfg,
		Status:    StatusRunning,
		Collector: metrics.NewCollector(),
		cancel:    cancel,
	}

	r.tests[id] = run

	r.wg.Go(func() {
		defer logging.RecoverPanic(r.logger, "test-runner-dispatch", map[string]any{"test_id": id})
		r.execute(ctx, run)
	})

	return run, nil
}

// Wait blocks until all test execution goroutines have completed.
func (r *Runner) Wait() {
	r.wg.Wait()
}

// Stop cancels a running test by ID.
func (r *Runner) Stop(id string) error {
	r.mu.RLock()
	run, ok := r.tests[id]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("test %s: %w", id, ErrTestNotFound)
	}

	run.cancel()
	return nil
}

// StopAll cancels all running tests. Used during graceful shutdown.
func (r *Runner) StopAll() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, run := range r.tests {
		run.cancel()
	}
}

// Get retrieves a test run by ID.
func (r *Runner) Get(id string) (*TestRun, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	run, ok := r.tests[id]
	if !ok {
		return nil, fmt.Errorf("test %s: %w", id, ErrTestNotFound)
	}
	return run, nil
}

// resolveAdminProvider returns the admin auth provider for a test run using a
// priority chain: (1) per-request bytes, (2) config file, (3) nil (ephemeral).
// Returning nil signals auth.Setup to generate an ephemeral keypair (local dev mode).
func resolveAdminProvider(cfg Config, testCfg TestConfig) (auth.Provider, error) {
	if len(testCfg.AdminKeyBytes) > 0 {
		key := ed25519.PrivateKey(testCfg.AdminKeyBytes)
		if err := auth.ValidateEd25519Key(key); err != nil {
			return nil, fmt.Errorf("resolveAdminProvider: per-request admin key: %w", err)
		}
		keyID := testCfg.AdminKeyID
		if keyID == "" {
			keyID = cfg.AdminKeyID
		}
		return auth.NewKeypairAuthProvider(key, keyID, auth.AdminKeyName), nil
	}
	if cfg.AdminKeyFile != "" {
		key, err := auth.LoadEd25519PrivateKey(cfg.AdminKeyFile)
		if err != nil {
			return nil, fmt.Errorf("resolveAdminProvider: %w", err)
		}
		return auth.NewKeypairAuthProvider(key, cfg.AdminKeyID, auth.AdminKeyName), nil
	}
	return nil, nil // nil → auth.Setup generates ephemeral (local dev mode)
}

// selfProvisionAPIKey creates a run-scoped API key for the run's tenant and returns its
// ID plus a best-effort revoke closure (fresh context + editionHTTPTimeout, per §III/§IV).
// Shared by mixed and api-key modes when no static key is supplied (§X/§XVIII dedup).
//
// Return contract:
//   - CreateAPIKey fails            → ("", nil, err): nothing was created; nothing to revoke.
//   - key created but key_id unparseable → ("", nil, err): logs a prominent "manual cleanup
//     required" warning (the key exists but its ID is unknown, so it cannot be revoked); the
//     caller MUST fail the run.
//   - success                       → (keyID, revoke, nil): the caller registers `defer revoke()`
//     (revoke is safe to call once, takes no arguments).
func selfProvisionAPIKey(ctx context.Context, authResult *auth.SetupResult, keyName string, logger zerolog.Logger) (keyID string, revoke func(), err error) {
	keyBody, err := authResult.ProvClient.CreateAPIKey(ctx, authResult.TenantID, keyName)
	if err != nil {
		return "", nil, fmt.Errorf("create api key: %w", err)
	}
	var keyResp struct {
		KeyID string `json:"key_id"`
	}
	if uerr := json.Unmarshal(keyBody, &keyResp); uerr != nil || keyResp.KeyID == "" {
		logger.Error().Err(uerr).Str(logging.LogKeyTenantSlug, authResult.TenantID).
			Msg("provision: api key created but key_id could not be parsed — cannot revoke; manual cleanup required")
		return "", nil, errors.New("parse key_id from create api key response")
	}
	keyID = keyResp.KeyID
	revoke = func() { //nolint:contextcheck // teardown: test ctx is canceled at this point; fresh context required
		revokeCtx, cancel := context.WithTimeout(context.Background(), editionHTTPTimeout)
		defer cancel()
		if rerr := authResult.ProvClient.RevokeAPIKey(revokeCtx, authResult.TenantID, keyID); rerr != nil {
			logger.Error().Err(rerr).Str("key_id", keyID).Msg("teardown: failed to revoke run-scoped api key")
		}
	}
	return keyID, revoke, nil
}

func (r *Runner) execute(ctx context.Context, run *TestRun) {
	logger := r.logger.With().Str("test_id", run.ID).Str("type", string(run.Config.Type)).Logger()

	defer logging.RecoverPanic(logger, "test-execution", map[string]any{"test_id": run.ID})

	logger.Info().Str("auth_mode", string(run.Config.AuthMode)).Msg("starting test")

	adminProvider, providerErr := resolveAdminProvider(r.cfg, run.Config)
	if providerErr != nil {
		r.failRun(run, fmt.Sprintf("resolve admin provider: %v", providerErr))
		logger.Error().Err(providerErr).Msg("admin provider resolution failed")
		return
	}

	// Auth setup: branch on auth mode.
	// api-key mode: skip JWT keypair registration; Minter and TokenFunc will be nil.
	// All other modes: full JWT setup (register keypair, create minter).
	var authResult *auth.SetupResult
	var authErr error

	if run.Config.AuthMode == AuthModeAPIKey {
		authResult, authErr = auth.SetupAPIKeyOnly(ctx, auth.SetupConfig{
			TestID:               run.ID,
			TenantID:             run.Config.TenantID,
			ProvisioningURL:      run.Config.ProvisioningURL,
			Logger:               logger,
			AdminProvider:        adminProvider,
			RequireAdminProvider: r.cfg.AdminKeyFile != "",
		})
	} else {
		authResult, authErr = auth.Setup(ctx, auth.SetupConfig{
			TestID:               run.ID,
			TenantID:             run.Config.TenantID,
			ProvisioningURL:      run.Config.ProvisioningURL,
			JWTLifetime:          r.cfg.JWTLifetime,
			KeyExpiry:            r.cfg.KeyExpiry,
			Logger:               logger,
			AdminProvider:        adminProvider,
			RequireAdminProvider: r.cfg.AdminKeyFile != "",
		})
	}
	if authErr != nil {
		r.failRun(run, fmt.Sprintf("auth setup: %v", authErr))
		logger.Error().Err(authErr).Msg("auth setup failed")
		return
	}
	defer authResult.Cleanup(context.Background()) //nolint:contextcheck // cleanup must survive parent cancellation

	// Populate auth state on run for test functions.
	// Config.TenantID is written under mu; readers use ConfigSnapshot() which also acquires mu.
	// The unexported fields are only accessed from this goroutine chain.
	run.mu.Lock()
	run.Config.TenantID = authResult.TenantID
	run.mu.Unlock()
	run.authResult = authResult
	run.jwtLifetime = r.cfg.JWTLifetime
	run.jwtRefreshBefore = r.cfg.JWTRefreshBefore
	run.authUpgradeTimeout = r.cfg.AuthUpgradeTimeout
	// Deny-wait bounds: unconditional, from the consts — NOT from r.cfg (no env var; the fields
	// are authoritative so no fallback). Serve every authorization deny check across all suites.
	run.denyWaitDeadline = defaultDenyWaitDeadline
	run.denyWaitRetryInterval = defaultDenyWaitRetryInterval
	run.webhookBaseURL = r.cfg.WebhookBaseURL
	run.webhookDeliveryTimeout = r.cfg.WebhookDeliveryTimeout
	run.webhookRetryTimeout = r.cfg.WebhookRetryTimeout
	run.webhookStore = r.webhookStore

	// kafka-ingest suite fields. Brokers: per-request override else runner config.
	// Namespace: an explicit per-request value overrides the tester's own KAFKA_TOPIC_NAMESPACE.
	// Normalized here because the per-request value bypasses config load; MUST match the server-under-test.
	run.kafkaSASL = r.cfg.KafkaSASL
	run.kafkaTLS = r.cfg.KafkaTLS
	run.kafkaBrokers = cmp.Or(run.Config.KafkaBrokers, r.cfg.KafkaBrokers)
	run.pushReceiverHost = r.cfg.PushReceiverHost
	run.pushReceiverPort = r.cfg.PushReceiverPort
	run.kafkaNamespace = resolveRunNamespace(run.Config.KafkaTopicNamespace, r.cfg.KafkaTopicNamespace)

	// Resolve effective API key: per-request key overrides runner-level config key.
	effectiveAPIKey := r.cfg.APIKey
	if run.Config.APIKey != "" {
		effectiveAPIKey = run.Config.APIKey
	}
	run.apiKey = effectiveAPIKey

	// Mixed mode with no static API key: create one dynamically at test start.
	// The key is revoked at teardown (best-effort) using a fresh context (§III, §IV).
	if run.Config.AuthMode == AuthModeMixed && run.apiKey == "" {
		keyID, revoke, keyErr := selfProvisionAPIKey(ctx, authResult, mixedKeyNamePrefix+run.ID, logger)
		if revoke != nil {
			defer revoke()
		}
		if keyErr != nil {
			r.failRun(run, fmt.Sprintf("create mixed-mode api key: %v", keyErr))
			logger.Error().Err(keyErr).Msg("failed to create mixed-mode api key")
			return
		}
		run.apiKey = keyID
	}

	// API-key mode with no supplied key: self-provision a run-scoped key for the run's
	// tenant (mirrors mixed mode above). authResult.Cleanup (deferred earlier) deletes the
	// throwaway tenant LAST; the revoke defer registered here runs FIRST (LIFO).
	if run.Config.AuthMode == AuthModeAPIKey && run.apiKey == "" {
		keyID, revoke, keyErr := selfProvisionAPIKey(ctx, authResult, apiKeyRunKeyNamePrefix+run.ID, logger)
		if revoke != nil {
			defer revoke()
		}
		if keyErr != nil {
			r.failRun(run, fmt.Sprintf("create api-key-mode api key: %v", keyErr))
			logger.Error().Err(keyErr).Msg("failed to create api-key-mode api key")
			return
		}
		run.apiKey = keyID
	}

	// Upgrade mode with no supplied key: self-provision a run-scoped key to make the initial
	// API-key connection (the JWT keypair + Minter + throwaway tenant already come from
	// auth.Setup). Teardown is three-resource LIFO: this revoke defer runs FIRST (api key),
	// then authResult.Cleanup (deferred earlier by auth.Setup) revokes the JWT keypair and
	// deletes the tenant.
	if run.Config.AuthMode == AuthModeUpgrade && run.apiKey == "" {
		keyID, revoke, keyErr := selfProvisionAPIKey(ctx, authResult, upgradeKeyNamePrefix+run.ID, logger)
		if revoke != nil {
			defer revoke()
		}
		if keyErr != nil {
			r.failRun(run, fmt.Sprintf("create upgrade-mode api key: %v", keyErr))
			logger.Error().Err(keyErr).Msg("failed to create upgrade-mode api key")
			return
		}
		run.apiKey = keyID
	}

	var report *metrics.Report
	var err error

	// Mixed mode dispatch: handled here (in execute, a *Runner method) because runLoad/runSoak
	// are package-level functions without access to r.cfg.
	if run.Config.AuthMode == AuthModeMixed {
		switch run.Config.Type {
		case TestLoad:
			report, err = runLoadMixed(ctx, run, logger)
		case TestSoak:
			report, err = runSoakMixed(ctx, run, logger)
		default:
			err = fmt.Errorf("mixed mode not valid for type %s", run.Config.Type)
		}
	} else {
		switch run.Config.Type {
		case TestSmoke:
			report, err = runSmoke(ctx, run, logger)
		case TestLoad:
			report, err = runLoad(ctx, run, logger)
		case TestStress:
			if run.Config.Suite == SuiteRevocation {
				report, err = runStressRevocation(ctx, run, logger)
			} else {
				report, err = runStress(ctx, run, logger)
			}
		case TestSoak:
			if run.Config.Suite == SuiteRevocation {
				report, err = runSoakRevocation(ctx, run, logger)
			} else {
				report, err = runSoak(ctx, run, logger)
			}
		case TestValidate:
			report, err = runValidate(ctx, run, logger)
		default:
			err = fmt.Errorf("unknown test type: %s", run.Config.Type)
		}
	}

	run.mu.Lock()
	switch {
	case err != nil:
		run.Status = StatusFailed
		if report == nil {
			report = &metrics.Report{
				TestType: string(run.Config.Type),
				Status:   metrics.ReportStatusError,
				Metrics:  run.Collector.Snapshot(),
				Errors:   []string{err.Error()},
			}
		}
	case ctx.Err() != nil:
		run.Status = StatusStopped
	default:
		run.Status = StatusComplete
	}
	run.Report = report
	run.mu.Unlock()

	logger.Info().Str("status", string(run.Status)).Msg("test completed")
}

// WebhookReceiveHandler returns the http.HandlerFunc that records incoming webhook
// deliveries. Register it at POST /webhook-receive/{runID} in the API router.
func (r *Runner) WebhookReceiveHandler() http.HandlerFunc {
	return webhookReceiveHandler(r.webhookStore)
}

func (r *Runner) failRun(run *TestRun, errMsg string) {
	run.mu.Lock()
	defer run.mu.Unlock()
	run.Status = StatusFailed
	run.Report = &metrics.Report{
		TestType: string(run.Config.Type),
		Status:   metrics.ReportStatusError,
		Metrics:  run.Collector.Snapshot(),
		Errors:   []string{errMsg},
	}
}
