package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// defaultKeyExpiry is the default expiration for registered test keys.
// Acts as a crash safety net — if the tester terminates abnormally,
// orphaned keys auto-expire rather than persisting indefinitely.
const defaultKeyExpiry = 24 * time.Hour

// throwawayTenantPrefix is prepended to the run's TestID to form the ID of a
// throwaway tenant auto-created when no TenantID is supplied (jwt and api-key modes).
const throwawayTenantPrefix = "tester-"

// throwawayTenantDescription is the description set on auto-created throwaway tenants.
const throwawayTenantDescription = "Tester auto-created tenant"

// SetupConfig configures the auth setup for a test run.
type SetupConfig struct {
	// TestID is the unique test run identifier (used for key ID and throwaway tenant ID).
	TestID string

	// TenantID is the target tenant. If empty, a throwaway tenant is created.
	TenantID string

	// ProvisioningURL is the base URL of the provisioning API.
	ProvisioningURL string

	// AdminProvider signs admin requests with a JWT keypair.
	// If nil, an ephemeral KeypairProvider is generated for this test run.
	AdminProvider Provider

	// RequireAdminProvider — if true, Setup returns an error when AdminProvider is nil
	// rather than generating an ephemeral keypair. Set by runner when TESTER_ADMIN_KEY_FILE is
	// configured (remote mode). Hardcoded true on secondary Setup calls in validate suites.
	RequireAdminProvider bool

	// JWTLifetime is the JWT expiration duration.
	JWTLifetime time.Duration

	// KeyExpiry is the registered key expiration (crash safety net). Defaults to 24h.
	KeyExpiry time.Duration

	// Logger for structured logging.
	Logger zerolog.Logger
}

// SetupResult contains the resolved auth state for a test run.
type SetupResult struct {
	// TenantID is the resolved tenant (provided or auto-created).
	TenantID string

	// Minter creates signed JWTs for this test run.
	Minter *Minter

	// TokenFunc returns a signed JWT for the given connection index.
	TokenFunc func(int) string

	// ProvClient is the provisioning API client (exposed for validate:auth suite).
	ProvClient *ProvisioningClient

	// Cleanup revokes the test key and deletes the throwaway tenant (if created).
	// Idempotent — logs errors but does not fail.
	Cleanup func(ctx context.Context)

	// AdminProvider is the resolved admin auth provider used for this setup.
	// Tagged json:"-" — holds a private key reference, must never be serialized.
	// Exposed for secondary auth.Setup() calls in validate suites.
	AdminProvider Provider `json:"-"`
}

// Setup orchestrates the full auth setup for a test run:
// 1. Resolve tenant (create throwaway if TenantID is empty)
// 2. Generate ES256 keypair
// 3. Register public key with provisioning (with expires_at safety net)
// 4. Create JWT minter
// 5. Return SetupResult with cleanup function
func Setup(ctx context.Context, cfg SetupConfig) (*SetupResult, error) {
	keyExpiry := cfg.KeyExpiry
	if keyExpiry <= 0 {
		keyExpiry = defaultKeyExpiry
	}

	logger := cfg.Logger.With().Str("test_id", cfg.TestID).Logger()

	// Use provided auth provider or generate ephemeral keypair
	authProvider := cfg.AdminProvider
	if authProvider == nil {
		if cfg.RequireAdminProvider {
			return nil, errors.New("auth.Setup: AdminProvider is nil but RequireAdminProvider is true (remote mode requires a pre-registered keypair via TESTER_ADMIN_KEY_FILE)")
		}
		ephemeral, _, err := NewEphemeralAuthProvider()
		if err != nil {
			return nil, fmt.Errorf("auth setup: generate ephemeral keypair: %w", err)
		}
		authProvider = ephemeral
		logger.Info().Msg("generated ephemeral admin keypair for test run")
	}

	provClient := NewProvisioningClient(cfg.ProvisioningURL, authProvider, logger)

	// Step 1: Resolve tenant
	tenantID := cfg.TenantID
	createdTenant := false
	if tenantID == "" {
		tenantID = throwawayTenantPrefix + cfg.TestID
		if err := provClient.CreateTenant(ctx, tenantID, throwawayTenantDescription); err != nil {
			return nil, fmt.Errorf("auth setup: create tenant: %w", err)
		}
		createdTenant = true
		logger.Info().Str(logging.LogKeyTenantSlug, tenantID).Msg("throwaway tenant created")
	}

	// Step 2: Generate keypair
	keypair, err := GenerateKeypair(cfg.TestID)
	if err != nil {
		if createdTenant {
			cleanupTenant(context.Background(), provClient, tenantID, logger) //nolint:contextcheck // cleanup must survive parent cancellation
		}
		return nil, fmt.Errorf("auth setup: generate keypair: %w", err)
	}

	// Step 3: Register public key
	expiresAt := time.Now().Add(keyExpiry)
	if err := provClient.RegisterKey(ctx, tenantID, RegisterKeyRequest{
		KeyID:     keypair.KeyID,
		Algorithm: "ES256",
		PublicKey: keypair.PublicPEM,
		ExpiresAt: &expiresAt,
	}); err != nil {
		if createdTenant {
			cleanupTenant(context.Background(), provClient, tenantID, logger) //nolint:contextcheck // cleanup must survive parent cancellation
		}
		return nil, fmt.Errorf("auth setup: register key: %w", err)
	}

	// Step 4: Create minter
	minter := NewMinter(MinterConfig{
		Keypair:  keypair,
		TenantID: tenantID,
		Lifetime: cfg.JWTLifetime,
	})

	logger.Info().
		Str(logging.LogKeyTenantSlug, tenantID).
		Str("key_id", keypair.KeyID).
		Bool("throwaway", createdTenant).
		Time("key_expires_at", expiresAt).
		Msg("auth setup complete")

	// Step 5: Build cleanup function
	cleanup := func(ctx context.Context) {
		// Revoke key (best-effort)
		if err := provClient.RevokeKey(ctx, tenantID, keypair.KeyID); err != nil {
			logger.Warn().Err(err).Str("key_id", keypair.KeyID).Msg("cleanup: failed to revoke key")
		}
		// Delete throwaway tenant (best-effort)
		if createdTenant {
			cleanupTenant(ctx, provClient, tenantID, logger)
		}
	}

	return &SetupResult{
		TenantID:      tenantID,
		Minter:        minter,
		TokenFunc:     minter.TokenFunc(),
		ProvClient:    provClient,
		Cleanup:       cleanup,
		AdminProvider: authProvider,
	}, nil
}

func cleanupTenant(ctx context.Context, client *ProvisioningClient, tenantID string, logger zerolog.Logger) {
	if err := client.DeleteTenant(ctx, tenantID); err != nil {
		logger.Warn().Err(err).Str(logging.LogKeyTenantSlug, tenantID).Msg("cleanup: failed to delete tenant")
	}
}
