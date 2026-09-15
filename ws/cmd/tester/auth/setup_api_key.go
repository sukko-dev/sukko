package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// SetupAPIKeyOnly initializes auth state for api-key mode: tenant resolution only — it
// creates a throwaway tenant when TenantID is empty (mirroring auth.Setup step 1) and does
// no JWT keypair registration. Returns a SetupResult with Minter=nil and TokenFunc=nil.
// All code paths that call TokenFunc or dereference Minter MUST nil-check first.
func SetupAPIKeyOnly(ctx context.Context, cfg SetupConfig) (*SetupResult, error) {
	authProvider := cfg.AdminProvider
	if authProvider == nil {
		if cfg.RequireAdminProvider {
			return nil, errors.New("SetupAPIKeyOnly: AdminProvider is nil but RequireAdminProvider is true (remote mode requires TESTER_ADMIN_KEY_FILE)")
		}
		ephemeral, _, err := NewEphemeralAuthProvider()
		if err != nil {
			return nil, fmt.Errorf("SetupAPIKeyOnly: generate ephemeral provider: %w", err)
		}
		authProvider = ephemeral
	}

	provClient := NewProvisioningClient(cfg.ProvisioningURL, authProvider, cfg.Logger)

	// Resolve tenant: create a throwaway tenant when none is supplied (mirrors auth.Setup
	// step 1). A supplied TenantID is used as-is and is never deleted.
	tenantID := cfg.TenantID
	createdTenant := false
	if tenantID == "" {
		tenantID = throwawayTenantPrefix + cfg.TestID
		if err := provClient.CreateTenant(ctx, tenantID, throwawayTenantDescription); err != nil {
			return nil, fmt.Errorf("SetupAPIKeyOnly: create tenant: %w", err)
		}
		createdTenant = true
		cfg.Logger.Info().Str(logging.LogKeyTenantSlug, tenantID).Msg("throwaway tenant created")
	}

	return &SetupResult{
		TenantID:   tenantID,
		Minter:     nil, // no JWT keypair in api-key mode
		TokenFunc:  nil, // no JWT tokens in api-key mode
		ProvClient: provClient,
		// Delete the throwaway tenant at teardown iff this run created it (best-effort).
		// A supplied tenant is never deleted.
		Cleanup: func(ctx context.Context) {
			if createdTenant {
				cleanupTenant(ctx, provClient, tenantID, cfg.Logger)
			}
		},
		AdminProvider: authProvider,
	}, nil
}
