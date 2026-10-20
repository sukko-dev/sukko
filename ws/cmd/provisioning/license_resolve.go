package main

import (
	"fmt"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

// resolveProvisioningLicense picks the license manager provisioning runs with.
//
// The DB-persisted key (written by a prior webhook/API reload) takes precedence
// over the env key, but provisioning does not require the env key specifically:
// it may boot from the DB key alone when the env key is absent or has expired
// (e.g. the original env key rotated out). It fails only when NEITHER source
// yields a valid license (ADR-0038 — Community requires an account-issued key).
//
//   - envMgr is the manager built from SUKKO_LICENSE_KEY during config load, or
//     nil when the env key was absent/expired (an invalid signature already failed
//     config load).
//   - When envMgr is non-nil, the env edition is the floor and a present DB key is
//     applied via Reload — which keeps the replay protection (an older-iat DB key
//     is rejected and the env key is kept).
//   - When envMgr is nil, the DB key is the only hope; a valid one is used, else
//     the error is terminal.
//
// Returns the resolved manager and the effective key string (for the WatchLicense
// snapshot), or an error when no valid license exists.
func resolveProvisioningLicense(envMgr *license.Manager, envKey, dbKey string, logger zerolog.Logger) (*license.Manager, string, error) {
	if envMgr != nil {
		if dbKey != "" {
			if err := envMgr.Reload(dbKey); err != nil {
				logger.Warn().Err(err).Msg("DB license key failed reload, using env var")
				return envMgr, envKey, nil
			}
			logger.Info().
				Str("edition", envMgr.Edition().String()).
				Msg("license loaded from DB (takes precedence over SUKKO_LICENSE_KEY)")
			return envMgr, dbKey, nil
		}
		return envMgr, envKey, nil
	}

	// No usable env license (empty or expired). The DB key is the only source left.
	if dbKey == "" {
		// Distinguish "never set" from "set but lapsed" so the operator gets an
		// actionable message. An invalid signature would have failed config load
		// before reaching here, so a non-empty env key with a nil manager can only
		// mean the env key expired (ADR-0038 restart-after-expiry path).
		if envKey != "" {
			return nil, "", fmt.Errorf("SUKKO_LICENSE_KEY is set but expired and no DB license is present; re-key to boot: %w", license.ErrLicenseExpired)
		}
		return nil, "", license.ErrNoLicense
	}
	dbMgr, err := license.NewManager(dbKey, logger)
	if err != nil {
		return nil, "", fmt.Errorf("no usable env license and DB license invalid: %w", err)
	}
	logger.Info().
		Str("edition", dbMgr.Edition().String()).
		Msg("license loaded from DB (env key absent or expired)")
	return dbMgr, dbKey, nil
}
