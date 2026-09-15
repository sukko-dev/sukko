package provapi

import (
	"context"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

// LicenseDowngradeShutdown returns the standard OnDowngrade callback.
// When the grace period expires it logs the edition transition and cancels
// the root context, triggering graceful shutdown of the calling service.
func LicenseDowngradeShutdown(logger zerolog.Logger, cancel context.CancelFunc) func(from, to license.Edition) {
	return func(from, to license.Edition) {
		logger.Warn().
			Str("from_edition", from.String()).
			Str("to_edition", to.String()).
			Msg("license downgrade grace period expired — initiating graceful shutdown")
		cancel()
	}
}
