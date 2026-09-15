package broadcast

import (
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// NewBus creates a new Bus based on the configuration type.
// Returns an error if the configuration is invalid or connection fails.
func NewBus(cfg Config, logger zerolog.Logger) (Bus, error) {
	switch cfg.Type {
	case platform.BroadcastTypeValkey:
		return newValkeyBus(cfg, logger)
	default:
		return nil, ErrUnknownBroadcastType
	}
}
