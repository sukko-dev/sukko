package provisioning

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/analytics"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// analyticsPromoteHourInterval and analyticsPromoteDayInterval are the rollup schedules.
// Named constants per §I.
const (
	analyticsPromoteHourInterval = time.Hour
	analyticsPromoteDayInterval  = 24 * time.Hour
)

// AnalyticsManager runs rollup promotion and partition maintenance for the analytics pipeline.
// Provisioning is the only service that runs these background jobs.
// Pattern matches LifecycleManager: Start/Stop with wg.Go goroutines.
type AnalyticsManager struct {
	pm              *analytics.PartitionManager
	rm              *analytics.RollupManager
	partmanInterval time.Duration
	logger          zerolog.Logger
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// AnalyticsManagerConfig configures the AnalyticsManager.
type AnalyticsManagerConfig struct {
	PartitionManager *analytics.PartitionManager
	RollupManager    *analytics.RollupManager
	PartmanInterval  time.Duration
	Logger           zerolog.Logger
}

// NewAnalyticsManager creates an AnalyticsManager.
func NewAnalyticsManager(cfg AnalyticsManagerConfig) *AnalyticsManager {
	return &AnalyticsManager{
		pm:              cfg.PartitionManager,
		rm:              cfg.RollupManager,
		partmanInterval: cfg.PartmanInterval,
		logger:          cfg.Logger.With().Str("component", "analytics_manager").Logger(),
	}
}

// Start runs initial partition maintenance synchronously, then launches three goroutines:
//  1. Partition maintenance ticker (every partmanInterval)
//  2. Hour rollup ticker (every hour)
//  3. Day rollup ticker (every 24h)
//
// All goroutines have logging.RecoverPanic as their FIRST defer (§V/§VII).
// Errors from each tick are logged at Warn and do not kill the goroutine.
func (am *AnalyticsManager) Start(ctx context.Context) {
	mgrCtx, cancel := context.WithCancel(ctx)
	am.cancel = cancel

	// Synchronous initial run — ensures named partitions exist before any analytics writes.
	if err := am.pm.RunMaintenance(mgrCtx); err != nil {
		am.logger.Warn().Err(err).Msg("analytics partition initial maintenance failed; continuing")
	}

	am.wg.Go(func() {
		defer logging.RecoverPanic(am.logger, "analytics.partition_manager", nil)
		t := time.NewTicker(am.partmanInterval)
		defer t.Stop()
		for {
			select {
			case <-mgrCtx.Done():
				return
			case <-t.C:
				if err := am.pm.RunMaintenance(mgrCtx); err != nil {
					am.logger.Warn().Err(err).Msg("partition maintenance failed; will retry next tick")
				}
			}
		}
	})

	am.wg.Go(func() {
		defer logging.RecoverPanic(am.logger, "analytics.rollup_hour", nil)
		t := time.NewTicker(analyticsPromoteHourInterval)
		defer t.Stop()
		for {
			select {
			case <-mgrCtx.Done():
				return
			case <-t.C:
				if err := am.rm.PromoteHour(mgrCtx); err != nil {
					am.logger.Warn().Err(err).Msg("hour rollup promotion failed; will retry next tick")
				}
			}
		}
	})

	am.wg.Go(func() {
		defer logging.RecoverPanic(am.logger, "analytics.rollup_day", nil)
		t := time.NewTicker(analyticsPromoteDayInterval)
		defer t.Stop()
		for {
			select {
			case <-mgrCtx.Done():
				return
			case <-t.C:
				if err := am.rm.PromoteDay(mgrCtx); err != nil {
					am.logger.Warn().Err(err).Msg("day rollup promotion failed; will retry next tick")
				}
			}
		}
	})

	am.logger.Info().
		Dur("partman_interval", am.partmanInterval).
		Msg("Analytics manager started")
}

// Stop cancels the manager context and waits for all goroutines to exit.
func (am *AnalyticsManager) Stop() {
	if am.cancel != nil {
		am.cancel()
	}
	am.wg.Wait()
	am.logger.Info().Msg("Analytics manager stopped")
}
