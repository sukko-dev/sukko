package analytics

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// realCollector is the live implementation of Collector.
type realCollector struct {
	cfg          CollectorConfig
	pool         *pgxpool.Pool
	metrics      *analyticsMetrics
	current      atomic.Pointer[counterMap]
	flushEnabled atomic.Bool
	pendingFlush chan *counterMap
	logger       zerolog.Logger
}

func newRealCollector(cfg CollectorConfig, pool *pgxpool.Pool, logger zerolog.Logger) *realCollector {
	queueCap := max(cfg.BufferSize/pendingFlushQueueDivisor, pendingFlushQueueMin)

	c := &realCollector{
		cfg:          cfg,
		pool:         pool,
		metrics:      newAnalyticsMetrics(cfg.ServicePrefix),
		pendingFlush: make(chan *counterMap, queueCap),
		logger:       logger.With().Str("component", "analytics_collector").Str("service", cfg.ServicePrefix).Logger(),
	}
	c.flushEnabled.Store(true)

	// Wire drop counter callback into counter maps.
	initial := newCounterMap(cfg.BufferSize)
	d := dropper(c.metrics.eventsDropped.Inc)
	initial.dropCounter = &d
	c.current.Store(initial)
	return c
}

// IncrementConnections records a connection delta. Hot path — must be zero-allocation.
func (c *realCollector) IncrementConnections(tenantID, transport string, delta int64) {
	if !c.flushEnabled.Load() {
		return
	}
	key := tenantKey{tenantID: tenantID, secondary: transport}
	cur := c.current.Load()
	entry := cur.getOrCreateConnection(key)
	if entry == nil {
		return // buffer full, event dropped (counter incremented in getOrCreate)
	}
	entry.active.Add(delta)
	if delta > 0 {
		entry.connects.Add(delta)
	} else if delta < 0 {
		entry.disconnects.Add(-delta)
	}
}

// IncrementMessages records message throughput. Hot path.
func (c *realCollector) IncrementMessages(tenantID, channelPrefix string, published, delivered, failed, latencyMs int64) {
	if !c.flushEnabled.Load() {
		return
	}
	// Truncate channelPrefix to first segment before "." to limit cardinality.
	if idx := strings.IndexByte(channelPrefix, '.'); idx >= 0 {
		channelPrefix = channelPrefix[:idx]
	}
	key := tenantKey{tenantID: tenantID, secondary: channelPrefix}
	cur := c.current.Load()
	entry := cur.getOrCreateMessage(key)
	if entry == nil {
		return
	}
	entry.published.Add(published)
	entry.delivered.Add(delivered)
	entry.failed.Add(failed)
	entry.totalLatencyMs.Add(latencyMs)
}

// RecordPushDelivery records a push delivery attempt. Hot path.
func (c *realCollector) RecordPushDelivery(tenantID, provider string, success, failed, expired, rateLimited, latencyMs int64) {
	if !c.flushEnabled.Load() {
		return
	}
	key := tenantKey{tenantID: tenantID, secondary: provider}
	cur := c.current.Load()
	entry := cur.getOrCreatePush(key)
	if entry == nil {
		return
	}
	total := success + failed + expired + rateLimited
	entry.sent.Add(total)
	entry.success.Add(success)
	entry.failed.Add(failed)
	entry.expired.Add(expired)
	entry.rateLimited.Add(rateLimited)
	entry.totalLatencyMs.Add(latencyMs)
}

// Start launches the flush and downgrade-poller goroutines.
// Both goroutines have logging.RecoverPanic as their FIRST defer (§V/§VII).
func (c *realCollector) Start(ctx context.Context, wg *sync.WaitGroup) error {
	// Flush goroutine
	wg.Go(func() {
		defer logging.RecoverPanic(c.logger, "analytics.flush", nil)
		c.runFlushLoop(ctx)
	})

	// Downgrade poller goroutine
	if c.cfg.EditionManager != nil {
		wg.Go(func() {
			defer logging.RecoverPanic(c.logger, "analytics.downgrade_poller", nil)
			c.runDowngradePoller(ctx)
		})
	}
	return nil
}

// Flush executes one synchronous flush. Called by the flush goroutine;
// external callers should not use this directly (see Collector interface docs).
func (c *realCollector) Flush(ctx context.Context) error {
	return c.flush(ctx)
}

// enqueuePendingOrMerge handles a failed flush snapshot: tries to enqueue it for
// later replay, discarding the oldest pending snapshot when the queue is full.
// Falls back to mergeBack if the queue is still full after eviction.
// Called on the failure path from both flush() and flushUnchecked().
func (c *realCollector) enqueuePendingOrMerge(snapshot *counterMap) {
	select {
	case c.pendingFlush <- snapshot:
	default:
		// Queue full — drop oldest and retry.
		select {
		case <-c.pendingFlush:
			c.metrics.snapshotsDropped.Inc()
		default:
		}
		select {
		case c.pendingFlush <- snapshot:
		default:
			mergeBack(c.current.Load(), snapshot)
		}
	}
}

// flushUnchecked drains the current counter map to DB without checking the edition gate.
// Used by runDowngradePoller and the shutdown path. The regular flush() checks the gate
// and would silently return nil if the edition just dropped — defeating the drain intent.
func (c *realCollector) flushUnchecked(ctx context.Context) error {
	fresh := newCounterMap(c.cfg.BufferSize)
	d := dropper(c.metrics.eventsDropped.Inc)
	fresh.dropCounter = &d
	snapshot := c.current.Swap(fresh)

	if snapshot.isEmpty() {
		return nil
	}
	c.metrics.bufferTenants.Set(float64(snapshot.totalTenants()))

	start := time.Now()
	err := batchUpsert(ctx, c.pool, snapshot, c.cfg.PodID)
	c.metrics.flushDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		c.metrics.flushErrors.Inc()
		c.enqueuePendingOrMerge(snapshot)
	}
	return err
}

// runFlushLoop is the flush goroutine body.
func (c *realCollector) runFlushLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush: current snapshot + all pending snapshots.
			// Use flushUnchecked — if shutdown arrives before the downgrade poller fires,
			// flush() would see the edition gate closed and return nil without draining.
			deadline := c.cfg.FlushDeadline
			if deadline == 0 {
				deadline = defaultFlushDeadline
			}
			// ctx is already canceled here; a fresh independent context is required for shutdown I/O.
			shutCtx, cancel := context.WithTimeout(context.Background(), deadline)
			if err := c.flushUnchecked(shutCtx); err != nil { //nolint:contextcheck // ctx canceled; shutCtx is intentionally independent
				c.logger.Warn().Err(err).Msg("analytics final flush failed on shutdown")
			}
			cancel() // release now — cannot use defer in for-select loop

			drainCtx, drainCancel := context.WithTimeout(context.Background(), deadline)
		drainLoop:
			for {
				select {
				case snapshot := <-c.pendingFlush:
					if err := batchUpsert(drainCtx, c.pool, snapshot, c.cfg.PodID); err != nil { //nolint:contextcheck // same: drainCtx is intentionally independent
						c.logger.Warn().Err(err).Msg("analytics pending flush failed on shutdown")
					}
				default:
					break drainLoop
				}
			}
			drainCancel()
			return

		case <-ticker.C:
			if err := c.flush(ctx); err != nil {
				c.logger.Warn().Err(err).Msg("analytics flush failed; counters retained for next tick")
			}
		}
	}
}

// runDowngradePoller checks the edition gate and transitions to noop on downgrade.
func (c *realCollector) runDowngradePoller(ctx context.Context) {
	poll := c.cfg.DowngradePoll
	if poll == 0 {
		poll = defaultDowngradePoll
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			gated := license.EditionHasFeature(c.cfg.EditionManager.Edition(), c.cfg.Feature)
			if !gated && c.flushEnabled.Load() {
				c.logger.Warn().
					Str("feature", string(c.cfg.Feature)).
					Msg("analytics edition gate closed; performing final flush and transitioning to noop")
				// Final flush for the current window before going dark.
				flushCtx, cancel := context.WithTimeout(ctx, defaultFlushDeadline)
				// Use flushUnchecked: flush() would return nil immediately because
				// the edition gate just closed, silently discarding accumulated data.
				if err := c.flushUnchecked(flushCtx); err != nil {
					c.logger.Warn().Err(err).Msg("analytics final flush on downgrade failed")
				}
				cancel()
				c.flushEnabled.Store(false)
			} else if gated && !c.flushEnabled.Load() {
				c.logger.Info().Str("feature", string(c.cfg.Feature)).
					Msg("analytics edition gate re-opened; resuming collection")
				c.flushEnabled.Store(true)
			}
		}
	}
}

// flush performs a single snapshot-swap + batchUpsert cycle.
func (c *realCollector) flush(ctx context.Context) error {
	// Check edition gate at flush time (not per-increment — hot path overhead).
	if c.cfg.EditionManager != nil {
		if !license.EditionHasFeature(c.cfg.EditionManager.Edition(), c.cfg.Feature) {
			return nil
		}
	}

	// Step 1: atomic snapshot-swap — new events go to fresh map immediately.
	fresh := newCounterMap(c.cfg.BufferSize)
	d := dropper(c.metrics.eventsDropped.Inc)
	fresh.dropCounter = &d
	snapshot := c.current.Swap(fresh)

	if snapshot.isEmpty() {
		return nil
	}

	// Step 2: update buffer gauge.
	c.metrics.bufferTenants.Set(float64(snapshot.totalTenants()))

	// Step 3: attempt batch upsert.
	start := time.Now()
	err := batchUpsert(ctx, c.pool, snapshot, c.cfg.PodID)
	c.metrics.flushDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		c.metrics.flushErrors.Inc()
		c.enqueuePendingOrMerge(snapshot)
		return err
	}

	// Step 4 (success): drain one pending snapshot.
	select {
	case pending := <-c.pendingFlush:
		if replayErr := batchUpsert(ctx, c.pool, pending, c.cfg.PodID); replayErr != nil {
			// Re-enqueue the pending snapshot on failure.
			select {
			case c.pendingFlush <- pending:
			default:
				c.metrics.snapshotsDropped.Inc()
			}
		}
	default:
		// No pending snapshots.
	}

	return nil
}
