package history

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// Writer subscribes to the broadcast bus and writes every received message to a
// per-channel Valkey Stream (XADD MAXLEN ~). Exactly one pod in the cluster holds the
// active-writer lock at a time; all other pods run as passive observers that skip XADD
// but still participate in lock contention so they can take over on failover.
type Writer struct {
	cfg            *platform.ServerConfig
	bus            broadcast.Bus
	valkeyClient   valkey.Client
	logger         zerolog.Logger
	ctx            context.Context
	isActiveWriter atomic.Bool

	env string
	// workChan is intentionally shared across runOnce restarts: messages queued
	// during a failed run are picked up by the next run's flushBatch, preserving
	// delivery continuity without losing messages at the restart boundary.
	workChan   chan *broadcast.Message
	notifyChan chan struct{}
	metrics    *Metrics
}

// ValkeyClient returns the dedicated Streams client. Used by history delivery handlers.
func (h *Writer) ValkeyClient() valkey.Client { return h.valkeyClient }

// Metrics returns the history metrics for use by delivery handlers.
func (h *Writer) Metrics() *Metrics { return h.metrics }

// Close closes the underlying Valkey client, releasing its connections and background goroutines.
// Must be called after Run() has returned.
func (h *Writer) Close() { h.valkeyClient.Close() }

// NewWriter creates a Writer. It registers Prometheus metrics against reg.
// Production callers pass prometheus.DefaultRegisterer; tests pass an isolated registry.
// valkeyClient is a dedicated Streams client, separate from the broadcast bus client.
func NewWriter(
	parentCtx context.Context,
	cfg *platform.ServerConfig,
	bus broadcast.Bus,
	valkeyClient valkey.Client,
	logger zerolog.Logger,
	reg prometheus.Registerer,
	env string,
) *Writer {
	h := &Writer{
		cfg:          cfg,
		bus:          bus,
		valkeyClient: valkeyClient,
		logger:       logger,
		ctx:          parentCtx,
		env:          env,
		workChan:     make(chan *broadcast.Message, cfg.HistoryWriterBuffer),
		notifyChan:   make(chan struct{}, 1),
		metrics:      newMetrics(reg),
	}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "ws_history_writer_buffer_depth",
		Help: "Current number of messages queued in the history writer work channel.",
	}, func() float64 { return float64(len(h.workChan)) }))

	// Reflect whether the bus type precludes Kafka fallback.
	if cfg.BroadcastType == platform.BroadcastTypeValkey {
		h.metrics.ValkeyBusKafkaFallbackDisabled.Set(1)
	}
	return h
}

// Run is the supervised restart loop. It should be launched via wg.Go.
// It exits only when h.ctx is canceled (server shutdown).
func (h *Writer) Run() {
	defer logging.RecoverPanic(h.logger, "historyWriter.Run", nil)

	currentBackoff := h.cfg.HistoryWriterRestartInitialBackoff
	for {
		if h.ctx.Err() != nil {
			return
		}

		processed, err := h.runOnce(h.ctx)

		if h.ctx.Err() != nil {
			return
		}

		if err != nil {
			h.logger.Warn().Err(err).Msg("historyWriter: runOnce exited with error, restarting")
		}

		h.metrics.WriterRestartTotal.Inc()

		if processed > 0 {
			// Reset backoff after a run that did useful work — retry promptly.
			currentBackoff = h.cfg.HistoryWriterRestartInitialBackoff
		} else {
			// No progress: double backoff, capped.
			currentBackoff *= 2
			if currentBackoff > h.cfg.HistoryWriterRestartMaxBackoff {
				currentBackoff = h.cfg.HistoryWriterRestartMaxBackoff
			}
		}

		// Jitter: use half-capped base so jitter has headroom below maxBackoff.
		// Apply jitter before capping so the random component is not immediately
		// nullified when currentBackoff == maxBackoff (thundering herd at saturation).
		jitterBase := min(currentBackoff, h.cfg.HistoryWriterRestartMaxBackoff/2)
		jitteredDelay := currentBackoff
		if ns := int64(jitterBase); ns > 0 {
			jitteredDelay += time.Duration(rand.Int64N(ns)) //nolint:gosec // G404: math/rand/v2 is sufficient for backoff jitter; cryptographic randomness is not required
		}
		if jitteredDelay > h.cfg.HistoryWriterRestartMaxBackoff {
			jitteredDelay = h.cfg.HistoryWriterRestartMaxBackoff
		}

		select {
		case <-time.After(jitteredDelay):
			h.logger.Warn().Dur("backoff", jitteredDelay).Msg("historyWriter: restarting after delay")
		case <-h.ctx.Done():
			return
		}
	}
}

// runOnce runs the writer until the context is canceled or a fatal error occurs.
// Returns the number of messages successfully written to Valkey.
func (h *Writer) runOnce(parentCtx context.Context) (processed int, _ error) {
	// pos1: panic recovery — runs last (LIFO), catches anything in between.
	defer logging.RecoverPanic(h.logger, "historyWriter.runOnce", nil)

	// Acquire lock before subscribing so we know our role before messages arrive.
	// parentCtx is sufficient here — lock acquisition is a single SET NX command.
	lockKey := HistoryWriterLockKeyPrefix + h.env
	h.tryAcquireLock(parentCtx, lockKey)

	// Subscribe to broadcast bus — all-tenant PSUBSCRIBE channel.
	// Done before context creation so a SubscribeAll error doesn't leak the cancel function.
	busCh, err := h.bus.SubscribeAll()
	if err != nil {
		return 0, fmt.Errorf("historyWriter: SubscribeAll failed: %w", err)
	}

	// Context for this run's goroutines (relay, etc.). Created after SubscribeAll so
	// any early return from SubscribeAll does not create a leaked context.
	runOnceCtx, runOnceCancelFn := context.WithCancel(parentCtx)

	ticker := time.NewTicker(h.cfg.HistoryWriterHeartbeatInterval)

	var relayWg sync.WaitGroup

	// Defer order (LIFO execution is pos5→pos4→pos3→pos2→pos1):
	// pos2 — unsubscribe and release distributed lock (after relay exits, no more readers on busCh)
	defer func() { //nolint:contextcheck // runOnceCtx is already canceled when this defer runs; context.Background() is required for the lock release call
		if err := h.bus.UnsubscribeAll(busCh); err != nil {
			h.logger.Warn().Err(err).Msg("historyWriter: unsubscribe error on runOnce exit")
		}
		// Release the writer lock on clean shutdown so the next pod can acquire it
		// immediately rather than waiting for TTL expiry. Only attempt if we own the lock.
		if h.isActiveWriter.Load() {
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), h.cfg.HistoryValkeyCmdTimeout)
			defer releaseCancel()
			lockKey := HistoryWriterLockKeyPrefix + h.env
			if err := h.valkeyClient.Do(releaseCtx,
				h.valkeyClient.B().Eval().Script(historyWriterLockReleaseLuaScript).Numkeys(1).
					Key(lockKey).Arg(h.cfg.PodID).Build(),
			).Error(); err != nil {
				h.logger.Warn().Err(err).Msg("historyWriter: lock release error on shutdown")
			}
		}
		h.isActiveWriter.Store(false)
		h.metrics.WriterActive.Set(0)
	}()
	// pos3 — stop ticker
	defer ticker.Stop()
	// pos4 — wait for relay goroutine to exit before unsubscribing
	defer relayWg.Wait()
	// pos5 — cancel relay context first so relay exits
	defer runOnceCancelFn()
	// (pos1 RecoverPanic was declared first, will run last)

	// Relay goroutine: forward bus messages to workChan without blocking the bus fanout.
	relayWg.Go(func() {
		defer logging.RecoverPanic(h.logger, "historyWriter.relay", nil)
		for {
			select {
			case msg := <-busCh:
				select {
				case h.workChan <- msg:
					// Non-blocking signal: wake main loop.
					select {
					case h.notifyChan <- struct{}{}:
					default:
					}
				default:
					h.metrics.WriteDropped.WithLabelValues(HistoryDropReasonFull).Inc()
				}
			case <-runOnceCtx.Done():
				return
			}
		}
	})

	var consecutiveLockFailures int
	for {
		select {
		case <-runOnceCtx.Done():
			return processed, nil

		case <-ticker.C:
			if err := h.handleHeartbeatTick(runOnceCtx, lockKey, &consecutiveLockFailures); err != nil {
				return processed, err
			}
			// Flush any residual messages on heartbeat tick as well.
			processed += h.flushBatch(runOnceCtx)

		case <-h.notifyChan:
			processed += h.flushBatch(runOnceCtx)
		}
	}
}

// tryAcquireLock attempts to acquire the distributed writer lock via SET NX.
// It sets h.isActiveWriter accordingly. Errors are logged but do not abort startup.
func (h *Writer) tryAcquireLock(ctx context.Context, lockKey string) {
	result := h.valkeyClient.Do(ctx,
		h.valkeyClient.B().Set().Key(lockKey).Value(h.cfg.PodID).Nx().
			PxMilliseconds(h.cfg.HistoryWriterLockTTLMs).Build(),
	)
	err := result.Error()
	if err == nil {
		// Acquired.
		h.isActiveWriter.Store(true)
		h.metrics.WriterActive.Set(1)
		h.logger.Info().Str("lock", lockKey).Msg("historyWriter: acquired active writer lock")
		return
	}
	if ve, ok := errors.AsType[*valkey.ValkeyError](err); ok && ve.IsNil() {
		// Another pod already holds the lock — run as passive.
		h.isActiveWriter.Store(false)
		h.metrics.WriterActive.Set(0)
		return
	}
	// Actual Valkey error — treat as passive to avoid split-brain.
	h.logger.Warn().Err(err).Msg("historyWriter: lock acquisition error, running passive")
	h.isActiveWriter.Store(false)
	h.metrics.WriterActive.Set(0)
}

// handleHeartbeatTick renews or attempts to acquire the writer lock on each tick.
// It also checks bus health and returns an error if the writer should restart.
func (h *Writer) handleHeartbeatTick(ctx context.Context, lockKey string, failures *int) error {
	// Restart gate: PUBLISH health only, deliberately not bus.IsHealthy()
	// (ADR-0016). IsHealthy also folds in subscription convergence, which
	// (a) reads transiently false after every first-subscribe/last-unsubscribe
	// on the pod, so sampling it here would flap the writer under ordinary
	// tenant churn, and (b) is the convergence loop's job to repair — a writer
	// restart cannot help it, and its only effect would be UnsubscribeAll →
	// backoff → SubscribeAll: a self-inflicted PSUBSCRIBE gap. A dead publish
	// path is the signal that the writer itself cannot do its job.
	if !h.bus.GetMetrics().PublishHealthy {
		return errors.New("historyWriter: broadcast bus publish path is unhealthy, triggering restart")
	}

	lockTTLStr := strconv.FormatInt(h.cfg.HistoryWriterLockTTLMs, 10)

	if h.isActiveWriter.Load() {
		// Renew the lock via CAS Lua: only renews if we still own it.
		result := h.valkeyClient.Do(ctx,
			h.valkeyClient.B().Eval().Script(historyWriterLockRenewLuaScript).Numkeys(1).
				Key(lockKey).Arg(h.cfg.PodID, lockTTLStr).Build(),
		)
		if err := result.Error(); err != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // context canceled during shutdown: Valkey error is spurious, clean exit
			}
			h.metrics.LockFailuresTotal.Inc()
			*failures++
			h.logger.Warn().Err(err).Int("failures", *failures).Msg("historyWriter: lock renewal error")
			if *failures >= h.cfg.HistoryMaxConsecutiveLockFailures {
				return fmt.Errorf("historyWriter: %d consecutive lock failures, restarting", *failures)
			}
			return nil
		}
		n, _ := result.AsInt64()
		if n == 0 {
			// CAS miss: another pod acquired the lock — flip to passive.
			h.isActiveWriter.Store(false)
			h.metrics.WriterActive.Set(0)
			*failures = 0
			h.logger.Info().Msg("historyWriter: lost writer lock, switching to passive")
		} else {
			*failures = 0
		}
		return nil
	}

	// Passive pod: attempt to acquire the lock.
	result := h.valkeyClient.Do(ctx,
		h.valkeyClient.B().Set().Key(lockKey).Value(h.cfg.PodID).Nx().
			PxMilliseconds(h.cfg.HistoryWriterLockTTLMs).Build(),
	)
	if err := result.Error(); err != nil {
		if ve, ok := errors.AsType[*valkey.ValkeyError](err); ok && ve.IsNil() {
			// Key still exists — another pod is active. Not an error.
			return nil
		}
		if ctx.Err() != nil {
			return nil //nolint:nilerr // context canceled during shutdown: Valkey error is spurious, clean exit
		}
		h.metrics.LockFailuresTotal.Inc()
		*failures++
		h.logger.Warn().Err(err).Int("failures", *failures).Msg("historyWriter: lock acquisition error")
		if *failures >= h.cfg.HistoryMaxConsecutiveLockFailures {
			return fmt.Errorf("historyWriter: %d consecutive lock failures, restarting", *failures)
		}
		return nil
	}
	// Acquired.
	*failures = 0
	h.isActiveWriter.Store(true)
	h.metrics.WriterActive.Set(1)
	h.logger.Info().Str("lock", lockKey).Msg("historyWriter: acquired active writer lock")
	return nil
}

// flushBatch drains up to cfg.WriterPipelineBatch messages from workChan and writes
// each to its Valkey Stream. Returns the number of messages successfully pipelined.
func (h *Writer) flushBatch(ctx context.Context) int {
	wasActive := h.isActiveWriter.Load()

	msgs := make([]*broadcast.Message, 0, h.cfg.HistoryWriterPipelineBatch)
	for range h.cfg.HistoryWriterPipelineBatch {
		select {
		case msg := <-h.workChan:
			msgs = append(msgs, msg)
		default:
			// Drained.
			goto drained
		}
	}
drained:

	if len(msgs) == 0 {
		return 0
	}

	if !wasActive {
		h.metrics.WriteDropped.WithLabelValues(HistoryDropReasonPassive).Add(float64(len(msgs)))
		return 0
	}

	ttlSecs := int64(h.cfg.HistoryTTL.Seconds())
	depthStr := strconv.Itoa(h.cfg.HistoryBufferDepth)

	execCtx, execCancel := context.WithTimeout(ctx, h.cfg.HistoryValkeyCmdTimeout)
	defer execCancel()

	var cmds []valkey.Completed
	validIdxs := make([]int, 0, len(msgs)) // tracks which msgs produced commands
	for i, msg := range msgs {
		if msg.TenantID == "" || msg.Subject == "" || msg.Channel == "" {
			h.metrics.WriteDropped.WithLabelValues(HistoryDropReasonEmptyMeta).Inc()
			continue
		}
		streamKey := StreamKey(h.env, msg.TenantID, msg.Channel)

		// Always use Valkey auto-ID ("*") so the stream maintains a strictly-increasing ID
		// sequence regardless of Kafka partition ordering. The Kafka pos is stored in the
		// coord field so the delivery loop can attach it as a replay cursor.
		coordVal := HistoryCoordAuto
		if msg.Pos != "" {
			coordVal = msg.Pos
		}

		xaddFields := h.valkeyClient.B().Xadd().Key(streamKey).
			Maxlen().Almost().Threshold(depthStr).
			Id("*").
			FieldValue().
			FieldValue(HistoryFieldPayload, string(msg.Payload)).
			FieldValue(HistoryFieldTenantID, msg.TenantID).
			FieldValue(HistoryFieldChannel, msg.Channel).
			FieldValue(HistoryFieldSubject, msg.Subject).
			FieldValue(HistoryFieldCoord, coordVal)
		if msg.Mid != "" {
			// omitempty semantics: mid-less messages (rolling deploy) store no field.
			xaddFields = xaddFields.FieldValue(HistoryFieldMid, msg.Mid)
		}
		xaddCmd := xaddFields.Build()

		expireCmd := h.valkeyClient.B().Expire().Key(streamKey).Seconds(ttlSecs).Build()

		cmds = append(cmds, xaddCmd, expireCmd)
		validIdxs = append(validIdxs, i)
	}

	if len(cmds) == 0 {
		return 0
	}

	results := h.valkeyClient.DoMulti(execCtx, cmds...)

	processed := 0
	for j, origIdx := range validIdxs {
		xaddRes := results[j*2]
		expireRes := results[j*2+1]

		if err := xaddRes.Error(); err != nil {
			h.logger.Warn().Err(err).Str("topic", msgs[origIdx].Channel).Msg("historyWriter: XADD failed")
			continue
		}
		if err := expireRes.Error(); err != nil {
			h.metrics.ExpireFailure.Inc()
			h.logger.Warn().Err(err).Str("topic", msgs[origIdx].Channel).Msg("historyWriter: EXPIRE failed")
		}
		processed++
	}

	return processed
}
