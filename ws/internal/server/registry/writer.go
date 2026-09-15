package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// writerMetrics holds Prometheus metrics for the registry writer.
type writerMetrics struct {
	EventsDropped     *prometheus.CounterVec
	SubscribeCapDrops prometheus.Counter
	FlushDuration     prometheus.Histogram
	WriterRestart     prometheus.Counter
	Connections       prometheus.Gauge
}

func newWriterMetrics(reg prometheus.Registerer) *writerMetrics {
	m := &writerMetrics{
		EventsDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_registry_events_dropped_total",
			Help: "Number of registry events dropped. reason: full (channel full) or shutdown (drain timeout).",
		}, []string{"reason"}),
		SubscribeCapDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ws_registry_writer_subscribe_cap_drops_total",
			Help: "Number of channel entries dropped due to MaxRegistryChannelsPerConnection cap.",
		}),
		FlushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ws_registry_flush_duration_seconds",
			Help:    "Duration of registry writer pipeline flush cycles.",
			Buckets: prometheus.DefBuckets,
		}),
		WriterRestart: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ws_registry_writer_restart_total",
			Help: "Number of times the registry writer runOnce loop has restarted.",
		}),
		Connections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ws_registry_connections_tracked",
			Help: "Number of active connections currently registered in Valkey by this pod.",
		}),
	}
	reg.MustRegister(
		m.EventsDropped,
		m.SubscribeCapDrops,
		m.FlushDuration,
		m.WriterRestart,
		m.Connections,
	)
	return m
}

// Writer is the pod-level registry event processor. It consumes registryEvents from
// a buffered workChan, flushes them to Valkey in pipeline batches, and manages per-pod
// health metadata. Exactly one Writer runs per pod; it is shared across all shards.
//
// Deviations from history/writer.go (documented in plan.md):
//   - Hash+Set instead of Streams
//   - No distributed lock (pod owns its entries)
//   - Two secondary indexes (tenant + api_key)
//   - Health key written on each heartbeat
//   - Two-phase drop counter: Writer.pendingDrops.Swap(0) → HealthWriter.AddDrops → HealthWriter.Drops()
type Writer struct {
	cfg          *platform.ServerConfig
	valkeyClient valkey.Client
	healthWriter *HealthWriter
	logger       zerolog.Logger
	ctx          context.Context

	// workChan is allocated once in NewWriter and shared across all runOnce cycles.
	// It is NEVER closed or drained on restart — events queued during a failure survive.
	workChan chan registryEvent

	// pendingDrops counts events dropped due to workChan full since the last heartbeat.
	// Two-phase drain: Swap(0) here → AddDrops to HealthWriter → HealthWriter.Drops() for Valkey.
	pendingDrops atomic.Int64

	// ownedConnIDs tracks connIDs written by this pod for shutdown sweep and heartbeat EXPIRE.
	// Protected only by the single runOnce goroutine — no lock needed.
	ownedConnIDs map[string]connOwnership

	env     string
	metrics *writerMetrics
}

// NewWriter creates a pod-level registry Writer. It must be launched via wg.Go(w.Run).
// valkeyClient must be dedicated (not shared with the broadcast bus).
func NewWriter(
	ctx context.Context,
	cfg *platform.ServerConfig,
	valkeyClient valkey.Client,
	healthWriter *HealthWriter,
	logger zerolog.Logger,
	reg prometheus.Registerer,
) *Writer {
	return &Writer{
		cfg:          cfg,
		valkeyClient: valkeyClient,
		healthWriter: healthWriter,
		logger:       logger,
		ctx:          ctx,
		workChan:     make(chan registryEvent, cfg.ConnectionsRegistryBuffer),
		ownedConnIDs: make(map[string]connOwnership),
		env:          cfg.Environment,
		metrics:      newWriterMetrics(reg),
	}
}

// push sends a registryEvent to the writer's workChan without blocking.
// If the channel is full, the event is dropped and counted via ws_registry_events_dropped_total.
func (w *Writer) push(event registryEvent) {
	select {
	case w.workChan <- event:
	default:
		w.pendingDrops.Add(1)
		w.metrics.EventsDropped.WithLabelValues(DropReasonFull).Inc()
	}
}

// PushConnect enqueues a connect event for the given connection.
func (w *Writer) PushConnect(connID, tenantID, apiKeyID, userID, podID string, shardID int, remoteIP, transport string, connectedAt time.Time) {
	w.push(registryEvent{
		Kind:        kindConnect,
		ConnID:      connID,
		TenantID:    tenantID,
		APIKeyID:    apiKeyID,
		UserID:      userID,
		PodID:       podID,
		ShardID:     shardID,
		RemoteIP:    remoteIP,
		Transport:   transport,
		ConnectedAt: connectedAt,
	})
}

// PushDisconnect enqueues a disconnect event for the given connection.
func (w *Writer) PushDisconnect(connID, tenantID, apiKeyID string) {
	w.push(registryEvent{
		Kind:     kindDisconnect,
		ConnID:   connID,
		TenantID: tenantID,
		APIKeyID: apiKeyID,
	})
}

// PushSubscribe enqueues a subscribe event with the current channel snapshot.
func (w *Writer) PushSubscribe(connID string, channels []string, capped bool) {
	w.push(registryEvent{
		Kind:           kindSubscribe,
		ConnID:         connID,
		Channels:       channels,
		ChannelsCapped: capped,
	})
}

// PushUnsubscribe enqueues an unsubscribe event with the updated channel snapshot.
func (w *Writer) PushUnsubscribe(connID string, channels []string, capped bool) {
	w.push(registryEvent{
		Kind:           kindUnsubscribe,
		ConnID:         connID,
		Channels:       channels,
		ChannelsCapped: capped,
	})
}

// Run is the supervised restart loop. MUST be called via wg.Go.
// defer logging.RecoverPanic is the FIRST defer per §VII.
func (w *Writer) Run() {
	defer logging.RecoverPanic(w.logger, "registry_writer", nil)

	currentBackoff := w.cfg.ConnectionsRegistryRestartInitialBackoff
	for {
		if w.ctx.Err() != nil {
			return
		}
		processed, err := w.runOnce(w.ctx)
		if w.ctx.Err() != nil {
			return
		}
		if err != nil {
			w.logger.Warn().Err(err).Msg("registryWriter: runOnce exited, restarting")
			w.metrics.WriterRestart.Inc()
		}

		// Reset backoff if we processed events (indicates healthy operation before the failure).
		if processed > 0 {
			currentBackoff = w.cfg.ConnectionsRegistryRestartInitialBackoff
		} else {
			currentBackoff = min(currentBackoff*2, w.cfg.ConnectionsRegistryRestartMaxBackoff)
		}

		// Jitter: up to half the current backoff, capped at max.
		jitterBase := min(currentBackoff, w.cfg.ConnectionsRegistryRestartMaxBackoff/2)
		jitteredDelay := currentBackoff
		if ns := int64(jitterBase); ns > 0 {
			jitteredDelay += time.Duration(rand.Int64N(ns)) //nolint:gosec // G404: math/rand/v2 is sufficient for backoff jitter; crypto/rand not needed
		}
		if jitteredDelay > w.cfg.ConnectionsRegistryRestartMaxBackoff {
			jitteredDelay = w.cfg.ConnectionsRegistryRestartMaxBackoff
		}

		t := time.NewTimer(jitteredDelay)
		select {
		case <-t.C:
		case <-w.ctx.Done():
			t.Stop()
			return
		}
	}
}

// runOnce executes one lifecycle of the writer: processes events until ctx is canceled or an error occurs.
// Returns (number of events processed, error).
func (w *Writer) runOnce(ctx context.Context) (processed int, err error) {
	flushTicker := time.NewTicker(w.cfg.ConnectionsRegistryFlushInterval)
	defer flushTicker.Stop()
	heartbeatTicker := time.NewTicker(w.cfg.ConnectionsRegistryHeartbeatInterval)
	defer heartbeatTicker.Stop()

	var batch []registryEvent

	for {
		select {
		case <-ctx.Done():
			// Drain with timeout on clean shutdown.
			if w.cfg.ConnectionsRegistryShutdownDrainTimeout > 0 {
				processed += w.drainWithTimeout() //nolint:contextcheck // drainWithTimeout and shutdownSweep use context.Background() intentionally — server ctx is already canceled
			}
			w.shutdownSweep() //nolint:contextcheck // uses context.Background() intentionally — server ctx is already canceled
			return processed, nil

		case event := <-w.workChan:
			batch = append(batch, event)
			processed++

		case <-flushTicker.C:
			if len(batch) > 0 {
				if err := w.flushBatch(ctx, batch); err != nil {
					return processed, fmt.Errorf("registryWriter: flushBatch: %w", err)
				}
				batch = batch[:0]
			}

		case <-heartbeatTicker.C:
			// Flush any pending batch first so heartbeat EXPIRE covers newly-added keys.
			if len(batch) > 0 {
				if err := w.flushBatch(ctx, batch); err != nil {
					return processed, fmt.Errorf("registryWriter: flushBatch before heartbeat: %w", err)
				}
				batch = batch[:0]
			}
			if err := w.handleHeartbeat(ctx); err != nil {
				return processed, fmt.Errorf("registryWriter: heartbeat: %w", err)
			}
		}
	}
}

// drainWithTimeout drains remaining workChan events for up to ShutdownDrainTimeout.
func (w *Writer) drainWithTimeout() int {
	drainCtx, cancel := context.WithTimeout(context.Background(), w.cfg.ConnectionsRegistryShutdownDrainTimeout)
	defer cancel()

	var batch []registryEvent
	processed := 0
	for {
		select {
		case event := <-w.workChan:
			batch = append(batch, event)
			processed++
		case <-drainCtx.Done():
			// Count undrained events as shutdown drops.
			remaining := len(w.workChan)
			if remaining > 0 {
				w.metrics.EventsDropped.WithLabelValues(DropReasonShutdown).Add(float64(remaining))
			}
			// Flush the partial batch with a fresh context — ctx is already canceled (shutdown).
			if len(batch) > 0 {
				w.finalDrainFlush(batch)
			}
			return processed
		}
	}
}

// finalDrainFlush flushes a partial batch using a fresh context after the shutdown drain timeout.
// Called once on clean shutdown; extracted to avoid defer-in-loop linter warnings.
func (w *Writer) finalDrainFlush(batch []registryEvent) {
	flushCtx, flushCancel := context.WithTimeout(context.Background(), drainFinalFlushTimeout)
	defer flushCancel()
	if err := w.flushBatch(flushCtx, batch); err != nil {
		w.logger.Warn().Err(err).Int("batch_size", len(batch)).Msg("registryWriter: final drain flush failed")
		w.metrics.EventsDropped.WithLabelValues(DropReasonShutdown).Add(float64(len(batch)))
	}
}

// flushBatch writes a batch of registry events to Valkey via pipeline.
func (w *Writer) flushBatch(ctx context.Context, batch []registryEvent) error {
	start := time.Now()
	defer func() {
		w.metrics.FlushDuration.Observe(time.Since(start).Seconds())
	}()

	var cmds []valkey.Completed
	ttlSecs := int64(w.cfg.ConnectionsRegistryTTL.Seconds())

	for _, ev := range batch {
		connKey := ConnKey(w.env, ev.ConnID)

		switch ev.Kind {
		case kindConnect:
			// Build channel JSON, capping at MaxRegistryChannelsPerConnection.
			channels := ev.Channels
			channelsCapped := ev.ChannelsCapped
			if len(channels) > platform.MaxRegistryChannelsPerConnection {
				channels = channels[:platform.MaxRegistryChannelsPerConnection]
				channelsCapped = true
				w.metrics.SubscribeCapDrops.Add(float64(len(ev.Channels) - platform.MaxRegistryChannelsPerConnection))
			}
			chanJSON, _ := json.Marshal(channels) // []string: marshal cannot error; channels is always non-nil here
			cappedStr := ""
			if channelsCapped {
				cappedStr = "1"
			}

			cmds = append(cmds,
				w.valkeyClient.B().Hset().Key(connKey).
					FieldValue().
					FieldValue(FieldTenantID, ev.TenantID).
					FieldValue(FieldAPIKeyID, ev.APIKeyID).
					FieldValue(FieldUserID, ev.UserID).
					FieldValue(FieldPodID, ev.PodID).
					FieldValue(FieldShardID, strconv.Itoa(ev.ShardID)).
					FieldValue(FieldRemoteIP, ev.RemoteIP).
					FieldValue(FieldTransport, ev.Transport).
					FieldValue(FieldConnectedAt, ev.ConnectedAt.UTC().Format(time.RFC3339)).
					FieldValue(FieldChannels, string(chanJSON)).
					FieldValue(FieldChannelsCapped, cappedStr).
					Build(),
				w.valkeyClient.B().Expire().Key(connKey).Seconds(ttlSecs).Build(),
				w.valkeyClient.B().Sadd().Key(TenantIdxKey(w.env, ev.TenantID)).Member(ev.ConnID).Build(),
				w.valkeyClient.B().Expire().Key(TenantIdxKey(w.env, ev.TenantID)).Seconds(ttlSecs).Build(),
			)
			if ev.APIKeyID != "" {
				cmds = append(cmds,
					w.valkeyClient.B().Sadd().Key(APIKeyIdxKey(w.env, ev.TenantID, ev.APIKeyID)).Member(ev.ConnID).Build(),
					w.valkeyClient.B().Expire().Key(APIKeyIdxKey(w.env, ev.TenantID, ev.APIKeyID)).Seconds(ttlSecs).Build(),
				)
			}
			w.ownedConnIDs[ev.ConnID] = connOwnership{tenantID: ev.TenantID, apiKeyID: ev.APIKeyID}
			w.metrics.Connections.Inc()

		case kindDisconnect:
			info, ok := w.ownedConnIDs[ev.ConnID]
			tenantID := ev.TenantID
			apiKeyID := ev.APIKeyID
			if ok {
				// Prefer stored values — they match what was used in the SADD at connect time.
				// ev.TenantID / ev.APIKeyID are fallback for conns not owned by this writer
				// (e.g., cross-restart recovery where the connect event was processed elsewhere).
				tenantID = info.tenantID
				apiKeyID = info.apiKeyID
			}
			cmds = append(cmds,
				w.valkeyClient.B().Del().Key(connKey).Build(),
				w.valkeyClient.B().Srem().Key(TenantIdxKey(w.env, tenantID)).Member(ev.ConnID).Build(),
			)
			if apiKeyID != "" {
				cmds = append(cmds,
					w.valkeyClient.B().Srem().Key(APIKeyIdxKey(w.env, tenantID, apiKeyID)).Member(ev.ConnID).Build(),
				)
			}
			delete(w.ownedConnIDs, ev.ConnID)
			// Only decrement the gauge when this writer actually tracked the connect.
			// If the connect event was dropped (workChan full), ok==false and Inc() never
			// fired — unconditional Dec() would drive the gauge negative.
			if ok {
				w.metrics.Connections.Dec()
			}

		case kindSubscribe, kindUnsubscribe:
			if _, owned := w.ownedConnIDs[ev.ConnID]; !owned {
				continue
			}
			channels := ev.Channels
			channelsCapped := ev.ChannelsCapped
			if len(channels) > platform.MaxRegistryChannelsPerConnection {
				channels = channels[:platform.MaxRegistryChannelsPerConnection]
				channelsCapped = true
				w.metrics.SubscribeCapDrops.Add(float64(len(ev.Channels) - platform.MaxRegistryChannelsPerConnection))
			}
			chanJSON, _ := json.Marshal(channels) // []string: marshal cannot error; channels is always non-nil here
			cappedStr := ""
			if channelsCapped {
				cappedStr = "1"
			}
			cmds = append(cmds,
				w.valkeyClient.B().Hset().Key(connKey).
					FieldValue().
					FieldValue(FieldChannels, string(chanJSON)).
					FieldValue(FieldChannelsCapped, cappedStr).
					Build(),
			)
		}
	}

	if len(cmds) == 0 {
		return nil
	}

	results := w.valkeyClient.DoMulti(ctx, cmds...)
	var firstErr error
	for i, res := range results {
		if err := res.Error(); err != nil {
			w.logger.Warn().Err(err).Int("cmd_index", i).Msg("registryWriter: pipeline command failed")
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// handleHeartbeat renews TTLs on all owned keys and writes the pod health hash.
func (w *Writer) handleHeartbeat(ctx context.Context) error {
	ttlSecs := int64(w.cfg.ConnectionsRegistryTTL.Seconds())
	// Renew TTL slightly beyond heartbeat interval so we don't race with expiry.
	idxTTL := int64(w.cfg.ConnectionsRegistryTTL.Seconds()) + int64(w.cfg.ConnectionsRegistryHeartbeatInterval.Seconds())

	var cmds []valkey.Completed
	seen := make(map[string]struct{})
	seenAPI := make(map[string]struct{})
	for connID, info := range w.ownedConnIDs {
		cmds = append(cmds,
			w.valkeyClient.B().Expire().Key(ConnKey(w.env, connID)).Seconds(ttlSecs).Build(),
		)
		if _, ok := seen[info.tenantID]; !ok {
			seen[info.tenantID] = struct{}{}
			cmds = append(cmds,
				w.valkeyClient.B().Expire().Key(TenantIdxKey(w.env, info.tenantID)).Seconds(idxTTL).Build(),
			)
		}
		if info.apiKeyID != "" {
			idxKey := APIKeyIdxKey(w.env, info.tenantID, info.apiKeyID)
			if _, ok := seenAPI[idxKey]; !ok {
				seenAPI[idxKey] = struct{}{}
				cmds = append(cmds,
					w.valkeyClient.B().Expire().Key(idxKey).Seconds(idxTTL).Build(),
				)
			}
		}
	}

	// Two-phase drop counter drain.
	captured := w.pendingDrops.Swap(0) // Step 1: atomically capture Writer drops
	w.healthWriter.AddDrops(captured)  // Step 2: feed into HealthWriter accumulator
	allDrops := w.healthWriter.Drops() // Step 3: Swap(0) on HealthWriter

	// Write health key.
	adminHealthyStr := ""
	if w.healthWriter.AdminHealthy() {
		adminHealthyStr = HealthValueTrue
	}
	healthKey := HealthKey(w.env, w.cfg.PodID)
	cmds = append(cmds,
		w.valkeyClient.B().Hset().Key(healthKey).
			FieldValue().
			FieldValue(HealthFieldDrops, strconv.FormatInt(allDrops, 10)).
			FieldValue(HealthFieldLastHeartbeat, time.Now().UTC().Format(time.RFC3339)).
			FieldValue(HealthFieldAdminChannelHealthy, adminHealthyStr).
			Build(),
		w.valkeyClient.B().Expire().Key(healthKey).Seconds(idxTTL).Build(),
	)

	results := w.valkeyClient.DoMulti(ctx, cmds...)
	var firstErr error
	for i, res := range results {
		if err := res.Error(); err != nil {
			w.logger.Warn().Err(err).Int("cmd_index", i).Msg("registryWriter: heartbeat command failed")
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

const (
	// shutdownSweepTimeout is the maximum time allowed for the clean-shutdown Valkey pipeline.
	shutdownSweepTimeout = 5 * time.Second
	// drainFinalFlushTimeout is the time allowed to flush a partial batch after drain timeout.
	// Uses context.Background() because the server context is already canceled at this point.
	drainFinalFlushTimeout = 2 * time.Second
)

// shutdownSweep DELs all owned connection hashes, SREMs from both tenant and API key indexes,
// and decrements the connections gauge for each entry.
// Called on clean shutdown to prevent stale entries surviving until TTL expiry.
func (w *Writer) shutdownSweep() {
	if len(w.ownedConnIDs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownSweepTimeout)
	defer cancel()

	var cmds []valkey.Completed
	for connID, info := range w.ownedConnIDs {
		cmds = append(cmds,
			w.valkeyClient.B().Del().Key(ConnKey(w.env, connID)).Build(),
			w.valkeyClient.B().Srem().Key(TenantIdxKey(w.env, info.tenantID)).Member(connID).Build(),
		)
		if info.apiKeyID != "" {
			cmds = append(cmds,
				w.valkeyClient.B().Srem().Key(APIKeyIdxKey(w.env, info.tenantID, info.apiKeyID)).Member(connID).Build(),
			)
		}
		w.metrics.Connections.Dec()
	}
	results := w.valkeyClient.DoMulti(ctx, cmds...)
	for i, res := range results {
		if err := res.Error(); err != nil {
			w.logger.Warn().Err(err).Int("cmd_index", i).Msg("registryWriter: shutdown sweep command failed")
		}
	}
	clear(w.ownedConnIDs)
}
