package worker

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/routing"
	"github.com/sukko-dev/sukko/internal/shared/types"
	webhookconst "github.com/sukko-dev/sukko/internal/webhook"
)

// intakeChannelMultiplier is the multiplier for the fresh-event intake channel buffer.
// intake size = WorkerConcurrency × intakeChannelMultiplier (§I: magic number must be a named constant).
const intakeChannelMultiplier = 2

// statusUpdateRequest is sent by delivery goroutines to the asyncStatusUpdater.
type statusUpdateRequest struct {
	webhookID  string
	tenantID   string
	status     string
	retryCount int
}

// degradedTimerRequest is sent to the degradedScheduler to register a new timer.
type degradedTimerRequest struct {
	webhookID string
	tenantID  string
	nextAt    time.Time
}

// degradedEntry is an element in the degradedScheduler's min-heap.
type degradedEntry struct {
	webhookID string
	tenantID  string
	nextAt    time.Time
	index     int
}

// degradedHeap implements heap.Interface for degradedEntry keyed on nextAt.
type degradedHeap []*degradedEntry

func (h degradedHeap) Len() int           { return len(h) }
func (h degradedHeap) Less(i, j int) bool { return h[i].nextAt.Before(h[j].nextAt) }
func (h degradedHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *degradedHeap) Push(x any) {
	e := x.(*degradedEntry) //nolint:errcheck // heap.Interface: only *degradedEntry is ever pushed
	e.index = len(*h)
	*h = append(*h, e)
}
func (h *degradedHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

// Config holds all injectable dependencies for the Runner.
type Config struct {
	// Injectable for testing — nil uses defaults.
	Clock         Clock
	RetrySchedule []time.Duration // nil → webhookconst.WebhookRetrySchedule[:]

	// Sizing
	WorkerConcurrency int
	RetryQueueSize    int
	DeliveryTimeout   time.Duration
	CacheTTL          time.Duration

	// Dependencies
	ProvisioningClient ProvisioningClient
	Subscriber         Subscriber
	// InvalidationCh is a channel that delivers tenant IDs needing a cache refresh.
	// The sender (wiring) subscribes to Valkey PSUBSCRIBE and extracts the tenantID
	// from the channel name. If nil, cache invalidation falls back to TTL-only.
	InvalidationCh <-chan string
	Cache          *WebhookCache
	Deliverer      *Deliverer
	Logger         zerolog.Logger
}

// Runner is the main webhook delivery engine.
// Goroutines: eventConsumer, ttlRefresher, invalidationConsumer (via Subscriber),
// degradedScheduler, deliveryWorkers (×WorkerConcurrency), asyncStatusUpdater.
type Runner struct {
	cfg           Config
	intake        chan DeliveryTask         // fresh events; bounded at WorkerConcurrency × intakeChannelMultiplier
	retryQueue    chan DeliveryTask         // retries only; bounded at RetryQueueSize
	statusUpdates chan statusUpdateRequest  // async status updates; bounded at WorkerConcurrency
	degradedCh    chan degradedTimerRequest // new degraded timers; bounded at RetryQueueSize
	dropMu        sync.Mutex                // serializes dropOldest drain/refill to prevent concurrent-sender race
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	clock         Clock
	retrySchedule []time.Duration
}

// NewRunner creates a Runner. Call Run() to start goroutines.
func NewRunner(cfg Config) (*Runner, error) {
	if cfg.ProvisioningClient == nil {
		return nil, errors.New("runner: ProvisioningClient is required")
	}
	if cfg.Subscriber == nil {
		return nil, errors.New("runner: Subscriber is required")
	}
	if cfg.Cache == nil {
		return nil, errors.New("runner: Cache is required")
	}
	if cfg.Deliverer == nil {
		return nil, errors.New("runner: Deliverer is required")
	}
	if cfg.WorkerConcurrency < 1 {
		return nil, errors.New("runner: WorkerConcurrency must be >= 1")
	}
	if cfg.RetryQueueSize < 1 {
		return nil, errors.New("runner: RetryQueueSize must be >= 1")
	}

	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	retrySchedule := cfg.RetrySchedule
	if retrySchedule == nil {
		s := webhookconst.WebhookRetrySchedule
		retrySchedule = s[:]
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Runner{
		cfg:           cfg,
		intake:        make(chan DeliveryTask, cfg.WorkerConcurrency*intakeChannelMultiplier),
		retryQueue:    make(chan DeliveryTask, cfg.RetryQueueSize),
		statusUpdates: make(chan statusUpdateRequest, cfg.WorkerConcurrency),
		degradedCh:    make(chan degradedTimerRequest, cfg.RetryQueueSize),
		ctx:           ctx,
		cancel:        cancel,
		clock:         clock,
		retrySchedule: retrySchedule,
	}, nil
}

// Run starts all goroutines and blocks until they complete.
// Returns after ctx is canceled and all goroutines exit.
func (r *Runner) Run(ctx context.Context) error {
	// Override internal context with the provided one.
	r.ctx, r.cancel = context.WithCancel(ctx)

	// Initial cache hydration — must succeed before subscribing.
	degraded, err := r.cfg.Cache.Hydrate(ctx)
	if err != nil {
		return fmt.Errorf("initial cache hydration: %w", err)
	}

	r.wg.Go(r.eventConsumer)
	r.wg.Go(r.ttlRefresher)
	r.wg.Go(r.asyncStatusUpdater)
	r.wg.Go(func() { r.degradedScheduler(degraded) })
	if r.cfg.InvalidationCh != nil {
		r.wg.Go(r.invalidationConsumer)
	}

	// Launch delivery workers.
	for range r.cfg.WorkerConcurrency {
		r.wg.Go(r.deliveryWorker)
	}

	r.wg.Wait()
	return nil
}

// Stop cancels the runner context.
func (r *Runner) Stop() {
	r.cancel()
}

// HealthStatus describes the current health of the runner.
type HealthStatus struct {
	GRPCUp               bool
	ValkeySubscriptionUp bool
	DegradedWebhookCount int
}

// Health returns current health state for the /health endpoint.
func (r *Runner) Health() HealthStatus {
	// Simplified health: always report gRPC up until we add probing.
	// Valkey subscription health tracked separately (future: store in atomic bool).
	return HealthStatus{GRPCUp: true, ValkeySubscriptionUp: true}
}

// --- Goroutines ---

// eventConsumer reads from the broadcast bus and enqueues matching webhooks to intake.
func (r *Runner) eventConsumer() {
	defer logging.RecoverPanic(r.cfg.Logger, "eventConsumer", nil)
	msgs := r.cfg.Subscriber.Messages()
	for {
		select {
		case <-r.ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			records := r.cfg.Cache.Get(msg.TenantID)
			for _, rec := range records {
				matched, err := routing.MatchRoutingPattern(rec.ChannelPattern, msg.Channel)
				if err != nil || !matched {
					continue
				}
				task := DeliveryTask{
					WebhookID:  rec.ID,
					TenantID:   rec.TenantID,
					URL:        rec.URL,
					SecretEnc:  rec.SecretEnc,
					Payload:    msg.Payload,
					Attempt:    1,
					MaxRetries: rec.MaxRetries,
					NextAt:     r.clock(),
					Mid:        msg.Mid,
				}
				// Non-blocking send (§VII fan-out): drop at bus layer if intake full.
				select {
				case r.intake <- task:
				default:
					// Dropped at bus layer; observable via ws_broadcast_bus_dropped_total (ws-server).
					deliveriesTotal.WithLabelValues("dropped").Inc()
				}
			}
		}
	}
}

// invalidationConsumer reads tenant IDs from the Valkey PSUBSCRIBE channel and
// triggers an on-demand cache refresh. The channel is populated by the subscription wiring
// which subscribes to WebhookInvalidationSubjectPrefix+"*" via PSUBSCRIBE and
// strips the prefix to extract the tenantID.
func (r *Runner) invalidationConsumer() {
	defer logging.RecoverPanic(r.cfg.Logger, "invalidationConsumer", nil)
	for {
		select {
		case <-r.ctx.Done():
			return
		case tenantID, ok := <-r.cfg.InvalidationCh:
			if !ok {
				return
			}
			if err := r.cfg.Cache.Refresh(r.ctx, tenantID); err != nil {
				r.cfg.Logger.Warn().Err(err).Str(logging.LogKeyTenantUUID, tenantID).
					Msg("invalidation-triggered cache refresh failed")
			}
		}
	}
}

// ttlRefresher re-hydrates all cached tenants every CacheTTL.
func (r *Runner) ttlRefresher() {
	defer logging.RecoverPanic(r.cfg.Logger, "ttlRefresher", nil)
	ticker := time.NewTicker(r.cfg.CacheTTL)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if err := r.cfg.Cache.RefreshAll(r.ctx); err != nil {
				r.cfg.Logger.Warn().Err(err).Msg("TTL refresh failed")
			}
		}
	}
}

// degradedScheduler manages 1-hour retry timers for degraded webhooks.
// Uses a min-heap to fire the nearest timer without spinning.
func (r *Runner) degradedScheduler(initial []WebhookRecord) {
	defer logging.RecoverPanic(r.cfg.Logger, "degradedScheduler", nil)
	h := &degradedHeap{}
	heap.Init(h)

	// Seed with degraded records from hydration.
	for _, rec := range initial {
		var nextAt time.Time
		if rec.LastDeliveryAt != nil {
			nextAt = rec.LastDeliveryAt.Add(webhookconst.WebhookDegradedRetryInterval)
		} else {
			nextAt = r.clock().Add(webhookconst.WebhookDegradedInitialRetryDelay)
		}
		heap.Push(h, &degradedEntry{
			webhookID: rec.ID, tenantID: rec.TenantID, nextAt: nextAt,
		})
	}

	var timer *time.Timer
	resetTimer := func() {
		if h.Len() == 0 {
			if timer != nil {
				timer.Stop()
				timer = nil
			}
			return
		}
		top := (*h)[0]
		delay := max(top.nextAt.Sub(r.clock()), 0)
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			// Stop the timer and drain the channel before Reset per Go timer documentation:
			// if Stop returns false the channel may already have a buffered value that
			// must be drained to avoid a spurious tick on the next loop iteration.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(delay)
		}
	}
	resetTimer()

	var timerC <-chan time.Time
	for {
		if timer != nil {
			timerC = timer.C
		} else {
			timerC = nil
		}
		select {
		case <-r.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case req := <-r.degradedCh:
			heap.Push(h, &degradedEntry{
				webhookID: req.webhookID, tenantID: req.tenantID, nextAt: req.nextAt,
			})
			resetTimer()
		case <-timerC:
			if h.Len() == 0 {
				continue
			}
			entry := heap.Pop(h).(*degradedEntry) //nolint:errcheck // heap only holds *degradedEntry
			// Pre-dispatch status check: cancel if no longer degraded.
			rec := r.cfg.Cache.GetByID(entry.tenantID, entry.webhookID)
			if rec == nil || rec.Status != types.WebhookStatusDegraded {
				resetTimer()
				continue
			}
			task := DeliveryTask{
				WebhookID:  rec.ID,
				TenantID:   rec.TenantID,
				URL:        rec.URL,
				SecretEnc:  rec.SecretEnc,
				Payload:    []byte(`{"type":"webhook.degraded_retry"}`),
				Attempt:    1,
				MaxRetries: rec.MaxRetries,
				NextAt:     r.clock(),
			}
			// Non-blocking send: if queue full, re-schedule rather than losing the retry.
			select {
			case r.retryQueue <- task:
			default:
				heap.Push(h, &degradedEntry{
					webhookID: entry.webhookID, tenantID: entry.tenantID,
					nextAt: r.clock().Add(webhookconst.WebhookDegradedQueueFullFallback),
				})
			}
			resetTimer()
		}
	}
}

// asyncStatusUpdater serially processes status update requests with exponential backoff.
// Exits immediately on ctx.Done() without attempting a final RPC.
func (r *Runner) asyncStatusUpdater() {
	defer logging.RecoverPanic(r.cfg.Logger, "asyncStatusUpdater", nil)
	const maxBackoff = 5 * time.Minute
	for {
		select {
		case <-r.ctx.Done():
			return
		case req := <-r.statusUpdates:
			backoff := time.Second
			for {
				err := r.cfg.ProvisioningClient.UpdateWebhookStatus(r.ctx, req.webhookID, req.tenantID, req.status, req.retryCount)
				if err == nil {
					break
				}
				statusUpdateFailuresTotal.Inc()
				r.cfg.Logger.Warn().Err(err).Str("webhook_id", req.webhookID).Msg("UpdateWebhookStatus failed; retrying")
				// Backoff using select so context cancellation is respected (§VII: never time.Sleep).
				select {
				case <-r.ctx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff < maxBackoff {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
			}
		}
	}
}

// deliveryWorker drains from both intake and retryQueue, executing deliveries.
func (r *Runner) deliveryWorker() {
	defer logging.RecoverPanic(r.cfg.Logger, "deliveryWorker", nil)
	for {
		// Prioritize retry queue with a non-blocking try, then block on either.
		select {
		case <-r.ctx.Done():
			return
		case task := <-r.retryQueue:
			r.doDeliver(task)
		default:
		}
		select {
		case <-r.ctx.Done():
			return
		case task := <-r.retryQueue:
			r.doDeliver(task)
		case task := <-r.intake:
			r.doDeliver(task)
		}
	}
}

// doDeliver handles a single delivery attempt, including retry scheduling and status transitions.
func (r *Runner) doDeliver(task DeliveryTask) {
	// Wait until NextAt if the task is a future retry.
	if delay := task.NextAt.Sub(r.clock()); delay > 0 {
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(delay):
		}
	}

	result := r.cfg.Deliverer.Deliver(r.ctx, task)

	switch result.StatusLabel {
	case StatusLabelSkipped:
		// pre-dispatch gate canceled. No recording, no metric (§VI: "skipped"
		// is not a delivery outcome — it means the webhook was disabled before dispatch).
		return

	case StatusLabelDecryptFail:
		// No HTTP attempt was made — no latency to record.
		deliveriesTotal.WithLabelValues(result.StatusLabel).Inc()
		// suspend + record (side-effects the caller owns per spec).
		r.sendStatusUpdate(task.WebhookID, task.TenantID, types.WebhookStatusSuspended, 0)
		r.recordDelivery(task, result)

	case StatusLabelSSRFBlocked:
		deliveriesTotal.WithLabelValues(result.StatusLabel).Inc()
		if result.LatencyMS > 0 {
			deliveryDuration.Observe(float64(result.LatencyMS) / 1000.0)
		}
		r.recordDelivery(task, result)
		r.scheduleRetry(task, result)

	case StatusLabelSuccess:
		deliveriesTotal.WithLabelValues(result.StatusLabel).Inc()
		if result.LatencyMS > 0 {
			deliveryDuration.Observe(float64(result.LatencyMS) / 1000.0)
		}
		r.recordDelivery(task, result)
		// Recovery: if was degraded, transition to enabled.
		rec := r.cfg.Cache.GetByID(task.TenantID, task.WebhookID)
		if rec != nil && rec.Status == types.WebhookStatusDegraded {
			r.sendStatusUpdate(task.WebhookID, task.TenantID, types.WebhookStatusEnabled, 0)
			degradedRecoveredTotal.Inc()
		}

	default: // StatusLabelFailure or StatusLabelTimeout
		deliveriesTotal.WithLabelValues(result.StatusLabel).Inc()
		if result.LatencyMS > 0 {
			deliveryDuration.Observe(float64(result.LatencyMS) / 1000.0)
		}
		r.recordDelivery(task, result)
		r.scheduleRetry(task, result)
	}
}

// retryDelay returns the delay for a given 1-indexed attempt from the injected retrySchedule.
// Mirrors webhookconst.RetryDelay but uses r.retrySchedule so test injection works.
func (r *Runner) retryDelay(attempt int) time.Duration {
	idx := max(attempt-1, 0)
	if idx >= len(r.retrySchedule) {
		idx = len(r.retrySchedule) - 1
	}
	return r.retrySchedule[idx]
}

// scheduleRetry re-enqueues a task for retry, or transitions to degraded after max retries.
func (r *Runner) scheduleRetry(task DeliveryTask, result DeliveryResult) {
	if task.Attempt <= 0 {
		task.Attempt = 1
	}

	// Count retries starting at attempt 1.
	attemptLabel := strconv.Itoa(task.Attempt)
	if task.Attempt >= 5 {
		attemptLabel = "5+"
	}
	retryTotal.WithLabelValues(attemptLabel).Inc()

	if task.Attempt >= task.MaxRetries {
		// Retry budget exhausted → degraded.
		r.sendStatusUpdate(task.WebhookID, task.TenantID, types.WebhookStatusDegraded, 0)
		degradedTotal.Inc()
		// Schedule 1-hour degraded retry timer (§VII: non-blocking fan-out send).
		select {
		case r.degradedCh <- degradedTimerRequest{
			webhookID: task.WebhookID,
			tenantID:  task.TenantID,
			nextAt:    r.clock().Add(webhookconst.WebhookDegradedRetryInterval),
		}:
		case <-r.ctx.Done():
			return
		default:
			// Channel full — degradedScheduler is behind. Re-enqueue via degradedScheduler
			// with a short fallback delay rather than losing the timer permanently.
			r.cfg.Logger.Warn().Str("webhook_id", task.WebhookID).
				Msg("degradedCh full; rescheduling degraded retry via fallback delay")
			select {
			case r.degradedCh <- degradedTimerRequest{
				webhookID: task.WebhookID,
				tenantID:  task.TenantID,
				nextAt:    r.clock().Add(webhookconst.WebhookDegradedQueueFullFallback),
			}:
			default:
				// Still full — will recover at next TTL refresh or restart.
				r.cfg.Logger.Warn().Str("webhook_id", task.WebhookID).
					Msg("degradedCh still full after fallback; degraded timer dropped")
			}
		}
		return
	}

	nextDelay := r.retryDelay(task.Attempt)
	next := task
	next.Attempt = task.Attempt + 1
	next.NextAt = r.clock().Add(nextDelay)

	// Try to enqueue; on overflow drop-oldest.
	// dropMu is acquired inside dropOldest to serialize the drain/refill window.
	select {
	case r.retryQueue <- next:
	default:
		r.dropOldest(next, result)
	}
}

// dropOldest drops the furthest-future task from retryQueue, then enqueues the new task.
// dropMu serializes the drain/refill window against concurrent senders (§VII).
// The mutex is released before any gRPC calls (recordDelivery) — §VII: no I/O under a lock.
func (r *Runner) dropOldest(incoming DeliveryTask, _ DeliveryResult) {
	var dropped DeliveryTask
	var overflowDrops []DeliveryTask

	r.dropMu.Lock()
	// Drain the channel to find the task with the furthest NextAt.
	var tasks []DeliveryTask
	drained := true
	for drained {
		select {
		case t := <-r.retryQueue:
			tasks = append(tasks, t)
		default:
			drained = false
		}
	}

	// Find the furthest-future task.
	dropIdx := 0
	for i, t := range tasks {
		if t.NextAt.After(tasks[dropIdx].NextAt) {
			dropIdx = i
		}
	}

	if len(tasks) > 0 {
		dropped = tasks[dropIdx]
		tasks = append(tasks[:dropIdx], tasks[dropIdx+1:]...)
	}

	// Re-enqueue the rest + the incoming task.
	tasks = append(tasks, incoming)
	for _, t := range tasks {
		select {
		case r.retryQueue <- t:
		default:
			// Can fire if a concurrent sender pushed to retryQueue during the drain window.
			overflowDrops = append(overflowDrops, t)
		}
	}
	r.dropMu.Unlock() // release before any I/O (§VII: no locks across network calls)

	// Record and log drops after releasing the mutex (§VII: no I/O under a lock).
	for _, t := range overflowDrops {
		deliveriesTotal.WithLabelValues("dropped").Inc()
		r.recordDelivery(t, DeliveryResult{StatusLabel: StatusLabelDropped, Error: "queue_full_refill"})
		r.cfg.Logger.Warn().
			Str("webhook_id", t.WebhookID).
			Str(logging.LogKeyTenantUUID, t.TenantID).
			Msg("task dropped during dropOldest refill (concurrent sender race)")
	}

	// Write RecordDelivery for the dropped task before discarding.
	if dropped.WebhookID != "" {
		deliveriesTotal.WithLabelValues("dropped").Inc()
		r.recordDelivery(dropped, DeliveryResult{StatusLabel: StatusLabelDropped, Error: "queue_full"})
	}
}

// sendStatusUpdate enqueues an async status update request (non-blocking, §VII).
func (r *Runner) sendStatusUpdate(webhookID, tenantID, status string, retryCount int) {
	select {
	case r.statusUpdates <- statusUpdateRequest{
		webhookID: webhookID, tenantID: tenantID,
		status: status, retryCount: retryCount,
	}:
	default:
		// Channel full — log and discard (§VII fan-out non-blocking rule).
		r.cfg.Logger.Warn().
			Str("webhook_id", webhookID).
			Str("status", status).
			Msg("statusUpdates channel full; status update dropped")
	}
}

// recordDelivery calls RecordDelivery best-effort.
func (r *Runner) recordDelivery(task DeliveryTask, result DeliveryResult) {
	deliveryID := uuid.NewString()
	d := &provisioning.WebhookDelivery{
		ID:          deliveryID,
		WebhookID:   task.WebhookID,
		TenantID:    task.TenantID,
		Attempt:     task.Attempt,
		StatusCode:  result.StatusCode,
		LatencyMS:   result.LatencyMS,
		Error:       result.Error,
		DeliveredAt: r.clock(),
	}
	if err := r.cfg.ProvisioningClient.RecordDelivery(r.ctx, d); err != nil {
		recordDeliveryFailuresTotal.Inc()
		r.cfg.Logger.Warn().Err(err).Str("webhook_id", task.WebhookID).
			Msg("RecordDelivery failed (best-effort; audit trail may have gap)")
	}
}
