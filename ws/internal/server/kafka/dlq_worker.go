package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// dlqQueueDepthPerWorker sizes the DLQ job channel as Workers × this factor. The DLQ is a
// background retry buffer (not a live-publish gate like the fan-out queue), so its depth is
// derived from worker count rather than exposed as a separate env var (#179 P1b-C5).
const dlqQueueDepthPerWorker = 16

// dlqJob represents a single dead-letter queue write attempt.
// Namespace is stored on DLQPool rather than per-job — a pool always writes
// to the same namespace, avoiding repetition in every job struct.
type dlqJob struct {
	record  *kgo.Record
	tenant  string
	reason  string
	attempt int
}

// DLQConfig configures the dead-letter queue worker pool.
type DLQConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Workers    int
	Namespace  string
}

// DLQPool retries failed Kafka writes to the dead-letter topic.
type DLQPool struct {
	jobs   chan dlqJob
	client *kgo.Client
	cfg    DLQConfig
	logger zerolog.Logger

	writeFailedCounter *prometheus.CounterVec
}

// NewDLQPool creates a DLQ worker pool. The provided kgo.Client is used for all dead-letter
// produce operations and must outlive the pool. writeFailedCounter is registered by the
// caller via newPoolMetrics (#179 P1b-C2 — the pool no longer registers its own metric).
func NewDLQPool(cfg DLQConfig, client *kgo.Client, logger zerolog.Logger, writeFailedCounter *prometheus.CounterVec) *DLQPool {
	return &DLQPool{
		jobs:               make(chan dlqJob, cfg.Workers*dlqQueueDepthPerWorker),
		client:             client,
		cfg:                cfg,
		logger:             logger.With().Str("component", "dlq_pool").Logger(),
		writeFailedCounter: writeFailedCounter,
	}
}

// Start launches cfg.Workers goroutines to drain the DLQ job queue.
func (p *DLQPool) Start(ctx context.Context, wg *sync.WaitGroup) {
	for range p.cfg.Workers {
		wg.Go(func() {
			defer logging.RecoverPanic(p.logger, "dlq_worker", nil)
			p.runWorker(ctx)
		})
	}
}

// TrySubmit enqueues a job for dead-letter retry using a non-blocking send.
// Returns true if the job was accepted, false if the queue was full (caller
// should increment a drop counter and continue). Use this from all hot paths.
func (p *DLQPool) TrySubmit(job dlqJob) bool {
	select {
	case p.jobs <- job:
		return true
	default:
		return false
	}
}

func (p *DLQPool) runWorker(ctx context.Context) {
	for {
		select {
		case job := <-p.jobs:
			p.process(ctx, job)
		case <-ctx.Done():
			p.drainAbandoned()
			return
		}
	}
}

// drainAbandoned counts and logs every job still queued when the pool is shut down.
// The producer's Close cancels this context only AFTER the fan-out pool has drained,
// so no producer is still submitting here — the remaining jobs are a bounded backlog
// (at most the channel's buffer). Each shutting-down worker drains what it can with a
// non-blocking receive; concurrent drains across workers are safe (channel receive and
// the counter are both concurrency-safe) and collectively empty the queue. §VII: bounded
// by the buffer, no blocking, no send on the never-closed channel. ADR-0018: a dropped
// dead-letter copy is surfaced, never silent.
func (p *DLQPool) drainAbandoned() {
	for {
		select {
		case job := <-p.jobs:
			p.logger.Warn().
				Str(logging.LogKeyTenantSlug, job.tenant).
				Str(LabelReason, job.reason).
				Msg("DLQ write abandoned: pool shut down with job still queued — dropping record")
			if p.writeFailedCounter != nil {
				p.writeFailedCounter.WithLabelValues(job.tenant, job.reason).Inc()
			}
		default:
			return
		}
	}
}

func (p *DLQPool) process(ctx context.Context, job dlqJob) {
	for attempt := job.attempt; attempt <= p.cfg.MaxRetries; attempt++ {
		results := p.client.ProduceSync(ctx, job.record)
		if results.FirstErr() == nil {
			return
		}

		if attempt == p.cfg.MaxRetries {
			p.logger.Warn().
				Str(logging.LogKeyTenantSlug, job.tenant).
				Str(LabelReason, job.reason).
				Int("attempts", attempt+1).
				Err(results.FirstErr()).
				Msg("DLQ write exhausted all retries — dropping record")
			if p.writeFailedCounter != nil {
				p.writeFailedCounter.WithLabelValues(job.tenant, job.reason).Inc()
			}
			return
		}

		delay := min(p.cfg.BaseDelay*(1<<attempt), p.cfg.MaxDelay)
		select {
		case <-ctx.Done():
			p.logger.Warn().
				Str(logging.LogKeyTenantSlug, job.tenant).
				Str(LabelReason, job.reason).
				Msg("DLQ write aborted: context canceled during backoff")
			if p.writeFailedCounter != nil {
				p.writeFailedCounter.WithLabelValues(job.tenant, job.reason).Inc()
			}
			return
		case <-time.After(delay):
		}
	}
}
