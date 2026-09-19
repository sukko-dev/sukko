package kafka

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// maxFailureCauseLen bounds the HeaderFailureCause value so a pathological error
// string cannot bloat every dead-lettered record.
const maxFailureCauseLen = 256

// fanoutJob is a single EGRESS topic write (ADR-0018). Egress copies are
// fire-and-forget relative to the publish ack: the ingress-topic write has
// already succeeded synchronously by the time a job is submitted, so no result
// flows back to the publisher. A failed egress write is dead-lettered by the
// worker (process) and surfaced via metrics — never silently dropped.
type fanoutJob struct {
	topic  string
	record *kgo.Record
	tenant string
}

// FanoutPool writes egress copies to Kafka topics concurrently. Failures are
// routed to the per-tenant DLQ and counted; they never affect the publish ack,
// which is determined solely by the ingress-topic write (ADR-0018). The pool's
// produces run on the SAME kgo.Client as the main producer, so every egress
// write inherits the client's bounded retry policy (KAFKA_PRODUCER_RECORD_RETRIES,
// default 8, exponential backoff from 100ms): an error reaching process/deadLetter
// is post-retry and effectively terminal.
type FanoutPool struct {
	handoff        chan fanoutJob
	dlq            *DLQPool
	client         *kgo.Client
	logger         zerolog.Logger
	workers        int
	topicNamespace string

	// wg tracks the pool's own worker goroutines so Shutdown can wait for the
	// backlog drain independently of the producer's DLQ workers (§VII).
	wg sync.WaitGroup

	// submitMu guards handoff sends against Shutdown's close (§VII: a send on a
	// closed channel panics; the flag+close flip happens under the write lock,
	// sends take the read lock). The critical section is a non-blocking channel
	// operation — never I/O.
	submitMu  sync.RWMutex
	closed    bool
	closeOnce sync.Once

	writeFailedCounter *prometheus.CounterVec
	droppedCounter     *prometheus.CounterVec
	dlqDropped         *prometheus.CounterVec
}

// NewFanoutPool creates a fan-out worker pool. workers controls concurrency;
// queueSize is the depth of the handoff channel; topicNamespace builds the DLQ
// topic name for failed egress writes. The counters are registered by the caller
// via newPoolMetrics (#179 P1b-C2 — the pool no longer registers its own metrics,
// so a second Producer in one process does not panic on duplicate registration).
func NewFanoutPool(workers, queueSize int, client *kgo.Client, dlq *DLQPool, logger zerolog.Logger, topicNamespace string, writeFailedCounter, droppedCounter, dlqDropped *prometheus.CounterVec) *FanoutPool {
	return &FanoutPool{
		handoff:            make(chan fanoutJob, queueSize),
		dlq:                dlq,
		client:             client,
		logger:             logger.With().Str("component", "fanout_pool").Logger(),
		workers:            workers,
		topicNamespace:     topicNamespace,
		writeFailedCounter: writeFailedCounter,
		droppedCounter:     droppedCounter,
		dlqDropped:         dlqDropped,
	}
}

// Start launches p.workers goroutines to drain the fanout job queue. Workers
// exit when the handoff channel is closed and drained (graceful Shutdown) or
// when ctx is canceled (hard stop — Shutdown counts what remains).
func (p *FanoutPool) Start(ctx context.Context) {
	for range p.workers {
		p.wg.Go(func() {
			defer logging.RecoverPanic(p.logger, "fanout_worker", nil)
			p.runWorker(ctx)
		})
	}
}

// Submit enqueues an egress write. When the queue is full the job is
// dead-lettered immediately (the write was never attempted) and counted. After
// Shutdown has begun the copy can no longer be attempted or dead-lettered
// reliably (the DLQ is shutting down too) — it is counted and logged instead.
func (p *FanoutPool) Submit(job fanoutJob) {
	p.submitMu.RLock()
	defer p.submitMu.RUnlock()
	if p.closed {
		if p.droppedCounter != nil {
			p.droppedCounter.WithLabelValues(job.tenant, FanoutDropReasonPostShutdown).Inc()
		}
		p.logger.Warn().
			Str(logging.LogKeyTenantSlug, job.tenant).
			Str(LabelTopic, job.topic).
			Str("failure_kind", FailureKindNotAttempted).
			Msg("egress copy submitted after shutdown began — dropped (counted)")
		return
	}
	select {
	case p.handoff <- job:
	default:
		if p.droppedCounter != nil {
			p.droppedCounter.WithLabelValues(job.tenant, FanoutDropReasonQueueFull).Inc()
		}
		p.deadLetter(job, FailureKindNotAttempted, nil)
	}
}

func (p *FanoutPool) runWorker(ctx context.Context) {
	for {
		select {
		case job, ok := <-p.handoff:
			if !ok {
				return // graceful shutdown: backlog drained
			}
			p.process(ctx, job)
		case <-ctx.Done():
			return // hard stop: Shutdown counts whatever remains queued
		}
	}
}

// Shutdown closes the handoff to new submissions and drains the queued egress
// backlog, bounded by timeout (§VII — shutdown must never hang). Every copy that
// could not be drained within the bound is received off the (closed) queue,
// counted in fanoutDropped, and logged; the count is returned. Safe to call
// multiple times. Jobs a worker is still blocked on are unstuck later by the
// producer's client.Close() and are accounted through fanoutWriteFailed instead.
func (p *FanoutPool) Shutdown(timeout time.Duration) int {
	p.closeOnce.Do(func() {
		p.submitMu.Lock()
		p.closed = true
		close(p.handoff)
		p.submitMu.Unlock()
	})
	if waitWithTimeout(&p.wg, timeout) {
		return 0 // workers exited via the closed-channel path: backlog fully written
	}
	// Timed out — the broker is not answering. Take the remaining jobs off the
	// closed queue so a late-unblocked worker cannot half-process them, and
	// count each as terminal loss (ADR-0018: surfaced, never silent).
	undrained := 0
	for job := range p.handoff {
		undrained++
		if p.droppedCounter != nil {
			p.droppedCounter.WithLabelValues(job.tenant, FanoutDropReasonShutdown).Inc()
		}
		p.logger.Warn().
			Str(logging.LogKeyTenantSlug, job.tenant).
			Str(LabelTopic, job.topic).
			Str("failure_kind", FailureKindNotAttempted).
			Msg("egress copy undrained at shutdown — dropped (counted)")
	}
	return undrained
}

func (p *FanoutPool) process(ctx context.Context, job fanoutJob) {
	// Copy headers into a new slice so per-topic mutations don't race.
	// HeaderChannel is already stamped on the base record by the producer.
	headers := make([]kgo.RecordHeader, len(job.record.Headers))
	copy(headers, job.record.Headers)
	rec := &kgo.Record{
		Topic:   job.topic,
		Key:     job.record.Key,
		Value:   job.record.Value,
		Headers: headers,
	}

	results := p.client.ProduceSync(ctx, rec)
	err := results.FirstErr()
	if err == nil {
		return
	}

	// Post-retry terminal failure (the shared client already applied its bounded
	// retry policy): count it and dead-letter the record so the gap in the egress
	// topic is observable and recoverable (ADR-0018 — an acked message may be
	// missing from an egress topic, but never silently).
	if p.writeFailedCounter != nil {
		p.writeFailedCounter.WithLabelValues(job.tenant, job.topic).Inc()
	}
	p.deadLetter(job, egressFailureKind(err), err)
}

// egressFailureKind classifies how an egress produce terminated. Kafka-level
// non-retriable errors (authorization, unknown topic) fail fast without
// consuming the retry budget; everything else surfaced here has outlived the
// client's bounded retry policy or hit a terminal transport condition. franz-go
// exposes no per-record attempt count, so this classification IS the triage
// fact — no attempt number is invented.
func egressFailureKind(err error) string {
	if kafkaErr, ok := errors.AsType[*kerr.Error](err); ok && !kafkaErr.Retriable {
		return FailureKindNonRetryable
	}
	return FailureKindRetriesExhausted
}

// deadLetter routes a failed (or never-attempted) egress write to the tenant's
// DLQ topic, tagging the record with the reason, the egress topic it missed,
// the failure classification, and the terminal error (cause may be nil for
// never-attempted writes). A full DLQ queue is terminal loss of the egress
// copy — counted via dlqDropped so this silent-loss leg stays observable
// (#179 P1b-C4, §VII).
func (p *FanoutPool) deadLetter(job fanoutJob, kind string, cause error) {
	if p.dlq == nil {
		return
	}
	headers := append(slices.Clone(job.record.Headers),
		kgo.RecordHeader{Key: HeaderReason, Value: []byte(ReasonFanoutTopicWriteFailed)},
		kgo.RecordHeader{Key: HeaderFailedTopics, Value: []byte(job.topic)},
		kgo.RecordHeader{Key: HeaderFailureKind, Value: []byte(kind)},
	)
	if cause != nil {
		msg := cause.Error()
		if len(msg) > maxFailureCauseLen {
			msg = msg[:maxFailureCauseLen]
		}
		headers = append(headers, kgo.RecordHeader{Key: HeaderFailureCause, Value: []byte(msg)})
	}
	dlqTopic := kafkashared.BuildTopicName(p.topicNamespace, job.tenant, routing.DeadLetterTopicSuffix)
	dlqRec := &kgo.Record{
		Topic:   dlqTopic,
		Key:     job.record.Key,
		Value:   job.record.Value,
		Headers: headers,
	}
	if !p.dlq.TrySubmit(dlqJob{record: dlqRec, tenant: job.tenant, reason: ReasonFanoutTopicWriteFailed}) {
		if p.dlqDropped != nil {
			p.dlqDropped.WithLabelValues(job.tenant).Inc()
		}
		p.logger.Warn().
			Str(logging.LogKeyTenantSlug, job.tenant).
			Str(LabelTopic, job.topic).
			Str("failure_kind", kind).
			AnErr("cause", cause).
			Msg("DLQ queue full — dropping failed egress record")
	}
}
