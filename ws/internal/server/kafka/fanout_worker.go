package kafka

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// fanoutJob represents a single topic write within a multi-topic fan-out.
// successCh and failCh are shared across all jobs for the same message so the
// aggregator can collect exactly N results (one per topic).
type fanoutJob struct {
	topic     string
	record    *kgo.Record
	channel   string
	tenant    string
	successCh chan<- string
	failCh    chan<- string
}

// FanoutPool writes records to multiple Kafka topics concurrently and reports
// per-topic success or failure back to the caller via result channels.
type FanoutPool struct {
	handoff chan fanoutJob
	dlq     *DLQPool
	client  *kgo.Client
	logger  zerolog.Logger
	workers int

	writeFailedCounter *prometheus.CounterVec
	droppedCounter     *prometheus.CounterVec
}

// NewFanoutPool creates a fan-out worker pool. workers controls concurrency;
// queueSize is the depth of the handoff channel. writeFailedCounter and droppedCounter are
// registered by the caller via newPoolMetrics (#179 P1b-C2 — the pool no longer registers
// its own metrics, so a second Producer in one process does not panic on duplicate
// registration).
func NewFanoutPool(workers, queueSize int, client *kgo.Client, dlq *DLQPool, logger zerolog.Logger, writeFailedCounter, droppedCounter *prometheus.CounterVec) *FanoutPool {
	return &FanoutPool{
		handoff:            make(chan fanoutJob, queueSize),
		dlq:                dlq,
		client:             client,
		logger:             logger.With().Str("component", "fanout_pool").Logger(),
		workers:            workers,
		writeFailedCounter: writeFailedCounter,
		droppedCounter:     droppedCounter,
	}
}

// Start launches p.workers goroutines to drain the fanout job queue.
func (p *FanoutPool) Start(ctx context.Context, wg *sync.WaitGroup) {
	for range p.workers {
		wg.Go(func() {
			defer logging.RecoverPanic(p.logger, "fanout_worker", nil)
			p.runWorker(ctx)
		})
	}
}

// Submit enqueues a fanout job. Returns false if the queue is full (caller
// should treat the topic as failed and route to DLQ immediately).
func (p *FanoutPool) Submit(job fanoutJob) bool {
	select {
	case p.handoff <- job:
		return true
	default:
		if p.droppedCounter != nil {
			p.droppedCounter.WithLabelValues(job.tenant).Inc()
		}
		return false
	}
}

func (p *FanoutPool) runWorker(ctx context.Context) {
	for {
		select {
		case job := <-p.handoff:
			p.process(ctx, job)
		case <-ctx.Done():
			return
		}
	}
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
	if results.FirstErr() == nil {
		job.successCh <- job.topic
		return
	}

	// Per-topic write failure. This is the ONLY site that increments fanoutWriteFailed;
	// fanoutDropped is reserved for the Submit-queue-full leg (#179 P1b-C4 — the ctx-cancel
	// path used to double-count here as both write-failed AND dropped).
	if p.writeFailedCounter != nil {
		p.writeFailedCounter.WithLabelValues(job.tenant, job.topic).Inc()
	}
	job.failCh <- job.topic
}
