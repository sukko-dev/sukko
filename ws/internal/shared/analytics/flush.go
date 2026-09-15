package analytics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// batchUpsert writes all counters from the snapshot to the database using pgx Batch.
// Returns nil immediately if the snapshot is empty (no unnecessary round-trip).
// SQL per table uses ON CONFLICT ... DO UPDATE SET to handle concurrent pod writes.
func batchUpsert(ctx context.Context, pool *pgxpool.Pool, snapshot *counterMap, podID string) error {
	if snapshot.isEmpty() {
		return nil
	}

	// Build the batch under snapshot.mu so producers mid-flight on the old pointer
	// can't race with our range iteration. buildBatch uses defer to release the lock
	// even on panic — §VII: defer mu.Unlock() MUST be used to prevent deadlocks.
	bucket := time.Now().UTC().Truncate(time.Minute)
	batch := buildBatch(snapshot, podID, bucket)

	if batch.Len() == 0 {
		return nil
	}

	results := pool.SendBatch(ctx, batch)
	defer func() { _ = results.Close() }()

	var errs []string
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("batch upsert: %d/%d failed: %s", len(errs), batch.Len(), strings.Join(errs, "; "))
	}
	return nil
}

// buildBatch constructs a pgx.Batch from a counterMap under snapshot.mu.
// Using defer for the unlock satisfies §VII ("defer mu.Unlock() MUST be used
// to prevent deadlocks from early returns or panics") while still releasing
// the lock before the caller performs network I/O.
func buildBatch(snapshot *counterMap, podID string, bucket time.Time) *pgx.Batch {
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()

	batch := &pgx.Batch{}

	for k, v := range snapshot.connections {
		batch.Queue(
			`INSERT INTO analytics_connections
				(pod_id, tenant_id, bucket_start, bucket_size, transport,
				 active_count, connect_count, disconnect_count, error_count)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (pod_id, tenant_id, bucket_start, bucket_size, transport)
			DO UPDATE SET
				active_count     = analytics_connections.active_count     + EXCLUDED.active_count,
				connect_count    = analytics_connections.connect_count    + EXCLUDED.connect_count,
				disconnect_count = analytics_connections.disconnect_count + EXCLUDED.disconnect_count,
				error_count      = analytics_connections.error_count      + EXCLUDED.error_count`,
			podID, k.tenantID, bucket, bucketSizeMinute, k.secondary,
			v.active.Load(), v.connects.Load(), v.disconnects.Load(), v.errors.Load(),
		)
	}

	for k, v := range snapshot.messages {
		batch.Queue(
			`INSERT INTO analytics_messages
				(pod_id, tenant_id, bucket_start, bucket_size, channel_prefix,
				 published_count, delivered_count, failed_count, total_latency_ms, sample_count)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			ON CONFLICT (pod_id, tenant_id, bucket_start, bucket_size, channel_prefix)
			DO UPDATE SET
				published_count  = analytics_messages.published_count  + EXCLUDED.published_count,
				delivered_count  = analytics_messages.delivered_count  + EXCLUDED.delivered_count,
				failed_count     = analytics_messages.failed_count     + EXCLUDED.failed_count,
				total_latency_ms = analytics_messages.total_latency_ms + EXCLUDED.total_latency_ms,
				sample_count     = analytics_messages.sample_count     + EXCLUDED.sample_count`,
			podID, k.tenantID, bucket, bucketSizeMinute, k.secondary,
			v.published.Load(), v.delivered.Load(), v.failed.Load(),
			v.totalLatencyMs.Load(),
			v.delivered.Load(), // sample_count = deliveries (latency measured per delivery)
		)
	}

	for k, v := range snapshot.push {
		batch.Queue(
			`INSERT INTO analytics_push
				(pod_id, tenant_id, bucket_start, bucket_size, provider,
				 sent_count, success_count, failed_count, expired_count,
				 rate_limited_count, total_latency_ms, sample_count)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			ON CONFLICT (pod_id, tenant_id, bucket_start, bucket_size, provider)
			DO UPDATE SET
				sent_count         = analytics_push.sent_count         + EXCLUDED.sent_count,
				success_count      = analytics_push.success_count      + EXCLUDED.success_count,
				failed_count       = analytics_push.failed_count       + EXCLUDED.failed_count,
				expired_count      = analytics_push.expired_count      + EXCLUDED.expired_count,
				rate_limited_count = analytics_push.rate_limited_count + EXCLUDED.rate_limited_count,
				total_latency_ms   = analytics_push.total_latency_ms   + EXCLUDED.total_latency_ms,
				sample_count       = analytics_push.sample_count       + EXCLUDED.sample_count`,
			podID, k.tenantID, bucket, bucketSizeMinute, k.secondary,
			v.sent.Load(), v.success.Load(), v.failed.Load(), v.expired.Load(),
			v.rateLimited.Load(), v.totalLatencyMs.Load(),
			v.sent.Load(), // sample_count = sent (latency measured per dispatch)
		)
	}

	return batch
}

// mergeBack adds all counters from snapshot into current using per-key atomic.Add.
// This is called when a flush fails to preserve data that arrived during the attempt.
// Must use Add (not Store/replace) — new increments may have arrived since the swap.
func mergeBack(current, snapshot *counterMap) {
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()

	for k, v := range snapshot.connections {
		dst := current.getOrCreateConnection(k)
		if dst == nil {
			continue // buffer full — drop rather than corrupt
		}
		dst.active.Add(v.active.Load())
		dst.connects.Add(v.connects.Load())
		dst.disconnects.Add(v.disconnects.Load())
		dst.errors.Add(v.errors.Load())
	}
	for k, v := range snapshot.messages {
		dst := current.getOrCreateMessage(k)
		if dst == nil {
			continue
		}
		dst.published.Add(v.published.Load())
		dst.delivered.Add(v.delivered.Load())
		dst.failed.Add(v.failed.Load())
		dst.totalLatencyMs.Add(v.totalLatencyMs.Load())
	}
	for k, v := range snapshot.push {
		dst := current.getOrCreatePush(k)
		if dst == nil {
			continue
		}
		dst.sent.Add(v.sent.Load())
		dst.success.Add(v.success.Load())
		dst.failed.Add(v.failed.Load())
		dst.expired.Add(v.expired.Load())
		dst.rateLimited.Add(v.rateLimited.Load())
		dst.totalLatencyMs.Add(v.totalLatencyMs.Load())
	}
}
