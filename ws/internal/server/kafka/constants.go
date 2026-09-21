package kafka

// Kafka message header names used for channel-topic routing.
// HeaderChannel, HeaderSource, and HeaderTimestamp are the cross-service wire
// contract and live in internal/shared/kafka (referenced here as kafkashared.*).
const (
	HeaderReason       = "x-sukko-reason"
	HeaderFailedTopics = "x-sukko-failed-topics"

	// Egress-failure triage headers on dead-lettered records (ADR-0018) —
	// mirrors EventBridge's RETRY_ATTEMPTS/EXHAUSTED_RETRY_CONDITION shape.
	// franz-go exposes no per-record attempt count, so the KIND is the fact:
	// the classification of how the write terminated, plus the terminal error.
	HeaderFailureKind  = "x-sukko-failure-kind"
	HeaderFailureCause = "x-sukko-failure-cause"

	// Client message provenance stamped by the producer on every outbound record.
	// HeaderClientID and SourceWSClient are server-specific; the source/timestamp
	// header keys are shared (kafkashared.HeaderSource / kafkashared.HeaderTimestamp).
	// Failure-kind values for HeaderFailureKind.
	//   retries_exhausted — the shared client's bounded retry policy
	//     (KAFKA_PRODUCER_RECORD_RETRIES, default 8, exponential backoff from
	//     100ms) was exhausted, or the produce hit a terminal transport error.
	//   non_retryable — Kafka classified the error as non-retriable
	//     (authorization, unknown topic); the client failed fast, no retries.
	//   not_attempted — the write was never attempted (egress queue overflow).
	FailureKindRetriesExhausted = "retries_exhausted"
	FailureKindNonRetryable     = "non_retryable"
	FailureKindNotAttempted     = "not_attempted"

	HeaderClientID = "client_id"
	SourceWSClient = "ws-client"
)

// Reason codes embedded in dead-letter message headers.
const (
	ReasonNoRoutingRuleMatched   = "no_routing_rule_matched"
	ReasonMissingChannelHeader   = "missing_channel_header"
	ReasonTenantPrefixMismatch   = "tenant_prefix_mismatch"
	ReasonInvalidChannelKey      = "invalid_channel_key"
	ReasonFanoutTopicWriteFailed = "fanout_topic_write_failed"
	// ReasonUnknownTopic marks a record whose topic is not in the registry topic→tenant map (#179 P3).
	// Such records are dropped WITHOUT committing the offset so they redeliver once the registry catches
	// up — they are never DLQ'd (the miss is expected to be transient).
	ReasonUnknownTopic = "unknown_topic"
)

// Prometheus metric names for routing and DLQ operations.
const (
	MetricDeadLetterTotal        = "ws_routing_dead_letter_total"
	MetricFanoutWriteFailedTotal = "ws_routing_fanout_write_failed_total"
	MetricDLQWriteFailedTotal    = "ws_routing_dlq_write_failed_total"
	MetricUnknownTopicTotal      = "ws_routing_unknown_topic_total"
	MetricFanoutDroppedTotal     = "ws_routing_fanout_dropped_total"
	// MetricDLQDroppedTotal counts records the producer could not enqueue to the DLQ pool
	// because its queue was full (TrySubmit returned false) — terminal loss, the DLQ write
	// was never attempted (#179 P1b-C4). Distinct from MetricDLQWriteFailedTotal (write
	// attempted, failed after retry exhaustion) and MetricFanoutDroppedTotal (fanout
	// enqueue-backpressure, still routed to DLQ).
	MetricDLQDroppedTotal = "ws_routing_dlq_dropped_total"
)

// Prometheus label key constants for routing metrics.
const (
	LabelTenant = "tenant"
	LabelTopic  = "topic"
	LabelReason = "reason"
)

// Fan-out dropped-copy reason label values for MetricFanoutDroppedTotal. They
// distinguish RECOVERABLE loss (queue-full copies are still dead-lettered) from
// TERMINAL loss (both shutdown legs), so alerts can act on the difference (ADR-0018).
const (
	FanoutDropReasonQueueFull    = "queue_full"         // handoff full; copy dead-lettered, not lost
	FanoutDropReasonShutdown     = "shutdown_undrained" // undrained when the pool timed out at shutdown
	FanoutDropReasonPostShutdown = "post_shutdown"      // submitted after shutdown began; not attempted
)

// Consumer type identifiers used as Prometheus label values and log field values.
const (
	ConsumerTypeKindShared    = "shared"
	ConsumerTypeKindDedicated = "dedicated"
)

// Structured log field keys used across kafka and orchestration packages.
const (
	LogFieldPartition = "partition" // pre-existing call sites in consumer.go and producer.go
	LogFieldOffset    = "offset"    // record offset within a partition
)

// Log message constants for broker-deleted-topic and fetch error events.
const (
	MsgTopicDeletedAtBroker = "topic deleted at broker — pausing fetches"
	MsgFetchError           = "kafka fetch error"
)

// Log message constants for rebalance duplicate delivery fix.
const (
	MsgCommitOnRevokeFailed  = "commit marked offsets on partition revoke failed"
	MsgCommitOnRevokeSuccess = "partitions revoked — marked offsets committed"
)

// Prometheus metric and label names for the broker-deleted-topic counter.
const (
	MetricConsumerTopicDeletedTotal = "ws_consumer_topic_deleted_total"
	LabelConsumerType               = "consumer_type"
)

// Prometheus metric names for rebalance commit tracking.
const (
	MetricRevokeCommitTotal           = "ws_consumer_revoke_commit_total"
	MetricRevokeCommitDurationSeconds = "ws_consumer_revoke_commit_duration_seconds"
)

// Prometheus metric names for broadcast publish retry (the at-least-once guard:
// a record is retried in place while the broadcast bus is down instead of being
// committed past — see Consumer.broadcastWithRetry).
const (
	MetricBroadcastRetriesTotal        = "ws_consumer_broadcast_retries_total"
	MetricBroadcastBlockedSecondsTotal = "ws_consumer_broadcast_blocked_seconds_total"
	// MetricRateLimitPacedSecondsTotal counts seconds the consume loop spent
	// pacing (bounded-blocking) on the WS_MAX_KAFKA_RATE limiter instead of
	// dropping — the at-least-once guard for recovery catch-up (ADR-0022).
	MetricRateLimitPacedSecondsTotal = "ws_consumer_rate_limit_paced_seconds_total"
)

// Label key and values for ws_consumer_revoke_commit_total{result=...}.
const (
	LabelResult   = "result"
	ResultSuccess = "success"
	ResultFailure = "failure"
)

// Structured log field keys for partition maps and timeout durations.
// LogFieldPartitions is plural (distinct from existing LogFieldPartition singular).
const (
	LogFieldPartitions = "partitions"
	LogFieldTimeout    = "timeout"
)
