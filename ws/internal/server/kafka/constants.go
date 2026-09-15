package kafka

// Kafka message header names used for channel-topic routing.
// HeaderChannel, HeaderSource, and HeaderTimestamp are the cross-service wire
// contract and live in internal/shared/kafka (referenced here as kafkashared.*).
const (
	HeaderReason          = "x-sukko-reason"
	HeaderFailedTopics    = "x-sukko-failed-topics"
	HeaderSucceededTopics = "x-sukko-succeeded-topics"

	// Client message provenance stamped by the producer on every outbound record.
	// HeaderClientID and SourceWSClient are server-specific; the source/timestamp
	// header keys are shared (kafkashared.HeaderSource / kafkashared.HeaderTimestamp).
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

// Consumer type identifiers used as Prometheus label values and log field values.
const (
	ConsumerTypeKindShared    = "shared"
	ConsumerTypeKindDedicated = "dedicated"
)

// Structured log field keys used across kafka and orchestration packages.
const (
	LogFieldPartition = "partition" // pre-existing call sites in consumer.go and producer.go
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
