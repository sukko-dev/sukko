package analytics

import "time"

// Advisory lock IDs for rollup promotion jobs.
// Must not collide with migrationLockID = 0x73756B6B6F (1936617583) in database/migrate.go.
const (
	rollupHourLockID int64 = 1001
	rollupDayLockID  int64 = 1002
)

// Bucket size label values — used as the bucket_size column in analytics tables.
const (
	bucketSizeMinute = "minute"
	bucketSizeHour   = "hour"
	bucketSizeDay    = "day"
)

// Analytics table names.
const (
	tableConnections     = "analytics_connections"
	tableMessages        = "analytics_messages"
	tablePush            = "analytics_push"
	tableConnectionsHour = "analytics_connections_hour"
	tableConnectionsDay  = "analytics_connections_day"
	tableMessagesHour    = "analytics_messages_hour"
	tableMessagesDay     = "analytics_messages_day"
	tablePushHour        = "analytics_push_hour"
	tablePushDay         = "analytics_push_day"
	tableRawEvents       = "analytics_raw_events"
)

// All minute-level partitioned tables (daily partitions, 48h retention for connections,
// 30d retention for messages/push).
var minuteTables = []string{tableConnections, tableMessages, tablePush}

// All hour-level rollup tables (daily partitions, 30d retention).
var hourTables = []string{tableConnectionsHour, tableMessagesHour, tablePushHour}

// All day-level rollup tables (monthly partitions, 365d retention).
var dayTables = []string{tableConnectionsDay, tableMessagesDay, tablePushDay}

// Timing constants (§I: magic numbers must be named constants).
const (
	defaultFlushDeadline = 10 * time.Second // flush goroutine final-flush deadline on shutdown
	// sseIdleTimeout removed — SSE handler lives in provisioning/api; §X prohibits cross-package duplicates.
	pendingFlushQueueDivisor = 100 // pendingFlush chan capacity = max(BufferSize/100, 10)
	pendingFlushQueueMin     = 10  // minimum pending-flush queue capacity

	defaultDowngradePoll       = 5 * time.Minute // fallback DowngradePoll when cfg value is zero
	analyticsDefaultBufferSize = 10000           // fallback BufferSize when CollectorConfig.BufferSize is zero
)

// Retention boundaries for partition manager.
const (
	retentionMinutePartitions = 2  // daily partitions: 2 × 24h = 48h
	retentionHourPartitions   = 30 // daily partitions: 30 days
	retentionDayPartitions    = 12 // monthly partitions: 12 months = 365d
)
