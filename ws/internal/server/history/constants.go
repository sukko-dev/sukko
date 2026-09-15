package history

// Valkey Streams field names for history entries.
const (
	HistoryFieldPayload  = "payload"
	HistoryFieldTenantID = "tenant_id"
	HistoryFieldChannel  = "channel"
	HistoryFieldSubject  = "subject"
	HistoryFieldCoord    = "coord"
	// HistoryFieldMid stores the stable message identity so history copies
	// carry the same mid as the live delivery (ADR-0008). Absent on entries
	// written before the field existed (readers treat absence as empty).
	HistoryFieldMid = "mid"
)

// HistoryCoordAuto is the coord field value written by the Valkey history writer
// when the message was stored using an auto-assigned (*) XADD ID rather than an
// encoded Kafka position. Clients receiving an entry with this coord value
// receive Pos="" (omitted via omitempty) and cannot use that entry as a replay cursor.
const HistoryCoordAuto = "auto"

// Key prefix templates for Valkey keys.
const (
	// HistoryStreamKeyPrefix is the prefix for per-channel Streams keys.
	// Full key: HistoryStreamKeyPrefix + env + ":" + tenantID + ":" + channel
	HistoryStreamKeyPrefix = "history:"

	// HistoryWriterLockKeyPrefix is the prefix for the distributed writer lock key.
	// Full key: HistoryWriterLockKeyPrefix + env
	HistoryWriterLockKeyPrefix = "history-writer-lock:"
)

// HistorySource values for history_complete and delivery metrics.
const (
	HistorySourceCache = "cache"
	// HistorySourceKafka and HistorySourceMixed will be added when Kafka fallback delivery is implemented.
)

// Error codes returned in history_error messages to clients.
const (
	ErrCodeHistoryDisabled           = "history_disabled"
	ErrCodeHistoryStorageUnavailable = "history_storage_unavailable"
	ErrCodeHistoryNotSubscribed      = "history_not_subscribed"
	ErrCodeHistoryInProgress         = "history_in_progress"
	ErrCodeHistoryInvalidLimit       = "history_invalid_limit"
	ErrCodeHistoryInvalidChannel     = "history_invalid_channel"
	ErrCodeHistoryInvalidRequest     = "history_invalid_request"
)

// Drop reason labels for the writeDropped and deliveryDropped metrics.
const (
	HistoryDropReasonPassive   = "passive"
	HistoryDropReasonFull      = "full"
	HistoryDropReasonEmptyMeta = "empty_meta"
	HistoryDropReasonPumpFull  = "pump_full"
)

// Skip reason labels for the skipTotal metric during history delivery.
const (
	HistorySkipReasonEmptyPayload = "empty_payload"
)

// StreamKey returns the Valkey Streams key for a given environment, tenant, and channel.
// Format: "history:{env}:{tenantID}:{channel}"
func StreamKey(env, tenantID, channel string) string {
	return HistoryStreamKeyPrefix + env + ":" + tenantID + ":" + channel
}

// Lua script: release the writer lock only if we own it (compare-and-delete).
// KEYS[1] = lock key, ARGV[1] = pod ID
// Returns 1 if deleted, 0 if not owner.
const historyWriterLockReleaseLuaScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`

// Lua script: renew the writer lock only if we still own it (compare-and-reset TTL).
// KEYS[1] = lock key, ARGV[1] = pod ID, ARGV[2] = TTL in milliseconds
// Returns 1 if renewed, 0 if not owner.
const historyWriterLockRenewLuaScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
`
