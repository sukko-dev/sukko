package registry

import "fmt"

// Valkey key prefix constants.
// All registry keys are namespaced by environment to support multi-environment Valkey clusters.
const (
	// ConnRegistryKeyPrefix is the prefix for per-connection hash keys.
	// Full key: "connections:{env}:{connID}"
	ConnRegistryKeyPrefix = "connections:"

	// ConnRegistryIdxPrefix is the prefix for secondary index set keys.
	// Full tenant index: "conn-idx:{env}:{tenantID}"
	// Full API key index: "conn-idx:{env}:{tenantID}:apikey:{apiKeyID}"
	ConnRegistryIdxPrefix = "conn-idx:"

	// ConnRegistryHealthPrefix is the prefix for per-pod health hash keys.
	// Full key: "conn-health:{env}:{podID}"
	ConnRegistryHealthPrefix = "conn-health:"

	// RegistryAdminChannelPrefix is the base prefix for admin pub/sub channels.
	// Reserved for future PSUBSCRIBE pattern matching — current code uses AdminChannelName() for all pub/sub.
	RegistryAdminChannelPrefix = "ws.admin."

	// AdminChannelNameFmt is the format string for the per-pod admin channel name.
	// Single source of truth: both the publisher (provisioning) and subscriber (AdminListener)
	// MUST derive the channel name from this constant via AdminChannelName(env, podID).
	AdminChannelNameFmt = "ws.admin.%s.%s"
)

// Admin message types.
const (
	AdminMsgTypeDisconnect = "disconnect"
)

// AdminDisconnect reason codes.
const (
	AdminDisconnectReasonOperatorRequest = "operator_request"
	AdminDisconnectReasonKeyRevoked      = "key_revoked"
)

// Drop reason labels for Prometheus counters.
const (
	DropReasonFull     = "full"
	DropReasonShutdown = "shutdown"
)

// AdminMsgChanSize is the buffer capacity for the per-shard admin message channel.
// Sized for burst admin operations; ws_admin_channel_msg_dropped_total fires when full.
const AdminMsgChanSize = 64

// Registry hash field names (written by registry writer, read by provisioning reader).
const (
	FieldTenantID       = "tenant_id"
	FieldAPIKeyID       = "api_key_id"
	FieldUserID         = "user_id"
	FieldPodID          = "pod_id"
	FieldShardID        = "shard_id"
	FieldRemoteIP       = "remote_ip"
	FieldTransport      = "transport"
	FieldConnectedAt    = "connected_at"
	FieldChannels       = "channels"
	FieldChannelsCapped = "channels_capped"
)

// Health hash field names (written by registry writer heartbeat, read by provisioning reader).
const (
	HealthFieldDrops               = "drops_since_last_flush"
	HealthFieldLastHeartbeat       = "last_heartbeat_at"
	HealthFieldAdminChannelHealthy = "admin_channel_healthy"
)

// Registry status values returned by the provisioning API.
const (
	RegistryStatusCurrent       = "current"
	RegistryStatusPossiblyStale = "possibly_stale"
)

// HealthValueTrue is the string written to admin_channel_healthy when the shard's admin
// channel subscription is confirmed healthy. Read rule: non-empty = healthy.
// Write rule: write HealthValueTrue for healthy, write "" or omit the field for unhealthy.
const HealthValueTrue = "true"

// Key builder functions.

// ConnKey returns the Valkey hash key for a given connection.
func ConnKey(env, connID string) string {
	return ConnRegistryKeyPrefix + env + ":" + connID
}

// TenantIdxKey returns the Valkey set key for the per-tenant connection index.
func TenantIdxKey(env, tenantID string) string {
	return ConnRegistryIdxPrefix + env + ":" + tenantID
}

// APIKeyIdxKey returns the Valkey set key for the per-tenant-API-key connection index.
func APIKeyIdxKey(env, tenantID, apiKeyID string) string {
	return ConnRegistryIdxPrefix + env + ":" + tenantID + ":apikey:" + apiKeyID
}

// HealthKey returns the Valkey hash key for a pod's health metadata.
func HealthKey(env, podID string) string {
	return ConnRegistryHealthPrefix + env + ":" + podID
}

// AdminChannelName returns the Valkey pub/sub channel name for a pod's admin listener.
// All publishers and subscribers MUST use this function — never inline the format string.
func AdminChannelName(env, podID string) string {
	return fmt.Sprintf(AdminChannelNameFmt, env, podID)
}
