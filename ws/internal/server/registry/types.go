package registry

import "time"

// registryEventKind identifies the type of registry event for processing in the writer.
type registryEventKind int

const (
	kindConnect registryEventKind = iota
	kindDisconnect
	kindSubscribe
	kindUnsubscribe
)

// registryEvent is the unit of work sent from server handlers to the registry writer.
// Non-blocking push via Writer.Push(); dropped events are counted by ws_registry_events_dropped_total.
type registryEvent struct {
	Kind           registryEventKind
	ConnID         string
	TenantID       string
	APIKeyID       string
	UserID         string
	PodID          string
	ShardID        int
	RemoteIP       string
	Transport      string
	ConnectedAt    time.Time
	Channels       []string // snapshot of subscribed channels at push time
	ChannelsCapped bool     // true when Channels was capped to MaxRegistryChannelsPerConnection
}

// AdminMsg is the JSON payload published to the per-pod admin channel.
// Provisioning publishes; AdminListener receives and dispatches.
type AdminMsg struct {
	Type         string `json:"type"`
	ConnectionID string `json:"connection_id"`
	TenantID     string `json:"tenant_id"`
	Reason       string `json:"reason"`
}

// connOwnership records identity information stored alongside each owned connection ID
// in the writer's in-memory index. Used for Valkey index cleanup on disconnect and shutdown.
type connOwnership struct {
	tenantID string
	apiKeyID string // empty when no API key was used for authentication
}
