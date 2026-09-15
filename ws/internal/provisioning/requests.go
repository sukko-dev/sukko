package provisioning

import (
	"time"
)

// CreateTenantRequest is the request to create a new tenant.
type CreateTenantRequest struct {
	// Slug is the human-readable Kafka namespace and URL identifier (e.g., "acme").
	Slug string `json:"slug"`

	// Name is the display name.
	Name string `json:"name"`

	// ConsumerType is the consumer group assignment (optional, defaults to shared).
	ConsumerType ConsumerType `json:"consumer_type,omitempty"`

	// Metadata is optional key-value data.
	Metadata Metadata `json:"metadata,omitempty"`

	// PublicKey is the initial public key to register (optional).
	PublicKey *CreateKeyRequest `json:"public_key,omitempty"`
}

// RenameTenantRequest is the request to rename a tenant's slug.
type RenameTenantRequest struct {
	// Slug is the desired new slug.
	Slug string `json:"slug"`
}

// CreateTenantResponse is the response from creating a tenant.
type CreateTenantResponse struct {
	// Tenant is the created tenant.
	Tenant *Tenant `json:"tenant"`

	// Key is the registered key (if provided).
	Key *TenantKey `json:"key,omitempty"`
}

// UpdateTenantRequest is the request to update a tenant.
type UpdateTenantRequest struct {
	// Name is the new display name (optional).
	Name *string `json:"name,omitempty"`

	// ConsumerType is the new consumer type (optional).
	ConsumerType *ConsumerType `json:"consumer_type,omitempty"`

	// Metadata is the new metadata (replaces existing).
	Metadata Metadata `json:"metadata,omitempty"`

	// Slug is rejected — slugs are immutable via PATCH (use /rename endpoint).
	Slug *string `json:"slug,omitempty"`
}

// CreateKeyRequest is the request to register a public key.
type CreateKeyRequest struct {
	// KeyID is the unique identifier (kid in JWT header).
	KeyID string `json:"key_id"`

	// Algorithm is the signing algorithm.
	Algorithm Algorithm `json:"algorithm"`

	// PublicKey is the PEM-encoded public key.
	PublicKey string `json:"public_key"`

	// ExpiresAt is the optional expiration time.
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// CreateAPIKeyRequest is the request to create a new API key.
type CreateAPIKeyRequest struct {
	// Name is an optional human-readable label for the key.
	Name string `json:"name"`
}

// ReplaceRoutingRulesRequest is the request to atomically replace all routing rules for a tenant.
type ReplaceRoutingRulesRequest struct {
	// Rules are the ordered topic routing rules.
	Rules []TopicRoutingRule `json:"rules"`
}

// AddRoutingRuleRequest is the request to add a single routing rule for a tenant.
type AddRoutingRuleRequest struct {
	// Rule is the routing rule to add.
	Rule TopicRoutingRule `json:"rule"`
}

// UpdateQuotaRequest is the request to update quotas.
type UpdateQuotaRequest struct {
	// MaxTopics is the new max topics limit.
	MaxTopics *int `json:"max_topics,omitempty"`

	// MaxPartitions is the new max partitions limit.
	MaxPartitions *int `json:"max_partitions,omitempty"`

	// MaxStorageBytes is the new max storage limit.
	MaxStorageBytes *int64 `json:"max_storage_bytes,omitempty"`

	// ProducerByteRate is the new producer rate limit.
	ProducerByteRate *int64 `json:"producer_byte_rate,omitempty"`

	// ConsumerByteRate is the new consumer rate limit.
	ConsumerByteRate *int64 `json:"consumer_byte_rate,omitempty"`

	// MaxConnections is the new max concurrent WebSocket connections limit.
	// 0 means use the system default.
	MaxConnections *int `json:"max_connections,omitempty"`
}

// SetChannelRulesRequest is the request to set channel rules for a tenant.
type SetChannelRulesRequest struct {
	// Public contains channel patterns accessible to all authenticated users.
	Public []string `json:"public,omitempty"`

	// GroupMappings maps group names to allowed channel patterns.
	GroupMappings map[string][]string `json:"group_mappings,omitempty"`

	// Default contains channel patterns allowed when no groups match.
	Default []string `json:"default,omitempty"`

	// PublishPublic contains channel patterns any authenticated user can publish to.
	PublishPublic []string `json:"publish_public,omitempty"`

	// PublishGroupMappings maps IdP group names to allowed publish channel patterns.
	PublishGroupMappings map[string][]string `json:"publish_group_mappings,omitempty"`

	// PublishDefault contains publish channel patterns when no groups match.
	PublishDefault []string `json:"publish_default,omitempty"`
}

// TestAccessRequest is the request to test channel access for given groups.
type TestAccessRequest struct {
	// Groups are the JWT groups to test access for.
	Groups []string `json:"groups"`
}

// TestAccessResponse is the response from testing channel access.
type TestAccessResponse struct {
	// AllowedPatterns are the channel patterns the groups have access to.
	AllowedPatterns []string `json:"allowed_patterns"`
}
