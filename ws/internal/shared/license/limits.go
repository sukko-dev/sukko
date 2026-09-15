package license

// Limits defines the hard limits for an edition. A value of 0 means unlimited.
type Limits struct {
	// Edition is which edition these limits belong to (included in error messages).
	Edition Edition `json:"edition,omitzero"`

	// MaxTenants is the maximum number of active (non-deleted) tenants.
	MaxTenants int `json:"max_tenants,omitzero"`

	// MaxTotalConnections is the maximum total WebSocket connections across all shards.
	MaxTotalConnections int `json:"max_total_connections,omitzero"`

	// MaxShards is the maximum number of ws-server shards (WS_NUM_SHARDS).
	MaxShards int `json:"max_shards,omitzero"`

	// MaxTopicsPerTenant is the maximum Kafka topics per tenant.
	MaxTopicsPerTenant int `json:"max_topics_per_tenant,omitzero"`

	// MaxRoutingRulesPerTenant is the maximum routing rules per tenant.
	MaxRoutingRulesPerTenant int `json:"max_routing_rules_per_tenant,omitzero"`
}

// DefaultLimits returns the hardcoded default limits for the given edition.
// These are the published tier limits. License key claims can override them
// per customer (non-zero claim values take precedence).
//
// Keep in sync with editionMatrix in sukko-cli (github.com/sukko-dev/cli
// commands/edition.go). Edition values change ~1-2x/year.
//
// Unknown or empty editions default to Community limits — the most restrictive
// tier is always the safe fallback.
func DefaultLimits(edition Edition) Limits {
	switch edition.normalize() {
	case Community:
		return Limits{
			Edition:                  Community,
			MaxTenants:               3,
			MaxTotalConnections:      500,
			MaxShards:                1,
			MaxTopicsPerTenant:       10,
			MaxRoutingRulesPerTenant: 10,
		}
	case Pro:
		return Limits{
			Edition:                  Pro,
			MaxTenants:               50,
			MaxTotalConnections:      10000,
			MaxShards:                8,
			MaxTopicsPerTenant:       50,
			MaxRoutingRulesPerTenant: 100,
		}
	case Enterprise:
		return Limits{
			Edition:                  Enterprise,
			MaxTenants:               0, // unlimited
			MaxTotalConnections:      0,
			MaxShards:                0,
			MaxTopicsPerTenant:       0,
			MaxRoutingRulesPerTenant: 0,
		}
	default:
		// Unknown editions fall back to Community (most restrictive).
		return DefaultLimits(Community)
	}
}

// CheckTenants checks if a new tenant can be created.
// current = existing active tenant count. Fails if current >= max (at the limit, can't add).
func (l Limits) CheckTenants(current int) error {
	return l.checkCount("tenants", current, l.MaxTenants)
}

// CheckTotalConnections checks if configured connection capacity exceeds the edition limit.
// current = MaxConnections × NumShards (configured total). Fails if current > max.
func (l Limits) CheckTotalConnections(current int) error {
	return l.checkValue("total_connections", current, l.MaxTotalConnections)
}

// CheckShards checks if the configured shard count is within the edition limit.
// current = WS_NUM_SHARDS (configured value). Fails if current > max.
func (l Limits) CheckShards(current int) error {
	return l.checkValue("shards", current, l.MaxShards)
}

// CheckTopicsPerTenant checks if a new topic can be created for a tenant.
// current = existing topic count. Fails if current >= max (at the limit, can't add).
func (l Limits) CheckTopicsPerTenant(current int) error {
	return l.checkCount("topics_per_tenant", current, l.MaxTopicsPerTenant)
}

// CheckRoutingRulesPerTenant checks if the requested rule count is within the edition limit.
// current = len(rules) in the request (Set is a full replace). Fails if current > max.
func (l Limits) CheckRoutingRulesPerTenant(current int) error {
	return l.checkValue("routing_rules_per_tenant", current, l.MaxRoutingRulesPerTenant)
}

// IsUnlimited returns true if the given limit value means "no limit" (0).
func IsUnlimited(val int) bool {
	return val == 0
}

// checkCount checks "existing count before adding one more".
// Used for tenants, topics — where current is the count and you're about to create another.
// Fails if current >= max (at the limit = can't add).
func (l Limits) checkCount(dimension string, current, limit int) error {
	if IsUnlimited(limit) {
		return nil
	}
	if current >= limit {
		return NewLimitError(dimension, current, limit, l.Edition)
	}
	return nil
}

// checkValue checks "configured or requested value against a ceiling".
// Used for shards, connections, routing rules — where current IS the value to validate.
// Fails if current > limit (the value itself exceeds the limit).
func (l Limits) checkValue(dimension string, current, limit int) error {
	if IsUnlimited(limit) {
		return nil
	}
	if current > limit {
		return NewLimitError(dimension, current, limit, l.Edition)
	}
	return nil
}
