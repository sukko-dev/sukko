package license

// Feature represents a gated capability in the Sukko platform.
type Feature string

// FeatureStatus indicates the implementation state of a feature.
type FeatureStatus string

const (
	// StatusImplemented — feature is functional with EditionHasFeature gate check wired.
	StatusImplemented FeatureStatus = "implemented"

	// StatusFuture — feature is not yet implemented, gate reserved for future use.
	StatusFuture FeatureStatus = "future"
)

// FeaturePriority indicates implementation priority for unimplemented features.
// Lower number = higher priority. Implemented features use PriorityNone.
type FeaturePriority int

// FeaturePriority constants for implementation ordering.
const (
	PriorityNone   FeaturePriority = 0 // Implemented — no priority needed
	PriorityHigh   FeaturePriority = 2 // Next to implement
	PriorityMedium FeaturePriority = 3 // Planned
	PriorityLow    FeaturePriority = 4 // Future / nice-to-have
)

// FeatureInfo holds metadata about a gated feature.
type FeatureInfo struct {
	// Description is a human-readable summary of what the feature does.
	Description string

	// Status indicates whether the feature is implemented or future.
	Status FeatureStatus

	// Priority indicates implementation priority (0 = implemented, 2 = high, 4 = low).
	Priority FeaturePriority
}

// Feature Matrix — Single Source of Truth
//
// Every gated feature is listed here with its edition requirement and metadata.
// The docs site (sukko-docs) auto-generates the editions comparison page from
// this file via the extract-editions script. Update metadata when implementing.
// The section headers below group features by their EDITION in featureEditions
// (the authoritative gate map). The inline `// Implemented` / `// Future` marker
// on each line is its implementation STATUS, orthogonal to edition.
const (
	// ── Community Features (available in every edition) ──────────────────
	//
	// The full data path (Kafka ingestion, message history, live gap
	// recovery, REST publish) is Community per ADR-0009: the free tier must
	// reproduce the public benchmark feature-wise; capacity caps in
	// limits.go are the tier wall. Explicit Community map entries make the
	// docs editions extractor render the rows.

	// PerTenantChannelRules is ungated: channel rules are the sole channel
	// authorization mechanism (provisioning-only), so every edition needs
	// them.
	PerTenantChannelRules Feature = "CHANNEL_RULES"         // Implemented — ungated
	KafkaBackend          Feature = "MESSAGE_BACKEND=kafka" // Implemented — ungated
	MessageHistory        Feature = "message history"       // Implemented — ungated
	LiveGapRecovery       Feature = "live gap recovery"     // Implemented — ungated
	RESTPublish           Feature = "REST publish"          // Implemented — ungated

	// ChannelTopicRouting is ungated per ADR-0014: routing rules are the sole
	// channel→topic mapping (no convention fallback, #179), so gating them
	// would leave the Community Kafka backend unusable for publish.
	// MaxRoutingRulesPerTenant in limits.go is the wall (Community 10).
	// The rule's ingress_topic mapping is Community; EGRESS topics on a rule
	// are the separate Pro-gated EgressTopics feature (ADR-0018).
	ChannelTopicRouting Feature = "CHANNEL_TOPIC_ROUTING" // Implemented — ungated

	// ── Pro Features ─────────────────────────────────────────────────────

	PerTenantConnectionLimits   Feature = "TENANT_CONNECTION_LIMIT_ENABLED" // Implemented
	Alerting                    Feature = "ALERT_ENABLED"                   // Implemented
	PerTenantConfigurableQuotas Feature = "per-tenant configurable quotas"  // Implemented
	TenantLifecycleManager      Feature = "tenant lifecycle manager"        // Implemented
	ConnectionTracing           Feature = "connection tracing"              // Implemented
	SSETransport                Feature = "SSE transport"                   // Implemented
	Analytics                   Feature = "real-time analytics"             // Implemented
	AdminUI                     Feature = "admin UI"                        // Implemented
	TokenRevocation             Feature = "token revocation"                // Implemented
	Webhooks                    Feature = "webhook delivery"                // Implemented
	ConnectionsAPI              Feature = "connections management API"      // Implemented
	WebPush                     Feature = "web push notifications"          // Implemented
	AnalyticsPush               Feature = "real-time analytics for push"    // Implemented
	// EgressTopics gates the egress_topics field on routing rules (ADR-0018):
	// best-effort copies of published messages to additional Kafka topics the
	// platform never consumes (audit/analytics export). A rule with only an
	// ingress_topic never trips this gate — Community keeps full routing.
	EgressTopics       Feature = "egress topics"          // Implemented
	ChannelPatternsCEL Feature = "channel patterns (CEL)" // Future
	DeltaCompression   Feature = "delta compression"      // Future

	// ── Enterprise Features ──────────────────────────────────────────────

	// Note: the AuditLogging gate covers only the audit-log query API (GET /audit-log). Audit record writes are unconditional — not gated.
	AuditLogging Feature = "audit logging" // Implemented
	// MobilePush (FCM + APNs) is frozen per ADR-0009 — e2e-unvalidated; no
	// further investment until a buyer asks. The gate stays enforced.
	MobilePush          Feature = "mobile push notifications (FCM + APNs)" // Implemented
	IPAllowlisting      Feature = "per-tenant IP allowlisting"             // Future
	PriorityRouting     Feature = "priority message routing"               // Future
	CustomQuotaPolicies Feature = "custom quota policies"                  // Future
)

// featureEditions maps each feature to its minimum required edition.
// Features not in this map default to Community (always available).
var featureEditions = map[Feature]Edition{
	// Community — explicit entries so the docs editions extractor, which
	// emits only map entries, renders the features as available in all
	// editions. EditionHasFeature returns true for every edition ≥ Community.
	// Data path is Community per ADR-0009 (capacity caps are the tier wall).
	PerTenantChannelRules: Community,
	KafkaBackend:          Community,
	MessageHistory:        Community,
	LiveGapRecovery:       Community,
	RESTPublish:           Community,
	ChannelTopicRouting:   Community,

	// Pro
	PerTenantConnectionLimits:   Pro,
	Alerting:                    Pro,
	PerTenantConfigurableQuotas: Pro,
	TenantLifecycleManager:      Pro,
	ConnectionTracing:           Pro,
	SSETransport:                Pro,
	Analytics:                   Pro,
	AdminUI:                     Pro,
	TokenRevocation:             Pro,
	Webhooks:                    Pro,
	ConnectionsAPI:              Pro,
	WebPush:                     Pro,
	AnalyticsPush:               Pro,
	EgressTopics:                Pro,
	ChannelPatternsCEL:          Pro,
	DeltaCompression:            Pro,

	// Enterprise
	AuditLogging:        Enterprise,
	MobilePush:          Enterprise,
	IPAllowlisting:      Enterprise,
	PriorityRouting:     Enterprise,
	CustomQuotaPolicies: Enterprise,
}

// featureMetadata holds implementation status and priority for each feature.
// This is the authoritative metadata — the docs site reads this via extraction.
var featureMetadata = map[Feature]FeatureInfo{
	// ── Implemented — Community ──────────────────────────────────────────

	PerTenantChannelRules: {Description: "Per-tenant channel subscribe/publish rules", Status: StatusImplemented, Priority: PriorityNone},
	KafkaBackend:          {Description: "Kafka/Redpanda message backend", Status: StatusImplemented, Priority: PriorityNone},
	MessageHistory:        {Description: "Queryable message history per channel", Status: StatusImplemented, Priority: PriorityNone},
	LiveGapRecovery:       {Description: "In-band gap detection and live replay on existing connections without reconnect", Status: StatusImplemented, Priority: PriorityNone},
	RESTPublish:           {Description: "REST publish endpoint (HTTP ingestion without Kafka or WebSocket)", Status: StatusImplemented, Priority: PriorityNone},

	// ── Implemented — Pro ────────────────────────────────────────────────

	PerTenantConnectionLimits:   {Description: "Per-tenant WebSocket connection limits", Status: StatusImplemented, Priority: PriorityNone},
	Alerting:                    {Description: "AlertManager integration for Prometheus alerts", Status: StatusImplemented, Priority: PriorityNone},
	PerTenantConfigurableQuotas: {Description: "Per-tenant configurable resource quotas (topics, connections, rules)", Status: StatusImplemented, Priority: PriorityNone},
	TenantLifecycleManager:      {Description: "Tenant suspend/reactivate lifecycle management", Status: StatusImplemented, Priority: PriorityNone},
	ConnectionTracing:           {Description: "OpenTelemetry distributed tracing for connections", Status: StatusImplemented, Priority: PriorityNone},
	SSETransport:                {Description: "SSE transport", Status: StatusImplemented, Priority: PriorityNone},
	ChannelTopicRouting:         {Description: "Per-tenant routing rules mapping channels to a ingress topic, with optional egress-only topic copies and header-based consumer routing", Status: StatusImplemented, Priority: PriorityNone},
	TokenRevocation:             {Description: "Revoke individual JWT tokens (not just keys)", Status: StatusImplemented, Priority: PriorityNone},
	ConnectionsAPI:              {Description: "Inspect and force-disconnect live connections per tenant", Status: StatusImplemented, Priority: PriorityNone},
	AdminUI:                     {Description: "Web-based tenant management interface", Status: StatusImplemented, Priority: PriorityNone},
	Analytics:                   {Description: "Real-time per-tenant usage analytics (connections, messages)", Status: StatusImplemented, Priority: PriorityNone},
	Webhooks:                    {Description: "HTTP webhook delivery as alternative to WebSocket", Status: StatusImplemented, Priority: PriorityNone},
	EgressTopics:                {Description: "Egress topics on routing rules — best-effort copies of published messages to additional Kafka topics for external consumers", Status: StatusImplemented, Priority: PriorityNone},
	WebPush:                     {Description: "Web Push notifications (VAPID) to browsers", Status: StatusImplemented, Priority: PriorityNone},
	AnalyticsPush:               {Description: "Real-time push delivery analytics (platform breakdown, failure reasons)", Status: StatusImplemented, Priority: PriorityNone},

	// ── Implemented — Enterprise ──────────────────────────────────────────

	// Note: AuditLogging covers only the audit-log query API (GET /audit-log); writes are unconditional.
	AuditLogging: {Description: "Audit trail of all provisioning API actions", Status: StatusImplemented, Priority: PriorityNone},
	MobilePush:   {Description: "Mobile push notifications (FCM + APNs)", Status: StatusImplemented, Priority: PriorityNone},

	// ── Future — Pro ─────────────────────────────────────────────────────

	ChannelPatternsCEL: {Description: "CEL expressions for complex channel authorization", Status: StatusFuture, Priority: PriorityLow},
	DeltaCompression:   {Description: "Send only changed fields in high-frequency updates", Status: StatusFuture, Priority: PriorityLow},

	// ── Future — Enterprise ──────────────────────────────────────────────

	IPAllowlisting:      {Description: "Per-tenant IP allowlisting for connection filtering", Status: StatusFuture, Priority: PriorityLow},
	PriorityRouting:     {Description: "Priority-based message delivery under load", Status: StatusFuture, Priority: PriorityLow},
	CustomQuotaPolicies: {Description: "Tenant-specific quota rules beyond simple limits", Status: StatusFuture, Priority: PriorityLow},
}

// GetFeatureInfo returns metadata for a feature. Returns zero value if not found.
func GetFeatureInfo(feature Feature) FeatureInfo {
	return featureMetadata[feature]
}

// RequiredEdition returns the minimum edition needed for the given feature.
// Returns Community if the feature is not gated (always available).
func RequiredEdition(feature Feature) Edition {
	if edition, ok := featureEditions[feature]; ok {
		return edition
	}
	return Community
}

// EditionHasFeature returns true if the given edition includes the feature.
// This is a pure function with no Manager dependency — suitable for use in
// Config.Validate() with a startup-resolved edition value.
func EditionHasFeature(edition Edition, feature Feature) bool {
	return edition.IsAtLeast(RequiredEdition(feature))
}
