package adminui

import "fmt"

// TemplateData is the base template data embedded by all page-level data structs.
type TemplateData struct {
	NeedsCodeMirror bool // set true for pages that use the JSON editor
}

// LoginData is passed to templates/login.html.
type LoginData struct {
	Error string // non-empty when the previous login attempt failed
}

// UpgradeData is passed to templates/upgrade.html.
type UpgradeData struct {
	TemplateData
}

// TenantRowData is one row in the tenants list table.
type TenantRowData struct {
	Slug            string
	Name            string
	Status          string
	ConnectionCount int
	CreatedAt       string
}

// TenantListData is passed to templates/tenants.html.
type TenantListData struct {
	TemplateData
	Rows       []TenantRowData
	TotalCount int
	Filter     string
}

// TenantSummary is the header card data for a single tenant.
type TenantSummary struct {
	Slug            string
	Name            string
	Status          string
	TopicCount      int
	KeyCount        int
	ConnectionCount int
}

// KeyRowData is one row in the tenant keys table.
type KeyRowData struct {
	KeyID     string
	Algorithm string
	CreatedAt string
	Active    bool
}

// AuditEntryData is one row in the audit log table.
type AuditEntryData struct {
	Timestamp string
	Action    string
	Actor     string
	Details   string
}

// ConnectionData is one row in the connections table.
type ConnectionData struct {
	ID            string
	RemoteAddr    string
	ConnectedAt   string
	Subscriptions []string
}

// ConnectionsData is passed to templates/partials/connections.html.
type ConnectionsData struct {
	TenantSlug  string
	Connections []ConnectionData
}

// RoutingRulesData is passed to templates/partials/routing_rules.html.
type RoutingRulesData struct {
	TenantSlug string
	Rules      *ChannelRulesView
}

// ChannelRulesView is a simplified view of provisioning.ChannelRules for the UI.
type ChannelRulesView struct {
	Public              []string
	GroupMappings       map[string]string
	PublishPublic       []string
	PublishGroupMapping map[string]string
}

// AuditData is passed to templates/partials/audit.html.
type AuditData struct {
	TenantSlug   string
	AuditEntries []AuditEntryData
}

// AnalyticsData is passed to templates/partials/analytics.html.
type AnalyticsData struct {
	TenantID         string
	CanPushAnalytics bool
}

// TenantDetailData is passed to templates/tenant_detail.html.
type TenantDetailData struct {
	TemplateData
	Tenant         TenantSummary
	Keys           []KeyRowData
	ActiveTab      string
	CanConnections bool
	CanAnalytics   bool
	CanAudit       bool
	TenantID       string
}

// StatusData is passed to templates/status.html.
type StatusData struct {
	TemplateData
	PollInterval string // e.g. "30s" — used in hx-trigger="every 30s"
}

// SystemHealthData is passed to templates/partials/system_health.html.
type SystemHealthData struct {
	Overall  string // "ok" | "degraded" | "unknown"
	Services []ServiceStatus
}

// ServiceStatus is one row in the health table.
type ServiceStatus struct {
	Name   string
	Status string // "ok" | "degraded" | "unknown" | "unconfigured"
}

// pollIntervalString formats adminUIStatusPollInterval for HTMX hx-trigger.
func pollIntervalString() string {
	return fmt.Sprintf("%.0fs", adminUIStatusPollInterval.Seconds())
}
