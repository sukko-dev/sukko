package adminui

import "time"

// Session cookie constants — §I: magic strings are named constants.
const (
	AdminSessionCookieName = "admin_session"
	AdminSessionCookiePath = "/admin"
)

// Health check timings for GET /admin/system/health.
const (
	adminHealthCheckTimeout = 5 * time.Second  // per-downstream probe timeout
	adminHealthCacheTTL     = 10 * time.Second // cached result reuse window
)

// adminUIStatusPollInterval is the HTMX hx-trigger poll interval for the status page.
const adminUIStatusPollInterval = 30 * time.Second

// maxAdminSessions is the upper bound on concurrent sessions before Prune() returns 503.
const maxAdminSessions = 100

// adminTenantListLimit is the maximum number of tenants fetched for the admin UI list.
// Client-side filtering runs over this set; raise if operators regularly manage more tenants.
const adminTenantListLimit = 200

// adminAuditLogLimit is the maximum number of audit entries fetched per partial refresh.
const adminAuditLogLimit = 50

// adminLoginMaxBodyBytes is the maximum accepted form body size for the login endpoint.
const adminLoginMaxBodyBytes = 4096

// Login result label values for provisioning_admin_ui_login_attempts_total metric.
// Defined as constants so the metric label and log field stay in sync (§I).
const (
	loginResultSuccess         = "success"
	loginResultInvalidToken    = "invalid_token"
	loginResultMissingToken    = "missing_token"
	loginResultTooManySessions = "too_many_sessions"
)
