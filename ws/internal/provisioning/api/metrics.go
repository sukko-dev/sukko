package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// Auth attempt result values (package-local).
const authResultFailure = "failure"

// Auth failure reason values (package-local).
const (
	authFailureReasonMissingToken   = "missing_token"
	authFailureReasonTokenExpired   = "token_expired"
	authFailureReasonKeyNotFound    = "key_not_found"
	authFailureReasonKeyRevoked     = "key_revoked"
	authFailureReasonInvalidToken   = "invalid_token"
	authFailureReasonTenantMismatch = "tenant_mismatch"
)

// Authorization denial reason values (package-local).
const (
	authzDenialInsufficientRole = "insufficient_role"
	authzDenialTenantMismatch   = "tenant_mismatch"
)

// Tenant operation label values (Constitution §I — metric label values are symbolic strings).
const (
	tenantOpCreate      = "create"
	tenantOpSuspend     = "suspend"
	tenantOpReactivate  = "reactivate"
	tenantOpDeprovision = "deprovision"
)

// Prometheus metrics for the provisioning service.
// Uses provisioning_ prefix for service-specific metrics.
var (
	// Tenant operations
	tenantsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "provisioning_tenants_created_total",
		Help: "Total number of tenants created",
	})

	tenantOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "provisioning_tenant_operations_total",
		Help: "Total tenant operations by type and result",
	}, []string{"operation", "result"})

	// Key operations
	keysCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "provisioning_keys_created_total",
		Help: "Total number of keys created",
	})

	keysRevoked = promauto.NewCounter(prometheus.CounterOpts{
		Name: "provisioning_keys_revoked_total",
		Help: "Total number of keys revoked",
	})

	// API key operations
	apiKeysCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "provisioning_api_keys_created_total",
		Help: "Total number of API keys created",
	})

	apiKeysRevoked = promauto.NewCounter(prometheus.CounterOpts{
		Name: "provisioning_api_keys_revoked_total",
		Help: "Total number of API keys revoked",
	})

	// Routing rules operations
	routingRulesSet = promauto.NewCounter(prometheus.CounterOpts{
		Name: "provisioning_routing_rules_set_total",
		Help: "Total number of routing rules set operations",
	})

	routingRulesDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "provisioning_routing_rules_deleted_total",
		Help: "Total number of routing rules delete operations",
	})

	// API request metrics
	apiRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "provisioning_api_requests_total",
		Help: "Total API requests by endpoint and status",
	}, []string{"endpoint", "method", "status"})

	apiLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "provisioning_api_latency_seconds",
		Help:    "API request latency in seconds",
		Buckets: pkgmetrics.APILatencyBuckets,
	}, []string{"endpoint", "method"})

	// Auth metrics for provisioning API
	authAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "provisioning_auth_attempts_total",
		Help: "Total authentication attempts",
	}, []string{"result", "failure_reason"})

	authorizationDenials = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "provisioning_authorization_denials_total",
		Help: "Total authorization denials by reason",
	}, []string{"reason"})
)

// RecordTenantCreated increments the tenant created counter.
func RecordTenantCreated() {
	tenantsCreated.Inc()
}

// RecordTenantOperation records a tenant operation result.
func RecordTenantOperation(operation, result string) {
	tenantOperations.WithLabelValues(operation, result).Inc()
}

// RecordKeyCreated increments the key created counter.
func RecordKeyCreated() {
	keysCreated.Inc()
}

// RecordKeyRevoked increments the key revoked counter.
func RecordKeyRevoked() {
	keysRevoked.Inc()
}

// RecordAPIKeyCreated increments the API key created counter.
func RecordAPIKeyCreated() {
	apiKeysCreated.Inc()
}

// RecordAPIKeyRevoked increments the API key revoked counter.
func RecordAPIKeyRevoked() {
	apiKeysRevoked.Inc()
}

// RecordRoutingRulesSet increments the routing rules set counter.
func RecordRoutingRulesSet() {
	routingRulesSet.Inc()
}

// RecordRoutingRulesDeleted increments the routing rules deleted counter.
func RecordRoutingRulesDeleted() {
	routingRulesDeleted.Inc()
}

// RecordAPIRequest records an API request with its result.
func RecordAPIRequest(endpoint, method, status string) {
	apiRequests.WithLabelValues(endpoint, method, status).Inc()
}

// RecordAPILatency records API request latency.
func RecordAPILatency(endpoint, method string, seconds float64) {
	apiLatency.WithLabelValues(endpoint, method).Observe(seconds)
}

// RecordAuthAttempt records an authentication attempt.
func RecordAuthAttempt(result, failureReason string) {
	authAttempts.WithLabelValues(result, failureReason).Inc()
}

// RecordAuthorizationDenial records an authorization denial.
func RecordAuthorizationDenial(reason string) {
	authorizationDenials.WithLabelValues(reason).Inc()
}
