package gateway

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// Channel check result values.
const (
	ChannelCheckAllowed = "allowed"
	ChannelCheckDenied  = "denied"
)

// AccessDenialResourceChannel is the resource type for channel-level access denials.
const AccessDenialResourceChannel = "channel"

// Access denial reason values.
const (
	AccessDenialReasonWrongTenant   = "wrong_tenant"
	AccessDenialReasonInvalidFormat = "invalid_format"
	AccessDenialReasonUnauthorized  = "unauthorized"
)

// Channel rules lookup source values.
const (
	LookupSourceCache         = "cache"
	LookupSourceDatabase      = "database"
	LookupSourceFallback      = "fallback"
	LookupSourceErrorFallback = "error_fallback" // Fallback due to unexpected provider error
)

// Proxy error type values.
const (
	ProxyErrorClientRead    = "client_read_error"
	ProxyErrorClientWrite   = "client_write_error"
	ProxyErrorBackendRead   = "backend_read_error"
	ProxyErrorBackendWrite  = "backend_write_error"
	ProxyErrorFrameTooLarge = "frame_too_large"
)

// Message direction values.
const (
	DirectionClientToBackend = "client_to_backend"
	DirectionBackendToClient = "backend_to_client"
)

// Connection close reason values.
const (
	CloseReasonNormal               = "normal"
	CloseReasonNoToken              = "no_token"
	CloseReasonInvalidToken         = "invalid_token"
	CloseReasonTenantLimitExceeded  = "tenant_limit_exceeded"
	CloseReasonUpgradeFailed        = "upgrade_failed"
	CloseReasonBackendUnavailable   = "backend_unavailable"
	CloseReasonNoCredentials        = "no_credentials"
	CloseReasonInvalidAPIKey        = "invalid_api_key"
	CloseReasonAPIKeyTenantMismatch = "api_key_tenant_mismatch" //nolint:gosec // close reason label, not a credential
	CloseReasonTenantUnavailable    = "tenant_unavailable"      // tenant-config projection cold; API-key slug resolution failed (retryable)
	// Revocation force-disconnect close_reason LABEL values — snake_case to match this vocabulary,
	// deliberately distinct from the space-containing WS close-frame text (CloseReasonTokenRevoked /
	// CloseReasonUserRevoked). resolveCloseReason maps frame text → these for the disconnect histogram.
	CloseReasonTokenRevokedLabel = "token_revoked"
	CloseReasonUserRevokedLabel  = "user_revoked"
)

// Token-revocation force-disconnect strings, shared by the fan-out (HandleRevocation) and
// the post-register re-check (§I — defined once, referenced everywhere).
const (
	// WS close-frame reason text for a revocation force-disconnect.
	CloseReasonTokenRevoked = "token revoked"
	CloseReasonUserRevoked  = "user revoked"
	// Transport label values (also returned by Proxy.Transport / sseConnection.Transport).
	TransportWS  = "ws"
	TransportSSE = "sse"
	// Detection-path values for the `detection` metric label / log field: which check found
	// the revocation. NOT named "source" — that label name is taken by channelRulesLookupTotal
	// (LookupSource*).
	DetectionFanOut       = "fan_out"       // closed by the one-shot HandleRevocation fan-out
	DetectionPostRegister = "post_register" // closed by the post-register re-check (race recovered)
)

// Metric label / log field key names for the token-revocation force-disconnect signal.
// The metric label "type" (below) and the log field key `provapi.LogFieldRevocationType`
// ("revocation_type") are DIFFERENT keys for the same concept: the metric label name is
// PRESERVED here for dashboard/alert compatibility (do not rename it to match the log field, and
// do not fold it into the shared provapi log-field const).
const (
	labelRevocationType = "type"
	labelTransport      = "transport"
	labelDetection      = "detection"
	logFieldDetection   = "detection"
)

// Prometheus metrics for the gateway service.
// Uses gateway_ prefix for service-specific metrics.

// =============================================================================
// Connection Metrics
// =============================================================================

var connectionsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "gateway_connections_total",
	Help: "Total WebSocket connections to gateway",
})

var connectionsActive = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "gateway_connections_active",
	Help: "Current active proxy sessions",
})

var connectionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "gateway_connection_duration_seconds",
	Help:    "Connection duration before disconnect",
	Buckets: pkgmetrics.ConnectionDurationBuckets,
}, []string{"close_reason"})

// =============================================================================
// Auth Metrics
// =============================================================================

var authValidations = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_auth_validations_total",
	Help: "Auth validation attempts by status and method",
}, []string{"status", "method"}) // status: success/failed/skipped; method: jwt/api_key/jwt+api_key/none

var authLatency = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "gateway_auth_latency_seconds",
	Help:    "JWT validation latency",
	Buckets: pkgmetrics.AuthLatencyBuckets,
})

// =============================================================================
// API Key Auth Metrics
// =============================================================================

// API key auth result values.
const (
	APIKeyAuthAccepted = "accepted"
	APIKeyAuthInvalid  = "invalid_key"
)

var apiKeyAuthTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_api_key_auth_total",
	Help: "API key authentication attempts by result",
}, []string{"result"}) // accepted, invalid_key

// =============================================================================
// Auth Refresh Metrics
// =============================================================================

var authRefreshTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_auth_refresh_total",
	Help: "Auth refresh attempts by result",
}, []string{"result"}) // success, invalid_token, token_expired, tenant_mismatch, rate_limited, not_available

var authRefreshLatency = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "gateway_auth_refresh_latency_seconds",
	Help:    "Auth refresh processing latency",
	Buckets: pkgmetrics.AuthLatencyBuckets,
})

var forcedUnsubscriptionsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "gateway_forced_unsubscriptions_total",
	Help: "Total forced unsubscriptions due to auth refresh permission changes",
})

// =============================================================================
// Permission Metrics
// =============================================================================

var channelChecks = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_channel_checks_total",
	Help: "Channel permission checks by result",
}, []string{"result"}) // allowed, denied

// =============================================================================
// Proxy Metrics
// =============================================================================

var messagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_messages_total",
	Help: "Messages proxied by direction",
}, []string{"direction"}) // client_to_backend, backend_to_client

var messageBytesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_message_bytes_total",
	Help: "Bytes proxied by direction",
}, []string{"direction"})

var proxyErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_proxy_errors_total",
	Help: "Proxy errors by type",
}, []string{"type"}) // read_error, write_error

// =============================================================================
// Publish Metrics
// =============================================================================

var publishTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_publish_total",
	Help: "Publish message intercepts by result",
}, []string{"result"}) // success, rate_limited, forbidden, etc.

var publishLatency = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "gateway_publish_latency_seconds",
	Help:    "Publish message interception latency",
	Buckets: pkgmetrics.AuthLatencyBuckets,
})

// =============================================================================
// Backend Metrics
// =============================================================================

var backendConnects = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_backend_connects_total",
	Help: "Backend connection attempts by status",
}, []string{"status"}) // success, failed

var backendLatency = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "gateway_backend_latency_seconds",
	Help:    "Backend dial latency",
	Buckets: pkgmetrics.BackendLatencyBuckets,
})

// =============================================================================
// Access Denial Metrics
// =============================================================================

var accessDenials = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_access_denials_total",
	Help: "Access denials by resource type and reason",
}, []string{"resource_type", "reason"})

// =============================================================================
// Key Cache Metrics
// =============================================================================

var keyCacheHits = promauto.NewCounter(prometheus.CounterOpts{
	Name: "gateway_key_cache_hits_total",
	Help: "Total key cache hits",
})

var keyCacheMisses = promauto.NewCounter(prometheus.CounterOpts{
	Name: "gateway_key_cache_misses_total",
	Help: "Total key cache misses",
})

var keyCacheSize = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "gateway_key_cache_size",
	Help: "Current number of keys in cache",
})

var keyCacheRefreshes = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_key_cache_refreshes_total",
	Help: "Key cache refresh operations by result",
}, []string{"result"}) // success, error

// =============================================================================
// Channel Rules Metrics
// =============================================================================

var channelRulesLookupTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_channel_rules_lookup_total",
	Help: "Channel rules lookups by source",
}, []string{"source"}) // source: cache, database, fallback

var channelAuthorizationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_channel_authorization_total",
	Help: "Channel authorization decisions by result",
}, []string{"result"}) // result: allowed, denied

// =============================================================================
// Helper Functions
// =============================================================================

// RecordConnection increments connection counter and active gauge.
func RecordConnection() {
	connectionsTotal.Inc()
	connectionsActive.Inc()
}

// RecordDisconnection decrements active gauge and records duration.
func RecordDisconnection(reason string, duration time.Duration) {
	connectionsActive.Dec()
	connectionDuration.WithLabelValues(reason).Observe(duration.Seconds())
}

// RecordAuthValidation records auth attempt with status, method, and latency.
func RecordAuthValidation(status, method string, latency time.Duration) {
	authValidations.WithLabelValues(status, method).Inc()
	authLatency.Observe(latency.Seconds())
}

// RecordChannelCheck records permission check result.
func RecordChannelCheck(result string) {
	channelChecks.WithLabelValues(result).Inc()
}

// RecordMessage records proxied message with direction and size.
func RecordMessage(direction string, bytes int) {
	messagesTotal.WithLabelValues(direction).Inc()
	messageBytesTotal.WithLabelValues(direction).Add(float64(bytes))
}

// RecordProxyError records proxy error by type.
func RecordProxyError(errorType string) {
	proxyErrors.WithLabelValues(errorType).Inc()
}

// RecordBackendConnect records backend connection attempt.
func RecordBackendConnect(status string, latency time.Duration) {
	backendConnects.WithLabelValues(status).Inc()
	backendLatency.Observe(latency.Seconds())
}

// RecordKeyCacheHit records a key cache hit.
func RecordKeyCacheHit() {
	keyCacheHits.Inc()
}

// RecordKeyCacheMiss records a key cache miss.
func RecordKeyCacheMiss() {
	keyCacheMisses.Inc()
}

// SetKeyCacheSize sets the current key cache size.
func SetKeyCacheSize(size int) {
	keyCacheSize.Set(float64(size))
}

// RecordKeyCacheRefresh records a cache refresh operation.
func RecordKeyCacheRefresh(success bool) {
	if success {
		keyCacheRefreshes.WithLabelValues(pkgmetrics.ResultSuccess).Inc()
	} else {
		keyCacheRefreshes.WithLabelValues(pkgmetrics.ResultError).Inc()
	}
}

// RecordAPIKeyAuth records API key authentication result.
func RecordAPIKeyAuth(result string) {
	apiKeyAuthTotal.WithLabelValues(result).Inc()
}

// RecordAccessDenial records an access denial with resource type and reason.
func RecordAccessDenial(resourceType, reason string) {
	accessDenials.WithLabelValues(resourceType, reason).Inc()
}

// RecordAuthRefresh records an auth refresh attempt result.
func RecordAuthRefresh(result string) {
	authRefreshTotal.WithLabelValues(result).Inc()
}

// RecordAuthRefreshLatency records auth refresh processing latency.
func RecordAuthRefreshLatency(seconds float64) {
	authRefreshLatency.Observe(seconds)
}

// RecordForcedUnsubscription records a forced unsubscription event.
func RecordForcedUnsubscription() {
	forcedUnsubscriptionsTotal.Inc()
}

// RecordPublishResult records a publish message interception result.
func RecordPublishResult(result string) {
	publishTotal.WithLabelValues(result).Inc()
}

// RecordPublishLatency records the latency of publish message interception.
func RecordPublishLatency(seconds float64) {
	publishLatency.Observe(seconds)
}

// RecordChannelRulesLookup records a channel rules lookup.
func RecordChannelRulesLookup(source string) {
	channelRulesLookupTotal.WithLabelValues(source).Inc()
}

// RecordChannelAuthorization records a channel authorization decision.
func RecordChannelAuthorization(result string) {
	channelAuthorizationTotal.WithLabelValues(result).Inc()
}

// AccessDenialMetricsAdapter implements auth.AccessDenialMetrics for Prometheus.
// This adapter allows the auth package to report access denial metrics without depending on gateway.
type AccessDenialMetricsAdapter struct{}

// OnAccessDenied records an access denial event.
func (a *AccessDenialMetricsAdapter) OnAccessDenied(resourceType, reason string) {
	accessDenials.WithLabelValues(resourceType, reason).Inc()
}

// KeyCacheMetricsAdapter implements auth.KeyCacheMetrics for Prometheus.
// This adapter allows the auth package to report metrics without depending on gateway.
type KeyCacheMetricsAdapter struct{}

// OnCacheHit records a key cache hit.
func (a *KeyCacheMetricsAdapter) OnCacheHit() {
	keyCacheHits.Inc()
}

// OnCacheMiss records a key cache miss.
func (a *KeyCacheMetricsAdapter) OnCacheMiss() {
	keyCacheMisses.Inc()
}

// OnCacheRefresh records a cache refresh operation.
func (a *KeyCacheMetricsAdapter) OnCacheRefresh(success bool, keyCount int) {
	if success {
		keyCacheRefreshes.WithLabelValues(pkgmetrics.ResultSuccess).Inc()
		keyCacheSize.Set(float64(keyCount))
	} else {
		keyCacheRefreshes.WithLabelValues(pkgmetrics.ResultError).Inc()
	}
}

// HandleMetrics serves Prometheus metrics.
func HandleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

// =============================================================================
// SSE + REST Publish Metrics
// =============================================================================

var (
	sseConnectionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_sse_connections_total",
		Help: "Total number of SSE connections established",
	})

	sseConnectionsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_sse_connections_active",
		Help: "Current number of active SSE connections",
	})

	sseConnectionDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_sse_connection_duration_seconds",
		Help:    "SSE connection duration before disconnect",
		Buckets: pkgmetrics.ConnectionDurationBuckets,
	})

	restPublishTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_rest_publish_total",
		Help: "Total REST publish requests by status",
	}, []string{"status"})

	restPublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_rest_publish_duration_seconds",
		Help:    "REST publish request duration",
		Buckets: pkgmetrics.APILatencyBuckets,
	})

	serverGRPCState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_server_grpc_state",
		Help: "gRPC connectivity to ws-server (1=connected, 0=disconnected)",
	})
)

// RecordSSEConnection records a new SSE connection.
func RecordSSEConnection() {
	sseConnectionsTotal.Inc()
	sseConnectionsActive.Inc()
}

// RecordSSEDisconnection records an SSE disconnection with duration.
func RecordSSEDisconnection(duration time.Duration) {
	sseConnectionsActive.Dec()
	sseConnectionDuration.Observe(duration.Seconds())
}

// RecordRestPublish records a REST publish result with latency.
func RecordRestPublish(status string, duration time.Duration) {
	restPublishTotal.WithLabelValues(status).Inc()
	restPublishDuration.Observe(duration.Seconds())
}

// SetServerGRPCState sets the gRPC connectivity state to ws-server.
func SetServerGRPCState(connected bool) {
	if connected {
		serverGRPCState.Set(1)
	} else {
		serverGRPCState.Set(0)
	}
}

// --- Token Revocation Metrics ---

var (
	tokenForceDisconnectsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_token_force_disconnects_total",
		Help: "Total connections force-disconnected due to token revocation",
	}, []string{labelRevocationType, labelTransport, labelDetection})
)

// RecordTokenForceDisconnect records a force-disconnect event by revocation type, transport,
// and detection path (fan-out vs post-register re-check — DetectionFanOut / DetectionPostRegister).
func RecordTokenForceDisconnect(revocationType, transport, detection string) {
	tokenForceDisconnectsTotal.WithLabelValues(revocationType, transport, detection).Inc()
}

// --- Identity Header Forwarding Metrics ---

var identityHeadersForwardedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "gateway_identity_headers_forwarded_total",
	Help: "Number of WebSocket upgrades where identity headers (X-Sukko-*) were forwarded to ws-server.",
})

// RecordIdentityHeadersForwarded records an identity header forwarding event.
func RecordIdentityHeadersForwarded() {
	identityHeadersForwardedTotal.Inc()
}

// Interface compliance checks.
var (
	_ pkgmetrics.AccessDenialMetrics = (*AccessDenialMetricsAdapter)(nil)
	_ pkgmetrics.CacheMetrics        = (*KeyCacheMetricsAdapter)(nil)
)
