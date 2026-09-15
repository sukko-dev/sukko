// Package gateway provides WebSocket connection handling with authentication,
// proxying to backend servers, and permission-based channel filtering.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gobwas/ws"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"

	"github.com/sukko-dev/sukko/internal/shared/analytics"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/profiling"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/version"
)

// wsServerEditionTimeout is the HTTP client timeout for fetching ws-server's /edition.
// Cold path only — called per /edition request, not on the WebSocket hot path.
const wsServerEditionTimeout = 2 * time.Second

// OTEL tracing constants — instrumentation scope and span names.
const (
	tracerName           = "github.com/sukko-dev/sukko/internal/gateway"
	spanWebSocketUpgrade = "websocket.upgrade"
)

// ServiceName is the canonical identifier for the ws-gateway service.
// Used in health responses, logging, tracing, and profiling.
const ServiceName = "ws-gateway"

// MetricPrefix is the Prometheus metric name prefix for all gateway metrics.
const MetricPrefix = "gateway"

// Gateway handles WebSocket connections, authenticating clients and proxying
// to the ws-server backend with permission-based channel filtering.
type Gateway struct {
	config    *platform.GatewayConfig
	validator *auth.MultiTenantValidator

	// analyticsCollector records per-tenant connection metrics.
	// Set via SetAnalyticsCollector after construction; always a valid interface (NoopCollector when nil not set).
	analyticsCollector analytics.Collector

	// gRPC stream registries for provisioning data (keys, channel rules, API keys)
	streamKeyRegistry    *provapi.StreamKeyRegistry
	streamChannelRules   *provapi.StreamChannelRulesProvider
	streamAPIKeyRegistry *provapi.StreamAPIKeyRegistry // concrete for State() in health
	streamLicenseWatcher licenseWatcher                // nil when provisioning not configured

	apiKeyRegistry       APIKeyLookup       // interface for Lookup() + mock injection in tests
	tenantSlugResolver   TenantSlugResolver // UUID->slug for API-key auth; nil in tests unless injected
	channelRulesProvider ChannelRulesProvider
	connTracker          *TenantConnectionTracker // Per-tenant connection tracking
	tenantPermChecker    *TenantPermissionChecker // Sole channel-authorization source (provisioning-only)
	wsServerHTTPClient   *http.Client             // Reused for ws-server /edition calls (cold path)

	// SSE + REST Publish (Pro edition)
	serverClient       *ServerClient       // gRPC client to ws-server RealtimeService
	publishRateLimiter *PublishRateLimiter // Per-tenant + per-IP rate limiting for REST publish

	// Web Push (Enterprise edition)
	pushClient PushForwarder // gRPC client to push service PushService (interface for testability)

	// Token revocation (Pro edition)
	connectionRegistry *ConnectionRegistry
	revocationRegistry RevocationChecker

	logger zerolog.Logger
}

// New creates a new Gateway instance.
// For multi-tenant mode, this connects to the provisioning service via gRPC streaming.
// Call Close() to release resources when shutting down.
func New(config *platform.GatewayConfig, logger zerolog.Logger) (*Gateway, error) {
	gw := &Gateway{
		config:             config,
		wsServerHTTPClient: &http.Client{Timeout: wsServerEditionTimeout},
		logger:             logger.With().Str("component", "gateway").Logger(),
	}

	// Set up per-tenant connection tracking if enabled
	if config.TenantConnectionLimitEnabled {
		gw.connTracker = NewTenantConnectionTracker(config.DefaultTenantConnectionLimit)
		gw.logger.Info().
			Int("default_limit", config.DefaultTenantConnectionLimit).
			Msg("Per-tenant connection limits enabled")
	}

	if err := gw.setupValidator(); err != nil {
		return nil, fmt.Errorf("setup validator: %w", err)
	}

	// Per-tenant channel rules are the sole channel-authorization source
	// (provisioning-only) — the checker is always constructed. The rules
	// provider is wired by setupValidator from the provisioning gRPC stream.
	permChecker, err := NewTenantPermissionChecker(
		gw.channelRulesProvider,
		gw.logger.With().Str("component", "tenant_permissions").Logger(),
	)
	if err != nil {
		_ = gw.Close() // cleanup license watcher + stream registries
		return nil, fmt.Errorf("create tenant permission checker: %w", err)
	}
	gw.tenantPermChecker = permChecker

	// Token revocation — connection registry always created for registration.
	// Revocation stream registry wired externally via SetRevocationRegistry (same pattern as SetPushClient).
	gw.connectionRegistry = NewConnectionRegistry()

	return gw, nil
}

// setupValidator configures the multi-tenant JWT validator with asymmetric keys.
// Keys and tenant configs are streamed from the provisioning service via gRPC.
func (gw *Gateway) setupValidator() error {
	// Create gRPC stream-backed key registry
	keyRegistry, err := provapi.NewStreamKeyRegistry(provapi.StreamKeyRegistryConfig{
		GRPCAddr:          gw.config.ProvisioningGRPCAddr,
		ReconnectDelay:    gw.config.GRPCReconnectDelay,
		ReconnectMaxDelay: gw.config.GRPCReconnectMaxDelay,
		MetricPrefix:      MetricPrefix,
		Logger:            gw.logger.With().Str("component", "key_registry").Logger(),
	})
	if err != nil {
		return fmt.Errorf("create stream key registry: %w", err)
	}
	gw.streamKeyRegistry = keyRegistry

	// Create gRPC stream-backed API key registry
	apiKeyRegistry, err := provapi.NewStreamAPIKeyRegistry(provapi.StreamAPIKeyRegistryConfig{
		GRPCAddr:          gw.config.ProvisioningGRPCAddr,
		ReconnectDelay:    gw.config.GRPCReconnectDelay,
		ReconnectMaxDelay: gw.config.GRPCReconnectMaxDelay,
		MetricPrefix:      MetricPrefix,
		Logger:            gw.logger.With().Str("component", "api_key_registry").Logger(),
	})
	if err != nil {
		_ = keyRegistry.Close() // best-effort cleanup during construction failure
		return fmt.Errorf("create stream api key registry: %w", err)
	}
	gw.streamAPIKeyRegistry = apiKeyRegistry
	gw.apiKeyRegistry = apiKeyRegistry

	// Create gRPC stream-backed channel rules provider
	channelRulesProvider, err := provapi.NewStreamChannelRulesProvider(provapi.StreamChannelRulesProviderConfig{
		GRPCAddr:          gw.config.ProvisioningGRPCAddr,
		ReconnectDelay:    gw.config.GRPCReconnectDelay,
		ReconnectMaxDelay: gw.config.GRPCReconnectMaxDelay,
		MetricPrefix:      MetricPrefix,
		Logger:            gw.logger.With().Str("component", "channel_rules_provider").Logger(),
	})
	if err != nil {
		_ = apiKeyRegistry.Close() // best-effort cleanup during construction failure
		_ = keyRegistry.Close()    // best-effort cleanup during construction failure
		return fmt.Errorf("create stream channel rules provider: %w", err)
	}
	gw.streamChannelRules = channelRulesProvider
	gw.channelRulesProvider = channelRulesProvider
	// The channel-rules provider also backs the reverse (UUID->slug) resolver used
	// by API-key auth — API keys carry only the tenant UUID.
	gw.tenantSlugResolver = channelRulesProvider

	// Build validator config. The channel-rules provider doubles as the tenant
	// resolver: it caches slug->UUID (rename-aware) from the tenant-config stream,
	// letting the binding map the JWT tenant_id claim to the signing key's owning
	// tenant UUID.
	validatorCfg := auth.MultiTenantValidatorConfig{
		KeyRegistry:     keyRegistry,
		RequireTenantID: gw.config.RequireTenantID,
		TenantResolver:  channelRulesProvider,
	}

	// Create multi-tenant validator
	validator, err := auth.NewMultiTenantValidator(validatorCfg)
	if err != nil {
		_ = channelRulesProvider.Close() // best-effort cleanup during construction failure
		_ = apiKeyRegistry.Close()       // best-effort cleanup during construction failure
		_ = keyRegistry.Close()          // best-effort cleanup during construction failure
		return fmt.Errorf("create validator: %w", err)
	}
	gw.validator = validator

	gw.logger.Info().
		Str("provisioning_grpc_addr", gw.config.ProvisioningGRPCAddr).
		Bool("require_tenant_id", gw.config.RequireTenantID).
		Msg("Configured multi-tenant authentication via gRPC streaming")

	return nil
}

// SetServerClient sets the gRPC client to ws-server for SSE and REST publish.
// Called from main.go after the Gateway is created.
func (gw *Gateway) SetServerClient(client *ServerClient) {
	gw.serverClient = client
}

// SetPublishRateLimiter sets the rate limiter for REST publish requests.
// Called from main.go after the Gateway is created.
func (gw *Gateway) SetPublishRateLimiter(limiter *PublishRateLimiter) {
	gw.publishRateLimiter = limiter
}

// SetPushClient sets the gRPC client to the push service for Web Push endpoints.
// Called from main.go after the Gateway is created.
func (gw *Gateway) SetPushClient(client PushForwarder) {
	gw.pushClient = client
}

// SetRevocationRegistry sets the token revocation stream registry.
// Called from main.go after the Gateway is created. The OnRevocation callback
// is set by the caller to invoke gw.HandleRevocation.
func (gw *Gateway) SetRevocationRegistry(reg RevocationChecker) {
	gw.revocationRegistry = reg
}

// SetLicenseWatcher sets the WatchLicense stream watcher for health reporting and shutdown.
// Called from main.go after the Gateway is created.
func (gw *Gateway) SetLicenseWatcher(w licenseWatcher) {
	gw.streamLicenseWatcher = w
}

// Close releases resources held by the gateway.
// Should be called during shutdown.
func (gw *Gateway) Close() error {
	var errs []error

	// Close gRPC stream registries (stops background streams + closes gRPC connections)
	if gw.streamChannelRules != nil {
		if err := gw.streamChannelRules.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close stream channel rules provider: %w", err))
		}
	}

	if gw.streamAPIKeyRegistry != nil {
		if err := gw.streamAPIKeyRegistry.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close stream api key registry: %w", err))
		}
	}

	if gw.streamKeyRegistry != nil {
		if err := gw.streamKeyRegistry.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close stream key registry: %w", err))
		}
	}

	if gw.revocationRegistry != nil {
		if err := gw.revocationRegistry.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close revocation registry: %w", err))
		}
	}

	if gw.streamLicenseWatcher != nil {
		if err := gw.streamLicenseWatcher.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close license watcher: %w", err))
		}
	}

	// Close push client if it implements io.Closer (PushClient does, test mocks may not).
	if closer, ok := gw.pushClient.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close push client: %w", err))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// HandleWebSocket handles incoming WebSocket upgrade requests.
// Authenticates the client, upgrades to WebSocket, and proxies to backend.
func (gw *Gateway) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	remoteAddr := r.RemoteAddr
	ctx := r.Context()

	// Cold-path tracing span for WebSocket upgrade + auth (not the proxy loop).
	// Noop when tracing disabled (Constitution VI).
	ctx, span := otel.Tracer(tracerName).Start(ctx, spanWebSocketUpgrade)
	defer span.End()

	// Record connection attempt and track disconnect reason for metrics
	RecordConnection()
	var proxy *Proxy // assigned once the connection is upgraded (below); stays nil on early returns
	closeReason := CloseReasonNormal
	defer func() {
		// recordWSDisconnect maps a revocation force-close reason (if the conn was force-closed) to
		// its snake_case close_reason label, else uses closeReason — so revocation closes are no
		// longer mislabeled "normal" in the disconnect histogram.
		recordWSDisconnect(proxy, closeReason, startTime)
	}()

	// Authenticate request — shared across WebSocket, SSE, and REST publish handlers.
	// Returns validated identity or error. Does NOT write to ResponseWriter.
	authRes, authErr := gw.authenticateRequest(ctx, r)
	if authErr != nil {
		switch {
		case errors.Is(authErr, ErrNoCredentials):
			closeReason = CloseReasonNoCredentials
			httputil.WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED", "token or api_key required")
		case errors.Is(authErr, ErrInvalidAPIKey):
			closeReason = CloseReasonInvalidAPIKey
			httputil.WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid api key")
		case errors.Is(authErr, ErrInvalidToken):
			closeReason = CloseReasonInvalidToken
			httputil.WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid token")
		case errors.Is(authErr, ErrTenantMismatch):
			closeReason = CloseReasonAPIKeyTenantMismatch
			httputil.WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED", "api key and token tenant mismatch")
		case errors.Is(authErr, ErrTenantUnavailable):
			closeReason = CloseReasonTenantUnavailable
			httputil.WriteError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "tenant resolution temporarily unavailable")
		default:
			closeReason = CloseReasonInvalidToken
			httputil.WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED", authErr.Error())
		}
		return
	}

	claims := authRes.Claims
	principal := authRes.Principal
	tenantID := authRes.TenantSlug
	apiKeyOnly := authRes.APIKeyOnly
	apiKeyTenantID := authRes.APIKeyTenantID

	// Check per-tenant connection limits
	if gw.connTracker != nil && tenantID != "" {
		if !gw.connTracker.TryAcquire(tenantID) {
			closeReason = CloseReasonTenantLimitExceeded
			gw.logger.Warn().
				Str(logging.LogKeyTenantSlug, tenantID).
				Str("remote_addr", remoteAddr).
				Int64("current_connections", gw.connTracker.GetConnectionCount(tenantID)).
				Int("limit", gw.connTracker.GetLimit(tenantID)).
				Msg("Connection rejected: tenant connection limit exceeded")
			// Capacity limit, not a token bucket — a slot frees whenever any tenant
			// connection closes, so advertise the minimum §IX probe interval.
			httputil.WriteRateLimited(w, time.Second, "TENANT_LIMIT_EXCEEDED", "tenant connection limit exceeded")
			return
		}
		// Ensure we release the connection slot on exit
		defer gw.connTracker.Release(tenantID)
	}

	// Record analytics connection. tenantID is known here (post-auth).
	// Transport is "websocket" for this path; SSE path records separately.
	if gw.analyticsCollector != nil && tenantID != "" {
		gw.analyticsCollector.IncrementConnections(tenantID, "websocket", 1)
		defer gw.analyticsCollector.IncrementConnections(tenantID, "websocket", -1)
	}

	// Upgrade client connection to WebSocket using gobwas/ws
	clientConn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		closeReason = CloseReasonUpgradeFailed
		// LOG-015: WS upgrade failure with structured context
		gw.logger.Warn().
			Err(err).
			Str("remote_addr", remoteAddr).
			Str("failure_reason", "upgrade_failed").
			Msg("WebSocket upgrade failed")
		return
	}
	defer func() { _ = clientConn.Close() }() // best-effort: connection is shutting down

	// Connect to backend ws-server using gobwas/ws, injecting identity headers.
	dialCtx, cancel := context.WithTimeout(ctx, gw.config.DialTimeout)
	defer cancel()

	// SECURITY: Strip inbound X-Sukko-* headers from the client's request before forwarding.
	// A WebSocket client must not be able to inject forged identity headers that reach ws-server.
	// The gateway's own headers (set below) are the sole source of truth for identity.
	r.Header.Del(protocol.HeaderTenantID)
	r.Header.Del(protocol.HeaderAPIKeyID)
	r.Header.Del(protocol.HeaderUserID)
	r.Header.Del(protocol.HeaderInternalSecret)

	// Build identity headers for ws-server. All auth paths populate X-Sukko-Tenant-ID.
	// X-Sukko-API-Key-ID is forwarded only when an API key was used.
	// X-Sukko-User-ID is forwarded only for JWT/jwt+api_key paths (non-empty UserID).
	// X-Sukko-Internal-Secret is always forwarded verbatim (empty when not configured).
	dialHeaders := http.Header{}
	dialHeaders.Set(protocol.HeaderTenantID, tenantID)
	dialHeaders.Set(protocol.HeaderInternalSecret, gw.config.InternalSecret)
	if authRes.APIKeyID != "" {
		dialHeaders.Set(protocol.HeaderAPIKeyID, authRes.APIKeyID)
	}
	if authRes.UserID != "" {
		dialHeaders.Set(protocol.HeaderUserID, authRes.UserID)
	}
	RecordIdentityHeadersForwarded()

	dialer := ws.Dialer{Header: ws.HandshakeHeaderHTTP(dialHeaders)}
	dialStart := time.Now()
	backendConn, _, _, err := dialer.Dial(dialCtx, gw.config.BackendURL)
	if err != nil {
		RecordBackendConnect(pkgmetrics.ResultFailed, time.Since(dialStart))
		closeReason = CloseReasonBackendUnavailable
		gw.logger.Error().
			Err(err).
			Str("backend_url", gw.config.BackendURL).
			Str("remote_addr", remoteAddr).
			Msg("Failed to connect to backend")

		// Send close frame to client
		closeFrame := ws.NewCloseFrameBody(ws.StatusInternalServerError, "Backend unavailable")
		_ = ws.WriteFrame(clientConn, ws.NewCloseFrame(closeFrame))
		return
	}
	RecordBackendConnect(pkgmetrics.ResultSuccess, time.Since(dialStart))
	defer func() { _ = backendConn.Close() }() // best-effort: connection is shutting down

	gw.logger.Info().
		Str("principal", principal).
		Str(logging.LogKeyTenantSlug, tenantID).
		Str("auth_method", authRes.AuthMethod).
		Str("remote_addr", remoteAddr).
		Dur("connect_time", time.Since(startTime)).
		Msg("Client connected and proxying to backend")

	// Create and run proxy. Authorization is injected as closures so the WS
	// proxy shares the exact same filter/check implementations as SSE, Web
	// Push, and REST publish (§XVIII).
	proxy = NewProxy(ProxyConfig{
		ClientConn:  clientConn,
		BackendConn: backendConn,
		Claims:      claims, // nil when API-key-only connection
		TenantID:    tenantID,
		FilterSubscribe: func(ctx context.Context, channels []string, cl *auth.Claims) []string {
			return gw.filterSubscribeChannels(ctx, channels, tenantID, cl)
		},
		CanPublish: func(ctx context.Context, cl *auth.Claims, channel string) bool {
			return gw.checkPublishAllowed(ctx, tenantID, cl, channel)
		},
		Validator:               gw.validator,
		AuthRefreshRateInterval: gw.config.AuthRefreshRateInterval,
		AuthValidationTimeout:   gw.config.AuthValidationTimeout,
		Logger:                  gw.logger.With().Str("principal", principal).Logger(),
		MessageTimeout:          gw.config.MessageTimeout,
		PublishRateLimit:        gw.config.PublishRateLimit,
		PublishBurst:            gw.config.PublishBurst,
		MaxPublishSize:          gw.config.MaxPublishSize,
		MaxFrameSize:            gw.config.MaxFrameSize,
		APIKeyOnly:              apiKeyOnly,
		APIKeyTenantID:          apiKeyTenantID,
	})
	// Register in connection registry for force-disconnect on token revocation
	if gw.connectionRegistry != nil && !apiKeyOnly && claims != nil {
		gw.connectionRegistry.Register(proxy, tenantID, claims.Subject, claims.ID)
		defer gw.connectionRegistry.Unregister(proxy, tenantID, claims.Subject, claims.ID)
		// Close the fan-out registration race: re-check revocation now that we are registered,
		// strictly after Register (§IX). A revoke that fanned out between the upgrade and
		// Register above would otherwise miss this connection permanently.
		gw.recheckRevocationAfterRegister(proxy, tenantID)
	}

	proxy.Run(ctx)

	gw.logger.Info().
		Str("principal", principal).
		Dur("session_duration", time.Since(startTime)).
		Msg("Client disconnected")
}

// streamStatus returns the overall status and per-stream states.
// channelRulesStreamLabel maps the rules stream's connection state and
// snapshot-received flag to a health label. Readiness gate: a stream
// that is TCP-connected but has not yet applied its initial snapshot cannot
// answer authorization queries — the gateway MUST report degraded (HandleReady
// then returns 503) until the snapshot is applied.
func channelRulesStreamLabel(state int32, snapshotReceived bool) (label string, degraded bool) {
	switch {
	case state == provapi.StreamStateDisconnected:
		return provapi.StreamLabelDisconnected, true
	case !snapshotReceived:
		return provapi.StreamLabelConnectedAwaitingSnapshot, true
	default:
		return provapi.StreamLabelConnected, false
	}
}

func (gw *Gateway) streamStatus() (status, keysStream, configStream, apiKeysStream string) {
	status = "ok"
	keysStream = provapi.StreamLabelConnected
	configStream = provapi.StreamLabelConnected
	apiKeysStream = provapi.StreamLabelConnected

	if gw.streamKeyRegistry != nil {
		if gw.streamKeyRegistry.State() == provapi.StreamStateDisconnected {
			status = "degraded"
			keysStream = provapi.StreamLabelDisconnected
		}
	} else {
		keysStream = provapi.StreamLabelDisabled
	}
	if gw.streamChannelRules != nil {
		label, degraded := channelRulesStreamLabel(
			gw.streamChannelRules.State(), gw.streamChannelRules.SnapshotReceived())
		if !degraded && !gw.streamChannelRules.TenantUUIDsPresent() {
			// DS-001: snapshot applied but no tenant UUIDs (older provisioning
			// peer) — the JWT tenant binding would reject all traffic, so stay
			// degraded until UUIDs arrive.
			label, degraded = provapi.StreamLabelConnectedAwaitingTenantUUIDs, true
		}
		if degraded {
			status = "degraded"
			configStream = label
		}
	} else {
		configStream = provapi.StreamLabelDisabled
	}
	if gw.streamAPIKeyRegistry != nil {
		if gw.streamAPIKeyRegistry.State() == provapi.StreamStateDisconnected {
			status = "degraded"
			apiKeysStream = provapi.StreamLabelDisconnected
		}
	} else {
		apiKeysStream = provapi.StreamLabelDisabled
	}
	return
}

// licenseStreamState returns the WatchLicense stream state for health reporting.
// Unlike streamStatus(), this never affects the top-level "status" field.
func (gw *Gateway) licenseStreamState() string {
	if gw.streamLicenseWatcher == nil {
		return provapi.StreamLabelDisabled
	}
	if gw.streamLicenseWatcher.State() == provapi.StreamStateDisconnected {
		return provapi.StreamLabelDisconnected
	}
	return provapi.StreamLabelConnected
}

// HandleHealth handles liveness checks. Always returns 200 — the process is alive.
// Use /ready for readiness checks that reflect stream connectivity.
func (gw *Gateway) HandleHealth(w http.ResponseWriter, _ *http.Request) {
	status, keysStream, configStream, apiKeysStream := gw.streamStatus()

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"status":                       status,
		"service":                      ServiceName,
		"provisioning_keys_stream":     keysStream,
		"provisioning_config_stream":   configStream,
		"provisioning_api_keys_stream": apiKeysStream,
		"provisioning_license_stream":  gw.licenseStreamState(),
	})
}

// HandleReady handles readiness checks. Returns 503 when streams are degraded,
// signaling Kubernetes to stop routing traffic until connectivity is restored.
func (gw *Gateway) HandleReady(w http.ResponseWriter, _ *http.Request) {
	status, keysStream, configStream, apiKeysStream := gw.streamStatus()

	httpStatus := http.StatusOK
	if status == "degraded" {
		httpStatus = http.StatusServiceUnavailable
	}

	_ = httputil.WriteJSON(w, httpStatus, map[string]string{
		"status":                       status,
		"service":                      ServiceName,
		"provisioning_keys_stream":     keysStream,
		"provisioning_config_stream":   configStream,
		"provisioning_api_keys_stream": apiKeysStream,
		"provisioning_license_stream":  gw.licenseStreamState(),
	})
}

// NewServer creates an HTTP server for the gateway.
func (gw *Gateway) NewServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", gw.HandleWebSocket)
	mux.HandleFunc("/health", gw.HandleHealth)
	mux.HandleFunc("/ready", gw.HandleReady)
	mux.HandleFunc("/version", version.Handler(MetricPrefix))
	mux.HandleFunc("/edition", license.EditionHandler(gw.config.EditionManager(), gw.editionUsage))
	mux.HandleFunc("/config", platform.ConfigHandler(gw.config))
	mux.HandleFunc("/metrics", HandleMetrics)
	profiling.InitPprof(mux.HandleFunc, gw.config.PprofEnabled, gw.logger)

	// SSE handler (edition-gated to Pro). REST publish is Community per
	// ADR-0009 (license.RESTPublish, ungated like channel rules) — it is the
	// no-Kafka ingestion on-ramp, so it registers without an edition gate.
	gate := RequireFeature(gw.config.EditionManager(), license.SSETransport)
	mux.HandleFunc("GET /sse", gate(gw.HandleSSE))
	mux.HandleFunc("POST /api/v1/publish", gw.HandlePublish)

	// Push notification handlers — registered only when GATEWAY_PUSH_ENABLED=true
	// (explicit deployment mode: the push service is deploy-optional, and a
	// gateway not fronting one exposes no push surface at all — 404, not a
	// misleading backend error). When enabled, routes stay edition-gated to
	// Pro (Web Push; ADR-0009).
	if gw.config.PushEnabled {
		pushGate := RequireFeature(gw.config.EditionManager(), license.WebPush)
		mux.HandleFunc("POST /api/v1/push/subscribe", pushGate(gw.HandlePushSubscribe))
		mux.HandleFunc("DELETE /api/v1/push/subscribe", pushGate(gw.HandlePushUnsubscribe))
		mux.HandleFunc("GET /api/v1/push/vapid-key", pushGate(gw.HandlePushVAPIDKey))
	}

	// Wrap with CORS middleware (gateway-wide, all HTTP endpoints)
	handler := CORSMiddleware(gw.config.CORSAllowedOrigins)(mux)

	return &http.Server{
		Addr:         fmt.Sprintf(":%d", gw.config.Port),
		Handler:      handler,
		ReadTimeout:  gw.config.ReadTimeout,
		WriteTimeout: gw.config.WriteTimeout,
		IdleTimeout:  gw.config.IdleTimeout,
	}
}

// editionUsage returns connection and shard counts for the /edition endpoint.
// Connections come from the gateway's own TenantConnectionTracker.
// Shards are fetched from ws-server's /edition (best-effort via GATEWAY_BACKEND_URL).
func (gw *Gateway) editionUsage(ctx context.Context) *license.EditionUsage {
	// Sum connections from all tenants (connTracker may be nil if TENANT_CONNECTION_LIMIT_ENABLED=false)
	var totalConns int
	if gw.connTracker != nil {
		for _, count := range gw.connTracker.GetAllCounts() {
			totalConns += int(count)
		}
	}

	usage := &license.EditionUsage{
		Connections: &totalConns,
	}

	// Best-effort: fetch shard count from ws-server
	shards := gw.fetchWsServerShards(ctx)
	if shards != nil {
		usage.Shards = shards
	}

	return usage
}

// fetchWsServerShards calls ws-server's /edition to get shard count.
// Derives the HTTP URL from GATEWAY_BACKEND_URL (ws://host:port/ws → http://host:port/edition).
// Returns nil on any failure (graceful degradation — Constitution IV).
func (gw *Gateway) fetchWsServerShards(ctx context.Context) *int {
	u, err := url.Parse(gw.config.BackendURL)
	if err != nil {
		gw.logger.Warn().Err(err).Str("backend_url", gw.config.BackendURL).Msg("Failed to parse backend URL for ws-server /edition")
		return nil
	}

	scheme := "http"
	if u.Scheme == "wss" {
		scheme = "https"
	}
	editionURL := scheme + "://" + u.Host + "/edition"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, editionURL, http.NoBody)
	if err != nil {
		gw.logger.Warn().Err(err).Msg("Failed to create ws-server /edition request")
		return nil
	}

	resp, err := gw.wsServerHTTPClient.Do(req)
	if err != nil {
		gw.logger.Debug().Err(err).Str("url", editionURL).Msg("ws-server /edition unreachable — shards not available")
		return nil
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		gw.logger.Debug().Int("status", resp.StatusCode).Str("url", editionURL).Msg("ws-server /edition returned non-OK — shards not available")
		return nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB limit
	if err != nil {
		gw.logger.Debug().Err(err).Msg("ws-server /edition: body read failed — shards not available")
		return nil
	}

	var result struct {
		Usage *struct {
			Shards *int `json:"shards"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		gw.logger.Debug().Err(err).Msg("ws-server /edition: JSON parse failed — shards not available")
		return nil
	}

	if result.Usage != nil {
		return result.Usage.Shards
	}
	return nil
}

// HandleRevocation processes a revocation event from the gRPC stream.
// Looks up matching connections via ConnectionRegistry and force-disconnects them.
// Force-disconnect is unconditional — auth refresh does not prevent disconnection.
// Called from main.go via StreamRevocationRegistry.OnRevocation callback.
func (gw *Gateway) HandleRevocation(entry provapi.RevocationEntry) {
	if gw.connectionRegistry == nil {
		return
	}

	switch entry.Type {
	case provapi.RevocationTypeToken:
		conn := gw.connectionRegistry.FindByJTI(entry.JTI)
		if conn == nil {
			return
		}
		transport := conn.Transport()
		conn.ForceClose(int(ws.StatusPolicyViolation), CloseReasonTokenRevoked)
		sub, jti, _ := conn.ConnectionClaims()
		gw.logger.Info().
			Str("jti", jti).
			Str("sub", sub).
			Str(logging.LogKeyTenantSlug, entry.TenantID).
			Str(labelTransport, transport).
			Str(provapi.LogFieldRevocationType, provapi.RevocationTypeToken).
			Str(logFieldDetection, DetectionFanOut).
			Msg("connection force-disconnected: token revoked")
		RecordTokenForceDisconnect(provapi.RevocationTypeToken, transport, DetectionFanOut)

	case provapi.RevocationTypeUser:
		conns := gw.connectionRegistry.FindBySub(entry.TenantID, entry.Sub)
		for _, conn := range conns {
			_, _, iat := conn.ConnectionClaims()
			if iat >= entry.RevokedAt {
				continue // token issued after revocation — re-enabled user, skip
			}
			transport := conn.Transport()
			conn.ForceClose(int(ws.StatusPolicyViolation), CloseReasonUserRevoked)
			sub, jti, _ := conn.ConnectionClaims()
			gw.logger.Info().
				Str("jti", jti).
				Str("sub", sub).
				Str(logging.LogKeyTenantSlug, entry.TenantID).
				Str(labelTransport, transport).
				Str(provapi.LogFieldRevocationType, provapi.RevocationTypeUser).
				Str(logFieldDetection, DetectionFanOut).
				Msg("connection force-disconnected: user revoked")
			RecordTokenForceDisconnect(provapi.RevocationTypeUser, transport, DetectionFanOut)
		}
	}
}

// recheckRevocationAfterRegister closes the revocation fan-out registration race (§IX): the
// one-shot HandleRevocation fan-out only force-closes connections registered at the instant it
// runs. A connection whose handler completed its client-facing upgrade but had not yet reached
// ConnectionRegistry.Register when a matching revoke fanned out is missed and never re-swept.
//
// MUST be called strictly AFTER Register returns, in the same goroutine, loading the snapshot
// fresh (via IsRevoked): the revocation stream stores its snapshot before firing OnRevocation
// (see revocation_stream.go), and register/find are serialized by the registry mutex, so for any
// revocation either the fan-out finds this connection or this re-check observes the stored
// revocation — the window cannot miss both. Reversing the order (or caching a pre-Register
// verdict) reopens the miss. No-op when no revocation registry is configured (§IV).
func (gw *Gateway) recheckRevocationAfterRegister(conn Connection, tenantID string) {
	if gw.revocationRegistry == nil {
		return
	}
	sub, jti, iat := conn.ConnectionClaims()
	if !gw.revocationRegistry.IsRevoked(jti, sub, tenantID, iat) {
		return
	}
	// Attribution-only: re-probe with sub="" to isolate the tenant-scoped jti branch (jti wins
	// if both match, mirroring IsRevoked's order). The close verdict is already committed above;
	// a snapshot change between the two lock-free loads is benign (only shifts a token/user
	// label). Do NOT collapse into one load — that loses the jti-vs-sub discrimination.
	revType, reason := provapi.RevocationTypeUser, CloseReasonUserRevoked
	if gw.revocationRegistry.IsRevoked(jti, "", tenantID, iat) {
		revType, reason = provapi.RevocationTypeToken, CloseReasonTokenRevoked
	}
	transport := conn.Transport()
	conn.ForceClose(int(ws.StatusPolicyViolation), reason)
	gw.logger.Info().
		Str("jti", jti).
		Str("sub", sub).
		Str(logging.LogKeyTenantSlug, tenantID).
		Str(labelTransport, transport).
		Str(provapi.LogFieldRevocationType, revType).
		Str(logFieldDetection, DetectionPostRegister).
		Msg("connection force-disconnected: revoked during registration window")
	RecordTokenForceDisconnect(revType, transport, DetectionPostRegister)
}

// SetAnalyticsCollector injects the analytics collector. Called from main.go after New().
// Until called, IncrementConnections is a no-op (analyticsCollector is nil → no-op guard below).
func (gw *Gateway) SetAnalyticsCollector(c analytics.Collector) {
	if c != nil {
		gw.analyticsCollector = c
	}
}
