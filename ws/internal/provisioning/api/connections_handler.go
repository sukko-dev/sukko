package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"

	provisioning "github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/server/registry"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// Pagination defaults.
const (
	defaultConnectionsPageLimit  = 50
	maxConnectionsPageLimit      = 100
	maxAdminConnectionsPageLimit = 1000
	// adminListTenantPageSize is the page size used when enumerating all tenants for
	// the cross-tenant admin list endpoint. Package-level so tests can reference the boundary.
	adminListTenantPageSize = 500
)

// HTTP error code constant for connection not found (joins errCodeServiceUnavailable and errCodeInvalidRequest from handlers.go).
const errCodeConnectionNotFound = "CONNECTION_NOT_FOUND"

// Human-readable 503 message constants (§I: magic strings forbidden).
const (
	errMsgAdminChannelUnavailable = "admin channel unavailable — retry after reconnect window"
	errMsgRegistryUnavailable     = "connection registry unavailable — retry after recovery"
	errMsgRegistryDeadPod         = "target pod has no active subscribers — connection may have closed"
)

// BulkDisconnectResponse is the 202 response for DELETE /connections bulk operations.
// MUST NOT use omitempty — all five fields must appear in every response (§XII).
type BulkDisconnectResponse struct {
	Disconnected          int  `json:"disconnected"`
	Errors                int  `json:"errors"`
	ChannelsCappedWarning bool `json:"channels_capped_warning"`
	ConnectionsCapped     bool `json:"connections_capped"`
	ReQuerySkipped        bool `json:"re_query_skipped"`
}

// connectionListResponse wraps a paginated list of ConnectionDetail items.
type connectionListResponse struct {
	Items  []ConnectionDetail `json:"items"`
	Total  int                `json:"total"`
	Limit  int                `json:"limit"`
	Offset int                `json:"offset"`
}

// connectionsHandlerMetricsType holds Prometheus metrics for the connections handler.
type connectionsHandlerMetricsType struct {
	DisconnectTotal       *prometheus.CounterVec
	DisconnectErrors      *prometheus.CounterVec
	BulkDisconnectReissue prometheus.Counter
	DeadPodDisconnect     prometheus.Counter
}

func newConnectionsHandlerMetrics(reg prometheus.Registerer) *connectionsHandlerMetricsType {
	m := &connectionsHandlerMetricsType{
		DisconnectTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "provisioning_connections_disconnect_total",
			Help: "Total force-disconnect operations issued by the connections API.",
		}, []string{"type"}), // type: single | bulk
		DisconnectErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "provisioning_connections_disconnect_errors_total",
			Help: "Total errors during force-disconnect operations (publish failures, unhealthy pods).",
		}, []string{"type"}),
		BulkDisconnectReissue: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "provisioning_registry_bulk_disconnect_reissued_total",
			Help: "Number of bulk disconnect operations where a re-query goroutine was launched.",
		}),
		DeadPodDisconnect: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "provisioning_registry_disconnect_dead_pod_total",
			Help: "Number of disconnect attempts where PUBLISH returned 0 subscribers (pod appears dead).",
		}),
	}
	reg.MustRegister(m.DisconnectTotal, m.DisconnectErrors, m.BulkDisconnectReissue, m.DeadPodDisconnect)
	return m
}

// ConnectionsHandlerParams groups construction parameters for NewConnectionsHandler.
type ConnectionsHandlerParams struct {
	// Client is the dedicated Valkey client for the connections registry reader.
	Client valkey.Client
	// Cfg is the provisioning service configuration.
	Cfg platform.ProvisioningConfig
	// EditionManager for gate checks.
	EditionManager *license.Manager
	// Logger for structured logging.
	Logger zerolog.Logger
	// WG is the provisioning server's WaitGroup — re-query goroutines are tracked here.
	WG *sync.WaitGroup
	// ServiceCtx is the provisioning process context. Re-query goroutines select on this
	// so they can exit early during SIGTERM instead of blocking wg.Wait() for the full delay.
	ServiceCtx context.Context
	// Service is the provisioning service for tenant enumeration (admin listing).
	Service *provisioning.Service
	// ReQueryDelay is how long to wait before the bulk re-query goroutine fires.
	// Defaults to 10s (2 × default WS_CONNECTIONS_REGISTRY_FLUSH_INTERVAL).
	ReQueryDelay time.Duration
	// Registerer is the Prometheus registerer for metrics. Defaults to prometheus.DefaultRegisterer.
	// Tests inject prometheus.NewRegistry() to avoid duplicate-metric panics.
	Registerer prometheus.Registerer
}

// defaultReQueryDelay is the default bulk re-query delay (2 × default flush interval).
const defaultReQueryDelay = 10 * time.Second

// connectionsRegistryReader is the subset of registryReader used by ConnectionsHandler.
// Defined as an interface so handler tests can inject a fake reader without Valkey.
type connectionsRegistryReader interface {
	listTenantConnections(ctx context.Context, tenantID string, filters connectionFilters) ([]ConnectionDetail, int, error)
	getConnection(ctx context.Context, connID, tenantID string) (*ConnectionDetail, error)
	publishDisconnect(ctx context.Context, podID, connID, tenantID, reason string) (int64, error)
	isAdminHealthy(ctx context.Context, podID string) bool
	fetchHealthKeys(ctx context.Context, pods map[string]bool) map[string]map[string]string
}

// connectionsService is the subset of provisioning.Service used by ConnectionsHandler.
// Defined as an interface so handler tests can inject a mock (mirrors webhookService).
type connectionsService interface {
	AuditLog(ctx context.Context, tenantID, action string, details provisioning.Metadata)
	ListTenants(ctx context.Context, opts provisioning.ListOptions) ([]*provisioning.Tenant, int, error)
}

// ConnectionsHandler handles the connections management API.
type ConnectionsHandler struct {
	reader         connectionsRegistryReader
	cfg            platform.ProvisioningConfig
	editionManager *license.Manager
	logger         zerolog.Logger
	wg             *sync.WaitGroup
	serviceCtx     context.Context
	service        connectionsService
	reQueryDelay   time.Duration
	metrics        *connectionsHandlerMetricsType
}

// NewConnectionsHandler creates a ConnectionsHandler. The registryReader is created internally
// so it remains unexported.
func NewConnectionsHandler(params ConnectionsHandlerParams) *ConnectionsHandler {
	env := params.Cfg.Environment
	reader := newRegistryReader(params.Client, env, params.Logger, params.WG)
	reQueryDelay := params.ReQueryDelay
	if reQueryDelay <= 0 {
		reQueryDelay = defaultReQueryDelay
	}
	svcCtx := params.ServiceCtx
	if svcCtx == nil {
		svcCtx = context.Background()
	}
	reg := params.Registerer
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	h := &ConnectionsHandler{
		reader:         reader,
		cfg:            params.Cfg,
		editionManager: params.EditionManager,
		logger:         params.Logger,
		wg:             params.WG,
		serviceCtx:     svcCtx,
		reQueryDelay:   reQueryDelay,
		metrics:        newConnectionsHandlerMetrics(reg),
	}
	// Assign service only when non-nil: a typed-nil *provisioning.Service stored in the
	// interface field would make `h.service != nil` true and panic on use. The handler
	// nil-guards h.service, so leaving it a true nil interface preserves that contract.
	if params.Service != nil {
		h.service = params.Service
	}
	return h
}

// HandleListConnections handles GET /tenants/{tenantSlug}/connections.
func (h *ConnectionsHandler) HandleListConnections(w http.ResponseWriter, r *http.Request) {
	// Connection registry is slug-keyed (data plane) — read the caller's authenticated slug.
	tenantSlug := getTenantSlugFromClaims(r)
	if tenantSlug == "" {
		httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "missing tenant context")
		return
	}

	filters, err := parseConnectionFilters(r, defaultConnectionsPageLimit, maxConnectionsPageLimit)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, err.Error())
		return
	}

	items, total, err := h.reader.listTenantConnections(r.Context(), tenantSlug, filters)
	if err != nil {
		h.logger.Warn().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("connections: listTenantConnections failed")
		w.Header().Set("Retry-After", strconv.Itoa(int(platform.RetryAfterRegistryUnavailable.Seconds())))
		httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable, errMsgRegistryUnavailable)
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, connectionListResponse{
		Items:  items,
		Total:  total,
		Limit:  filters.Limit,
		Offset: filters.Offset,
	})
}

// HandleGetConnection handles GET /tenants/{tenantSlug}/connections/{connId}.
func (h *ConnectionsHandler) HandleGetConnection(w http.ResponseWriter, r *http.Request) {
	// Connection registry is slug-keyed (data plane) — read the caller's authenticated slug.
	tenantSlug := getTenantSlugFromClaims(r)
	if tenantSlug == "" {
		httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "missing tenant context")
		return
	}
	connID := chi.URLParam(r, "connId")
	if connID == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "missing connId")
		return
	}

	detail, err := h.reader.getConnection(r.Context(), connID, tenantSlug)
	if err != nil {
		h.logger.Warn().Err(err).Str("conn_id", connID).Msg("connections: getConnection failed")
		w.Header().Set("Retry-After", strconv.Itoa(int(platform.RetryAfterRegistryUnavailable.Seconds())))
		httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable, errMsgRegistryUnavailable)
		return
	}
	if detail == nil {
		httputil.WriteError(w, http.StatusNotFound, errCodeConnectionNotFound, "connection not found")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, detail)
}

// HandleDeleteConnection handles DELETE /tenants/{tenantSlug}/connections/{connId}.
func (h *ConnectionsHandler) HandleDeleteConnection(w http.ResponseWriter, r *http.Request) {
	// Registry reads/publish are slug-keyed (data plane) — from the caller's authenticated
	// slug; the audit record is UUID-keyed (control plane) — from the UUID RequireTenant
	// stashed. Guard both: both are present together for a validated tenant caller, so an
	// empty UUID means no validated tenant context (fail closed, like the webhook handlers).
	tenantSlug := getTenantSlugFromClaims(r)
	tenantUUID := getTenantUUIDFromContext(r)
	if tenantSlug == "" || tenantUUID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "missing tenant context")
		return
	}
	connID := chi.URLParam(r, "connId")
	if connID == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "missing connId")
		return
	}

	detail, err := h.reader.getConnection(r.Context(), connID, tenantSlug)
	if err != nil {
		h.logger.Warn().Err(err).Str("conn_id", connID).Msg("connections: getConnection for delete failed")
		w.Header().Set("Retry-After", strconv.Itoa(int(platform.RetryAfterRegistryUnavailable.Seconds())))
		httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable, errMsgRegistryUnavailable)
		return
	}
	if detail == nil {
		// Not found or cross-tenant — 404 with Retry-After: 1 (may not yet be in registry).
		w.Header().Set("Retry-After", "1")
		httputil.WriteError(w, http.StatusNotFound, errCodeConnectionNotFound, "connection not found")
		return
	}

	// Pre-flight: check admin channel health for this pod.
	if !h.reader.isAdminHealthy(r.Context(), detail.PodID) {
		w.Header().Set("Retry-After", strconv.Itoa(int(platform.RetryAfterRegistryUnavailable.Seconds())))
		httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable, errMsgAdminChannelUnavailable)
		return
	}

	count, err := h.reader.publishDisconnect(r.Context(), detail.PodID, connID, tenantSlug, registry.AdminDisconnectReasonOperatorRequest)
	if err != nil {
		h.logger.Warn().Err(err).Str("conn_id", connID).Str("pod_id", detail.PodID).Msg("connections: publishDisconnect failed")
		h.metrics.DisconnectErrors.WithLabelValues("single").Inc()
		if h.service != nil {
			h.service.AuditLog(r.Context(), tenantUUID, provisioning.ActionForceDisconnect, provisioning.Metadata{
				"connection_id": connID,
				"pod_id":        detail.PodID,
				"outcome":       "publish_error",
			})
		}
		w.Header().Set("Retry-After", strconv.Itoa(int(platform.RetryAfterRegistryUnavailable.Seconds())))
		httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable, errMsgRegistryUnavailable)
		return
	}

	if count == 0 {
		// Pod appears dead — stale registry entry.
		h.metrics.DeadPodDisconnect.Inc()
		if h.service != nil {
			h.service.AuditLog(r.Context(), tenantUUID, provisioning.ActionForceDisconnect, provisioning.Metadata{
				"connection_id": connID,
				"pod_id":        detail.PodID,
				"outcome":       "dead_pod",
			})
		}
		w.Header().Set("Retry-After", "1")
		httputil.WriteError(w, http.StatusNotFound, errCodeConnectionNotFound, errMsgRegistryDeadPod)
		return
	}

	h.metrics.DisconnectTotal.WithLabelValues("single").Inc()

	// Audit log — successful force-disconnect.
	if h.service != nil {
		h.service.AuditLog(r.Context(), tenantUUID, provisioning.ActionForceDisconnect, provisioning.Metadata{
			"connection_id": connID,
			"pod_id":        detail.PodID,
			"outcome":       "success",
		})
	}

	w.WriteHeader(http.StatusAccepted)
}

// HandleBulkDisconnect handles DELETE /tenants/{tenantSlug}/connections (bulk by filter).
func (h *ConnectionsHandler) HandleBulkDisconnect(w http.ResponseWriter, r *http.Request) {
	// Registry reads/publish are slug-keyed (data plane) — from the caller's authenticated
	// slug; the audit record is UUID-keyed (control plane) — from the UUID RequireTenant
	// stashed. Guard both: both are present together for a validated tenant caller, so an
	// empty UUID means no validated tenant context (fail closed, like the webhook handlers).
	tenantSlug := getTenantSlugFromClaims(r)
	tenantUUID := getTenantUUIDFromContext(r)
	if tenantSlug == "" || tenantUUID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "missing tenant context")
		return
	}
	apiKeyID := r.URL.Query().Get("api_key_id")
	channel := r.URL.Query().Get("channel")
	if apiKeyID == "" && channel == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "at least one filter (api_key_id or channel) is required")
		return
	}

	filters := connectionFilters{
		APIKeyID: apiKeyID,
		Channel:  channel,
		Limit:    maxAdminConnectionsPageLimit,
	}
	items, _, err := h.reader.listTenantConnections(r.Context(), tenantSlug, filters)
	if err != nil {
		h.logger.Warn().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("connections: bulk listTenantConnections failed")
		w.Header().Set("Retry-After", strconv.Itoa(int(platform.RetryAfterRegistryUnavailable.Seconds())))
		httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable, errMsgRegistryUnavailable)
		return
	}

	// connectionsCapped signals that the result set was truncated at maxAdminConnectionsPageLimit.
	// When true, additional matching connections exist beyond those queued for disconnect.
	connectionsCapped := len(items) == maxAdminConnectionsPageLimit

	// Batch-fetch health data for all unique pod IDs before the worker loop — avoids N Valkey
	// round-trips (one per connection) collapsing to P (one per unique pod). §VI hot-path.
	podIDSet := make(map[string]bool, len(items))
	for _, item := range items {
		podIDSet[item.PodID] = true
	}
	healthMap := h.reader.fetchHealthKeys(r.Context(), podIDSet)

	sem := make(chan struct{}, h.cfg.BulkDisconnectConcurrency)
	var mu sync.Mutex
	var disconnected, errCount int
	var channelsCapped bool
	var workerWg sync.WaitGroup

	for _, item := range items {
		if item.ChannelsCapped {
			channelsCapped = true
		}

		workerWg.Go(func() { //nolint:contextcheck // goroutine uses r.Context() for publish; context.Background() inside is for re-query only
			defer logging.RecoverPanic(h.logger, "bulk_disconnect_worker", nil)
			sem <- struct{}{}        // acquire inside goroutine — HTTP handler is not stalled waiting
			defer func() { <-sem }() // release on return
			conn := item
			// Health check from pre-fetched map (no per-connection Valkey round-trip).
			hf := healthMap[conn.PodID]
			if hf[registry.HealthFieldAdminChannelHealthy] != registry.HealthValueTrue {
				h.metrics.DisconnectErrors.WithLabelValues("bulk").Inc()
				h.logger.Warn().Str("conn_id", conn.ConnectionID).Str("pod_id", conn.PodID).Msg("connections: bulk disconnect skipped — admin channel unhealthy")
				mu.Lock()
				errCount++
				mu.Unlock()
				return
			}
			reason := registry.AdminDisconnectReasonOperatorRequest
			if apiKeyID != "" {
				reason = registry.AdminDisconnectReasonKeyRevoked
			}
			count, pubErr := h.reader.publishDisconnect(r.Context(), conn.PodID, conn.ConnectionID, tenantSlug, reason)
			switch {
			case pubErr != nil:
				h.metrics.DisconnectErrors.WithLabelValues("bulk").Inc()
				mu.Lock()
				errCount++
				mu.Unlock()
			case count == 0:
				// Dead pod — stale registry entry; expected during rollouts, not a publish error.
				h.metrics.DeadPodDisconnect.Inc()
			default:
				h.metrics.DisconnectTotal.WithLabelValues("bulk").Inc()
				mu.Lock()
				disconnected++
				mu.Unlock()
			}
		})
	}
	workerWg.Wait()

	// Audit log.
	if h.service != nil {
		h.service.AuditLog(r.Context(), tenantUUID, provisioning.ActionBulkDisconnect, provisioning.Metadata{
			"api_key_id": apiKeyID,
			"channel":    channel,
		})
	}

	// Launch re-query goroutine after 2×FlushInterval to catch connections not yet visible.
	reQuerySkipped := h.wg == nil
	if h.wg != nil {
		h.metrics.BulkDisconnectReissue.Inc()
		// Capture locals for the goroutine — r.Context() is NOT used (it cancels on handler return).
		reQueryDelay := h.reQueryDelay
		tenantSlugCopy := tenantSlug
		apiKeyIDCopy := apiKeyID
		filtersCopy := filters
		h.wg.Go(func() { //nolint:contextcheck // goroutine intentionally uses context.Background() — r.Context() is canceled when the HTTP response returns
			defer logging.RecoverPanic(h.logger, "bulk_disconnect_requery", nil)
			t := time.NewTimer(reQueryDelay)
			defer t.Stop()
			select {
			case <-t.C:
			case <-h.serviceCtx.Done():
				return
			}
			// Use a bounded context for all Valkey I/O so wg.Wait() cannot hang indefinitely
			// if Valkey is slow at shutdown time. reQueryDelay is a reasonable upper bound.
			reCtx, reCancel := context.WithTimeout(context.Background(), reQueryDelay)
			defer reCancel()
			reItems, _, reErr := h.reader.listTenantConnections(reCtx, tenantSlugCopy, filtersCopy)
			if reErr != nil {
				h.logger.Warn().Err(reErr).Str(logging.LogKeyTenantSlug, tenantSlugCopy).Msg("connections: bulk re-query failed")
				return
			}
			// Batch-fetch health keys for unique pods before the loop (avoids N round-trips).
			rePodIDs := make(map[string]bool, len(reItems))
			for _, conn := range reItems {
				rePodIDs[conn.PodID] = true
			}
			reHealthMap := h.reader.fetchHealthKeys(reCtx, rePodIDs)
			reReason := registry.AdminDisconnectReasonOperatorRequest
			if apiKeyIDCopy != "" {
				reReason = registry.AdminDisconnectReasonKeyRevoked
			}
			for _, conn := range reItems {
				hf := reHealthMap[conn.PodID]
				if hf[registry.HealthFieldAdminChannelHealthy] != registry.HealthValueTrue {
					continue
				}
				// Best-effort: re-query is a fire-and-forget sweep. Errors and zero-subscriber
				// count (dead pod) are not surfaced — the caller already received a 202.
				_, _ = h.reader.publishDisconnect(reCtx, conn.PodID, conn.ConnectionID, tenantSlugCopy, reReason)
				h.metrics.DisconnectTotal.WithLabelValues("bulk_requery").Inc()
			}
		})
	}

	_ = httputil.WriteJSON(w, http.StatusAccepted, BulkDisconnectResponse{
		Disconnected:          disconnected,
		Errors:                errCount,
		ChannelsCappedWarning: channelsCapped,
		ConnectionsCapped:     connectionsCapped,
		ReQuerySkipped:        reQuerySkipped,
	})
}

// HandleAdminListConnections handles GET /admin/connections.
func (h *ConnectionsHandler) HandleAdminListConnections(w http.ResponseWriter, r *http.Request) {
	filters, err := parseConnectionFilters(r, defaultConnectionsPageLimit, maxAdminConnectionsPageLimit)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, err.Error())
		return
	}

	tenantID := r.URL.Query().Get("tenant_id")

	if tenantID != "" {
		// Single-tenant query — use the tenant index directly.
		items, total, err := h.reader.listTenantConnections(r.Context(), tenantID, filters)
		if err != nil {
			h.logger.Warn().Err(err).Str(logging.LogKeyTenantSlug, tenantID).Msg("connections: admin single-tenant list failed")
			w.Header().Set("Retry-After", strconv.Itoa(int(platform.RetryAfterRegistryUnavailable.Seconds())))
			httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable, errMsgRegistryUnavailable)
			return
		}
		_ = httputil.WriteJSON(w, http.StatusOK, connectionListResponse{
			Items:  items,
			Total:  total,
			Limit:  filters.Limit,
			Offset: filters.Offset,
		})
		return
	}

	// Cross-tenant query — enumerate all tenants from provisioning DB.
	// Scale note: this fetches all matching connections into memory before paginating.
	// For very large deployments (1000s of tenants × 1000s of connections), consider
	// a cursor-based approach. Acceptable for v1 admin tooling.
	if h.service == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable, "provisioning service not available")
		return
	}

	// Fetch all tenants in pages.
	var allItems []ConnectionDetail
	offset := 0
	for {
		tenants, _, err := h.service.ListTenants(r.Context(), provisioning.ListOptions{Limit: adminListTenantPageSize, Offset: offset})
		if err != nil {
			h.logger.Warn().Err(err).Msg("connections: admin list: tenant enumeration failed")
			break
		}
		if len(tenants) == 0 {
			break
		}
		for _, t := range tenants {
			tFilters := connectionFilters{
				Transport: filters.Transport,
				Channel:   filters.Channel,
				// PodID intentionally omitted: listTenantConnections does not filter by pod ID.
				// The outer loop (below) applies the PodID filter post-fetch.
				Limit: maxAdminConnectionsPageLimit,
			}
			// Registry is slug-keyed (data plane) — query by slug, not the tenant UUID.
			items, _, err := h.reader.listTenantConnections(r.Context(), t.Slug, tFilters)
			if err != nil {
				h.logger.Warn().Err(err).Str(logging.LogKeyTenantSlug, t.Slug).Msg("connections: admin list: tenant read failed, skipping")
				continue
			}
			// Apply pod_id post-fetch filter.
			for _, item := range items {
				if filters.PodID != "" && item.PodID != filters.PodID {
					continue
				}
				allItems = append(allItems, item)
			}
		}
		offset += len(tenants)
		if len(tenants) < adminListTenantPageSize {
			break
		}
	}

	total := len(allItems)
	end := min(filters.Offset+filters.Limit, total)
	var page []ConnectionDetail
	if filters.Offset < total {
		page = allItems[filters.Offset:end]
	} else {
		page = []ConnectionDetail{}
	}

	_ = httputil.WriteJSON(w, http.StatusOK, connectionListResponse{
		Items:  page,
		Total:  total,
		Limit:  filters.Limit,
		Offset: filters.Offset,
	})
}

// parseConnectionFilters extracts and validates pagination + filter params from a request.
func parseConnectionFilters(r *http.Request, defaultLimit, maxLimit int) (connectionFilters, error) {
	q := r.URL.Query()
	limit := defaultLimit
	if s := q.Get("limit"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 1 {
			return connectionFilters{}, fmt.Errorf("invalid limit: %s", s)
		}
		if v > maxLimit {
			v = maxLimit
		}
		limit = v
	}
	offset := 0
	if s := q.Get("offset"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 0 {
			return connectionFilters{}, fmt.Errorf("invalid offset: %s", s)
		}
		offset = v
	}
	transport := q.Get("transport")
	if transport != "" && transport != "ws" && transport != "sse" {
		return connectionFilters{}, fmt.Errorf("invalid transport %q: must be 'ws' or 'sse'", transport)
	}
	return connectionFilters{
		APIKeyID:  q.Get("api_key_id"),
		Channel:   q.Get("channel"),
		Transport: transport,
		PodID:     q.Get("pod_id"),
		Limit:     limit,
		Offset:    offset,
	}, nil
}
