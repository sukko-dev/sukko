package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// analyticsSSEDroppedCounter counts events dropped due to slow SSE clients (§VI prefix).
var analyticsSSEDroppedCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "provisioning_analytics_sse_dropped_total",
	Help: "Total SSE analytics events dropped due to slow client (provisioning service).",
})

// sseIdleTimeout is the max idle time before the server closes an SSE connection.
// Must be > FlushInterval to have any effect — with default FlushInterval=60s, idle
// timeout fires only if a tick succeeds but the next tick somehow never fires (stuck ticker).
// Its primary purpose is guarding against connections where the client vanished silently.
const sseIdleTimeout = 90 * time.Second

// pushPerProviderSQL queries the most recent complete per-provider push metrics.
// Uses GROUP BY provider to produce one row per provider ("web", "android", "ios").
const pushPerProviderSQL = `
			SELECT
				provider,
				COALESCE(SUM(sent_count), 0),
				COALESCE(SUM(success_count), 0),
				COALESCE(SUM(failed_count), 0),
				COALESCE(SUM(expired_count), 0),
				COALESCE(SUM(rate_limited_count), 0)
			FROM analytics_push
			WHERE bucket_size = 'minute'
			  AND ($2::uuid IS NULL OR tenant_id = $2)
			  AND bucket_start = (
				SELECT MAX(bucket_start) FROM analytics_push
				WHERE bucket_start < $1
				  AND ($2::uuid IS NULL OR tenant_id = $2)
				  AND bucket_size = 'minute'
			  )
			GROUP BY provider`

// AnalyticsSSEHandler serves GET /api/v1/admin/analytics/stream?tenant_id={id}.
// It is a polling reader of the analytics DB — not a flush-event subscriber —
// so it always reflects the multi-pod aggregate, independent of per-pod flush timing.
type AnalyticsSSEHandler struct {
	pool           *pgxpool.Pool
	maxConns       int
	sem            chan struct{} // semaphore: cap = ANALYTICS_SSE_MAX_CONNS
	interval       time.Duration
	editionManager *license.Manager // nil disables push-analytics edition gate check
	logger         zerolog.Logger
}

// NewAnalyticsSSEHandler creates an AnalyticsSSEHandler.
// editionManager is used to gate push analytics to Enterprise edition; pass nil to disable gating.
func NewAnalyticsSSEHandler(pool *pgxpool.Pool, maxConns int, interval time.Duration, mgr *license.Manager, logger zerolog.Logger) *AnalyticsSSEHandler {
	return &AnalyticsSSEHandler{
		pool:           pool,
		maxConns:       maxConns,
		sem:            make(chan struct{}, maxConns),
		interval:       interval,
		editionManager: mgr,
		logger:         logger.With().Str("component", "analytics_sse").Logger(),
	}
}

// ServeHTTP handles the SSE stream. Admin-JWT is enforced by the router group.
func (h *AnalyticsSSEHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	select {
	case h.sem <- struct{}{}:
		defer func() { <-h.sem }()
	default:
		// Capacity semaphore — a slot frees when any stream ends; minimum probe interval.
		httputil.WriteRateLimited(w, time.Second, "TOO_MANY_SSE_CONNECTIONS",
			fmt.Sprintf("max concurrent analytics stream connections (%d) reached", h.maxConns))
		return
	}

	tenantID := r.URL.Query().Get("tenant_id")

	// tenant_id, when present, MUST be a well-formed UUID (admin tooling passes UUIDs; the
	// column is UUID-typed). Reject a malformed value up front with 400 — before the SSE
	// stream opens — instead of letting it reach the $2::uuid cast, which errors silently and
	// returns an empty 200. An empty tenant_id is a valid mode (aggregate across all tenants).
	if tenantID != "" {
		if _, err := uuid.Parse(tenantID); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "tenant_id must be a valid UUID")
			return
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	flusher, ok := w.(http.Flusher)
	if !ok {
		h.logger.Error().Msg("SSE: response writer does not support flushing")
		return
	}

	// Disable the HTTP server's WriteTimeout for this SSE connection.
	// Without this, the default WriteTimeout (typically 15s) kills the stream
	// before the first poll fires (default FlushInterval is 60s).
	if err := http.NewResponseController(w).SetWriteDeadline(time.Time{}); err != nil {
		h.logger.Warn().Err(err).Msg("SSE: could not disable write deadline; stream may be killed by WriteTimeout")
	}

	// Initial snapshot on connect.
	if err := h.sendSnapshot(r.Context(), w, flusher, tenantID); err != nil {
		h.logger.Warn().Err(err).Str(logging.LogKeyTenantUUID, tenantID).Msg("SSE initial snapshot failed")
		return
	}

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	idleTimer := time.NewTimer(sseIdleTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := h.sendSnapshot(r.Context(), w, flusher, tenantID); err != nil {
				analyticsSSEDroppedCounter.Inc()
				return // client gone or write error
			}
			idleTimer.Reset(sseIdleTimeout)
		case <-idleTimer.C:
			return // client idle
		}
	}
}

func (h *AnalyticsSSEHandler) sendSnapshot(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, tenantID string) error {
	snap := h.queryMetrics(ctx, tenantID)
	return sseWriteEvent(w, flusher, "snapshot", snap)
}

// analyticsSnapshot is the SSE payload for the analytics stream.
type analyticsSnapshot struct {
	TenantID    string                         `json:"tenant_id,omitempty"`
	Connections *connMetrics                   `json:"connections,omitempty"`
	Messages    *msgMetrics                    `json:"messages,omitempty"`
	Push        map[string]pushProviderMetrics `json:"push,omitempty"` // key = "web" | "android" | "ios"
	BucketStart *time.Time                     `json:"bucket_start,omitempty"`
}

type connMetrics struct {
	Active      int64 `json:"active"`
	WebSocket   int64 `json:"websocket"`
	SSE         int64 `json:"sse"`
	Connects    int64 `json:"connects"`
	Disconnects int64 `json:"disconnects"`
}

type msgMetrics struct {
	Published int64 `json:"published"`
	Delivered int64 `json:"delivered"`
	Failed    int64 `json:"failed"`
}

// pushProviderMetrics holds push delivery counters for a single provider (web/android/ios).
type pushProviderMetrics struct {
	Sent        int64 `json:"sent"`
	Success     int64 `json:"success"`
	Failed      int64 `json:"failed"`
	Expired     int64 `json:"expired"`
	RateLimited int64 `json:"rate_limited"`
}

// queryMetrics reads only the most recent complete 1-minute bucket from the analytics tables.
// tenantID="" aggregates across all pods for all tenants.
// Each table is queried independently: errors on individual tables are logged and cause
// that section to be omitted from the snapshot (nil field) rather than returning zeros,
// so clients can distinguish "no data" from "zero activity".
func (h *AnalyticsSSEHandler) queryMetrics(ctx context.Context, tenantID string) *analyticsSnapshot {
	snap := &analyticsSnapshot{TenantID: tenantID}
	if h.pool == nil {
		return snap // pool unavailable (e.g., analytics disabled); return empty snapshot
	}
	bucketCutoff := time.Now().UTC().Truncate(time.Minute)
	var tid *string
	if tenantID != "" {
		tid = &tenantID
	}

	// Connections — sum across all pods for the most recent complete 1-minute bucket.
	var cm connMetrics
	var bs *time.Time
	if err := h.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(active_count), 0),
			COALESCE(SUM(CASE WHEN transport='websocket' THEN active_count ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN transport='sse'       THEN active_count ELSE 0 END), 0),
			COALESCE(SUM(connect_count), 0),
			COALESCE(SUM(disconnect_count), 0),
			MAX(bucket_start)
		FROM analytics_connections
		WHERE bucket_size = 'minute'
		  AND ($2::uuid IS NULL OR tenant_id = $2)
		  AND bucket_start = (
			SELECT MAX(bucket_start) FROM analytics_connections
			WHERE bucket_start < $1
			  AND ($2::uuid IS NULL OR tenant_id = $2)
			  AND bucket_size = 'minute'
		  )`,
		bucketCutoff, tid,
	).Scan(&cm.Active, &cm.WebSocket, &cm.SSE, &cm.Connects, &cm.Disconnects, &bs); err == nil {
		snap.Connections = &cm
		snap.BucketStart = bs
	} else {
		h.logger.Warn().Err(err).Str(logging.LogKeyTenantUUID, tenantID).Msg("SSE: connections query failed")
	}

	// Messages — most recent complete 1-minute bucket only.
	var mm msgMetrics
	if err := h.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(published_count), 0),
			COALESCE(SUM(delivered_count), 0),
			COALESCE(SUM(failed_count), 0)
		FROM analytics_messages
		WHERE bucket_size = 'minute'
		  AND ($2::uuid IS NULL OR tenant_id = $2)
		  AND bucket_start = (
			SELECT MAX(bucket_start) FROM analytics_messages
			WHERE bucket_start < $1
			  AND ($2::uuid IS NULL OR tenant_id = $2)
			  AND bucket_size = 'minute'
		  )`,
		bucketCutoff, tid,
	).Scan(&mm.Published, &mm.Delivered, &mm.Failed); err == nil {
		snap.Messages = &mm
	} else {
		h.logger.Warn().Err(err).Str(logging.LogKeyTenantUUID, tenantID).Msg("SSE: messages query failed")
	}

	// Push — most recent complete 1-minute bucket only, broken down per provider.
	// Only included when push analytics (Enterprise edition) is available.
	if h.editionManager == nil || license.EditionHasFeature(h.editionManager.Edition(), license.AnalyticsPush) {
		rows, err := h.pool.Query(ctx, pushPerProviderSQL, bucketCutoff, tid)
		if err != nil {
			h.logger.Warn().Err(err).Str(logging.LogKeyTenantUUID, tenantID).Msg("SSE: push query failed")
		} else {
			// Zero-fill all known providers so the client always sees three keys.
			pushMap := map[string]pushProviderMetrics{
				"web":     {},
				"android": {},
				"ios":     {},
			}
			for rows.Next() {
				var provider string
				var pm pushProviderMetrics
				if err := rows.Scan(&provider, &pm.Sent, &pm.Success, &pm.Failed, &pm.Expired, &pm.RateLimited); err != nil {
					h.logger.Warn().Err(err).Msg("SSE: push row scan failed")
					continue
				}
				pushMap[provider] = pm
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				h.logger.Warn().Err(err).Str(logging.LogKeyTenantUUID, tenantID).Msg("SSE: push rows error")
			} else {
				snap.Push = pushMap
			}
		}
	}

	return snap
}

// sseWriteEvent serializes data as JSON and writes a named SSE event.
func sseWriteEvent(w http.ResponseWriter, flusher http.Flusher, eventType string, data any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal SSE event %s: %w", eventType, err)
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventType, b); err != nil {
		return fmt.Errorf("write SSE event %s: %w", eventType, err)
	}
	flusher.Flush()
	return nil
}
