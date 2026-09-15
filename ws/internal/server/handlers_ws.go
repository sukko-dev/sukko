package server

import (
	"crypto/subtle"
	"net/http"
	"strconv"
	"time"

	"github.com/gobwas/ws"
	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// WebSocket upgrade handler
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	clientIP := httputil.GetClientIP(r)

	s.logger.Debug().
		Str("remote_addr", r.RemoteAddr).
		Str("client_ip", clientIP).
		Str("user_agent", r.Header.Get("User-Agent")).
		Str("origin", r.Header.Get("Origin")).
		Str("upgrade", r.Header.Get("Upgrade")).
		Str("connection", r.Header.Get("Connection")).
		Str("sec_websocket_version", r.Header.Get("Sec-WebSocket-Version")).
		Msg("WebSocket upgrade request received")

	// Reject new connections during graceful shutdown
	// Client receives: HTTP 503 "Server is shutting down"
	// See: docs/API_REJECTION_RESPONSES.md (Scenario 1)
	if s.shuttingDown.Load() == 1 {
		s.logger.Debug().
			Str("client_ip", clientIP).
			Msg("Connection rejected: server shutting down")
		http.Error(w, "Server is shutting down", http.StatusServiceUnavailable)
		return
	}

	// Connection rate limiting (DoS protection)
	// IMPORTANT: Skip rate limiting for internal LoadBalancer traffic (127.0.0.1)
	// The LoadBalancer proxies external clients to shards via localhost,
	// and we don't want to rate limit our own internal connections.
	// Client receives: HTTP 429 "Rate limit exceeded" (non-localhost only)
	// See: docs/API_REJECTION_RESPONSES.md (Scenario 2)
	if s.connectionRateLimiter != nil && clientIP != "127.0.0.1" {
		if !s.connectionRateLimiter.CheckConnectionAllowed(clientIP) {
			s.logger.Warn().
				Str("client_ip", clientIP).
				Dur("elapsed_ms", time.Since(startTime)).
				Msg("Connection rejected: rate limit exceeded")
			// §IX: 429 MUST carry Retry-After. The body stays plain text — this is a
			// WebSocket upgrade, not a JSON API, and an upgrade client does not parse a
			// JSON error envelope — so only the header is added here.
			//
			// The limiter is a per-IP + global token bucket (limits.ConnectionRateLimiter),
			// so advertise the per-IP one-token refill time. A 429 can also come from the
			// global bucket, which refills faster, so a single scalar is necessarily the
			// conservative of the two.
			w.Header().Set("Retry-After", strconv.Itoa(httputil.RetryAfterSeconds(s.connectionRateLimiter.RetryAfter())))
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}
	}

	// ResourceGuard admission control - static limits with safety checks
	// Checks: goroutine limit, CPU usage, memory usage, connection count
	// Client receives: HTTP 503 "Server overloaded"
	// See: docs/API_REJECTION_RESPONSES.md (Scenario 3)
	shouldAccept, reason := s.resourceGuard.ShouldAcceptConnection()
	if !shouldAccept {
		currentConnections := s.stats.CurrentConnections.Load()
		s.logger.Warn().
			Str("client_ip", clientIP).
			Int64("current_connections", currentConnections).
			Int("max_connections", s.maxConns).
			Str("reason", reason).
			Dur("elapsed_ms", time.Since(startTime)).
			Msg("Connection rejected by ResourceGuard")
		metrics.ConnectionsFailed.Inc()
		http.Error(w, "Server overloaded", http.StatusServiceUnavailable)
		return
	}

	// Guard with level check to avoid unnecessary atomic load when debug disabled
	if s.logger.GetLevel() <= zerolog.DebugLevel {
		s.logger.Debug().
			Str("client_ip", clientIP).
			Int64("current_connections", s.stats.CurrentConnections.Load()).
			Int("max_connections", s.maxConns).
			Msg("ResourceGuard accepted connection")
	}

	// Read and validate identity headers forwarded by the gateway.
	// Must happen BEFORE ws.UpgradeHTTP — after upgrade, http.Error cannot be sent.
	tenantID := r.Header.Get(protocol.HeaderTenantID)
	if tenantID == "" {
		http.Error(w, "missing tenant ID", http.StatusBadRequest)
		return
	}
	if s.config != nil && s.config.InternalSecretEnabled {
		received := r.Header.Get(protocol.HeaderInternalSecret)
		if subtle.ConstantTimeCompare([]byte(received), []byte(s.config.InternalSecret)) != 1 {
			s.logger.Warn().
				Str(logging.LogKeyTenantSlug, tenantID).
				Str("client_ip", clientIP).
				Msg("WebSocket upgrade rejected: invalid internal secret")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	apiKeyID := r.Header.Get(protocol.HeaderAPIKeyID)
	userID := r.Header.Get(protocol.HeaderUserID) // "" for API-key-only auth (same as absent header)

	// NOTE: Authentication is now handled by ws-gateway (proxy layer)
	// ws-server is a dumb broadcaster - no auth logic here
	// Network security is enforced via Kubernetes NetworkPolicy

	// Try to acquire connection slot (non-blocking)
	// Rejects immediately at capacity rather than queueing indefinitely
	select {
	case s.connectionsSem <- struct{}{}:
		// Acquired connection slot
	default:
		s.logger.Warn().
			Str("client_ip", clientIP).
			Int("max_connections", s.maxConns).
			Msg("Connection rejected: at capacity")
		metrics.ConnectionsFailed.Inc()
		http.Error(w, "Server at capacity", http.StatusServiceUnavailable)
		return
	}

	upgradeStart := time.Now()
	s.logger.Debug().
		Str("client_ip", clientIP).
		Dur("pre_upgrade_elapsed_ms", time.Since(startTime)).
		Msg("Attempting WebSocket upgrade")

	conn, _, _, err := ws.UpgradeHTTP(r, w)
	upgradeDuration := time.Since(upgradeStart)

	if err != nil {
		<-s.connectionsSem // Release slot
		s.alertLogger.Error("WebSocketUpgradeFailed", "Failed to upgrade HTTP connection to WebSocket", map[string]any{
			"error":            err.Error(),
			"remoteAddr":       r.RemoteAddr,
			"client_ip":        clientIP,
			"upgrade_duration": upgradeDuration.String(),
			"total_elapsed":    time.Since(startTime).String(),
		})
		metrics.ConnectionsFailed.Inc()
		s.logger.Error().
			Err(err).
			Str("client_ip", clientIP).
			Dur("upgrade_duration_ms", upgradeDuration).
			Dur("total_elapsed_ms", time.Since(startTime)).
			Str("user_agent", r.Header.Get("User-Agent")).
			Str("sec_websocket_version", r.Header.Get("Sec-WebSocket-Version")).
			Msg("WebSocket upgrade failed")
		return
	}

	s.logger.Debug().
		Str("client_ip", clientIP).
		Dur("upgrade_duration_ms", upgradeDuration).
		Dur("total_elapsed_ms", time.Since(startTime)).
		Msg("WebSocket upgrade successful")

	client := s.connections.Get()
	client.transport = NewWebSocketTransport(conn)
	client.server = s
	client.id = s.clientCount.Add(1)
	client.remoteAddr = clientIP
	client.tenantID = tenantID
	client.apiKeyID = apiKeyID
	client.userID = userID
	if s.config != nil && s.config.ConnectionsRegistryEnabled {
		client.connID = uuid.New().String()
	}

	// Wire per-tenant broadcast subscription now that tenantID is known from the header.
	// OnTenantClientConnect is idempotent-guarded inside the hooks implementation — safe to call here.
	if s.tenantHooks != nil && tenantID != "" {
		if err := s.tenantHooks.OnTenantClientConnect(tenantID); err != nil {
			// Fail the connection — cannot receive broadcast without the tenant channel.
			client.transport.Close() //nolint:errcheck,gosec // G104: best-effort; connection is being dropped
			s.connections.Put(client)
			<-s.connectionsSem
			s.logger.Error().
				Err(err).
				Int64("client_id", client.id).
				Str(logging.LogKeyTenantSlug, tenantID).
				Msg("OnTenantClientConnect failed, rejecting connection")
			return
		}
	}

	s.clients.Store(client, true)
	s.stats.TotalConnections.Add(1)
	currentConns := s.stats.CurrentConnections.Add(1)

	// Update Prometheus metrics
	metrics.UpdateConnectionMetrics(string(TransportWebSocket))

	// Push registry connect event (non-blocking).
	if s.config != nil && s.config.ConnectionsRegistryEnabled && s.registryWriter != nil && client.connID != "" {
		s.registryWriter.PushConnect(
			client.connID, tenantID, apiKeyID, userID,
			s.config.PodID, s.shardID,
			clientIP, string(TransportWebSocket),
			client.connectedAt,
		)
	}

	// Client fully initialized, starting pumps
	s.logger.Info().
		Str("client_ip", clientIP).
		Str(logging.LogKeyTenantSlug, tenantID).
		Int64("client_id", client.id).
		Int64("current_connections", currentConns).
		Dur("total_setup_time_ms", time.Since(startTime)).
		Msg("Client connected successfully - pumps starting")

	// Modernized goroutine launch per Constitution VII: wg.Go handles Add(1) + Done().
	// RecoverPanic is first defer. WriteLoop/ReadLoop already have internal panic recovery
	// (belt and suspenders — harmless, only fires if inner recovery somehow fails).
	s.wg.Go(func() {
		defer logging.RecoverPanic(s.logger, "writePump", nil)
		s.pump.WriteLoop(s.ctx, client)
	})
	s.wg.Go(func() {
		defer logging.RecoverPanic(s.logger, "readPump", nil)
		s.pump.ReadLoop(s.ctx, client, s.disconnectClient, s.handleClientMessage)
	})
}

// disconnectClient handles client disconnect with proper instrumentation
// Centralizes all disconnect logic to ensure consistent metrics and logging
