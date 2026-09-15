// Package server provides a high-performance WebSocket server optimized for
// trading platforms. It handles connection management, message broadcasting,
// rate limiting, and subscription-based filtering with hierarchical channels.
//
// Key features:
//   - Token bucket rate limiting per client
//   - CPU-aware resource guards with hysteresis
//   - Subscription indexing for efficient message routing (93% CPU savings)
//   - Graceful shutdown with connection draining
//   - Integration with pluggable message backends
package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/prometheus/client_golang/prometheus"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/server/history"
	"github.com/sukko-dev/sukko/internal/server/limits"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/server/registry"
	"github.com/sukko-dev/sukko/internal/server/stats"
	"github.com/sukko-dev/sukko/internal/shared/alerting"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// DefaultHTTPMaxHeaderBytes is the maximum number of bytes the server will
// read parsing the request header's keys and values, including the request line.
const DefaultHTTPMaxHeaderBytes = 1 << 20 // 1 MB

// Params bundles the shared platform config with per-shard overrides.
// Each shard receives the same *platform.ServerConfig pointer plus its own
// bind address, connection limit, and backend instance.
type Params struct {
	Config         *platform.ServerConfig
	Addr           string                 // Per-shard bind address (e.g., "0.0.0.0:3002")
	MaxConnections int                    // Per-shard connection limit
	Backend        backend.MessageBackend // Pluggable message backend instance
	BroadcastBus   broadcast.Bus          // Required when HistoryEnabled=true; used by historyWriter
	TenantHooks    TenantHooks            // Optional; nil = no per-tenant channel management (test or direct mode)
	RegistryWriter *registry.Writer       // Pod-level registry event writer; nil when registry disabled
	HealthWriter   *registry.HealthWriter // Pod-level health state tracker; nil when registry disabled
	ShardID        int                    // Shard index for per-shard GaugeVec labels in AdminListener
}

// Server is the main WebSocket server that manages client connections, message
// broadcasting, and resource protection. It integrates with pluggable message
// backends for message ingestion and uses subscription-based filtering to route
// messages only to interested clients.
//
// Thread Safety: All public methods are safe for concurrent use. Internal state
// is protected by mutexes, atomic operations, and channel-based synchronization.
//
// Lifecycle: Create with NewServer, start with Start, stop with Shutdown.
// The server supports graceful shutdown with configurable connection draining.
type Server struct {
	config   *platform.ServerConfig // Shared platform config (immutable)
	addr     string                 // Per-shard bind address
	maxConns int                    // Per-shard connection limit
	logger   zerolog.Logger         // Structured logger for all server events
	listener net.Listener           // TCP listener for accepting connections
	backend  backend.MessageBackend // Pluggable message backend (direct, kafka)

	// Connection management
	connections       *ConnectionPool    // Pre-allocated connection pool
	clients           sync.Map           // map[*Client]bool - active client tracking
	clientCount       atomic.Int64       // Atomic counter for connection IDs
	connectionsSem    chan struct{}      // Semaphore enforcing MaxConnections limit
	subscriptionIndex *SubscriptionIndex // Channel → subscribers index (93% CPU savings)

	// Rate limiting
	rateLimiter           *limits.RateLimiter           // Per-client message rate limiting
	connectionRateLimiter *limits.ConnectionRateLimiter // Per-IP and global connection throttling

	// Monitoring
	alertLogger   AlertLogger           // Security and operational alert events
	resourceGuard *limits.ResourceGuard // CPU-aware backpressure with hysteresis

	// Lifecycle
	ctx          context.Context    // Root context for all server goroutines
	cancel       context.CancelFunc // Cancels ctx to signal shutdown
	wg           sync.WaitGroup     // Tracks all server goroutines for clean shutdown
	shuttingDown atomic.Int32       // Atomic flag: 1 = rejecting new connections

	// Stats
	stats *stats.Stats // Runtime statistics and metrics

	// Pump for testable read/write operations
	pump *Pump // Handles WebSocket read/write with dependency injection

	// History
	broadcastBus  broadcast.Bus   // Bus reference for historyWriter subscription
	historyWriter *history.Writer // nil when HistoryEnabled=false
	historyEnv    string          // history stream/lock-key prefix, derived from ENVIRONMENT via historyNamespace()

	// Registry (Connections Management API)
	registryWriter    *registry.Writer        // pod-level; nil when ConnectionsRegistryEnabled=false
	healthWriter      *registry.HealthWriter  // pod-level; nil when ConnectionsRegistryEnabled=false
	adminListener     *registry.AdminListener // per-shard; nil when ConnectionsRegistryEnabled=false
	adminValkeyClient valkey.Client           // dedicated Valkey client for adminListener
	shardID           int                     // shard index for GaugeVec labels
	tenantHooks       TenantHooks             // Optional; nil = no per-tenant channel management

	// NOTE: Authentication is now handled by ws-gateway
	// ws-server is a dumb broadcaster with network-level security via NetworkPolicy
}

// NewServer creates a new WebSocket server with the provided configuration.
//
// The server initializes:
//   - Connection pool with pre-allocated client slots
//   - Subscription index for efficient message routing
//   - Rate limiters (per-client and per-IP if enabled)
//   - Resource guard for CPU-based backpressure
//
// Message ingestion is handled by the pluggable MessageBackend passed via params.
func NewServer(params Params, alerter alerting.Alerter) (*Server, error) {
	if params.Config == nil {
		return nil, errors.New("server params: Config must not be nil")
	}
	if params.Addr == "" {
		return nil, errors.New("server params: Addr must not be empty")
	}
	if params.MaxConnections < 1 {
		return nil, fmt.Errorf("server params: MaxConnections must be > 0, got %d", params.MaxConnections)
	}

	config := params.Config
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize structured logger
	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevel(config.LogLevel),
		Format:      logging.LogFormat(config.LogFormat),
		ServiceName: "ws-server",
	})

	s := &Server{
		config:            config,
		addr:              params.Addr,
		maxConns:          params.MaxConnections,
		backend:           params.Backend,
		broadcastBus:      params.BroadcastBus,
		tenantHooks:       params.TenantHooks,
		registryWriter:    params.RegistryWriter,
		healthWriter:      params.HealthWriter,
		shardID:           params.ShardID,
		logger:            logger,
		ctx:               ctx,
		cancel:            cancel,
		connections:       NewConnectionPool(params.MaxConnections, config.ClientSendBufferSize, config.GapNotifyBufferSize),
		connectionsSem:    make(chan struct{}, params.MaxConnections),
		subscriptionIndex: NewSubscriptionIndex(), // Fast channel → subscribers lookup
		rateLimiter:       limits.NewRateLimiter(config.ClientMsgBurstLimit, config.ClientMsgRatePerSec),
		stats:             stats.NewStats(),
	}

	// Initialize alert logger with zerolog and provided alerter
	s.alertLogger = newAlertLogger(logger, alerter)

	// Initialize ResourceGuard with static configuration
	s.resourceGuard = limits.NewResourceGuard(limits.ResourceGuardConfig{
		MaxConnections:           params.MaxConnections,
		MemoryLimit:              config.MemoryLimit,
		MaxKafkaMessagesPerSec:   config.MaxKafkaMessagesPerSec,
		MaxBroadcastsPerSec:      config.MaxBroadcastsPerSec,
		MaxGoroutines:            config.MaxGoroutines,
		RateLimitBurstMultiplier: config.RateLimitBurstMultiplier,
		CPURejectThreshold:       config.CPURejectThreshold,
		CPURejectThresholdLower:  config.CPURejectThresholdLower,
		CPUPauseThreshold:        config.CPUPauseThreshold,
		CPUPauseThresholdLower:   config.CPUPauseThresholdLower,
	}, logger, &s.stats.CurrentConnections)

	// Initialize connection rate limiter (if enabled)
	if config.ConnectionRateLimitEnabled {
		s.connectionRateLimiter = limits.NewConnectionRateLimiter(limits.ConnectionRateLimiterConfig{
			IPBurst:         config.ConnRateLimitIPBurst,
			IPRate:          config.ConnRateLimitIPRate,
			IPTTL:           config.ConnRateLimitIPTTL,
			GlobalBurst:     config.ConnRateLimitGlobalBurst,
			GlobalRate:      config.ConnRateLimitGlobalRate,
			CleanupInterval: config.ConnRateLimitCleanupInterval,
			Logger:          logger,
		})
		logger.Info().Msg("Connection rate limiting enabled")
	}

	logger.Info().
		Str("addr", params.Addr).
		Int("max_connections", params.MaxConnections).
		Int("kafka_rate_limit", config.MaxKafkaMessagesPerSec).
		Int("broadcast_rate_limit", config.MaxBroadcastsPerSec).
		Msg("Server initialized with ResourceGuard")

	if params.Backend != nil {
		logger.Info().Str("backend_type", fmt.Sprintf("%T", params.Backend)).Msg("Message backend configured")
	}

	// NOTE: Authentication is now handled by ws-gateway
	// ws-server is a dumb broadcaster - no auth logic here

	// Initialize Pump with adapters for testability
	// Timing values come from environment config (envDefault provides defaults)
	pumpCfg := NewPumpConfig(config.PongWait, config.PingPeriod, config.WriteWait, logger)
	pumpCfg.ClientMsgBurstLimit = config.ClientMsgBurstLimit
	pumpCfg.ClientMsgRatePerSec = config.ClientMsgRatePerSec
	s.pump = NewPump(
		pumpCfg,
		NewZerologAdapter(logger),
		logger, // ZerologLogger for panic recovery
		NewRateLimiterAdapter(s.rateLimiter),
		s.alertLogger,
		s.stats,
		&RealClock{},
	)

	return s, nil
}

// GetStats returns the server's runtime statistics.
func (s *Server) GetStats() *stats.Stats {
	return s.stats
}

// historyNamespace derives the history writer's Valkey stream/lock-key prefix from ENVIRONMENT
// (a non-topic use, normalized trim+lowercase). It is deliberately decoupled from the Kafka topic
// namespace (KAFKA_TOPIC_NAMESPACE): in kafka mode the two may differ, and history keys track the
// deployment identity, not the topic prefix. Matches the value the deleted ResolveNamespace produced.
func historyNamespace(environment string) string {
	return strings.ToLower(strings.TrimSpace(environment))
}

// Start begins accepting WebSocket connections on the configured address.
// It starts several background goroutines:
//   - HTTP server for WebSocket upgrades, health checks, and Prometheus metrics
//   - Kafka consumer for real-time message consumption
//   - Metrics collection at configured intervals
//   - Memory monitoring and buffer saturation sampling
//   - ResourceGuard CPU monitoring for backpressure
//
// Start returns immediately after launching goroutines. Use Shutdown to stop.
// Returns an error if the TCP listener cannot be created or Kafka fails to start.
func (s *Server) Start() error {
	// Create TCP listener with custom backlog for burst tolerance
	lc := net.ListenConfig{}
	listener, err := lc.Listen(s.ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	// Apply custom TCP backlog if configured (trading platform optimization)
	if s.config.TCPListenBacklog > 0 {
		if tcpLn, ok := listener.(*net.TCPListener); ok {
			file, err := tcpLn.File()
			if err == nil {
				// syscall.Listen sets the TCP accept queue size
				// This allows the OS to queue more pending connections during bursts
				// Critical for trading platforms where connection timing affects fairness
				_ = syscall.Listen(int(file.Fd()), s.config.TCPListenBacklog)
				_ = file.Close()

				s.logger.Info().
					Int("backlog", s.config.TCPListenBacklog).
					Msg("Set custom TCP listen backlog for burst tolerance")
			}
		}
	}

	s.listener = listener

	s.logger.Info().
		Str("address", s.addr).
		Msg("Server listening")

	// Note: Kafka consumer is managed by MultiTenantConsumerPool.
	// Individual servers don't start/stop the consumer - they just hold a reference
	// for metrics and replay operations. The pool handles lifecycle.

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWebSocket)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)           // Kubernetes readiness probe (backend snapshot gate)
	mux.HandleFunc("/metrics", metrics.HandleMetrics) // Prometheus metrics endpoint

	// NOTE: Token issuance moved to auth-service (internal/authservice)
	// ws-server only handles JWT validation, not token issuance

	server := &http.Server{
		Handler:        mux,
		ReadTimeout:    s.config.HTTPReadTimeout,
		WriteTimeout:   s.config.HTTPWriteTimeout,
		IdleTimeout:    s.config.HTTPIdleTimeout,
		MaxHeaderBytes: DefaultHTTPMaxHeaderBytes,
	}

	s.wg.Go(func() {
		defer logging.RecoverPanic(s.logger, "server.Serve", nil)
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			s.logger.Error().
				Err(err).
				Msg("Server accept loop error")
		}
	})

	// Start metrics collection
	s.wg.Go(s.collectMetrics)

	// Start memory monitoring
	s.wg.Go(s.monitorMemory)

	// Start buffer saturation sampling
	s.wg.Go(s.sampleClientBuffers)

	// Start history writer if enabled (requires BroadcastBus and Valkey).
	if s.config.HistoryEnabled {
		if s.broadcastBus == nil {
			s.logger.Error().Msg("history writer: HistoryEnabled=true but BroadcastBus is nil; history disabled")
		} else {
			s.historyEnv = historyNamespace(s.config.Environment)
			historyValkeyClient, hvErr := s.buildHistoryValkeyClient()
			if hvErr != nil {
				s.logger.Error().Err(hvErr).Msg("history writer: failed to create Valkey client; history disabled")
			} else {
				s.historyWriter = history.NewWriter(
					s.ctx,
					s.config,
					s.broadcastBus,
					historyValkeyClient,
					s.logger,
					prometheus.DefaultRegisterer,
					s.historyEnv,
				)
				s.wg.Go(s.historyWriter.Run)
				s.logger.Info().Msg("history writer started")
			}
		}
	}

	// Start admin listener if registry enabled (per-shard, synchronous SUBSCRIBE before HTTP listener binds).
	if s.config.ConnectionsRegistryEnabled {
		adminValkeyClient, avErr := registry.BuildAdminValkeyClient(s.config)
		if avErr != nil {
			return fmt.Errorf("admin listener shard %d: build Valkey client: %w", s.shardID, avErr)
		}
		s.adminValkeyClient = adminValkeyClient
		s.adminListener = registry.NewAdminListener(
			s.shardID,
			s.config,
			adminValkeyClient,
			s.healthWriter,
			s.logger,
			&s.clients,
			prometheus.DefaultRegisterer,
		)
		if err := s.adminListener.Subscribe(s.ctx); err != nil {
			adminValkeyClient.Close()
			s.adminValkeyClient = nil
			return fmt.Errorf("admin listener shard %d: %w", s.shardID, err)
		}
		s.wg.Go(func() {
			defer logging.RecoverPanic(s.logger, "admin_listener_outer", nil)
			s.adminListener.Run(s.ctx)
		})
		s.logger.Info().Int("shard_id", s.shardID).Msg("admin listener started")
	}

	// Start ResourceGuard monitoring (static limits with safety checks)
	// Now also updates server stats for unified CPU measurement
	s.resourceGuard.StartMonitoring(s.ctx, &s.wg, s.config.MetricsInterval, s.stats)

	s.alertLogger.Info("ServerStarted", "WebSocket server started successfully", map[string]any{
		"addr":           s.addr,
		"maxConnections": s.maxConns,
	})

	return nil
}

// Shutdown performs a graceful server shutdown with connection draining.
// The shutdown sequence:
//  1. Sets shutdown flag to reject new connections immediately
//  2. Closes the TCP listener (stops accepting new connections)
//  3. Stops the Kafka consumer (no new messages)
//  4. Waits up to 30 seconds for existing connections to drain gracefully
//  5. Force-closes remaining connections after grace period
//  6. Cancels the root context to stop all background goroutines
//  7. Waits for all goroutines to complete
//
// During the grace period, clients can complete pending operations and
// disconnect cleanly. This prevents data loss for in-flight messages.
func (s *Server) Shutdown() error {
	s.logger.Info().Msg("Initiating graceful shutdown")

	// Set shutdown flag to reject new connections
	s.shuttingDown.Store(1)

	// Stop accepting new connections
	if s.listener != nil {
		s.logger.Info().Msg("Closing listener (no new connections accepted)")
		if err := s.listener.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing listener")
		}
	}

	// Note: Kafka consumer lifecycle is managed by MultiTenantConsumerPool.
	// Servers don't stop the shared consumer - the pool handles it during shutdown.

	// Count current connections
	currentConns := s.stats.CurrentConnections.Load()
	s.logger.Info().
		Int64("active_connections", currentConns).
		Dur("grace_period", s.config.ShutdownGracePeriod).
		Msg("Draining active connections")

	// Grace period for connection draining
	drainTimer := time.NewTimer(s.config.ShutdownGracePeriod)
	checkTicker := time.NewTicker(s.config.ShutdownCheckInterval)
	defer checkTicker.Stop()
	defer drainTimer.Stop()

	// Monitor connection draining
	for {
		select {
		case <-drainTimer.C:
			// Grace period expired, force close remaining connections
			remaining := s.stats.CurrentConnections.Load()
			if remaining > 0 {
				s.logger.Warn().
					Int64("remaining_connections", remaining).
					Msg("Grace period expired, force closing remaining connections")
			}
			goto forceClose

		case <-checkTicker.C:
			// Check if all connections drained
			remaining := s.stats.CurrentConnections.Load()
			if remaining == 0 {
				s.logger.Info().Msg("All connections drained gracefully")
				goto cleanup
			}
			s.logger.Info().
				Int64("remaining_connections", remaining).
				Msg("Waiting for connections to drain")
		}
	}

forceClose:
	// Force close all remaining connections with proper metrics
	s.clients.Range(func(key, _ any) bool {
		if client, ok := key.(*Client); ok {
			// Record shutdown disconnect (both Prometheus and Stats)
			duration := time.Since(client.connectedAt)
			metrics.RecordDisconnectWithStats(s.stats, string(client.TransportType()), pkgmetrics.DisconnectServerShutdown, pkgmetrics.InitiatedByServer, duration)

			if client.clientCancel != nil {
				client.clientCancel()
			}
			if client.clientWg != nil {
				client.clientWg.Wait()
			}
			client.closeSend()
		}
		return true
	})

cleanup:
	// Cancel context to stop all goroutines
	s.cancel()

	// Stop connection rate limiter cleanup goroutine
	if s.connectionRateLimiter != nil {
		s.connectionRateLimiter.Stop()
	}

	// Wait for all goroutines to finish
	s.logger.Info().Msg("Waiting for all goroutines to finish")
	s.wg.Wait()

	// Close the history writer's Valkey client after all goroutines have exited.
	if s.historyWriter != nil {
		s.historyWriter.Close()
	}

	// Close the admin listener's Valkey client after all goroutines have exited.
	if s.adminValkeyClient != nil {
		s.adminValkeyClient.Close()
		s.adminValkeyClient = nil
	}

	s.logger.Info().Msg("Graceful shutdown completed")
	return nil
}

// buildHistoryValkeyClient creates a dedicated valkey.Client for Streams I/O.
// Separate from the broadcast bus client to avoid contention on the pub/sub connection.
// Mirrors the TLS config used by the broadcast bus client.
func (s *Server) buildHistoryValkeyClient() (valkey.Client, error) {
	opt := valkey.ClientOption{
		InitAddress: s.config.ValkeyAddrs,
		Password:    s.config.ValkeyPassword,
		SelectDB:    s.config.ValkeyDB,
	}
	if platform.UseValkeySentinel(s.config.ValkeyAddrs, s.config.ValkeyMasterName) {
		opt.Sentinel = valkey.SentinelOption{
			MasterSet: s.config.ValkeyMasterName,
		}
	}
	if s.config.ValkeyTLSEnabled {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: s.config.ValkeyTLSInsecure, //nolint:gosec // Controlled by ValkeyTLSInsecure config for dev/testing environments
			MinVersion:         tls.VersionTLS12,
		}
		if s.config.ValkeyTLSCAPath != "" {
			caCert, err := os.ReadFile(s.config.ValkeyTLSCAPath)
			if err != nil {
				return nil, fmt.Errorf("history valkey: read CA cert: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("history valkey: parse CA cert from %s", s.config.ValkeyTLSCAPath)
			}
			tlsCfg.RootCAs = pool
		}
		opt.TLSConfig = tlsCfg
	}
	client, err := valkey.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("history valkey client: %w", err)
	}
	return client, nil
}
