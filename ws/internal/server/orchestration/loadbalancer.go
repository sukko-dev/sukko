// Package orchestration provides multi-tenant consumer pool management,
// shard-based connection distribution, and load balancing for ws-server.
package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/profiling"
	"github.com/sukko-dev/sukko/internal/shared/version"
)

// ShardMetrics defines the interface for shard load balancing metrics.
// This interface enables testing with mock shards that don't require
// full Server/BroadcastBus dependencies.
type ShardMetrics interface {
	GetCurrentConnections() int64
	GetMaxConnections() int
	GetAddr() string
}

// Verify that *Shard implements ShardMetrics at compile time
var _ ShardMetrics = (*Shard)(nil)

// LoadBalancer distributes incoming WebSocket connections to available shards.
// It uses a "least connections" strategy to ensure even distribution.
type LoadBalancer struct {
	addr    string
	shards  []*Shard
	proxies []http.Handler // One WebSocket proxy per shard
	logger  zerolog.Logger

	// HTTP timeouts (configured from platform config)
	httpReadTimeout  time.Duration
	httpWriteTimeout time.Duration
	httpIdleTimeout  time.Duration

	configHandler              http.HandlerFunc
	metricsAggregationInterval time.Duration
	editionManager             *license.Manager
	pprofEnabled               bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// LoadBalancerConfig holds configuration for the LoadBalancer
type LoadBalancerConfig struct {
	Addr   string
	Shards []*Shard
	Logger zerolog.Logger

	// HTTP timeouts (for trading platform burst tolerance)
	HTTPReadTimeout  time.Duration
	HTTPWriteTimeout time.Duration
	HTTPIdleTimeout  time.Duration

	// ConfigHandler serves the /config endpoint (set via platform.ConfigHandler)
	ConfigHandler http.HandlerFunc

	// Shard proxy timeouts
	ShardDialTimeout    time.Duration
	ShardMessageTimeout time.Duration

	// MetricsAggregationInterval controls how often shard metrics are aggregated.
	MetricsAggregationInterval time.Duration

	// EditionManager provides expiry-aware edition limits for /health capacity reporting.
	// May be nil — edition-aware capacity is skipped when nil.
	EditionManager *license.Manager

	// PprofEnabled registers /debug/pprof/ handlers on the LB's HTTP mux when true.
	// Disabled by default (Constitution IX: debug endpoints must be opt-in).
	PprofEnabled bool
}

// NewLoadBalancer creates a new LoadBalancer instance.
func NewLoadBalancer(cfg LoadBalancerConfig) (*LoadBalancer, error) {
	if len(cfg.Shards) == 0 {
		return nil, errors.New("no shards provided to load balancer")
	}

	ctx, cancel := context.WithCancel(context.Background())

	proxies := make([]http.Handler, len(cfg.Shards))
	for i, shard := range cfg.Shards {
		// Parse shard URL - use ws:// scheme for WebSocket proxy
		// CRITICAL: Must include /ws path to match shard's WebSocket endpoint
		shardAddr := shard.GetAddr()
		shardURL, err := url.Parse(fmt.Sprintf("ws://%s/ws", shardAddr))
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to parse shard address %s: %w", shardAddr, err)
		}
		// Create ShardProxy that forwards connections to the shard
		// Capacity control is handled by the shard's ResourceGuard
		proxies[i] = NewShardProxy(shard, shardURL, cfg.Logger, cfg.ShardDialTimeout, cfg.ShardMessageTimeout)

		// Log proxy target (one-time, no performance impact)
		cfg.Logger.Info().
			Int("shard_id", shard.ID).
			Str("target_url", shardURL.String()).
			Msg("Created ShardProxy for shard")
	}

	lb := &LoadBalancer{
		addr:    cfg.Addr,
		shards:  cfg.Shards,
		proxies: proxies,
		logger:  cfg.Logger.With().Str("component", "load_balancer").Logger(),
		// Store HTTP timeouts from config
		httpReadTimeout:            cfg.HTTPReadTimeout,
		httpWriteTimeout:           cfg.HTTPWriteTimeout,
		httpIdleTimeout:            cfg.HTTPIdleTimeout,
		configHandler:              cfg.ConfigHandler,
		metricsAggregationInterval: cfg.MetricsAggregationInterval,
		editionManager:             cfg.EditionManager,
		pprofEnabled:               cfg.PprofEnabled,
		ctx:                        ctx,
		cancel:                     cancel,
	}

	return lb, nil
}

// Start begins the LoadBalancer's HTTP server.
func (lb *LoadBalancer) Start() error {
	lb.logger.Info().Str("address", lb.addr).Msg("LoadBalancer starting")

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", lb.handleWebSocket)
	mux.HandleFunc("/health", lb.handleHealth)
	mux.HandleFunc("/version", version.Handler("ws-server"))
	editionUsageFunc := func(_ context.Context) *license.EditionUsage {
		var totalConns int64
		for _, shard := range lb.shards {
			totalConns += shard.GetCurrentConnections()
		}
		conns := int(totalConns)
		shards := len(lb.shards)
		return &license.EditionUsage{Connections: &conns, Shards: &shards}
	}
	editionHandler := license.EditionHandler(lb.editionManager, editionUsageFunc)
	mux.HandleFunc("/edition", func(w http.ResponseWriter, r *http.Request) {
		editionHandler(w, r)
	})
	if lb.configHandler != nil {
		mux.HandleFunc("/config", lb.configHandler)
	}
	mux.HandleFunc("/metrics", metrics.HandleMetrics)
	profiling.InitPprof(mux.HandleFunc, lb.pprofEnabled, lb.logger)

	httpServer := &http.Server{
		Addr:    lb.addr,
		Handler: mux,
		// Timeouts now configurable for trading platform burst tolerance
		// Defaults in platform/config.go: Read 15s, Write 15s, Idle 60s
		ReadTimeout:    lb.httpReadTimeout,
		WriteTimeout:   lb.httpWriteTimeout,
		IdleTimeout:    lb.httpIdleTimeout,
		MaxHeaderBytes: server.DefaultHTTPMaxHeaderBytes,
	}

	lb.wg.Go(func() {
		defer logging.RecoverPanic(lb.logger, "loadbalancer.ListenAndServe", nil)
		lb.logger.Info().Str("address", httpServer.Addr).Msg("LoadBalancer listening")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			lb.logger.Error().Err(err).Msg("LoadBalancer HTTP server error")
		}
	})

	lb.logger.Info().Msg("LoadBalancer started")

	// Start metrics aggregation goroutine
	lb.wg.Go(lb.runMetricsAggregation)

	return nil
}

// runMetricsAggregation periodically aggregates metrics from all shards
// This fixes the bug where per-shard collectors overwrite each other's metrics
func (lb *LoadBalancer) runMetricsAggregation() {
	defer logging.RecoverPanic(lb.logger, "loadbalancer.runMetricsAggregation", nil)
	ticker := time.NewTicker(lb.metricsAggregationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lb.aggregateMetrics()
		case <-lb.ctx.Done():
			return
		}
	}
}

// aggregateMetrics sums connection counts from all shards and updates prometheus metrics.
// This is a cold path (runs on a periodic ticker, ~5s interval).
func (lb *LoadBalancer) aggregateMetrics() {
	var totalConnections int64
	var totalMaxConnections int64

	for _, shard := range lb.shards {
		totalConnections += shard.GetCurrentConnections()
		totalMaxConnections += int64(shard.GetMaxConnections())
	}

	// Cap reported capacity to edition limit (monitoring/reporting only).
	// Actual connection enforcement uses per-shard connectionsSem (sized at startup).
	if lb.editionManager != nil {
		editionMax := lb.editionManager.CurrentLimits().MaxTotalConnections
		if editionMax > 0 && totalMaxConnections > int64(editionMax) {
			totalMaxConnections = int64(editionMax)
		}
	}

	metrics.SetAggregatedConnectionMetrics(totalConnections, totalMaxConnections)
}

// Shutdown gracefully stops the LoadBalancer.
func (lb *LoadBalancer) Shutdown() {
	lb.logger.Info().Msg("Shutting down LoadBalancer")
	lb.cancel()
	lb.wg.Wait() // Wait for the Start goroutine to finish
	lb.logger.Info().Msg("LoadBalancer shut down")
}

// handleWebSocket handles incoming WebSocket upgrade requests.
// Selects a shard using "least connections" strategy and delegates to ShardProxy.
// Capacity control is handled by the shard's ResourceGuard which may return 503.
func (lb *LoadBalancer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Select shard with least connections
	// Client receives: HTTP 503 "Server overloaded" (all shards full)
	// See: docs/API_REJECTION_RESPONSES.md (Scenario 4)
	selectedShardIndex, selectedShard := lb.selectShard()
	if selectedShard == nil {
		lb.logger.Warn().Msg("No available shards to accept connection")
		http.Error(w, "Server overloaded", http.StatusServiceUnavailable)
		return
	}

	lb.logger.Debug().
		Int("shard_id", selectedShard.ID).
		Int64("current_connections", selectedShard.GetCurrentConnections()).
		Msg("Routing connection to shard")

	lb.proxies[selectedShardIndex].ServeHTTP(w, r)
}

// selectShard selects a shard using the "least connections" strategy.
// It also respects the WS_MAX_CONNECTIONS limit per shard.
func (lb *LoadBalancer) selectShard() (int, *Shard) {
	// Convert to interface slice for testable selection logic
	shardMetrics := make([]ShardMetrics, len(lb.shards))
	for i, s := range lb.shards {
		shardMetrics[i] = s
	}

	idx := SelectShardByMetrics(shardMetrics)
	if idx < 0 {
		return -1, nil
	}
	return idx, lb.shards[idx]
}

// SelectShardByMetrics implements the "least connections" selection algorithm.
// This function is exported for testing with mock ShardMetrics implementations.
// Returns the index of the selected shard, or -1 if all shards are at capacity.
func SelectShardByMetrics(shards []ShardMetrics) int {
	var (
		leastConnections int64 = math.MaxInt64
		selectedIndex          = -1
	)

	for i, shard := range shards {
		currentConns := shard.GetCurrentConnections()
		maxConns := int64(shard.GetMaxConnections())

		// Skip shards that are at or over capacity
		if currentConns >= maxConns {
			continue
		}

		// Use < to select first shard with fewest connections
		// This reduces bias toward higher-indexed shards
		if currentConns < leastConnections {
			leastConnections = currentConns
			selectedIndex = i
		}
	}

	return selectedIndex
}

// handleHealth aggregates health status from all shards.
// Returns a simplified health response matching the expected format.
func (lb *LoadBalancer) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")

	// Handle preflight requests
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Aggregate metrics from all shards and collect per-shard stats
	var totalConnections int64
	var totalMaxConnections int64
	allShardsHealthy := true

	// Build per-shard stats array (zero performance impact - just atomic reads)
	type ShardStats struct {
		ID             int     `json:"id"`
		Connections    int64   `json:"connections"`
		MaxConnections int     `json:"max_connections"`
		Utilization    float64 `json:"utilization"`
		Status         string  `json:"status"`
	}
	shardStats := make([]ShardStats, 0, len(lb.shards))

	for _, shard := range lb.shards {
		currentConns := shard.GetCurrentConnections()
		maxConns := int64(shard.GetMaxConnections())

		totalConnections += currentConns
		totalMaxConnections += maxConns

		// Calculate utilization percentage
		utilization := 0.0
		if maxConns > 0 {
			utilization = (float64(currentConns) / float64(maxConns)) * 100
		}

		// Determine shard status
		var shardStatus string
		switch {
		case currentConns >= maxConns:
			shardStatus = "full"
		case utilization > 90:
			shardStatus = "high"
		case utilization > 75:
			shardStatus = "medium"
		default:
			shardStatus = "available"
		}

		// Simple health check: if shard is at over capacity, mark as unhealthy
		if currentConns > maxConns {
			allShardsHealthy = false
		}

		// Add per-shard stats (all atomic reads - zero performance cost)
		shardStats = append(shardStats, ShardStats{
			ID:             shard.ID,
			Connections:    currentConns,
			MaxConnections: int(maxConns),
			Utilization:    utilization,
			Status:         shardStatus,
		})
	}

	// Cap reported capacity to edition limit (monitoring/reporting only).
	if lb.editionManager != nil {
		editionMax := lb.editionManager.CurrentLimits().MaxTotalConnections
		if editionMax > 0 && totalMaxConnections > int64(editionMax) {
			totalMaxConnections = int64(editionMax)
		}
	}

	// Calculate capacity percentage
	var capacityPercent float64
	if totalMaxConnections > 0 {
		capacityPercent = float64(totalConnections) / float64(totalMaxConnections) * 100
	}

	// Get system-wide CPU/memory metrics (same for all shards - single process)
	// Query from shard 0 (arbitrary choice - all shards share the same metrics)
	cpuPercent, memoryMB, memoryPercent := 0.0, 0.0, 0.0
	if len(lb.shards) > 0 {
		cpuPercent, memoryMB, memoryPercent = lb.shards[0].GetSystemStats()
	}

	// Build simplified health response matching expected format
	isHealthy := allShardsHealthy && totalConnections <= totalMaxConnections
	status := "healthy"
	statusCode := http.StatusOK

	if !isHealthy {
		status = "unhealthy"
		statusCode = http.StatusServiceUnavailable
	} else if capacityPercent > 90 {
		status = "degraded"
	}

	checksMap := map[string]any{
		"capacity": map[string]any{
			"current":    int(totalConnections),
			"max":        int(totalMaxConnections),
			"percentage": capacityPercent,
		},
		"cpu": map[string]any{
			"percentage": cpuPercent, // System-wide CPU (all shards share same process)
		},
		"memory": map[string]any{
			"used_mb":    memoryMB,      // System-wide memory (all shards share same heap)
			"percentage": memoryPercent, // Percentage of container memory limit (0 if limit unknown)
		},
	}

	response := map[string]any{
		"status":  status,
		"healthy": isHealthy,
		"version": version.Get("ws-server"),
		"checks":  checksMap,
		"shards":  shardStats, // Per-shard connection breakdown (zero performance cost)
	}

	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// WriteHeader already sent — can't change status code, just log for debugging
		lb.logger.Error().Err(err).Msg("Failed to encode health response")
	}
}
