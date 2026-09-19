package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/sukko-dev/sukko/internal/shared/httputil"
)

// handleReady is the Kubernetes readiness probe. It returns 200 iff the message backend is ready to
// serve traffic, else 503. In kafka mode the backend is not ready until the topic-registry routing
// snapshot has been applied, so ws-server does not consume or broadcast before it can resolve
// topic->tenant (#179 P3). This is distinct from /health (liveness): a not-ready pod is pulled from
// the Service endpoints but MUST NOT be restarted, so readiness never gates /health.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// A nil backend is direct mode with nothing to wait for → ready.
	ready := s.backend == nil || s.backend.Ready()
	status := http.StatusOK
	state := "ready"
	if !ready {
		status = http.StatusServiceUnavailable
		state = "not_ready"
	}
	if err := httputil.WriteJSON(w, status, map[string]any{"status": state, "ready": ready}); err != nil {
		s.logger.Debug().Err(err).Msg("Failed to encode readiness response")
	}
}

// HTTP handlers for health and stats endpoints
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
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

	// Get current metrics
	cpuPercent, memoryMB := s.stats.ResourceMetrics()

	currentConns := s.stats.CurrentConnections.Load()
	maxConns := int64(s.maxConns) // Static configuration
	slowClients := s.stats.SlowClientsDisconnected.Load()
	totalConns := s.stats.TotalConnections.Load()

	// Get ResourceGuard stats
	resourceStats := s.resourceGuard.GetStats()

	// Use actual configured limits
	memLimitMB := float64(s.config.MemoryLimit) / 1024.0 / 1024.0 // Convert bytes to MB
	goroutinesCurrent, _ := resourceStats["goroutines_current"].(int)
	goroutinesLimit := s.config.MaxGoroutines

	// Calculate health status
	// Logic: if resource > configured limit, set unhealthy
	isHealthy := true
	warnings := []string{}
	errors := []string{}

	// Check message backend health
	backendStatus := "not_configured"
	backendHealthy := true // no backend configured is not a failure (direct mode)
	if s.backend != nil {
		if s.backend.IsHealthy() {
			backendStatus = "healthy"
			backendHealthy = true
		} else {
			backendStatus = "unhealthy"
			backendHealthy = false
			isHealthy = false
			errors = append(errors, "Message backend unhealthy")
			s.logger.Error().Msg("Health check failed: message backend unhealthy")
		}
	}

	// Check broadcast bus state (ADR-0016). DEGRADED ONLY — never an error:
	// /health is the LIVENESS probe (Helm livenessProbe path), and a bus outage
	// or a converging subscription set is not fixed by restarting the pod.
	// Failing liveness here would restart pods fleet-wide during a recoverable
	// Valkey blip; failing readiness would pull the whole fleet from the load
	// balancer. Both amplify a blip into a total outage, so bus state is
	// reported in the body and in metrics but never flips isHealthy. Readiness
	// (/ready) does not consult the bus at all.
	broadcastCheck := map[string]any{"status": "not_configured", "healthy": true}
	if s.broadcastBus != nil {
		bm := s.broadcastBus.GetMetrics()
		busStatus := "healthy"
		if !bm.PublishHealthy || !bm.SubscriptionsConverged {
			busStatus = "degraded"
		}
		broadcastCheck = map[string]any{
			"status":                    busStatus,
			"healthy":                   bm.Healthy,
			"publish_healthy":           bm.PublishHealthy,
			"subscriptions_converged":   bm.SubscriptionsConverged,
			"subscriptions_desired":     bm.SubscriptionsDesired,
			"subscriptions_established": bm.SubscriptionsEstablished,
		}
		if !bm.SubscriptionsConverged {
			warnings = append(warnings, fmt.Sprintf(
				"Broadcast subscriptions not converged (established %d < desired %d) — some broadcasts are not being received on this pod",
				bm.SubscriptionsEstablished, bm.SubscriptionsDesired))
			s.logger.Warn().
				Int("desired", bm.SubscriptionsDesired).
				Int("established", bm.SubscriptionsEstablished).
				Msg("Health check degraded: broadcast subscriptions not converged")
		}
		if !bm.PublishHealthy {
			warnings = append(warnings, "Broadcast bus publish path unhealthy")
			s.logger.Warn().Msg("Health check degraded: broadcast bus publish path unhealthy")
		}
	}

	// Check CPU (against configured reject threshold)
	cpuHealthy := true
	if cpuPercent > s.config.CPURejectThreshold {
		isHealthy = false
		cpuHealthy = false
		errors = append(errors, fmt.Sprintf("CPU exceeds reject threshold (%.1f%% > %.1f%%)", cpuPercent, s.config.CPURejectThreshold))
		s.logger.Error().
			Float64("cpu_percent", cpuPercent).
			Float64("cpu_threshold", s.config.CPURejectThreshold).
			Msg("Health check failed: CPU exceeds threshold")
	}

	// Check memory (against configured limit)
	memPercent := (memoryMB / memLimitMB) * 100
	memHealthy := true
	if memoryMB > memLimitMB {
		isHealthy = false
		memHealthy = false
		errors = append(errors, fmt.Sprintf("Memory exceeds limit (%.1fMB > %.1fMB)", memoryMB, memLimitMB))
		s.logger.Error().
			Float64("used_mb", memoryMB).
			Float64("limit_mb", memLimitMB).
			Msg("Health check failed: Memory exceeds limit")
	}

	// Check goroutines (against configured limit)
	goroutinesPercent := float64(goroutinesCurrent) / float64(goroutinesLimit) * 100
	goroutinesHealthy := true
	if goroutinesCurrent > goroutinesLimit {
		isHealthy = false
		goroutinesHealthy = false
		errors = append(errors, fmt.Sprintf("Goroutines exceed limit (%d > %d)", goroutinesCurrent, goroutinesLimit))
		s.logger.Error().
			Int("current", goroutinesCurrent).
			Int("limit", goroutinesLimit).
			Msg("Health check failed: Goroutines exceed limit")
	}

	// Check capacity (NOT a failure condition - server operates within design limits at 100%)
	capacityPercent := float64(currentConns) / float64(maxConns) * 100
	capacityHealthy := true
	switch {
	case capacityPercent > 100:
		// If it comes here, it means the server is over loaded and the limit check is failing
		capacityHealthy = false
		errors = append(errors, fmt.Sprintf("Server at over capacity (%d/%d)", currentConns, maxConns))
		s.logger.Error().
			Int64("current", currentConns).
			Int64("max", maxConns).
			Msg("Health check failed: Server at over capacity")
	case capacityPercent == 100:
		capacityHealthy = true
		warnings = append(warnings, fmt.Sprintf("Server at full capacity (%d/%d)", currentConns, maxConns))
		s.logger.Warn().
			Int64("current", currentConns).
			Int64("max", maxConns).
			Msg("Server at full capacity - consider scaling")
	case capacityPercent > 90:
		capacityHealthy = true
		warnings = append(warnings, fmt.Sprintf("Server near capacity (%.1f%%)", capacityPercent))
	}

	// Check slow client rate
	if totalConns > 0 {
		slowClientPercent := float64(slowClients) / float64(totalConns) * 100
		if slowClientPercent > 10 {
			warnings = append(warnings, "High slow client disconnect rate (>10%)")
		}
	}

	// Determine overall status
	status := "healthy"
	statusCode := http.StatusOK
	if !isHealthy {
		status = "unhealthy"
		statusCode = http.StatusServiceUnavailable
	} else if len(warnings) > 0 {
		status = "degraded"
		statusCode = http.StatusOK // Still accepting traffic
	}

	// Build response
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"status":  status,
		"healthy": isHealthy,
		"checks": map[string]any{
			"backend": map[string]any{
				"status":  backendStatus,
				"healthy": backendHealthy,
			},
			"broadcast": broadcastCheck,
			"capacity": map[string]any{
				"current":    currentConns,
				"max":        maxConns,
				"percentage": capacityPercent,
				"healthy":    capacityHealthy,
			},
			"goroutines": map[string]any{
				"current":    goroutinesCurrent,
				"limit":      goroutinesLimit,
				"percentage": goroutinesPercent,
				"healthy":    goroutinesHealthy,
			},
			"memory": map[string]any{
				"used_mb":    memoryMB,
				"limit_mb":   memLimitMB,
				"percentage": memPercent,
				"healthy":    memHealthy,
			},
			"cpu": map[string]any{
				"percentage": cpuPercent,
				"threshold":  s.config.CPURejectThreshold,
				"healthy":    cpuHealthy,
			},
			"limits": map[string]any{
				"max_connections":  s.maxConns,
				"max_goroutines":   s.config.MaxGoroutines,
				"cpu_threshold":    s.config.CPURejectThreshold,
				"memory_limit_mb":  memLimitMB,
				"worker_pool_size": resourceStats["worker_pool_size"],
			},
		},
		"observability": s.getObservabilityStats(),
		"warnings":      warnings,
		"errors":        errors,
		"uptime":        time.Since(s.stats.StartTime).Seconds(),
	}); err != nil {
		// Can't do much since WriteHeader already called, just log for debugging
		s.logger.Debug().Err(err).Msg("Failed to encode health response")
	}
}

// getObservabilityStats returns Phase 2 observability metrics for /health endpoint
func (s *Server) getObservabilityStats() map[string]any {
	// Get disconnect stats (returns snapshot copy)
	disconnects, totalDisconnects := s.stats.GetDisconnectsByReason()

	// Get dropped broadcast stats (returns snapshot copy)
	droppedBroadcasts, totalDropped := s.stats.GetDroppedBroadcastsByChannel()

	// Calculate buffer saturation statistics (returns snapshot copy)
	bufferStats := calculateBufferStats(s.stats.GetBufferSaturationSamples())

	return map[string]any{
		"disconnects": map[string]any{
			"total":     totalDisconnects,
			"by_reason": disconnects,
		},
		"dropped_broadcasts": map[string]any{
			"total":      totalDropped,
			"by_channel": droppedBroadcasts,
		},
		"buffer_saturation": bufferStats,
	}
}

// calculateBufferStats computes statistics from buffer saturation samples
func calculateBufferStats(samples []int) map[string]any {
	if len(samples) == 0 {
		return map[string]any{
			"samples": 0,
			"avg":     0,
			"p50":     0,
			"p95":     0,
			"p99":     0,
			"max":     0,
		}
	}

	// Sort for percentile calculation
	sorted := make([]int, len(samples))
	copy(sorted, samples)

	// Simple bubble sort for small arrays (max 100 elements)
	for i := range sorted {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	// Calculate average
	sum := 0
	maxVal := 0
	for _, v := range sorted {
		sum += v
		if v > maxVal {
			maxVal = v
		}
	}
	avg := float64(sum) / float64(len(sorted))

	// Calculate percentiles
	p50idx := int(float64(len(sorted)) * 0.50)
	p95idx := int(float64(len(sorted)) * 0.95)
	p99idx := int(float64(len(sorted)) * 0.99)

	return map[string]any{
		"samples": len(samples),
		"avg":     avg,
		"p50":     sorted[p50idx],
		"p95":     sorted[p95idx],
		"p99":     sorted[p99idx],
		"max":     maxVal,
	}
}
