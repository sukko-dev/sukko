package limits

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/time/rate"

	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/server/stats"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// SystemMonitorInterface defines the interface for system monitoring
// This allows for dependency injection and easier testing
type SystemMonitorInterface interface {
	GetCPUPercent() float64
	GetSmoothedCPUPercent() float64
	GetMemoryBytes() int64
	GetGoroutines() int
	GetMetrics() metrics.SystemMetrics
	GetCPUAllocation() float64
}

// ResourceGuardConfig contains only the fields that ResourceGuard reads.
// This focused config replaces the full ServerConfig dependency, allowing callers
// to construct it from their own config without importing the full platform config.
type ResourceGuardConfig struct {
	MaxConnections           int
	MemoryLimit              int64
	MaxKafkaMessagesPerSec   int
	MaxBroadcastsPerSec      int
	MaxGoroutines            int
	RateLimitBurstMultiplier int // Burst capacity as a multiple of steady-state rate
	CPURejectThreshold       float64
	CPURejectThresholdLower  float64
	CPUPauseThreshold        float64
	CPUPauseThresholdLower   float64
}

// ResourceGuard enforces static resource limits and prevents server overload.
//
// Philosophy:
//   - Static configuration (predictable behavior)
//   - Rate limiting (prevent work overload)
//   - Safety valves (emergency brakes)
//   - No auto-calculation (deterministic)
//   - Single source of truth via SystemMonitor (eliminates duplicate measurements)
//
// Unlike DynamicCapacityManager, ResourceGuard does NOT:
//   - Calculate capacity from measurements
//   - Auto-adjust limits
//   - Track historical trends
//
// ResourceGuard DOES:
//   - Enforce configured limits strictly
//   - Rate limit Kafka consumption
//   - Rate limit broadcasts
//   - Provide safety checks (CPU, memory, goroutines)
//   - Log all decisions to Loki
//   - Query SystemMonitor for resource metrics (no duplicate measurements)
//   - Use hysteresis for CPU thresholds to prevent oscillation
type ResourceGuard struct {
	// Static configuration
	config ResourceGuardConfig
	logger zerolog.Logger

	// Rate limiters
	kafkaLimiter     *rate.Limiter // Limits Kafka message consumption
	broadcastLimiter *rate.Limiter // Limits broadcast operations

	// Goroutine limiter
	goroutineLimiter *GoroutineLimiter

	// System monitor (singleton, shared across all ResourceGuards)
	// Uses interface to allow dependency injection for testing
	systemMonitor SystemMonitorInterface

	// External state (pointers to server stats)
	currentConns *atomic.Int64 // Pointer to server's current connection count (typed atomic)

	// Hysteresis state for CPU-based decisions
	// Uses atomic for thread-safe access without mutex overhead
	//
	// State machine for each:
	//   ACCEPTING/RUNNING                REJECTING/PAUSED
	//   ┌─────────────┐                 ┌─────────────┐
	//   │             │  CPU > upper    │             │
	//   │  Normal     │ ─────────────►  │  Triggered  │
	//   │             │                 │             │
	//   │             │  CPU < lower    │             │
	//   │             │ ◄─────────────  │             │
	//   └─────────────┘                 └─────────────┘
	isRejectingCPU atomic.Bool // true when in CPU rejection state
	isPausingKafka atomic.Bool // true when in Kafka pause state
}

// GoroutineLimiter limits concurrent goroutines using a semaphore
type GoroutineLimiter struct {
	sem chan struct{}
	max int
}

// NewGoroutineLimiter creates a limiter that allows maxGoroutines concurrent goroutines.
func NewGoroutineLimiter(maxGoroutines int) *GoroutineLimiter {
	return &GoroutineLimiter{
		sem: make(chan struct{}, maxGoroutines),
		max: maxGoroutines,
	}
}

// Acquire attempts to acquire a goroutine slot
// Returns true if acquired, false if at limit
func (gl *GoroutineLimiter) Acquire() bool {
	select {
	case gl.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release releases a goroutine slot
func (gl *GoroutineLimiter) Release() {
	<-gl.sem
}

// Current returns the current number of active goroutines
func (gl *GoroutineLimiter) Current() int {
	return len(gl.sem)
}

// Max returns the maximum allowed goroutines
func (gl *GoroutineLimiter) Max() int {
	return gl.max
}

// NewResourceGuard creates a new resource guard with static configuration
//
// Parameters:
//   - config: Server configuration with explicit resource limits
//   - logger: Structured logger for Loki
//   - currentConns: Pointer to server's current connection count (atomic.Int64)
//
// Example:
//
//	guard := NewResourceGuard(config, logger, &server.stats.CurrentConnections)
func NewResourceGuard(config ResourceGuardConfig, logger zerolog.Logger, currentConns *atomic.Int64) *ResourceGuard {
	// Create Kafka rate limiter
	// Limit: MaxKafkaMessagesPerSec per second
	// Burst: configurable multiplier × rate (for traffic spikes)
	kafkaLimiter := rate.NewLimiter(
		rate.Limit(config.MaxKafkaMessagesPerSec),
		config.MaxKafkaMessagesPerSec*config.RateLimitBurstMultiplier,
	)

	// Create broadcast rate limiter
	broadcastLimiter := rate.NewLimiter(
		rate.Limit(config.MaxBroadcastsPerSec),
		config.MaxBroadcastsPerSec*config.RateLimitBurstMultiplier,
	)

	// Create goroutine limiter
	goroutineLimiter := NewGoroutineLimiter(config.MaxGoroutines)

	// Get SystemMonitor singleton (shared across all ResourceGuards)
	systemMonitor := metrics.GetSystemMonitor(logger)

	rg := &ResourceGuard{
		config:           config,
		logger:           logger,
		kafkaLimiter:     kafkaLimiter,
		broadcastLimiter: broadcastLimiter,
		goroutineLimiter: goroutineLimiter,
		systemMonitor:    systemMonitor,
		currentConns:     currentConns,
	}

	logger.Info().
		Float64("cpu_allocation", systemMonitor.GetCPUAllocation()).
		Int64("memory_limit", config.MemoryLimit).
		Int("max_connections", config.MaxConnections).
		Int("max_kafka_rate", config.MaxKafkaMessagesPerSec).
		Int("max_broadcast_rate", config.MaxBroadcastsPerSec).
		Int("max_goroutines", config.MaxGoroutines).
		Float64("cpu_reject_upper", config.CPURejectThreshold).
		Float64("cpu_reject_lower", config.CPURejectThresholdLower).
		Float64("cpu_reject_band", config.CPURejectThreshold-config.CPURejectThresholdLower).
		Float64("cpu_pause_upper", config.CPUPauseThreshold).
		Float64("cpu_pause_lower", config.CPUPauseThresholdLower).
		Float64("cpu_pause_band", config.CPUPauseThreshold-config.CPUPauseThresholdLower).
		Msgf("ResourceGuard initialized: reject at >%.0f%%, resume at <%.0f%% (%.0f%% hysteresis band)",
			config.CPURejectThreshold,
			config.CPURejectThresholdLower,
			config.CPURejectThreshold-config.CPURejectThresholdLower)

	return rg
}

// NewResourceGuardWithMonitor creates a ResourceGuard with a custom SystemMonitor
// This is primarily used for testing to inject mock monitors
func NewResourceGuardWithMonitor(config ResourceGuardConfig, logger zerolog.Logger, currentConns *atomic.Int64, monitor SystemMonitorInterface) *ResourceGuard {
	kafkaLimiter := rate.NewLimiter(
		rate.Limit(config.MaxKafkaMessagesPerSec),
		config.MaxKafkaMessagesPerSec*config.RateLimitBurstMultiplier,
	)

	broadcastLimiter := rate.NewLimiter(
		rate.Limit(config.MaxBroadcastsPerSec),
		config.MaxBroadcastsPerSec*config.RateLimitBurstMultiplier,
	)

	goroutineLimiter := NewGoroutineLimiter(config.MaxGoroutines)

	return &ResourceGuard{
		config:           config,
		logger:           logger,
		kafkaLimiter:     kafkaLimiter,
		broadcastLimiter: broadcastLimiter,
		goroutineLimiter: goroutineLimiter,
		systemMonitor:    monitor,
		currentConns:     currentConns,
	}
}

// ShouldAcceptConnection checks if a new connection can be accepted
//
// Checks (in order):
//  1. Hard connection limit
//  2. CPU emergency brake
//  3. Memory emergency brake
//  4. Goroutine limit
//
// Returns:
//   - accept: true if connection should be accepted
//   - reason: human-readable rejection reason (if rejected)
func (rg *ResourceGuard) ShouldAcceptConnection() (accept bool, reason string) {
	// Query current resource metrics from SystemMonitor (single source of truth)
	// Use EWMA-smoothed CPU to prevent transient spikes from triggering false rejections
	currentConns := rg.currentConns.Load()
	currentCPU := rg.systemMonitor.GetSmoothedCPUPercent()
	currentMemory := rg.systemMonitor.GetMemoryBytes()
	currentGoros := rg.systemMonitor.GetGoroutines()

	// Check 1: Hard connection limit
	if currentConns >= int64(rg.config.MaxConnections) {
		metrics.IncrementCapacityRejection("at_max_connections")
		rg.logger.Debug().
			Int64("current_conns", currentConns).
			Int("max_conns", rg.config.MaxConnections).
			Msg("Connection rejected: at max connections")
		return false, fmt.Sprintf("at max connections (%d)", rg.config.MaxConnections)
	}

	// Check 2: CPU emergency brake with hysteresis
	//
	// Hysteresis prevents rapid oscillation when CPU hovers near the threshold.
	// Two thresholds are used:
	//   - Upper (CPURejectThreshold): Start rejecting when CPU exceeds this
	//   - Lower (CPURejectThresholdLower): Stop rejecting when CPU drops below this
	//
	// Between the thresholds (the "deadband"), the current state is maintained.
	currentlyRejecting := rg.isRejectingCPU.Load()

	if currentlyRejecting {
		// Currently rejecting - check if we should stop (CPU below lower threshold)
		if currentCPU < rg.config.CPURejectThresholdLower {
			rg.isRejectingCPU.Store(false)
			rg.logger.Info().
				Float64("cpu", currentCPU).
				Float64("lower_threshold", rg.config.CPURejectThresholdLower).
				Msg("CPU hysteresis: exiting rejection state (CPU dropped below lower threshold)")
			// Fall through to accept
		} else {
			// Still in rejection state (CPU in deadband or still high)
			metrics.IncrementCapacityRejection("cpu_overload")
			rg.logger.Debug().
				Float64("current_cpu", currentCPU).
				Float64("upper_threshold", rg.config.CPURejectThreshold).
				Float64("lower_threshold", rg.config.CPURejectThresholdLower).
				Msg("Connection rejected: CPU in rejection state (hysteresis)")
			return false, fmt.Sprintf("CPU %.1f%% (rejecting until < %.1f%%)",
				currentCPU, rg.config.CPURejectThresholdLower)
		}
	} else {
		// Currently accepting - check if we should start rejecting (CPU above upper threshold)
		if currentCPU > rg.config.CPURejectThreshold {
			rg.isRejectingCPU.Store(true)
			metrics.IncrementCapacityRejection("cpu_overload")
			rg.logger.Info().
				Float64("cpu", currentCPU).
				Float64("upper_threshold", rg.config.CPURejectThreshold).
				Msg("CPU hysteresis: entering rejection state (CPU exceeded upper threshold)")
			return false, fmt.Sprintf("CPU %.1f%% > %.1f%%", currentCPU, rg.config.CPURejectThreshold)
		}
		// Still accepting (CPU below upper threshold)
	}

	// Check 3: Memory emergency brake
	if currentMemory > rg.config.MemoryLimit {
		metrics.IncrementCapacityRejection("memory_limit")
		rg.logger.Debug().
			Int64("current_memory_mb", currentMemory/(1024*1024)).
			Int64("limit_mb", rg.config.MemoryLimit/(1024*1024)).
			Msg("Connection rejected: memory limit exceeded")
		return false, "memory limit exceeded"
	}

	// Check 4: Goroutine limit
	if currentGoros > rg.config.MaxGoroutines {
		metrics.IncrementCapacityRejection("goroutine_limit")
		rg.logger.Debug().
			Int("current_goroutines", currentGoros).
			Int("max_goroutines", rg.config.MaxGoroutines).
			Msg("Connection rejected: goroutine limit exceeded")
		return false, fmt.Sprintf("goroutine limit exceeded (%d > %d)", currentGoros, rg.config.MaxGoroutines)
	}

	rg.logger.Debug().
		Int64("current_conns", currentConns).
		Float64("cpu", currentCPU).
		Int64("memory_mb", currentMemory/(1024*1024)).
		Int("goroutines", currentGoros).
		Msg("Connection accepted")

	return true, "OK"
}

// ShouldPauseKafka checks if Kafka consumption should be paused
//
// This provides backpressure when CPU is critically high.
// Consumer will pause partition consumption temporarily.
//
// Uses hysteresis to prevent rapid pause/resume oscillation:
//   - Pause when CPU > CPUPauseThreshold (upper, default 80%)
//   - Resume when CPU < CPUPauseThresholdLower (lower, default 70%)
func (rg *ResourceGuard) ShouldPauseKafka() bool {
	// Use EWMA-smoothed CPU to prevent transient spikes from triggering false pauses
	currentCPU := rg.systemMonitor.GetSmoothedCPUPercent()
	currentlyPausing := rg.isPausingKafka.Load()

	if currentlyPausing {
		// Currently pausing - check if we should resume (CPU below lower threshold)
		if currentCPU < rg.config.CPUPauseThresholdLower {
			rg.isPausingKafka.Store(false)
			rg.logger.Info().
				Float64("cpu", currentCPU).
				Float64("lower_threshold", rg.config.CPUPauseThresholdLower).
				Msg("CPU hysteresis: exiting Kafka pause state (CPU dropped below lower threshold)")
			return false
		}
		// Still pausing (CPU in deadband or still high)
		return true
	}

	// Currently running - check if we should pause (CPU above upper threshold)
	if currentCPU > rg.config.CPUPauseThreshold {
		rg.isPausingKafka.Store(true)
		rg.logger.Info().
			Float64("cpu", currentCPU).
			Float64("upper_threshold", rg.config.CPUPauseThreshold).
			Msg("CPU hysteresis: entering Kafka pause state (CPU exceeded upper threshold)")
		return true
	}
	// Still running (CPU below upper threshold)
	return false
}

// AllowKafkaMessage checks if a Kafka message should be processed (rate limiting)
//
// This prevents Kafka from flooding the server with more work than it can handle.
//
// Returns:
//   - allow: true if message should be processed
//   - waitDuration: how long caller should wait before retrying (if blocked)
func (rg *ResourceGuard) AllowKafkaMessage(_ context.Context) (allow bool, waitDuration time.Duration) {
	// Non-blocking check
	reservation := rg.kafkaLimiter.Reserve()
	if !reservation.OK() {
		// Rate limit exceeded
		rg.logger.Warn().Msg("Kafka rate limit exceeded")
		return false, 0
	}

	delay := reservation.Delay()
	if delay == 0 {
		// Allowed immediately
		return true, 0
	}

	// Would need to wait
	reservation.Cancel() // Don't consume token
	return false, delay
}

// AllowBroadcast checks if a broadcast should be processed (rate limiting)
func (rg *ResourceGuard) AllowBroadcast() bool {
	return rg.broadcastLimiter.Allow()
}

// AcquireGoroutine attempts to acquire permission to start a new goroutine
//
// Returns true if allowed, false if at limit.
// MUST call ReleaseGoroutine() when goroutine completes.
//
// Example:
//
//	if rg.AcquireGoroutine() {
//	    go func() {
//	        defer rg.ReleaseGoroutine()
//	        // ... work ...
//	    }()
//	}
func (rg *ResourceGuard) AcquireGoroutine() bool {
	acquired := rg.goroutineLimiter.Acquire()
	if !acquired {
		rg.logger.Warn().
			Int("current", rg.goroutineLimiter.Current()).
			Int("max", rg.goroutineLimiter.Max()).
			Msg("Goroutine limit reached")
	}
	return acquired
}

// ReleaseGoroutine releases a goroutine slot
func (rg *ResourceGuard) ReleaseGoroutine() {
	rg.goroutineLimiter.Release()
}

// UpdateResources updates server stats with current resource metrics from SystemMonitor.
//
// This method no longer performs measurements - it just copies metrics from the
// global SystemMonitor singleton to the server's stats structure for health endpoints.
// The SystemMonitor handles all actual measurement work.
func (rg *ResourceGuard) UpdateResources(serverStats *stats.Stats) {
	// Get metrics from SystemMonitor (single source of truth)
	sysMetrics := rg.systemMonitor.GetMetrics()

	// Copy to server stats (for health endpoint)
	if serverStats != nil {
		serverStats.SetResourceMetrics(sysMetrics.CPUPercent, sysMetrics.MemoryMB)
	}

	rg.logger.Debug().
		Float64("cpu_percent", sysMetrics.CPUPercent).
		Float64("memory_mb", sysMetrics.MemoryMB).
		Int64("connections", rg.currentConns.Load()).
		Int("goroutines", sysMetrics.Goroutines).
		Msg("Server stats updated from SystemMonitor")
}

// StartMonitoring begins periodic stats synchronization from SystemMonitor.
//
// This method no longer performs measurements - it just periodically copies metrics
// from SystemMonitor to server stats and updates Prometheus metrics.
// The SystemMonitor singleton handles all actual CPU/memory measurements.
//
// The wg parameter tracks this goroutine's lifecycle for clean shutdown.
func (rg *ResourceGuard) StartMonitoring(ctx context.Context, wg *sync.WaitGroup, interval time.Duration, serverStats *stats.Stats) {
	ticker := time.NewTicker(interval)

	wg.Go(func() {
		defer logging.RecoverPanic(rg.logger, "ResourceGuard.StartMonitoring", nil)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Copy metrics from SystemMonitor to server stats
				rg.UpdateResources(serverStats)

				// Get metrics for Prometheus updates
				sysMetrics := rg.systemMonitor.GetMetrics()

				// Update Prometheus capacity metrics
				cpuHeadroom := 100.0 - sysMetrics.CPUPercent
				memPercent := 0.0
				if rg.config.MemoryLimit > 0 {
					memPercent = (float64(sysMetrics.MemoryBytes) / float64(rg.config.MemoryLimit)) * 100
				}
				memHeadroom := 100.0 - memPercent

				metrics.UpdateCapacityHeadroom(cpuHeadroom, memHeadroom)
				metrics.UpdateCapacityMetrics(rg.config.MaxConnections, rg.config.CPURejectThreshold)

			case <-ctx.Done():
				rg.logger.Info().Msg("ResourceGuard stats sync stopped")
				return
			}
		}
	})

	rg.logger.Info().
		Dur("interval", interval).
		Msg("ResourceGuard stats sync started (metrics from SystemMonitor)")
}

// GetStats returns current resource statistics for debugging
func (rg *ResourceGuard) GetStats() map[string]any {
	sysMetrics := rg.systemMonitor.GetMetrics()

	return map[string]any{
		"max_connections":            rg.config.MaxConnections,
		"current_connections":        rg.currentConns.Load(),
		"cpu_percent":                sysMetrics.CPUPercent,
		"cpu_reject_threshold":       rg.config.CPURejectThreshold,
		"cpu_reject_threshold_lower": rg.config.CPURejectThresholdLower,
		"cpu_rejecting":              rg.isRejectingCPU.Load(), // current hysteresis state
		"cpu_pause_threshold":        rg.config.CPUPauseThreshold,
		"cpu_pause_threshold_lower":  rg.config.CPUPauseThresholdLower,
		"cpu_pausing_kafka":          rg.isPausingKafka.Load(), // current hysteresis state
		"memory_bytes":               sysMetrics.MemoryBytes,
		"memory_limit_bytes":         rg.config.MemoryLimit,
		"goroutines_current":         sysMetrics.Goroutines,
		"goroutines_limit":           rg.config.MaxGoroutines,
		"kafka_rate_limit":           rg.config.MaxKafkaMessagesPerSec,
		"broadcast_rate_limit":       rg.config.MaxBroadcastsPerSec,
	}
}
