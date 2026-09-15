package metrics

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// SystemMonitor is a singleton that centralizes system resource monitoring.
// It eliminates duplicate CPU/memory measurements across multiple ResourceGuards.
var (
	systemMonitorInstance *SystemMonitor
	systemMonitorOnce     sync.Once
)

// SystemMetrics holds current system resource measurements.
type SystemMetrics struct {
	CPUPercent       float64                // Current CPU usage percentage (container-aware)
	CPUSmoothed      float64                // EWMA-smoothed CPU percentage (for load-shedding decisions)
	MemoryBytes      int64                  // Current memory usage in bytes
	MemoryMB         float64                // Current memory usage in MB
	MemoryLimitBytes int64                  // Container memory limit in bytes (0 = unknown/unlimited)
	MemoryPercent    float64                // Memory usage as percentage of limit (0 if limit unknown)
	Goroutines       int                    // Current goroutine count
	CPUAllocation    float64                // CPU allocation (cores) from container limits
	ThrottleStats    platform.ThrottleStats // CPU throttling statistics
	Timestamp        time.Time              // When these metrics were captured
}

// SystemMonitor centralizes system resource monitoring.
type SystemMonitor struct {
	cpuMonitor *platform.CPUMonitor
	logger     zerolog.Logger

	// Current metrics (protected by mutex)
	mu      sync.RWMutex
	metrics SystemMetrics

	// Container memory limit (read once at startup, 0 = unknown)
	memoryLimitBytes int64

	// EWMA state
	ewmaBeta        float64 // Decay factor (0-1), higher = smoother
	ewmaInitialized bool    // Whether first sample has been received

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// GetSystemMonitor returns the singleton SystemMonitor instance.
// The initializing call (from main) MUST pass ewmaBeta from validated config.
// Subsequent calls (from shards, resource guards) omit ewmaBeta and get the
// existing instance — logger is ignored on subsequent calls.
func GetSystemMonitor(logger zerolog.Logger, ewmaBeta ...float64) *SystemMonitor {
	systemMonitorOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())

		var beta float64
		if len(ewmaBeta) > 0 {
			beta = ewmaBeta[0]
		}

		systemMonitorInstance = &SystemMonitor{
			cpuMonitor: platform.NewCPUMonitor(logger),
			logger:     logger.With().Str("component", "system_monitor").Logger(),
			ewmaBeta:   beta,
			ctx:        ctx,
			cancel:     cancel,
		}

		// Initialize metrics with zero values
		systemMonitorInstance.metrics = SystemMetrics{
			Timestamp: time.Now(),
		}

		// Set cgroup memory limit once at startup
		memLimit, err := platform.GetMemoryLimit()
		if err == nil && memLimit > 0 {
			memoryLimitBytes.Set(float64(memLimit))
			systemMonitorInstance.memoryLimitBytes = memLimit
		}

		logger.Info().
			Str("cpu_mode", systemMonitorInstance.cpuMonitor.Mode()).
			Float64("cpu_allocation", systemMonitorInstance.cpuMonitor.GetAllocation()).
			Float64("ewma_beta", beta).
			Msg("SystemMonitor singleton initialized")
	})

	return systemMonitorInstance
}

// StartMonitoring begins periodic system metric updates with two tickers:
// - cpuPollInterval: Fast CPU-only updates for protection decisions (default: 1s)
// - metricsInterval: Full metrics updates including memory, goroutines (default: 15s)
func (sm *SystemMonitor) StartMonitoring(metricsInterval, cpuPollInterval time.Duration) {
	sm.wg.Go(func() {
		defer logging.RecoverPanic(sm.logger, "SystemMonitor", nil)

		// Two tickers: fast for CPU, slow for full metrics
		cpuTicker := time.NewTicker(cpuPollInterval)
		metricsTicker := time.NewTicker(metricsInterval)
		defer cpuTicker.Stop()
		defer metricsTicker.Stop()

		sm.logger.Info().
			Dur("metrics_interval", metricsInterval).
			Dur("cpu_poll_interval", cpuPollInterval).
			Msg("SystemMonitor started with two-ticker approach")

		// Initial full update
		sm.updateMetrics()

		for {
			select {
			case <-cpuTicker.C:
				sm.updateCPUOnly()

			case <-metricsTicker.C:
				sm.updateMetrics()

			case <-sm.ctx.Done():
				sm.logger.Info().Msg("SystemMonitor stopped")
				return
			}
		}
	})
}

// updateCPUOnly performs a fast CPU-only measurement.
func (sm *SystemMonitor) updateCPUOnly() {
	cpuPercent, throttleStats, err := sm.cpuMonitor.GetPercent()
	if err != nil {
		// Graceful degradation: keep the previous CPU reading rather than
		// reporting zero, which would incorrectly release load-shedding.
		sm.logger.Warn().Err(err).Msg("Failed to update CPU, using previous value")
		sm.mu.RLock()
		cpuPercent = sm.metrics.CPUPercent
		sm.mu.RUnlock()
	}

	sm.mu.Lock()
	sm.metrics.CPUPercent = cpuPercent
	sm.metrics.CPUSmoothed = sm.computeEWMA(cpuPercent)
	sm.metrics.ThrottleStats = throttleStats
	sm.metrics.Timestamp = time.Now()
	smoothed := sm.metrics.CPUSmoothed
	sm.mu.Unlock()

	// Update Prometheus CPU metrics
	CPUUsagePercent.Set(cpuPercent)
	CPUContainerPercent.Set(cpuPercent)
	CPUSmoothedPercent.Set(smoothed)

	if throttleStats.NrThrottled > 0 {
		CPUThrottleEventsTotal.Add(float64(throttleStats.NrThrottled))
	}
	if throttleStats.ThrottledSec > 0 {
		CPUThrottledSecondsTotal.Add(throttleStats.ThrottledSec)
	}
}

// updateMetrics performs a full measurement of all system resources.
func (sm *SystemMonitor) updateMetrics() {
	cpuPercent, throttleStats, err := sm.cpuMonitor.GetPercent()
	if err != nil {
		sm.logger.Error().Err(err).Msg("Failed to get CPU usage")
		sm.mu.RLock()
		cpuPercent = sm.metrics.CPUPercent
		sm.mu.RUnlock()
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	goroutines := runtime.NumGoroutine()

	sm.mu.Lock()
	smoothed := sm.computeEWMA(cpuPercent)
	memBytes := int64(mem.Alloc) //nolint:gosec // Memory allocation fits in int64
	memMB := float64(mem.Alloc) / (1024 * 1024)
	var memPercent float64
	if sm.memoryLimitBytes > 0 {
		memPercent = float64(memBytes) / float64(sm.memoryLimitBytes) * 100
	}
	sm.metrics = SystemMetrics{
		CPUPercent:       cpuPercent,
		CPUSmoothed:      smoothed,
		MemoryBytes:      memBytes,
		MemoryMB:         memMB,
		MemoryLimitBytes: sm.memoryLimitBytes,
		MemoryPercent:    memPercent,
		Goroutines:       goroutines,
		CPUAllocation:    sm.cpuMonitor.GetAllocation(),
		ThrottleStats:    throttleStats,
		Timestamp:        time.Now(),
	}
	sm.mu.Unlock()

	// Update Prometheus metrics
	CPUUsagePercent.Set(cpuPercent)
	CPUContainerPercent.Set(cpuPercent)
	CPUSmoothedPercent.Set(smoothed)
	CPUAllocationCores.Set(sm.cpuMonitor.GetAllocation())
	memoryUsageBytes.Set(float64(mem.Alloc))
	goroutinesActive.Set(float64(goroutines))

	if hostCPU, err := sm.cpuMonitor.GetHostPercent(); err == nil {
		CPUHostPercent.Set(hostCPU)
	}

	if throttleStats.NrThrottled > 0 {
		CPUThrottleEventsTotal.Add(float64(throttleStats.NrThrottled))
	}
	if throttleStats.ThrottledSec > 0 {
		CPUThrottledSecondsTotal.Add(throttleStats.ThrottledSec)
	}

	sm.logger.Debug().
		Float64("cpu_percent", cpuPercent).
		Uint64("cpu_throttled_events", throttleStats.NrThrottled).
		Float64("cpu_throttled_sec", throttleStats.ThrottledSec).
		Float64("memory_mb", sm.metrics.MemoryMB).
		Int("goroutines", goroutines).
		Msg("System metrics updated")
}

// GetMetrics returns a copy of the current system metrics.
func (sm *SystemMonitor) GetMetrics() SystemMetrics {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.metrics
}

// GetCPUPercent returns the current raw (unsmoothed) CPU usage percentage.
func (sm *SystemMonitor) GetCPUPercent() float64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.metrics.CPUPercent
}

// GetSmoothedCPUPercent returns the EWMA-smoothed CPU percentage.
// This should be used for load-shedding decisions (ResourceGuard) to avoid
// false triggers from transient CPU spikes.
func (sm *SystemMonitor) GetSmoothedCPUPercent() float64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.metrics.CPUSmoothed
}

// computeEWMA calculates the EWMA-smoothed CPU value.
// Must be called with sm.mu write-locked.
func (sm *SystemMonitor) computeEWMA(rawCPU float64) float64 {
	if !sm.ewmaInitialized {
		sm.ewmaInitialized = true
		return rawCPU
	}
	return sm.metrics.CPUSmoothed*sm.ewmaBeta + rawCPU*(1-sm.ewmaBeta)
}

// GetMemoryBytes returns the current memory usage in bytes.
func (sm *SystemMonitor) GetMemoryBytes() int64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.metrics.MemoryBytes
}

// GetMemoryMB returns the current memory usage in megabytes.
func (sm *SystemMonitor) GetMemoryMB() float64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.metrics.MemoryMB
}

// GetGoroutines returns the current goroutine count.
func (sm *SystemMonitor) GetGoroutines() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.metrics.Goroutines
}

// GetCPUAllocation returns the CPU allocation (cores) from container limits.
func (sm *SystemMonitor) GetCPUAllocation() float64 {
	return sm.cpuMonitor.GetAllocation()
}

// Shutdown gracefully stops the SystemMonitor.
func (sm *SystemMonitor) Shutdown() {
	sm.logger.Info().Msg("Shutting down SystemMonitor")
	sm.cancel()
	sm.wg.Wait()
	sm.logger.Info().Msg("SystemMonitor shut down")
}
