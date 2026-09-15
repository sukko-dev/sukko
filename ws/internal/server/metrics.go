package server

import (
	"fmt"
	"os"
	"time"

	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"

	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// Internal monitoring and metric collection
func (s *Server) collectMetrics() {
	defer logging.RecoverPanic(s.logger, "collectMetrics", nil)

	ticker := time.NewTicker(s.config.MetricsCollectInterval)
	defer ticker.Stop()

	// Get current process for memory stats
	// NOTE: CPU is now measured by ResourceGuard via container-aware CPUMonitor
	// to provide single source of truth and avoid divergence
	proc, err := process.NewProcess(int32(os.Getpid())) //nolint:gosec // PID always fits in int32 on all supported platforms
	if err != nil {
		s.logger.Error().
			Err(err).
			Msg("Failed to get process info")
		proc = nil
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// CPU measurement REMOVED - now handled by ResourceGuard.UpdateResources()
			// This eliminates dual measurement paths and prevents divergence

			// Get memory usage
			if proc != nil {
				memInfo, err := proc.MemoryInfo()
				if err == nil {
					memMB := float64(memInfo.RSS) / 1024 / 1024
					cpuPercent, _ := s.stats.ResourceMetrics()
					s.stats.SetResourceMetrics(cpuPercent, memMB)
				}
			} else {
				// Fallback to system memory
				vmem, err := mem.VirtualMemory()
				if err == nil {
					memMB := float64(vmem.Used) / 1024 / 1024
					cpuPercent, _ := s.stats.ResourceMetrics()
					s.stats.SetResourceMetrics(cpuPercent, memMB)
				}
			}
		}
	}
}

func (s *Server) monitorMemory() {
	defer logging.RecoverPanic(s.logger, "monitorMemory", nil)

	ticker := time.NewTicker(s.config.MemoryMonitorInterval)
	defer ticker.Stop()

	memLimitMB := float64(s.config.MemoryLimit) / 1024.0 / 1024.0 // Container memory limit from config

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			_, memUsedMB := s.stats.ResourceMetrics()

			memPercent := (memUsedMB / memLimitMB) * 100
			currentConns := s.stats.CurrentConnections.Load()

			// Critical threshold
			if memPercent > float64(s.config.MemoryCriticalPercent) {
				s.alertLogger.Critical("HighMemoryUsage", fmt.Sprintf("Memory usage above %d%%, OOM risk", s.config.MemoryCriticalPercent), map[string]any{
					"memory_used_mb":  memUsedMB,
					"memory_limit_mb": memLimitMB,
					"percentage":      memPercent,
					"connections":     currentConns,
				})
			} else if memPercent > float64(s.config.MemoryWarningPercent) {
				// Warning threshold
				s.alertLogger.Warning("ModerateMemoryUsage", fmt.Sprintf("Memory usage above %d%%", s.config.MemoryWarningPercent), map[string]any{
					"memory_used_mb":  memUsedMB,
					"memory_limit_mb": memLimitMB,
					"percentage":      memPercent,
					"connections":     currentConns,
				})
			}
		}
	}
}

// sampleClientBuffers periodically samples client send buffer usage for saturation metrics
// Samples a subset of clients to avoid expensive iteration over all connections
func (s *Server) sampleClientBuffers() {
	defer logging.RecoverPanic(s.logger, "sampleClientBuffers", nil)

	ticker := time.NewTicker(s.config.BufferSampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Sample up to 100 random clients to avoid iterating all connections
			// This gives statistically significant buffer saturation data without overhead
			samplesCollected := 0
			maxSamples := s.config.BufferMaxSamples
			highSaturationCount := 0 // Track clients near capacity

			s.clients.Range(func(key, _ any) bool {
				if samplesCollected >= maxSamples {
					return false // Stop iteration
				}

				client, ok := key.(*Client)
				if !ok {
					return true // Skip invalid entry
				}

				// Record buffer usage (both Prometheus and Stats)
				bufferLen := len(client.send)
				bufferCap := cap(client.send)
				usagePercent := float64(bufferLen) / float64(bufferCap) * 100
				metrics.RecordClientBufferSizeWithStats(s.stats, bufferLen, bufferCap, maxSamples)

				// Track high saturation clients
				if usagePercent >= float64(s.config.BufferHighSaturationPercent) {
					highSaturationCount++
				}

				samplesCollected++
				return true // Continue iteration
			})

			// Log warning if many clients are near buffer capacity
			if samplesCollected > 0 {
				highSaturationPercent := float64(highSaturationCount) / float64(samplesCollected) * 100
				if highSaturationPercent >= float64(s.config.BufferPopulationWarnPercent) {
					s.logger.Warn().
						Int("high_saturation_count", highSaturationCount).
						Int("total_sampled", samplesCollected).
						Float64("high_saturation_pct", highSaturationPercent).
						Int64("current_connections", s.stats.CurrentConnections.Load()).
						Msg("High buffer saturation detected across client population")

					s.alertLogger.Warning("HighBufferSaturation", "Many clients near buffer capacity", map[string]any{
						"high_saturation_count": highSaturationCount,
						"total_sampled":         samplesCollected,
						"saturation_percent":    highSaturationPercent,
						"current_connections":   s.stats.CurrentConnections.Load(),
					})
				}
			}
		}
	}
}
