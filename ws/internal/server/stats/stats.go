// Package stats defines runtime statistics for the WebSocket server.
// Stats is a server-only data struct shared across server sub-packages
// (server, server/metrics, server/limits) to avoid import cycles.
package stats

import (
	"sync"
	"sync/atomic"
	"time"
)

// Stats tracks server statistics.
//
// Atomic fields are exported and inherently thread-safe.
// Mutex-protected fields are unexported and accessed via methods.
type Stats struct {
	TotalConnections   atomic.Int64
	CurrentConnections atomic.Int64
	MessagesSent       atomic.Int64
	MessagesReceived   atomic.Int64
	BytesSent          atomic.Int64
	BytesReceived      atomic.Int64

	// StartTime is set once at construction; immutable after initialization.
	StartTime time.Time

	// Message delivery reliability metrics
	SlowClientsDisconnected atomic.Int64 // Count of clients disconnected for being too slow
	RateLimitedMessages     atomic.Int64 // Count of messages dropped due to rate limiting
	MessageReplayRequests   atomic.Int64 // Count of replay requests served (gap recovery)

	// Phase 4 logging counters
	DroppedBroadcastLogCounter atomic.Int64 // Counter for sampled logging (every 100th drop)

	// mu protects cpuPercent and memoryMB
	mu         sync.RWMutex
	cpuPercent float64
	memoryMB   float64

	// disconnectsMu protects disconnectsByReason
	disconnectsMu       sync.RWMutex
	disconnectsByReason map[string]int64

	// dropsMu protects droppedBroadcastsByChannel
	dropsMu                    sync.RWMutex
	droppedBroadcastsByChannel map[string]int64

	// buffersMu protects bufferSaturationSamples
	buffersMu               sync.RWMutex
	bufferSaturationSamples []int
}

// NewStats creates a Stats instance with all maps initialized.
func NewStats() *Stats {
	return &Stats{
		StartTime:                  time.Now(),
		disconnectsByReason:        make(map[string]int64),
		droppedBroadcastsByChannel: make(map[string]int64),
		bufferSaturationSamples:    make([]int, 0, 100),
	}
}

// SetResourceMetrics updates the CPU and memory metrics.
func (s *Stats) SetResourceMetrics(cpuPercent, memoryMB float64) {
	s.mu.Lock()
	s.cpuPercent = cpuPercent
	s.memoryMB = memoryMB
	s.mu.Unlock()
}

// ResourceMetrics returns the current CPU and memory metrics.
func (s *Stats) ResourceMetrics() (cpuPercent, memoryMB float64) {
	s.mu.RLock()
	cpuPercent = s.cpuPercent
	memoryMB = s.memoryMB
	s.mu.RUnlock()
	return
}

// RecordDisconnect increments the disconnect counter for the given reason.
func (s *Stats) RecordDisconnect(reason string) {
	s.disconnectsMu.Lock()
	s.disconnectsByReason[reason]++
	s.disconnectsMu.Unlock()
}

// GetDisconnectsByReason returns a snapshot copy of disconnect counts by reason,
// along with the total count across all reasons.
func (s *Stats) GetDisconnectsByReason() (reasons map[string]int64, total int64) {
	s.disconnectsMu.RLock()
	defer s.disconnectsMu.RUnlock()
	reasons = make(map[string]int64, len(s.disconnectsByReason))
	for k, v := range s.disconnectsByReason {
		reasons[k] = v
		total += v
	}
	return
}

// RecordDroppedBroadcast increments the dropped broadcast counter for the given channel.
func (s *Stats) RecordDroppedBroadcast(channel string) {
	s.dropsMu.Lock()
	s.droppedBroadcastsByChannel[channel]++
	s.dropsMu.Unlock()
}

// GetDroppedBroadcastsByChannel returns a snapshot copy of dropped broadcast counts,
// along with the total count across all channels.
func (s *Stats) GetDroppedBroadcastsByChannel() (channels map[string]int64, total int64) {
	s.dropsMu.RLock()
	defer s.dropsMu.RUnlock()
	channels = make(map[string]int64, len(s.droppedBroadcastsByChannel))
	for k, v := range s.droppedBroadcastsByChannel {
		channels[k] = v
		total += v
	}
	return
}

// AddBufferSample records a buffer saturation sample, maintaining a sliding window of maxSamples.
func (s *Stats) AddBufferSample(usagePercent, maxSamples int) {
	s.buffersMu.Lock()
	s.bufferSaturationSamples = append(s.bufferSaturationSamples, usagePercent)
	if len(s.bufferSaturationSamples) > maxSamples {
		s.bufferSaturationSamples = s.bufferSaturationSamples[1:]
	}
	s.buffersMu.Unlock()
}

// GetBufferSaturationSamples returns a snapshot copy of buffer saturation samples.
func (s *Stats) GetBufferSaturationSamples() []int {
	s.buffersMu.RLock()
	defer s.buffersMu.RUnlock()
	result := make([]int, len(s.bufferSaturationSamples))
	copy(result, s.bufferSaturationSamples)
	return result
}
