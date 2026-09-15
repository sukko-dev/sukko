package stats

import (
	"sync"
	"testing"
	"time"
)

func TestStats_ZeroValues(t *testing.T) {
	t.Parallel()
	s := &Stats{}

	if s.TotalConnections.Load() != 0 {
		t.Errorf("TotalConnections: got %d, want 0", s.TotalConnections.Load())
	}
	if s.CurrentConnections.Load() != 0 {
		t.Errorf("CurrentConnections: got %d, want 0", s.CurrentConnections.Load())
	}
	if s.MessagesSent.Load() != 0 {
		t.Errorf("MessagesSent: got %d, want 0", s.MessagesSent.Load())
	}
}

func TestNewStats_InitializesMaps(t *testing.T) {
	t.Parallel()
	s := NewStats()

	if s.disconnectsByReason == nil {
		t.Error("disconnectsByReason should be initialized")
	}
	if s.droppedBroadcastsByChannel == nil {
		t.Error("droppedBroadcastsByChannel should be initialized")
	}
	if s.bufferSaturationSamples == nil {
		t.Error("bufferSaturationSamples should be initialized")
	}
	if s.StartTime.IsZero() {
		t.Error("StartTime should be set")
	}
}

func TestStats_ResourceMetrics(t *testing.T) {
	t.Parallel()
	s := NewStats()

	s.SetResourceMetrics(45.5, 256.0)

	cpu, mem := s.ResourceMetrics()
	if cpu != 45.5 {
		t.Errorf("CPUPercent: got %f, want 45.5", cpu)
	}
	if mem != 256.0 {
		t.Errorf("MemoryMB: got %f, want 256.0", mem)
	}
}

func TestStats_Fields(t *testing.T) {
	t.Parallel()
	now := time.Now()
	s := NewStats()
	s.StartTime = now
	s.SetResourceMetrics(45.5, 256.0)
	s.TotalConnections.Store(1000)
	s.CurrentConnections.Store(500)
	s.MessagesSent.Store(10000)
	s.MessagesReceived.Store(5000)
	s.BytesSent.Store(1024 * 1024)
	s.BytesReceived.Store(512 * 1024)
	s.SlowClientsDisconnected.Store(10)
	s.RateLimitedMessages.Store(50)
	s.MessageReplayRequests.Store(5)

	if s.TotalConnections.Load() != 1000 {
		t.Errorf("TotalConnections: got %d, want 1000", s.TotalConnections.Load())
	}
	if s.CurrentConnections.Load() != 500 {
		t.Errorf("CurrentConnections: got %d, want 500", s.CurrentConnections.Load())
	}
	if !s.StartTime.Equal(now) {
		t.Errorf("StartTime mismatch")
	}
	cpu, _ := s.ResourceMetrics()
	if cpu != 45.5 {
		t.Errorf("CPUPercent: got %f, want 45.5", cpu)
	}
}

func TestStats_ConcurrentMapAccess(t *testing.T) {
	t.Parallel()
	s := NewStats()

	var wg sync.WaitGroup

	// Concurrent writes to DisconnectsByReason via accessor
	for i := range 10 {
		wg.Go(func() {
			for range 100 {
				reason := "reason" + string(rune('0'+(i%10)))
				s.RecordDisconnect(reason)
			}
		})
	}

	// Concurrent writes to DroppedBroadcastsByChannel via accessor
	for i := range 10 {
		wg.Go(func() {
			for range 100 {
				channel := "channel" + string(rune('0'+(i%10)))
				s.RecordDroppedBroadcast(channel)
			}
		})
	}

	// Concurrent writes to BufferSaturationSamples via accessor
	for i := range 10 {
		wg.Go(func() {
			for j := range 10 {
				s.AddBufferSample(i*j, 100)
			}
		})
	}

	wg.Wait()

	// Verify data was written correctly via accessors
	disconnects, _ := s.GetDisconnectsByReason()
	if len(disconnects) == 0 {
		t.Error("DisconnectsByReason should have entries")
	}

	drops, _ := s.GetDroppedBroadcastsByChannel()
	if len(drops) == 0 {
		t.Error("DroppedBroadcastsByChannel should have entries")
	}
}

func TestStats_DisconnectReasonTracking(t *testing.T) {
	t.Parallel()
	s := NewStats()

	// Simulate tracking different disconnect reasons
	reasons := []string{"read_error", "write_timeout", "slow_client", "idle_timeout"}

	for i, reason := range reasons {
		// Use direct access (same package) to set specific counts
		s.disconnectsMu.Lock()
		s.disconnectsByReason[reason] = int64(i + 1)
		s.disconnectsMu.Unlock()
	}

	disconnects, _ := s.GetDisconnectsByReason()
	if len(disconnects) != 4 {
		t.Errorf("DisconnectsByReason count: got %d, want 4", len(disconnects))
	}
	if disconnects["read_error"] != 1 {
		t.Errorf("read_error count: got %d, want 1", disconnects["read_error"])
	}
}

func TestStats_BufferSaturationSampling(t *testing.T) {
	t.Parallel()
	s := NewStats()

	// Add samples via accessor
	for i := range 100 {
		s.AddBufferSample(i, 100)
	}

	samples := s.GetBufferSaturationSamples()
	if len(samples) != 100 {
		t.Errorf("BufferSaturationSamples count: got %d, want 100", len(samples))
	}
}
