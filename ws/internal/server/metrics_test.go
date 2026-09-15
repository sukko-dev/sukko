package server

import (
	"sync"
	"testing"

	"github.com/sukko-dev/sukko/internal/server/stats"
)

// =============================================================================
// Memory Calculation Tests
// =============================================================================

func TestMemoryBytesToMB_Conversion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		bytes    uint64
		expected float64
	}{
		{
			name:     "0 bytes",
			bytes:    0,
			expected: 0,
		},
		{
			name:     "1 MB",
			bytes:    1024 * 1024,
			expected: 1.0,
		},
		{
			name:     "256 MB",
			bytes:    256 * 1024 * 1024,
			expected: 256.0,
		},
		{
			name:     "512 MB",
			bytes:    512 * 1024 * 1024,
			expected: 512.0,
		},
		{
			name:     "1 GB",
			bytes:    1024 * 1024 * 1024,
			expected: 1024.0,
		},
		{
			name:     "2 GB",
			bytes:    2 * 1024 * 1024 * 1024,
			expected: 2048.0,
		},
		{
			name:     "fractional MB",
			bytes:    1536 * 1024, // 1.5 MB
			expected: 1.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := float64(tt.bytes) / 1024 / 1024

			if result != tt.expected {
				t.Errorf("MemoryBytesToMB: got %f, want %f", result, tt.expected)
			}
		})
	}
}

// =============================================================================
// Memory Percentage Calculation Tests
// =============================================================================

func TestMemoryPercentage_Calculation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		usedMB      float64
		limitMB     float64
		expected    float64
		description string
	}{
		{
			name:        "empty usage",
			usedMB:      0,
			limitMB:     1024,
			expected:    0,
			description: "0 / 1024 = 0%",
		},
		{
			name:        "50% usage",
			usedMB:      512,
			limitMB:     1024,
			expected:    50,
			description: "512 / 1024 = 50%",
		},
		{
			name:        "80% threshold",
			usedMB:      820,
			limitMB:     1024,
			expected:    80.078125,
			description: "820 / 1024 ≈ 80%",
		},
		{
			name:        "90% threshold",
			usedMB:      922,
			limitMB:     1024,
			expected:    90.0390625,
			description: "922 / 1024 ≈ 90%",
		},
		{
			name:        "100% usage",
			usedMB:      1024,
			limitMB:     1024,
			expected:    100,
			description: "1024 / 1024 = 100%",
		},
		{
			name:        "over limit",
			usedMB:      1100,
			limitMB:     1024,
			expected:    107.421875,
			description: "1100 / 1024 > 100%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			percent := (tt.usedMB / tt.limitMB) * 100

			if percent != tt.expected {
				t.Errorf("MemoryPercent: got %f, want %f (%s)", percent, tt.expected, tt.description)
			}
		})
	}
}

// =============================================================================
// Memory Threshold Tests
// =============================================================================

func TestMemoryThreshold_Critical(t *testing.T) {
	t.Parallel()
	limitMB := 1024.0

	tests := []struct {
		name       string
		usedMB     float64
		isCritical bool
	}{
		{"83%_not_critical", 850, false},
		{"88%_not_critical", 900, false},
		{"90%_not_critical", 920, false},
		{"90.04%_critical", 922, true},
		{"93%_critical", 950, true},
		{"98%_critical", 1000, true},
		{"100%_critical", 1024, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			percent := (tt.usedMB / limitMB) * 100
			isCritical := percent > 90

			if isCritical != tt.isCritical {
				t.Errorf("usedMB=%f: isCritical got %v, want %v (percent=%.2f)", tt.usedMB, isCritical, tt.isCritical, percent)
			}
		})
	}
}

func TestMemoryThreshold_Warning(t *testing.T) {
	t.Parallel()
	limitMB := 1024.0

	tests := []struct {
		name      string
		usedMB    float64
		isWarning bool
	}{
		{"68%_not_warning", 700, false},
		{"78%_not_warning", 800, false},
		{"80%_warning", 820, true},
		{"83%_warning", 850, true},
		{"88%_warning", 900, true},
		{"90%_warning", 920, true},
		{"90.04%_critical_not_warning", 922, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			percent := (tt.usedMB / limitMB) * 100
			isWarning := percent > 80 && percent <= 90

			if isWarning != tt.isWarning {
				t.Errorf("usedMB=%f: isWarning got %v, want %v (percent=%.2f)", tt.usedMB, isWarning, tt.isWarning, percent)
			}
		})
	}
}

// =============================================================================
// Buffer Sampling Logic Tests
// =============================================================================

func TestBufferSaturation_Calculation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		bufferLen   int
		bufferCap   int
		expected    float64
		isHighUsage bool // >= 90%
	}{
		{
			name:        "empty",
			bufferLen:   0,
			bufferCap:   256,
			expected:    0,
			isHighUsage: false,
		},
		{
			name:        "quarter",
			bufferLen:   64,
			bufferCap:   256,
			expected:    25,
			isHighUsage: false,
		},
		{
			name:        "half",
			bufferLen:   128,
			bufferCap:   256,
			expected:    50,
			isHighUsage: false,
		},
		{
			name:        "89%",
			bufferLen:   228,
			bufferCap:   256,
			expected:    89.0625,
			isHighUsage: false,
		},
		{
			name:        "90%",
			bufferLen:   230,
			bufferCap:   256,
			expected:    89.84375,
			isHighUsage: false,
		},
		{
			name:        "91%",
			bufferLen:   233,
			bufferCap:   256,
			expected:    91.015625,
			isHighUsage: true,
		},
		{
			name:        "full",
			bufferLen:   256,
			bufferCap:   256,
			expected:    100,
			isHighUsage: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			usagePercent := float64(tt.bufferLen) / float64(tt.bufferCap) * 100
			isHighUsage := usagePercent >= 90

			if usagePercent != tt.expected {
				t.Errorf("usagePercent: got %f, want %f", usagePercent, tt.expected)
			}
			if isHighUsage != tt.isHighUsage {
				t.Errorf("isHighUsage: got %v, want %v", isHighUsage, tt.isHighUsage)
			}
		})
	}
}

// =============================================================================
// High Saturation Percentage Tests
// =============================================================================

func TestHighSaturationPercentage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                string
		highSaturationCount int
		totalSampled        int
		isWarningLevel      bool // >= 25%
	}{
		{
			name:                "no high saturation",
			highSaturationCount: 0,
			totalSampled:        100,
			isWarningLevel:      false,
		},
		{
			name:                "10% high saturation",
			highSaturationCount: 10,
			totalSampled:        100,
			isWarningLevel:      false,
		},
		{
			name:                "24% high saturation",
			highSaturationCount: 24,
			totalSampled:        100,
			isWarningLevel:      false,
		},
		{
			name:                "25% high saturation",
			highSaturationCount: 25,
			totalSampled:        100,
			isWarningLevel:      true,
		},
		{
			name:                "50% high saturation",
			highSaturationCount: 50,
			totalSampled:        100,
			isWarningLevel:      true,
		},
		{
			name:                "all high saturation",
			highSaturationCount: 100,
			totalSampled:        100,
			isWarningLevel:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.totalSampled == 0 {
				return // Skip division by zero
			}

			highSaturationPercent := float64(tt.highSaturationCount) / float64(tt.totalSampled) * 100
			isWarningLevel := highSaturationPercent >= 25

			if isWarningLevel != tt.isWarningLevel {
				t.Errorf("isWarningLevel: got %v, want %v (percent=%.2f)", isWarningLevel, tt.isWarningLevel, highSaturationPercent)
			}
		})
	}
}

// =============================================================================
// Stats Memory Update Tests
// =============================================================================

func TestStats_MemoryUpdate_ThreadSafe(t *testing.T) {
	t.Parallel()
	s := stats.NewStats()

	var wg sync.WaitGroup

	// Simulate concurrent memory updates
	for i := range 100 {
		wg.Go(func() {
			s.SetResourceMetrics(0, float64(i))
		})
	}

	wg.Wait()

	// Verify we can read the value
	_, mem := s.ResourceMetrics()

	// Value should be between 0 and 99
	if mem < 0 || mem > 99 {
		t.Errorf("MemoryMB: got %f, expected value between 0 and 99", mem)
	}
}

// =============================================================================
// Stats Connection Count Tests
// =============================================================================

func TestStats_CurrentConnections_Atomic(t *testing.T) {
	t.Parallel()
	s := stats.NewStats()

	var wg sync.WaitGroup

	// 100 increments
	for range 100 {
		wg.Go(func() {
			s.CurrentConnections.Add(1)
		})
	}

	// 50 decrements
	for range 50 {
		wg.Go(func() {
			s.CurrentConnections.Add(-1)
		})
	}

	wg.Wait()

	expected := int64(50)
	if s.CurrentConnections.Load() != expected {
		t.Errorf("CurrentConnections: got %d, want %d", s.CurrentConnections.Load(), expected)
	}
}

// =============================================================================
// Sample Limit Tests
// =============================================================================

func TestSampleLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		maxSamples     int
		totalAvailable int
		expected       int
	}{
		{"capped_at_max", 100, 1000, 100},
		{"fewer_than_max", 100, 50, 50},
		{"exact_match", 100, 100, 100},
		{"zero_available", 100, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			samplesCollected := 0

			for range tt.totalAvailable {
				if samplesCollected >= tt.maxSamples {
					break
				}
				samplesCollected++
			}

			if samplesCollected != tt.expected {
				t.Errorf("samplesCollected: got %d, want %d", samplesCollected, tt.expected)
			}
		})
	}
}

// =============================================================================
// Config Memory Limit Tests
// =============================================================================

func TestConfigMemoryLimit_BytesToMB(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		limitBytes int64
		expectedMB float64
	}{
		{
			name:       "256MB container",
			limitBytes: 256 * 1024 * 1024,
			expectedMB: 256.0,
		},
		{
			name:       "512MB container",
			limitBytes: 512 * 1024 * 1024,
			expectedMB: 512.0,
		},
		{
			name:       "1GB container",
			limitBytes: 1024 * 1024 * 1024,
			expectedMB: 1024.0,
		},
		{
			name:       "2GB container",
			limitBytes: 2 * 1024 * 1024 * 1024,
			expectedMB: 2048.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			memLimitMB := float64(tt.limitBytes) / 1024.0 / 1024.0

			if memLimitMB != tt.expectedMB {
				t.Errorf("memLimitMB: got %f, want %f", memLimitMB, tt.expectedMB)
			}
		})
	}
}

// =============================================================================
// Client Buffer Range Iteration Tests
// =============================================================================

func TestClientBufferIteration_WithSyncMap(t *testing.T) {
	t.Parallel()
	var clients sync.Map

	// Add 10 mock clients with buffers
	for i := range 10 {
		client := &Client{
			id:   int64(i),
			send: make(chan OutgoingMsg, 256),
		}
		// Fill buffer to varying levels
		for range i * 25 {
			client.send <- RawMsg([]byte("msg"))
		}
		clients.Store(client, true)
	}

	// Iterate and count samples
	samplesCollected := 0
	highSaturation := 0

	clients.Range(func(key, _ any) bool {
		client, ok := key.(*Client)
		if !ok {
			return true
		}

		bufferLen := len(client.send)
		bufferCap := cap(client.send)
		usagePercent := float64(bufferLen) / float64(bufferCap) * 100

		if usagePercent >= 90 {
			highSaturation++
		}

		samplesCollected++
		return true
	})

	if samplesCollected != 10 {
		t.Errorf("samplesCollected: got %d, want 10", samplesCollected)
	}

	// Client 10 has 225/256 = 87.9%, so no high saturation
	if highSaturation != 0 {
		t.Errorf("highSaturation: got %d, want 0", highSaturation)
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkMemoryBytesToMB(b *testing.B) {
	bytes := uint64(1536 * 1024 * 1024) // 1.5 GB

	for b.Loop() {
		_ = float64(bytes) / 1024 / 1024
	}
}

func BenchmarkMemoryPercentage(b *testing.B) {
	usedMB := 850.0
	limitMB := 1024.0

	for b.Loop() {
		_ = (usedMB / limitMB) * 100
	}
}

func BenchmarkBufferUsagePercent(b *testing.B) {
	bufferLen := 200
	bufferCap := 256

	for b.Loop() {
		_ = float64(bufferLen) / float64(bufferCap) * 100
	}
}

func BenchmarkStatsMemoryUpdate(b *testing.B) {
	s := stats.NewStats()

	for i := 0; b.Loop(); i++ {
		s.SetResourceMetrics(0, float64(i))
	}
}

func BenchmarkStatsConnectionsAtomic(b *testing.B) {
	s := stats.NewStats()

	for b.Loop() {
		s.CurrentConnections.Add(1)
		s.CurrentConnections.Add(-1)
	}
}
