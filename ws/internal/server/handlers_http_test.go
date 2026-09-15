package server

import (
	"testing"
)

// =============================================================================
// calculateBufferStats Tests
// =============================================================================

func TestCalculateBufferStats_Empty(t *testing.T) {
	t.Parallel()
	stats := calculateBufferStats([]int{})

	if stats["samples"].(int) != 0 {
		t.Error("samples should be 0 for empty input")
	}
	if stats["avg"].(int) != 0 {
		t.Error("avg should be 0 for empty input")
	}
	if stats["p50"].(int) != 0 {
		t.Error("p50 should be 0 for empty input")
	}
	if stats["p95"].(int) != 0 {
		t.Error("p95 should be 0 for empty input")
	}
	if stats["p99"].(int) != 0 {
		t.Error("p99 should be 0 for empty input")
	}
	if stats["max"].(int) != 0 {
		t.Error("max should be 0 for empty input")
	}
}

func TestCalculateBufferStats_SingleElement(t *testing.T) {
	t.Parallel()
	stats := calculateBufferStats([]int{50})

	if stats["samples"].(int) != 1 {
		t.Errorf("samples should be 1, got %d", stats["samples"])
	}
	if stats["avg"].(float64) != 50.0 {
		t.Errorf("avg should be 50.0, got %v", stats["avg"])
	}
	if stats["max"].(int) != 50 {
		t.Errorf("max should be 50, got %v", stats["max"])
	}
}

func TestCalculateBufferStats_Sorted(t *testing.T) {
	t.Parallel()
	// Already sorted input
	samples := []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	stats := calculateBufferStats(samples)

	if stats["samples"].(int) != 10 {
		t.Errorf("samples should be 10, got %d", stats["samples"])
	}

	// Average = (10+20+30+40+50+60+70+80+90+100) / 10 = 550 / 10 = 55
	expectedAvg := 55.0
	if stats["avg"].(float64) != expectedAvg {
		t.Errorf("avg should be %f, got %v", expectedAvg, stats["avg"])
	}

	if stats["max"].(int) != 100 {
		t.Errorf("max should be 100, got %v", stats["max"])
	}
}

func TestCalculateBufferStats_Unsorted(t *testing.T) {
	t.Parallel()
	// Unsorted input - should still work
	samples := []int{50, 10, 90, 30, 70, 20, 80, 40, 60, 100}
	stats := calculateBufferStats(samples)

	if stats["samples"].(int) != 10 {
		t.Errorf("samples should be 10, got %d", stats["samples"])
	}

	// Same average regardless of order
	expectedAvg := 55.0
	if stats["avg"].(float64) != expectedAvg {
		t.Errorf("avg should be %f, got %v", expectedAvg, stats["avg"])
	}

	if stats["max"].(int) != 100 {
		t.Errorf("max should be 100, got %v", stats["max"])
	}
}

func TestCalculateBufferStats_AllSame(t *testing.T) {
	t.Parallel()
	samples := []int{50, 50, 50, 50, 50}
	stats := calculateBufferStats(samples)

	if stats["avg"].(float64) != 50.0 {
		t.Errorf("avg should be 50.0, got %v", stats["avg"])
	}
	if stats["p50"].(int) != 50 {
		t.Errorf("p50 should be 50, got %v", stats["p50"])
	}
	if stats["p95"].(int) != 50 {
		t.Errorf("p95 should be 50, got %v", stats["p95"])
	}
	if stats["max"].(int) != 50 {
		t.Errorf("max should be 50, got %v", stats["max"])
	}
}

func TestCalculateBufferStats_Percentiles(t *testing.T) {
	t.Parallel()
	// Create 100 samples for cleaner percentile testing
	samples := make([]int, 100)
	for i := range 100 {
		samples[i] = i + 1 // 1, 2, 3, ..., 100
	}

	stats := calculateBufferStats(samples)

	// p50 should be around the 50th element
	p50 := stats["p50"].(int)
	if p50 < 45 || p50 > 55 {
		t.Errorf("p50 should be around 50, got %d", p50)
	}

	// p95 should be around the 95th element
	p95 := stats["p95"].(int)
	if p95 < 90 || p95 > 98 {
		t.Errorf("p95 should be around 95, got %d", p95)
	}

	// p99 should be around the 99th element
	p99 := stats["p99"].(int)
	if p99 < 95 || p99 > 100 {
		t.Errorf("p99 should be around 99, got %d", p99)
	}
}

func TestCalculateBufferStats_LargeValues(t *testing.T) {
	t.Parallel()
	samples := []int{1000000, 2000000, 3000000}
	stats := calculateBufferStats(samples)

	if stats["max"].(int) != 3000000 {
		t.Errorf("max should be 3000000, got %v", stats["max"])
	}

	expectedAvg := 2000000.0
	if stats["avg"].(float64) != expectedAvg {
		t.Errorf("avg should be %f, got %v", expectedAvg, stats["avg"])
	}
}

func TestCalculateBufferStats_WithZeros(t *testing.T) {
	t.Parallel()
	samples := []int{0, 0, 10, 20, 0}
	stats := calculateBufferStats(samples)

	if stats["samples"].(int) != 5 {
		t.Errorf("samples should be 5, got %d", stats["samples"])
	}

	// Average = (0+0+10+20+0) / 5 = 6
	expectedAvg := 6.0
	if stats["avg"].(float64) != expectedAvg {
		t.Errorf("avg should be %f, got %v", expectedAvg, stats["avg"])
	}

	if stats["max"].(int) != 20 {
		t.Errorf("max should be 20, got %v", stats["max"])
	}
}

func TestCalculateBufferStats_DoesNotModifyInput(t *testing.T) {
	t.Parallel()
	samples := []int{50, 10, 90, 30, 70}
	original := make([]int, len(samples))
	copy(original, samples)

	calculateBufferStats(samples)

	// Input should not be modified
	for i, v := range samples {
		if v != original[i] {
			t.Errorf("Input was modified at index %d: expected %d, got %d", i, original[i], v)
		}
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkCalculateBufferStats_Small(b *testing.B) {
	samples := []int{10, 20, 30, 40, 50}

	for b.Loop() {
		calculateBufferStats(samples)
	}
}

func BenchmarkCalculateBufferStats_Large(b *testing.B) {
	samples := make([]int, 100)
	for i := range 100 {
		samples[i] = i
	}

	for b.Loop() {
		calculateBufferStats(samples)
	}
}
