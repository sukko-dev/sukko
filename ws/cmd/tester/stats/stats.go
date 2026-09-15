package stats

import (
	"math"
	"sort"
	"sync"
	"time"
)

// Histogram tracks latency measurements with thread-safe access.
type Histogram struct {
	mu      sync.RWMutex
	samples []float64 // milliseconds
}

// NewHistogram creates a Histogram with pre-allocated sample storage.
func NewHistogram() *Histogram {
	return &Histogram{
		samples: make([]float64, 0, 1024),
	}
}

// Record adds a latency sample to the histogram.
func (h *Histogram) Record(d time.Duration) {
	h.mu.Lock()
	h.samples = append(h.samples, float64(d.Microseconds())/1000.0) // ms
	h.mu.Unlock()
}

// Snapshot represents a point-in-time summary.
type Snapshot struct {
	Count int     `json:"count"`
	Min   float64 `json:"min_ms"`
	Max   float64 `json:"max_ms"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
}

// Snapshot computes and returns a statistical summary of all recorded samples.
func (h *Histogram) Snapshot() Snapshot {
	h.mu.RLock()
	copied := make([]float64, len(h.samples))
	copy(copied, h.samples)
	h.mu.RUnlock()

	if len(copied) == 0 {
		return Snapshot{}
	}

	sort.Float64s(copied)
	n := len(copied)

	sum := 0.0
	for _, v := range copied {
		sum += v
	}

	return Snapshot{
		Count: n,
		Min:   copied[0],
		Max:   copied[n-1],
		Mean:  sum / float64(n),
		P50:   percentile(copied, 0.50),
		P95:   percentile(copied, 0.95),
		P99:   percentile(copied, 0.99),
		P999:  percentile(copied, 0.999),
	}
}

// Reset clears all recorded samples.
func (h *Histogram) Reset() {
	h.mu.Lock()
	h.samples = h.samples[:0]
	h.mu.Unlock()
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := p * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}
	frac := rank - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}
