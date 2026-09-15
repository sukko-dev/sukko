package metrics

import (
	"math"
	"testing"
)

func newTestMonitor(beta float64) *SystemMonitor {
	return &SystemMonitor{
		ewmaBeta: beta,
	}
}

// simulateEWMA feeds a sequence of raw CPU samples through computeEWMA
// and returns the smoothed values. The monitor's CPUSmoothed field is
// updated after each call to mirror how updateCPUOnly/updateMetrics work.
func simulateEWMA(sm *SystemMonitor, samples []float64) []float64 {
	results := make([]float64, len(samples))
	for i, raw := range samples {
		smoothed := sm.computeEWMA(raw)
		sm.metrics.CPUSmoothed = smoothed
		results[i] = smoothed
	}
	return results
}

func TestComputeEWMA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		beta    float64
		samples []float64
		check   func(t *testing.T, results []float64)
	}{
		{
			name:    "first sample seeds directly",
			beta:    0.8,
			samples: []float64{42.0},
			check: func(t *testing.T, results []float64) {
				if results[0] != 42.0 {
					t.Errorf("first sample: got %.2f, want 42.00", results[0])
				}
			},
		},
		{
			name:    "second sample applies EWMA formula",
			beta:    0.8,
			samples: []float64{50.0, 100.0},
			check: func(t *testing.T, results []float64) {
				// smoothed = 50*0.8 + 100*0.2 = 40 + 20 = 60
				want := 60.0
				if math.Abs(results[1]-want) > 0.001 {
					t.Errorf("second sample: got %.4f, want %.4f", results[1], want)
				}
			},
		},
		{
			name:    "spike dampening with beta 0.8",
			beta:    0.8,
			samples: []float64{10.0, 100.0, 10.0, 10.0, 10.0},
			check: func(t *testing.T, results []float64) {
				// After spike at sample[1], smoothed should be much less than 100
				if results[1] >= 100.0 {
					t.Error("spike not dampened: smoothed >= 100")
				}
				// After spike returns to 10, smoothed should decay toward 10
				for i := 2; i < len(results); i++ {
					if results[i] >= results[i-1] {
						t.Errorf("sample[%d]=%.2f should be less than sample[%d]=%.2f (decaying toward baseline)",
							i, results[i], i-1, results[i-1])
					}
				}
			},
		},
		{
			name:    "steady state converges to constant input",
			beta:    0.8,
			samples: []float64{50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0},
			check: func(t *testing.T, results []float64) {
				for i, r := range results {
					if math.Abs(r-50.0) > 0.001 {
						t.Errorf("sample[%d]: got %.4f, want 50.0000 (constant input should converge)", i, r)
					}
				}
			},
		},
		{
			name:    "low beta tracks raw closely",
			beta:    0.1,
			samples: []float64{10.0, 100.0},
			check: func(t *testing.T, results []float64) {
				// smoothed = 10*0.1 + 100*0.9 = 1 + 90 = 91
				want := 91.0
				if math.Abs(results[1]-want) > 0.001 {
					t.Errorf("low beta: got %.4f, want %.4f", results[1], want)
				}
			},
		},
		{
			name:    "high beta smooths aggressively",
			beta:    0.95,
			samples: []float64{10.0, 100.0},
			check: func(t *testing.T, results []float64) {
				// smoothed = 10*0.95 + 100*0.05 = 9.5 + 5 = 14.5
				want := 14.5
				if math.Abs(results[1]-want) > 0.001 {
					t.Errorf("high beta: got %.4f, want %.4f", results[1], want)
				}
			},
		},
		{
			name:    "zero CPU stays zero",
			beta:    0.8,
			samples: []float64{0.0, 0.0, 0.0},
			check: func(t *testing.T, results []float64) {
				for i, r := range results {
					if r != 0.0 {
						t.Errorf("sample[%d]: got %.4f, want 0.0", i, r)
					}
				}
			},
		},
		{
			name:    "max CPU 100 percent",
			beta:    0.8,
			samples: []float64{100.0, 100.0, 100.0},
			check: func(t *testing.T, results []float64) {
				for i, r := range results {
					if math.Abs(r-100.0) > 0.001 {
						t.Errorf("sample[%d]: got %.4f, want 100.0 (sustained max should stay at max)", i, r)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sm := newTestMonitor(tt.beta)
			results := simulateEWMA(sm, tt.samples)
			tt.check(t, results)
		})
	}
}

func TestGetSmoothedCPUPercent(t *testing.T) {
	t.Parallel()
	sm := newTestMonitor(0.8)
	sm.metrics.CPUSmoothed = 42.5

	got := sm.GetSmoothedCPUPercent()
	if got != 42.5 {
		t.Errorf("GetSmoothedCPUPercent: got %.2f, want 42.50", got)
	}
}
