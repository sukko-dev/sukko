package stats

import (
	"math"
	"testing"
	"time"
)

func TestNewHistogram(t *testing.T) {
	t.Parallel()

	h := NewHistogram()
	if h == nil {
		t.Fatal("expected non-nil histogram")
	}
	snap := h.Snapshot()
	if snap.Count != 0 {
		t.Errorf("expected empty histogram count=0, got %d", snap.Count)
	}
}

func TestHistogram_Record(t *testing.T) {
	t.Parallel()

	h := NewHistogram()
	h.Record(10 * time.Millisecond)
	h.Record(20 * time.Millisecond)
	h.Record(30 * time.Millisecond)

	snap := h.Snapshot()
	if snap.Count != 3 {
		t.Errorf("expected count=3, got %d", snap.Count)
	}
	if snap.Min < 9 || snap.Min > 11 {
		t.Errorf("expected min ~10ms, got %.2f", snap.Min)
	}
	if snap.Max < 29 || snap.Max > 31 {
		t.Errorf("expected max ~30ms, got %.2f", snap.Max)
	}
}

func TestHistogram_Snapshot_Percentiles(t *testing.T) {
	t.Parallel()

	h := NewHistogram()
	// Record 100 values from 1ms to 100ms
	for i := 1; i <= 100; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}

	snap := h.Snapshot()
	if snap.Count != 100 {
		t.Fatalf("expected count=100, got %d", snap.Count)
	}

	// p50 should be ~50ms
	if math.Abs(snap.P50-50.0) > 1.5 {
		t.Errorf("expected p50 ~50ms, got %.2f", snap.P50)
	}
	// p95 should be ~95ms
	if math.Abs(snap.P95-95.0) > 1.5 {
		t.Errorf("expected p95 ~95ms, got %.2f", snap.P95)
	}
	// p99 should be ~99ms
	if math.Abs(snap.P99-99.0) > 1.5 {
		t.Errorf("expected p99 ~99ms, got %.2f", snap.P99)
	}
}

func TestHistogram_Reset(t *testing.T) {
	t.Parallel()

	h := NewHistogram()
	h.Record(10 * time.Millisecond)
	h.Record(20 * time.Millisecond)

	h.Reset()
	snap := h.Snapshot()
	if snap.Count != 0 {
		t.Errorf("expected count=0 after reset, got %d", snap.Count)
	}
}

func TestHistogram_SnapshotEmpty(t *testing.T) {
	t.Parallel()

	h := NewHistogram()
	snap := h.Snapshot()
	if snap.Count != 0 || snap.Min != 0 || snap.Max != 0 || snap.Mean != 0 {
		t.Errorf("expected all zeros for empty histogram, got %+v", snap)
	}
}

func TestHistogram_SnapshotSingleValue(t *testing.T) {
	t.Parallel()

	h := NewHistogram()
	h.Record(42 * time.Millisecond)

	snap := h.Snapshot()
	if snap.Count != 1 {
		t.Fatalf("expected count=1, got %d", snap.Count)
	}
	// All percentiles should be ~42ms for single value
	if math.Abs(snap.P50-42.0) > 0.5 {
		t.Errorf("expected p50 ~42ms, got %.2f", snap.P50)
	}
	if math.Abs(snap.P99-42.0) > 0.5 {
		t.Errorf("expected p99 ~42ms, got %.2f", snap.P99)
	}
}

func TestHistogram_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	h := NewHistogram()
	done := make(chan struct{})

	// Writer goroutine
	go func() {
		for i := range 1000 {
			h.Record(time.Duration(i) * time.Microsecond)
		}
		close(done)
	}()

	// Reader goroutine — take snapshots while writing
	for range 100 {
		_ = h.Snapshot()
	}

	<-done

	snap := h.Snapshot()
	if snap.Count != 1000 {
		t.Errorf("expected count=1000, got %d", snap.Count)
	}
}

func TestPercentile_BoundaryValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sorted []float64
		p      float64
		want   float64
	}{
		{"empty", nil, 0.5, 0},
		{"single", []float64{42.0}, 0.5, 42.0},
		{"two_p0", []float64{10.0, 20.0}, 0.0, 10.0},
		{"two_p100", []float64{10.0, 20.0}, 1.0, 20.0},
		{"two_p50", []float64{10.0, 20.0}, 0.5, 15.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := percentile(tt.sorted, tt.p)
			if math.Abs(got-tt.want) > 0.01 {
				t.Errorf("percentile(%v, %.2f) = %.2f, want %.2f", tt.sorted, tt.p, got, tt.want)
			}
		})
	}
}
