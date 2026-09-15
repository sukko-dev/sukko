package metrics

import (
	"testing"
)

func TestBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		buckets []float64
	}{
		{"ConnectionDurationBuckets", ConnectionDurationBuckets},
		{"AuthLatencyBuckets", AuthLatencyBuckets},
		{"BackendLatencyBuckets", BackendLatencyBuckets},
		{"APILatencyBuckets", APILatencyBuckets},
		{"BufferSaturationBuckets", BufferSaturationBuckets},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_not_empty", func(t *testing.T) {
			t.Parallel()
			if len(tt.buckets) == 0 {
				t.Errorf("%s should not be empty", tt.name)
			}
		})

		t.Run(tt.name+"_strictly_increasing", func(t *testing.T) {
			t.Parallel()
			for i := 1; i < len(tt.buckets); i++ {
				if tt.buckets[i] <= tt.buckets[i-1] {
					t.Errorf("%s: buckets not strictly increasing at index %d: %v <= %v",
						tt.name, i, tt.buckets[i], tt.buckets[i-1])
				}
			}
		})

		t.Run(tt.name+"_positive_values", func(t *testing.T) {
			t.Parallel()
			for i, v := range tt.buckets {
				if v < 0 {
					t.Errorf("%s: negative value at index %d: %v", tt.name, i, v)
				}
			}
		})
	}
}

func TestConnectionDurationBuckets_Range(t *testing.T) {
	t.Parallel()

	// Should start at 1 second
	if ConnectionDurationBuckets[0] != 1 {
		t.Errorf("ConnectionDurationBuckets should start at 1, got %v", ConnectionDurationBuckets[0])
	}

	// Should end at 1 hour (3600 seconds)
	last := ConnectionDurationBuckets[len(ConnectionDurationBuckets)-1]
	if last != 3600 {
		t.Errorf("ConnectionDurationBuckets should end at 3600, got %v", last)
	}
}

func TestAuthLatencyBuckets_Range(t *testing.T) {
	t.Parallel()

	// Should start at sub-millisecond (0.001 = 1ms)
	if AuthLatencyBuckets[0] != 0.001 {
		t.Errorf("AuthLatencyBuckets should start at 0.001, got %v", AuthLatencyBuckets[0])
	}

	// Should end at 250ms
	last := AuthLatencyBuckets[len(AuthLatencyBuckets)-1]
	if last != 0.25 {
		t.Errorf("AuthLatencyBuckets should end at 0.25, got %v", last)
	}
}

func TestBufferSaturationBuckets_Range(t *testing.T) {
	t.Parallel()

	// Should start at 0
	if BufferSaturationBuckets[0] != 0 {
		t.Errorf("BufferSaturationBuckets should start at 0, got %v", BufferSaturationBuckets[0])
	}

	// Should end at 512 (common buffer size)
	last := BufferSaturationBuckets[len(BufferSaturationBuckets)-1]
	if last != 512 {
		t.Errorf("BufferSaturationBuckets should end at 512, got %v", last)
	}
}
