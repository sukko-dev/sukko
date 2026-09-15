package metrics

import "github.com/prometheus/client_golang/prometheus"

// ConnectionDurationBuckets defines histogram buckets for long-lived connection durations.
// Range: 1 second to 1 hour, suitable for WebSocket connection lifetimes.
var ConnectionDurationBuckets = []float64{1, 5, 10, 30, 60, 300, 600, 1800, 3600}

// AuthLatencyBuckets defines histogram buckets for fast authentication operations.
// Range: sub-millisecond to 250ms, suitable for JWT validation and key lookups.
var AuthLatencyBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25}

// BackendLatencyBuckets defines histogram buckets for backend service calls.
// Range: 10ms to 1 second, suitable for database queries and internal API calls.
var BackendLatencyBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0}

// APILatencyBuckets defines histogram buckets for general API request latency.
// Matches prometheus.DefBuckets for consistency with standard Prometheus tooling.
var APILatencyBuckets = prometheus.DefBuckets

// BufferSaturationBuckets defines histogram buckets for buffer fill level tracking.
// Range: 0 to 512, with higher resolution near saturation (full buffer).
// Useful for detecting slow clients approaching buffer overflow.
var BufferSaturationBuckets = []float64{0, 64, 128, 256, 384, 448, 480, 496, 504, 510, 511, 512}
