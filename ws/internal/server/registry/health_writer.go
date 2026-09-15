package registry

import "sync/atomic"

// HealthWriter tracks per-shard admin channel health and pod-level drop counts.
// It is pod-level (one instance per pod, shared across all shards and the registry writer).
// It has NO goroutine and NO Valkey client — it is pure atomic state.
// Thread-safe; all methods are goroutine-safe via atomics.
type HealthWriter struct {
	shards   []atomic.Bool
	podDrops atomic.Int64
}

// NewHealthWriter creates a HealthWriter pre-sized for numShards shards.
// shards[i] is zero-value atomic.Bool (false = unhealthy) until SetAdminHealthy is called.
func NewHealthWriter(numShards int) *HealthWriter {
	return &HealthWriter{
		shards: make([]atomic.Bool, numShards),
	}
}

// SetAdminHealthy records the health state of a specific shard's admin channel.
// shardID must be in [0, numShards) — the caller is responsible for bounds safety.
func (hw *HealthWriter) SetAdminHealthy(shardID int, healthy bool) {
	hw.shards[shardID].Store(healthy)
}

// AdminHealthy returns true only if ALL shards have reported healthy.
// Returns false when len(shards)==0 (safe default until shards confirm healthy).
func (hw *HealthWriter) AdminHealthy() bool {
	if len(hw.shards) == 0 {
		return false
	}
	for i := range hw.shards {
		if !hw.shards[i].Load() {
			return false
		}
	}
	return true
}

// AddDrops atomically adds n to the pod-level drop counter.
// Called by Writer.Push() when the workChan is full.
func (hw *HealthWriter) AddDrops(n int64) {
	hw.podDrops.Add(n)
}

// Drops atomically swaps the pod-level drop counter to zero and returns the captured value.
// The Swap(0) is a single atomic operation — concurrent AddDrops calls that fire during
// this call are captured in the next call, not lost.
func (hw *HealthWriter) Drops() int64 {
	return hw.podDrops.Swap(0)
}
