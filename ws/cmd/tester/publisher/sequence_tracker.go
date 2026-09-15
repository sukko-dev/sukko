package publisher

import "sync"

// SequenceTracker detects message loss and duplication using monotonic sequence numbers.
// O(1) memory — tracks only the highest seen sequence, gap count, and duplicate count.
// Thread-safe via mutex for correct gap/duplicate accounting under concurrent access.
type SequenceTracker struct {
	mu          sync.Mutex
	highestSeen int64
	received    int64
	gaps        int64
	duplicates  int64
}

// SequenceStats is a point-in-time snapshot of tracking state.
type SequenceStats struct {
	HighestSeen int64
	Received    int64
	Gaps        int64
	Duplicates  int64
}

// Track records a received sequence number. Detects gaps (loss) and duplicates.
// Must be called under consistent ordering — gap detection assumes forward progress.
func (t *SequenceTracker) Track(seq int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.received++

	if seq > t.highestSeen+1 {
		t.gaps += seq - t.highestSeen - 1
	} else if seq <= t.highestSeen && seq > 0 {
		t.duplicates++
	}

	if seq > t.highestSeen {
		t.highestSeen = seq
	}
}

// Stats returns a point-in-time snapshot of tracking state.
func (t *SequenceTracker) Stats() SequenceStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return SequenceStats{
		HighestSeen: t.highestSeen,
		Received:    t.received,
		Gaps:        t.gaps,
		Duplicates:  t.duplicates,
	}
}
