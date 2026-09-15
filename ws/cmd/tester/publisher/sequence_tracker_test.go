package publisher

import (
	"sync"
	"testing"
)

func TestSequenceTracker_InOrder(t *testing.T) {
	t.Parallel()

	tracker := &SequenceTracker{}
	for i := int64(1); i <= 10; i++ {
		tracker.Track(i)
	}

	stats := tracker.Stats()
	if stats.Received != 10 {
		t.Errorf("Received = %d, want 10", stats.Received)
	}
	if stats.Gaps != 0 {
		t.Errorf("Gaps = %d, want 0", stats.Gaps)
	}
	if stats.Duplicates != 0 {
		t.Errorf("Duplicates = %d, want 0", stats.Duplicates)
	}
	if stats.HighestSeen != 10 {
		t.Errorf("HighestSeen = %d, want 10", stats.HighestSeen)
	}
}

func TestSequenceTracker_GapDetection(t *testing.T) {
	t.Parallel()

	tracker := &SequenceTracker{}
	tracker.Track(1)
	tracker.Track(5) // gap: missed 2, 3, 4

	stats := tracker.Stats()
	if stats.Gaps != 3 {
		t.Errorf("Gaps = %d, want 3", stats.Gaps)
	}
	if stats.HighestSeen != 5 {
		t.Errorf("HighestSeen = %d, want 5", stats.HighestSeen)
	}
}

func TestSequenceTracker_DuplicateDetection(t *testing.T) {
	t.Parallel()

	tracker := &SequenceTracker{}
	tracker.Track(1)
	tracker.Track(2)
	tracker.Track(1) // duplicate

	stats := tracker.Stats()
	if stats.Duplicates != 1 {
		t.Errorf("Duplicates = %d, want 1", stats.Duplicates)
	}
}

func TestSequenceTracker_Concurrent(t *testing.T) {
	t.Parallel()

	tracker := &SequenceTracker{}
	var wg sync.WaitGroup

	// 10 goroutines each tracking 100 sequences
	for g := range 10 {
		wg.Go(func() {
			for i := range 100 {
				tracker.Track(int64(g*100 + i + 1))
			}
		})
	}

	wg.Wait()
	stats := tracker.Stats()
	if stats.Received != 1000 {
		t.Errorf("Received = %d, want 1000", stats.Received)
	}
}
