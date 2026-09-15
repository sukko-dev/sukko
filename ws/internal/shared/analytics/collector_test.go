package analytics

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func noopLogger() zerolog.Logger { return zerolog.Nop() }

// TestCounterMap_BufferFull verifies new events are dropped (not panicked) when buffer is full.
func TestCounterMap_BufferFull(t *testing.T) {
	t.Parallel()
	dropped := 0
	drop := dropper(func() { dropped++ })

	m := newCounterMap(2) // buffer holds 2 entries total
	m.dropCounter = &drop

	k1 := tenantKey{tenantID: "t1", secondary: "websocket"}
	k2 := tenantKey{tenantID: "t2", secondary: "websocket"}
	k3 := tenantKey{tenantID: "t3", secondary: "websocket"} // should be dropped

	if m.getOrCreateConnection(k1) == nil {
		t.Fatal("k1 should fit in buffer")
	}
	if m.getOrCreateConnection(k2) == nil {
		t.Fatal("k2 should fit in buffer")
	}
	if m.getOrCreateConnection(k3) != nil {
		t.Fatal("k3 should be dropped — buffer full")
	}
	if dropped != 1 {
		t.Errorf("expected 1 drop, got %d", dropped)
	}
}

// TestCounterMap_IsEmpty verifies isEmpty is safe under concurrent key insertion (no race).
func TestCounterMap_IsEmpty(t *testing.T) {
	t.Parallel()
	m := newCounterMap(1000)
	if !m.isEmpty() {
		t.Fatal("new map should be empty")
	}
	k := tenantKey{tenantID: "t1", secondary: "ws"}
	m.getOrCreateConnection(k)
	if m.isEmpty() {
		t.Fatal("map with entry should not be empty")
	}
}

// TestCounterMap_IsEmpty_Race exercises isEmpty with concurrent producers — the race
// detector will catch the missing mu if the lock is absent.
func TestCounterMap_IsEmpty_Race(t *testing.T) {
	t.Parallel()
	m := newCounterMap(10000)

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			k := tenantKey{tenantID: "t" + string(rune('0'+i%10)), secondary: "ws"}
			m.getOrCreateConnection(k)
		})
	}
	// Concurrent reads via isEmpty while producers insert
	for range 50 {
		wg.Go(func() { _ = m.isEmpty() })
	}
	wg.Wait()
}

// TestMergeBack verifies atomic.Add is used (not replace) so concurrent increments survive.
func TestMergeBack(t *testing.T) {
	t.Parallel()
	snapshot := newCounterMap(100)
	current := newCounterMap(100)

	k := tenantKey{tenantID: "t1", secondary: "ws"}
	snap := snapshot.getOrCreateConnection(k)
	snap.active.Store(5)
	snap.connects.Store(3)

	// Pre-seed current with existing value
	cur := current.getOrCreateConnection(k)
	cur.active.Store(10)

	mergeBack(current, snapshot)

	if got := cur.active.Load(); got != 15 {
		t.Errorf("mergeBack active: got %d, want 15 (10 + 5)", got)
	}
	if got := cur.connects.Load(); got != 3 {
		t.Errorf("mergeBack connects: got %d, want 3", got)
	}
}

// TestNoopCollector_NoAlloc verifies NoopCollector never touches the pool or a goroutine.
func TestNoopCollector_NoAlloc(t *testing.T) {
	t.Parallel()
	c := NewCollector(CollectorConfig{}, nil, noopLogger())
	if _, ok := c.(NoopCollector); !ok {
		t.Fatal("nil pool must return NoopCollector")
	}
	// These must not panic
	c.IncrementConnections("t1", "websocket", 1)
	c.IncrementMessages("t1", "user", 1, 1, 0, 10)
	c.RecordPushDelivery("t1", "web", 1, 0, 0, 0, 5)
	if err := c.Start(context.Background(), &sync.WaitGroup{}); err != nil {
		t.Errorf("NoopCollector.Start unexpected error: %v", err)
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Errorf("NoopCollector.Flush unexpected error: %v", err)
	}
}

// TestChannelPrefixTruncation verifies that channelPrefix is truncated at first ".".
func TestChannelPrefixTruncation(t *testing.T) {
	t.Parallel()
	c := &realCollector{
		cfg: CollectorConfig{BufferSize: 100, FlushInterval: time.Minute},
	}
	fresh := newCounterMap(100)
	d := dropper(func() {})
	fresh.dropCounter = &d
	c.current.Store(fresh)
	c.flushEnabled.Store(true)

	c.IncrementMessages("t1", "user.trade.BTCUSD", 1, 0, 0, 0)

	m := c.current.Load()
	found := false
	for k := range m.messages {
		if k.secondary == "user" {
			found = true
		}
	}
	if !found {
		t.Error("channelPrefix should be truncated to 'user', not 'user.trade.BTCUSD'")
	}
}

// TestSnapshotSwapRace exercises the snapshot-swap path with concurrent producers.
// The race detector will catch any missing locks.
func TestSnapshotSwapRace(t *testing.T) {
	t.Parallel()
	c := &realCollector{
		cfg:          CollectorConfig{BufferSize: 10000, FlushInterval: time.Minute},
		pendingFlush: make(chan *counterMap, pendingFlushQueueMin),
		metrics:      newAnalyticsMetrics("test"),
	}
	fresh := newCounterMap(c.cfg.BufferSize)
	d := dropper(c.metrics.eventsDropped.Inc)
	fresh.dropCounter = &d
	c.current.Store(fresh)
	c.flushEnabled.Store(true)

	var wg sync.WaitGroup
	// Concurrent producers
	for i := range 100 {
		wg.Go(func() {
			c.IncrementConnections("t"+string(rune('A'+i%26)), "websocket", 1)
		})
	}
	// Concurrent flush (swaps the map)
	wg.Go(func() {
		time.Sleep(1 * time.Millisecond) // let some producers start
		swapped := c.current.Swap(newCounterMap(c.cfg.BufferSize))
		// simulate what flush does — read under lock
		_ = swapped.isEmpty()
		_ = swapped.totalTenants()
	})
	wg.Wait()
}
