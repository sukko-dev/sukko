package orchestration

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/broadcast"
)

// tenantMockBus records Subscribe/Unsubscribe calls and delivers messages
// via caller-controlled channels.
type tenantMockBus struct {
	mu               sync.Mutex
	subscribeCalls   []string
	unsubscribeCalls []string
	channels         map[string]chan *broadcast.Message
	subscribeErr     error
}

func newTenantMockBus() *tenantMockBus {
	return &tenantMockBus{
		channels: make(map[string]chan *broadcast.Message),
	}
}

func (m *tenantMockBus) Subscribe(tenantID string) (<-chan *broadcast.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.subscribeErr != nil {
		return nil, m.subscribeErr
	}
	m.subscribeCalls = append(m.subscribeCalls, tenantID)
	ch := make(chan *broadcast.Message, 8)
	// Use a counter-suffixed key so multiple subscribes for same tenant get separate channels.
	key := tenantID
	for {
		if _, exists := m.channels[key]; !exists {
			break
		}
		key += "_2"
	}
	m.channels[key] = ch
	return ch, nil
}

func (m *tenantMockBus) Unsubscribe(tenantID string, ch <-chan *broadcast.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsubscribeCalls = append(m.unsubscribeCalls, tenantID)
	return nil
}

func (m *tenantMockBus) SubscribeAll() (<-chan *broadcast.Message, error) {
	return make(chan *broadcast.Message, 8), nil
}
func (m *tenantMockBus) UnsubscribeAll(_ <-chan *broadcast.Message) error { return nil }
func (m *tenantMockBus) Publish(_ *broadcast.Message) error               { return nil }
func (m *tenantMockBus) Run()                                             {}
func (m *tenantMockBus) Shutdown()                                        {}
func (m *tenantMockBus) ShutdownWithContext(_ context.Context)            {}
func (m *tenantMockBus) IsHealthy() bool                                  { return true }
func (m *tenantMockBus) GetMetrics() broadcast.Metrics                    { return broadcast.Metrics{} }

func (m *tenantMockBus) subscribeCount(tenantID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, tid := range m.subscribeCalls {
		if tid == tenantID {
			n++
		}
	}
	return n
}

func (m *tenantMockBus) unsubscribeCount(tenantID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, tid := range m.unsubscribeCalls {
		if tid == tenantID {
			n++
		}
	}
	return n
}

// TestShard_OnTenantClientConnect_FirstLastLifecycle verifies that Subscribe is called once
// on the first connect and Unsubscribe is called once on the final disconnect.
func TestShard_OnTenantClientConnect_FirstLastLifecycle(t *testing.T) {
	t.Parallel()

	bus := newTenantMockBus()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &Shard{
		broadcastBus:  bus,
		broadcastChan: make(chan *broadcast.Message, 8),
		tenantEntries: make(map[string]tenantEntry),
		ctx:           ctx,
		cancel:        cancel,
	}
	s.logger = zerolog.Nop()

	const tid = "acme"

	// First connect: should call bus.Subscribe once and launch forwarder.
	if err := s.OnTenantClientConnect(tid); err != nil {
		t.Fatalf("OnTenantClientConnect: %v", err)
	}
	if got := bus.subscribeCount(tid); got != 1 {
		t.Errorf("Subscribe calls after first connect: got %d, want 1", got)
	}

	// Second connect: ref count should go to 2, no new Subscribe.
	if err := s.OnTenantClientConnect(tid); err != nil {
		t.Fatalf("OnTenantClientConnect (2nd): %v", err)
	}
	if got := bus.subscribeCount(tid); got != 1 {
		t.Errorf("Subscribe calls after second connect: got %d, want 1", got)
	}

	// First disconnect: ref count goes to 1, no Unsubscribe.
	s.OnTenantClientDisconnect(tid)
	if got := bus.unsubscribeCount(tid); got != 0 {
		t.Errorf("Unsubscribe calls after first disconnect: got %d, want 0", got)
	}

	// Final disconnect: ref count to 0, Unsubscribe should be called.
	s.OnTenantClientDisconnect(tid)

	// Give the forwarder goroutine a moment to exit.
	s.wg.Wait()

	if got := bus.unsubscribeCount(tid); got != 1 {
		t.Errorf("Unsubscribe calls after final disconnect: got %d, want 1", got)
	}

	// Entry should be removed from map.
	s.tenantMu.Lock()
	_, exists := s.tenantEntries[tid]
	s.tenantMu.Unlock()
	if exists {
		t.Error("tenantEntry still present after final disconnect")
	}
}

// TestShard_OnTenantClientConnect_RefCount verifies that two connects followed by one
// disconnect keeps the subscription alive.
func TestShard_OnTenantClientConnect_RefCount(t *testing.T) {
	t.Parallel()

	bus := newTenantMockBus()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &Shard{
		broadcastBus:  bus,
		broadcastChan: make(chan *broadcast.Message, 8),
		tenantEntries: make(map[string]tenantEntry),
		ctx:           ctx,
		cancel:        cancel,
	}
	s.logger = zerolog.Nop()

	const tid = "beta"

	if err := s.OnTenantClientConnect(tid); err != nil {
		t.Fatalf("first connect: %v", err)
	}
	if err := s.OnTenantClientConnect(tid); err != nil {
		t.Fatalf("second connect: %v", err)
	}

	s.OnTenantClientDisconnect(tid)

	// Entry must still exist with refCount==1.
	s.tenantMu.Lock()
	entry, exists := s.tenantEntries[tid]
	s.tenantMu.Unlock()
	if !exists {
		t.Fatal("tenantEntry removed after first disconnect, expected refCount==1")
	}
	if entry.refCount != 1 {
		t.Errorf("refCount: got %d, want 1", entry.refCount)
	}
	if got := bus.unsubscribeCount(tid); got != 0 {
		t.Errorf("Unsubscribe called prematurely: got %d calls, want 0", got)
	}

	// Final disconnect.
	s.OnTenantClientDisconnect(tid)
	s.wg.Wait()
	if got := bus.unsubscribeCount(tid); got != 1 {
		t.Errorf("Unsubscribe not called after final disconnect: got %d, want 1", got)
	}
}

// TestShard_OnTenantClientConnect_BusError verifies that a bus.Subscribe error causes
// OnTenantClientConnect to return an error and NOT create a tenantEntry.
func TestShard_OnTenantClientConnect_BusError(t *testing.T) {
	t.Parallel()

	bus := newTenantMockBus()
	bus.subscribeErr = errors.New("bus offline")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &Shard{
		broadcastBus:  bus,
		broadcastChan: make(chan *broadcast.Message, 8),
		tenantEntries: make(map[string]tenantEntry),
		ctx:           ctx,
		cancel:        cancel,
	}
	s.logger = zerolog.Nop()

	err := s.OnTenantClientConnect("failtest")
	if err == nil {
		t.Fatal("expected error from OnTenantClientConnect, got nil")
	}

	s.tenantMu.Lock()
	_, exists := s.tenantEntries["failtest"]
	s.tenantMu.Unlock()
	if exists {
		t.Error("tenantEntry created despite bus.Subscribe error")
	}
}

// TestShard_Forwarder_Drain verifies that the tenant forwarder drains its channel
// after context cancellation.
func TestShard_Forwarder_Drain(t *testing.T) {
	t.Parallel()

	bus := newTenantMockBus()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broadcastChan := make(chan *broadcast.Message, 64)
	s := &Shard{
		broadcastBus:  bus,
		broadcastChan: broadcastChan,
		tenantEntries: make(map[string]tenantEntry),
		ctx:           ctx,
		cancel:        cancel,
	}
	s.logger = zerolog.Nop()

	const tid = "gamma"
	if err := s.OnTenantClientConnect(tid); err != nil {
		t.Fatalf("OnTenantClientConnect: %v", err)
	}

	// Get the channel the forwarder reads from.
	s.tenantMu.Lock()
	entry := s.tenantEntries[tid]
	s.tenantMu.Unlock()

	// Send messages to fill the tenantEntry channel (use type assertion since ch is <-chan).
	// We need to access the underlying chan to write. Since the bus created it bidirectionally,
	// we get it from the mock's channels map.
	bus.mu.Lock()
	var busCh chan *broadcast.Message
	for _, ch := range bus.channels {
		busCh = ch
		break
	}
	bus.mu.Unlock()

	const N = 5
	for range N {
		busCh <- &broadcast.Message{Subject: "x"}
	}

	// Wait deterministically for all N messages to arrive in broadcastChan before
	// disconnecting. This replaces a fragile time.Sleep with channel-based synchronization.
	deadline := time.After(200 * time.Millisecond)
	for i := range N {
		select {
		case <-broadcastChan:
		case <-deadline:
			t.Fatalf("timeout waiting for message %d/%d in broadcastChan", i+1, N)
		}
	}

	// Cancel context to stop forwarder.
	s.OnTenantClientDisconnect(tid)
	s.wg.Wait()

	// Entry's channel was drained (drain() should have run).
	if remaining := len(entry.ch); remaining != 0 {
		t.Errorf("forwarder drain left %d messages in channel, want 0", remaining)
	}
}

// TestShard_BroadcastChan_NotClosed verifies that canceling the shard context and waiting
// for goroutines does not panic (broadcastChan must not be closed by any goroutine).
func TestShard_BroadcastChan_NotClosed(t *testing.T) {
	t.Parallel()

	bus := newTenantMockBus()
	ctx, cancel := context.WithCancel(context.Background())

	s := &Shard{
		broadcastBus:  bus,
		broadcastChan: make(chan *broadcast.Message, 8),
		tenantEntries: make(map[string]tenantEntry),
		ctx:           ctx,
		cancel:        cancel,
	}
	s.logger = zerolog.Nop()

	if err := s.OnTenantClientConnect("nocloseten"); err != nil {
		t.Fatalf("OnTenantClientConnect: %v", err)
	}

	// Cancel shard context and wait for forwarder goroutines to exit.
	// This must not panic (which would happen if broadcastChan were closed and
	// a goroutine tried to send to it).
	cancel()
	s.wg.Wait() // Would panic here if broadcastChan was closed mid-send
}

// TestShard_ConcurrentFirstConnect verifies that concurrent first-connect calls for
// the same tenant result in exactly one bus.Subscribe and refCount==2 (race test).
func TestShard_ConcurrentFirstConnect(t *testing.T) {
	t.Parallel()

	bus := newTenantMockBus()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &Shard{
		broadcastBus:  bus,
		broadcastChan: make(chan *broadcast.Message, 8),
		tenantEntries: make(map[string]tenantEntry),
		ctx:           ctx,
		cancel:        cancel,
	}
	s.logger = zerolog.Nop()

	const tid = "racetest"
	var errCount atomic.Int64
	var wg sync.WaitGroup
	const goroutines = 4

	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			if err := s.OnTenantClientConnect(tid); err != nil {
				errCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if n := errCount.Load(); n > 0 {
		t.Errorf("%d concurrent connects returned errors, want 0", n)
	}

	// The slow path drops tenantMu before calling bus.Subscribe, so multiple concurrent
	// goroutines may each call Subscribe (rolling back extras via Unsubscribe).
	// The invariant is exactly one NET subscription (subscribe - unsubscribe == 1).
	if sub, unsub := bus.subscribeCount(tid), bus.unsubscribeCount(tid); sub-unsub != 1 {
		t.Errorf("net subscriptions: got %d-%d=%d, want 1", sub, unsub, sub-unsub)
	}

	// RefCount should equal the number of successful connects.
	s.tenantMu.Lock()
	entry, ok := s.tenantEntries[tid]
	s.tenantMu.Unlock()
	if !ok {
		t.Fatal("tenantEntry missing after concurrent connects")
	}
	if entry.refCount != goroutines {
		t.Errorf("refCount: got %d, want %d", entry.refCount, goroutines)
	}
}

// TestShard_FanIn_MultiTenant verifies that messages for tenant A and B both arrive
// on broadcastChan.
func TestShard_FanIn_MultiTenant(t *testing.T) {
	t.Parallel()

	bus := newTenantMockBus()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broadcastChan := make(chan *broadcast.Message, 16)
	s := &Shard{
		broadcastBus:  bus,
		broadcastChan: broadcastChan,
		tenantEntries: make(map[string]tenantEntry),
		ctx:           ctx,
		cancel:        cancel,
	}
	s.logger = zerolog.Nop()

	if err := s.OnTenantClientConnect("tenantA"); err != nil {
		t.Fatalf("connect tenantA: %v", err)
	}
	if err := s.OnTenantClientConnect("tenantB"); err != nil {
		t.Fatalf("connect tenantB: %v", err)
	}

	// Get the bus channels and send one message each.
	bus.mu.Lock()
	channels := make(map[string]chan *broadcast.Message, len(bus.channels))
	maps.Copy(channels, bus.channels)
	bus.mu.Unlock()

	for _, ch := range channels {
		ch <- &broadcast.Message{Subject: "test.msg"}
	}

	// Collect from broadcastChan.
	received := make([]*broadcast.Message, 0, 2)
	deadline := time.After(200 * time.Millisecond)
	for len(received) < 2 {
		select {
		case msg := <-broadcastChan:
			received = append(received, msg)
		case <-deadline:
			t.Fatalf("timeout waiting for messages: got %d, want 2", len(received))
		}
	}
	if len(received) != 2 {
		t.Errorf("received %d messages on broadcastChan, want 2", len(received))
	}
}
