package history_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	prometheustestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/server/history"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

func noopLogger() zerolog.Logger { return zerolog.Nop() }

// testWriterOpts allows tests to override individual config fields.
type testWriterOpts struct {
	podID                 string
	lockTTLMs             int64
	heartbeatInterval     time.Duration
	restartInitialBackoff time.Duration
	restartMaxBackoff     time.Duration
	cmdTimeout            time.Duration
	writerBuffer          int
	pipelineBatch         int
	maxConsecLockFailures int
	env                   string
}

// defaultTestOpts returns safe, fast defaults suitable for unit tests.
func defaultTestOpts() testWriterOpts {
	return testWriterOpts{
		podID:                 "test-pod",
		lockTTLMs:             2000,
		heartbeatInterval:     50 * time.Millisecond,
		restartInitialBackoff: 10 * time.Millisecond,
		restartMaxBackoff:     100 * time.Millisecond,
		cmdTimeout:            500 * time.Millisecond,
		writerBuffer:          256,
		pipelineBatch:         50,
		maxConsecLockFailures: 3,
		env:                   "test",
	}
}

// newTestMiniredis creates a miniredis instance that is cleaned up at test end.
func newTestMiniredis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(mr.Close)
	return mr
}

// newTestValkeyClient creates a valkey-go client pointing at the given miniredis instance.
func newTestValkeyClient(t *testing.T, mr *miniredis.Miniredis) valkey.Client {
	t.Helper()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:  []string{mr.Addr()},
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("valkey.NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// newTestWriter creates a HistoryWriter backed by miniredis.
// Returns the writer, its cancel func, and the mock bus.
func newTestWriter(t *testing.T, mr *miniredis.Miniredis, opts testWriterOpts) (*history.Writer, context.CancelFunc, *mockBus) {
	t.Helper()

	cfg := &platform.ServerConfig{
		PodID:                              opts.podID,
		HistoryWriterLockTTLMs:             opts.lockTTLMs,
		HistoryWriterHeartbeatInterval:     opts.heartbeatInterval,
		HistoryWriterRestartInitialBackoff: opts.restartInitialBackoff,
		HistoryWriterRestartMaxBackoff:     opts.restartMaxBackoff,
		HistoryValkeyCmdTimeout:            opts.cmdTimeout,
		HistoryWriterBuffer:                opts.writerBuffer,
		HistoryWriterPipelineBatch:         opts.pipelineBatch,
		HistoryMaxConsecutiveLockFailures:  opts.maxConsecLockFailures,
		HistoryBufferDepth:                 100,
		HistoryTTL:                         24 * time.Hour,
	}

	bus := newMockBus()
	client := newTestValkeyClient(t, mr)

	ctx, cancel := context.WithCancel(context.Background())
	reg := prometheus.NewRegistry()

	w := history.NewWriter(ctx, cfg, bus, client, noopLogger(), reg, opts.env)
	t.Cleanup(cancel)
	return w, cancel, bus
}

// mockBus is a minimal broadcast.Bus implementation for testing.
type mockBus struct {
	mu                  sync.Mutex
	tenantSubs          map[string]chan *broadcast.Message // per-tenant subscriber channels
	allSubs             []chan *broadcast.Message          // SubscribeAll subscriber channels
	healthy             bool
	publishLog          []*broadcast.Message
	subscribeAllCount   int // number of SubscribeAll calls
	unsubscribeAllCalls int // number of UnsubscribeAll calls
}

func newMockBus() *mockBus {
	return &mockBus{
		tenantSubs: make(map[string]chan *broadcast.Message),
		healthy:    true,
	}
}

func (b *mockBus) Subscribe(tenantID string) (<-chan *broadcast.Message, error) {
	ch := make(chan *broadcast.Message, 64)
	b.mu.Lock()
	b.tenantSubs[tenantID] = ch
	b.mu.Unlock()
	return ch, nil
}

func (b *mockBus) SubscribeAll() (<-chan *broadcast.Message, error) {
	ch := make(chan *broadcast.Message, 256)
	b.mu.Lock()
	b.subscribeAllCount++
	b.allSubs = append(b.allSubs, ch)
	b.mu.Unlock()
	return ch, nil
}

func (b *mockBus) Unsubscribe(tenantID string, _ <-chan *broadcast.Message) error {
	b.mu.Lock()
	delete(b.tenantSubs, tenantID)
	b.mu.Unlock()
	return nil
}

func (b *mockBus) UnsubscribeAll(ch <-chan *broadcast.Message) error {
	b.mu.Lock()
	b.unsubscribeAllCalls++
	for i, s := range b.allSubs {
		if s == ch {
			b.allSubs = append(b.allSubs[:i], b.allSubs[i+1:]...)
			b.mu.Unlock()
			return nil
		}
	}
	b.mu.Unlock()
	return broadcast.ErrSubscriberNotFound
}

func (b *mockBus) getSubscribeAllCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subscribeAllCount
}

func (b *mockBus) getUnsubscribeAllCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.unsubscribeAllCalls
}

func (b *mockBus) Publish(msg *broadcast.Message) {
	b.mu.Lock()
	b.publishLog = append(b.publishLog, msg)
	if sub, ok := b.tenantSubs[msg.TenantID]; ok {
		select {
		case sub <- msg:
		default:
		}
	}
	for _, s := range b.allSubs {
		select {
		case s <- msg:
		default:
		}
	}
	b.mu.Unlock()
}

func (b *mockBus) Run()                                  {}
func (b *mockBus) Shutdown()                             {}
func (b *mockBus) ShutdownWithContext(_ context.Context) {}
func (b *mockBus) IsHealthy() bool                       { b.mu.Lock(); defer b.mu.Unlock(); return b.healthy }
func (b *mockBus) GetMetrics() broadcast.Metrics         { return broadcast.Metrics{} }

func (b *mockBus) setHealthy(v bool) {
	b.mu.Lock()
	b.healthy = v
	b.mu.Unlock()
}

// fanOut pushes a broadcast.Message directly to all subscribers (per-tenant and SubscribeAll).
func (b *mockBus) fanOut(msg *broadcast.Message) {
	b.mu.Lock()
	if sub, ok := b.tenantSubs[msg.TenantID]; ok {
		select {
		case sub <- msg:
		default:
		}
	}
	for _, s := range b.allSubs {
		select {
		case s <- msg:
		default:
		}
	}
	b.mu.Unlock()
}

// syncWaitGroup wraps sync.WaitGroup to match the wg.Go(func()) API expected by Run().
type syncWaitGroup struct {
	wg sync.WaitGroup
}

func (w *syncWaitGroup) Go(fn func()) {
	w.wg.Go(func() {
		fn()
	})
}

func (w *syncWaitGroup) Wait() { w.wg.Wait() }

// metricCounterValue reads the current value of a prometheus.Counter.
func metricCounterValue(_ *testing.T, c prometheus.Counter) float64 {
	return prometheustestutil.ToFloat64(c)
}

// waitForStreamEntry polls until the history stream key has at least one entry, or fails
// after timeout. It replaces fixed sleeps that race the asynchronous HistoryWriter — under
// CI load (and -race) a fixed sleep can elapse before the relay+flush writes the entry,
// producing a spurious failure (e.g. TTL -2 on a not-yet-created key).
func waitForStreamEntry(t *testing.T, client valkey.Client, streamKey string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n, err := client.Do(context.Background(), client.B().Xlen().Key(streamKey).Build()).AsInt64()
		if err == nil && n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("stream key %q did not receive an entry within %s (HistoryWriter did not process in time)", streamKey, timeout)
}
