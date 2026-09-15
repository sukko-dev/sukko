package worker

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/push/provider"
	"github.com/sukko-dev/sukko/internal/push/repository"
)

// mockProvider records all sent jobs and returns a configurable error.
type mockProvider struct {
	mu   sync.Mutex
	jobs []provider.PushJob
	err  error // error to return from Send
}

func (m *mockProvider) Send(_ context.Context, job provider.PushJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs = append(m.jobs, job)
	return m.err
}

func (m *mockProvider) SendBatch(ctx context.Context, jobs []provider.PushJob) error {
	for _, j := range jobs {
		if err := m.Send(ctx, j); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockProvider) Name() string              { return "mock" }
func (m *mockProvider) Close() error              { return nil }
func (m *mockProvider) InvalidateClient(_ string) {}

func (m *mockProvider) sentCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.jobs)
}

// mockRepo records DeleteByToken calls.
type mockRepo struct {
	mu             sync.Mutex
	deletedTokens  []string
	deletedTenants []string
}

func (r *mockRepo) Create(_ context.Context, _ *repository.PushSubscription) (int64, error) {
	return 0, nil
}

func (r *mockRepo) Delete(_ context.Context, _ int64, _ string) error { return nil }

func (r *mockRepo) DeleteByToken(_ context.Context, tenantID, token string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deletedTenants = append(r.deletedTenants, tenantID)
	r.deletedTokens = append(r.deletedTokens, token)
	return nil
}

func (r *mockRepo) FindByTenant(_ context.Context, _ string) ([]repository.PushSubscription, error) {
	return nil, nil
}

func (r *mockRepo) UpdateLastSuccess(_ context.Context, _ int64) error               { return nil }
func (r *mockRepo) DeleteByJTI(_ context.Context, _, _ string) (int, error)          { return 0, nil }
func (r *mockRepo) DeleteBySub(_ context.Context, _, _ string, _ int64) (int, error) { return 0, nil }

func (r *mockRepo) deleteCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.deletedTokens)
}

func newTestPool(t *testing.T, prov provider.Provider, repo repository.SubscriptionRepository, workers, queueSize int) *Pool {
	t.Helper()
	p, err := NewPool(PoolConfig{
		WorkerCount: workers,
		QueueSize:   queueSize,
		Providers:   map[string]provider.Provider{"web": prov},
		Repo:        repo,
		MaxRetries:  0,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	return p
}

func TestBasicDispatch(t *testing.T) {
	t.Parallel()

	prov := &mockProvider{}
	repo := &mockRepo{}
	p := newTestPool(t, prov, repo, 2, 10)

	ctx := context.Background()
	p.Start(ctx)
	p.StartWorkers(2)

	for i := range 5 {
		p.Enqueue(provider.PushJob{
			TenantID: "acme",
			Platform: "web",
			Endpoint: "https://push.example.com/" + string(rune('0'+i)),
			Body:     "test",
		})
	}

	p.Stop()

	if got := prov.sentCount(); got != 5 {
		t.Errorf("expected 5 dispatched jobs, got %d", got)
	}
}

func TestBackpressure(t *testing.T) {
	t.Parallel()

	// Slow provider that blocks for a while.
	slowProv := &mockProvider{}
	repo := &mockRepo{}

	// Queue size 1, 1 worker — one job being processed + one in queue = full.
	p := newTestPool(t, slowProv, repo, 1, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)

	// Override provider to block until we signal.
	blocker := make(chan struct{})
	blockingProv := &blockingProvider{blocker: blocker}
	p.providers["web"] = blockingProv

	p.StartWorkers(1)

	// First job — picked up by worker (blocks).
	p.Enqueue(provider.PushJob{Platform: "web", TenantID: "t1", Body: "1"})
	// Second job — fills the queue buffer (size 1).
	p.Enqueue(provider.PushJob{Platform: "web", TenantID: "t1", Body: "2"})

	// Third enqueue should block because queue is full and worker is stuck.
	done := make(chan struct{})
	go func() {
		p.Enqueue(provider.PushJob{Platform: "web", TenantID: "t1", Body: "3"})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Enqueue should have blocked but returned immediately")
	case <-time.After(100 * time.Millisecond):
		// Expected: enqueue is blocked.
	}

	// Unblock the worker so all jobs drain.
	close(blocker)

	// Cancel and stop to clean up.
	cancel()

	// Wait for the blocked enqueue goroutine to finish (context canceled).
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked enqueue goroutine did not exit after context cancel")
	}

	close(p.jobs)
	p.wg.Wait()
}

// blockingProvider blocks on Send until blocker is closed.
type blockingProvider struct {
	blocker chan struct{}
}

func (b *blockingProvider) Send(_ context.Context, _ provider.PushJob) error {
	<-b.blocker
	return nil
}

func (b *blockingProvider) SendBatch(_ context.Context, _ []provider.PushJob) error { return nil }
func (b *blockingProvider) Name() string                                            { return "blocking" }
func (b *blockingProvider) Close() error                                            { return nil }
func (b *blockingProvider) InvalidateClient(_ string)                               {}

// syncWriter is a thread-safe writer for capturing zerolog output in tests.
type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func TestBackpressureWarnLog(t *testing.T) {
	t.Parallel()

	slowProv := &mockProvider{}
	repo := &mockRepo{}

	// Capture log output with thread-safe writer
	buf := &syncWriter{}
	logger := zerolog.New(buf)

	p, err := NewPool(PoolConfig{
		WorkerCount: 1,
		QueueSize:   1,
		Providers:   map[string]provider.Provider{"web": slowProv},
		Repo:        repo,
		MaxRetries:  0,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)

	// Block the worker
	blocker := make(chan struct{})
	blockingProv := &blockingProvider{blocker: blocker}
	p.providers["web"] = blockingProv

	p.StartWorkers(1)

	// First job — picked up by worker (blocks).
	p.Enqueue(provider.PushJob{Platform: "web", TenantID: "t1", Body: "1"})
	// Second job — fills the queue buffer (size 1).
	p.Enqueue(provider.PushJob{Platform: "web", TenantID: "t1", Body: "2"})

	// Third enqueue blocks because queue is full — should trigger WARN log.
	done := make(chan struct{})
	go func() {
		p.Enqueue(provider.PushJob{Platform: "web", TenantID: "t1", Body: "3"})
		close(done)
	}()

	// Poll for the backpressure WARN log instead of a fixed sleep (races the blocked
	// enqueue goroutine under CI load).
	logDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(logDeadline) {
		if strings.Contains(buf.String(), "push job queue full") {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Unblock everything
	close(blocker)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked enqueue goroutine did not exit")
	}

	close(p.jobs)
	p.wg.Wait()

	// Verify WARN log was emitted
	logOutput := buf.String()
	if !strings.Contains(logOutput, "push job queue full") {
		t.Errorf("expected backpressure WARN log, got:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, `"queue_size":1`) {
		t.Errorf("expected queue_size field in log, got:\n%s", logOutput)
	}
}

func TestErrSubscriptionExpiredDeletesToken(t *testing.T) {
	t.Parallel()

	prov := &mockProvider{err: provider.ErrSubscriptionExpired}
	repo := &mockRepo{}
	p := newTestPool(t, prov, repo, 1, 10)

	ctx := context.Background()
	p.Start(ctx)
	p.StartWorkers(1)

	p.Enqueue(provider.PushJob{
		TenantID: "acme",
		Platform: "web",
		Endpoint: "https://push.example.com/expired",
		Body:     "test",
	})

	p.Stop()

	if got := repo.deleteCount(); got != 1 {
		t.Errorf("expected 1 DeleteByToken call, got %d", got)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.deletedTenants[0] != "acme" {
		t.Errorf("expected tenant 'acme', got %q", repo.deletedTenants[0])
	}
	if repo.deletedTokens[0] != "https://push.example.com/expired" {
		t.Errorf("expected endpoint as token, got %q", repo.deletedTokens[0])
	}
}

func TestConcurrentDispatch(t *testing.T) {
	t.Parallel()

	var counter atomic.Int64
	countingProv := &countingProvider{counter: &counter}
	repo := &mockRepo{}

	p, err := NewPool(PoolConfig{
		WorkerCount: 10,
		QueueSize:   100,
		Providers:   map[string]provider.Provider{"web": countingProv},
		Repo:        repo,
		MaxRetries:  0,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	ctx := context.Background()
	p.Start(ctx)
	p.StartWorkers(10)

	for i := range 100 {
		p.Enqueue(provider.PushJob{
			TenantID: "acme",
			Platform: "web",
			Body:     string(rune(i)),
		})
	}

	p.Stop()

	if got := counter.Load(); got != 100 {
		t.Errorf("expected 100 dispatched jobs, got %d", got)
	}
}

// countingProvider atomically counts Send calls.
type countingProvider struct {
	counter *atomic.Int64
}

func (c *countingProvider) Send(_ context.Context, _ provider.PushJob) error {
	c.counter.Add(1)
	return nil
}

func (c *countingProvider) SendBatch(_ context.Context, _ []provider.PushJob) error { return nil }
func (c *countingProvider) Name() string                                            { return "counting" }
func (c *countingProvider) Close() error                                            { return nil }
func (c *countingProvider) InvalidateClient(_ string)                               {}

func TestStopDrainsQueue(t *testing.T) {
	t.Parallel()

	prov := &mockProvider{}
	repo := &mockRepo{}
	p := newTestPool(t, prov, repo, 2, 20)

	ctx := context.Background()
	p.Start(ctx)
	p.StartWorkers(2)

	// Enqueue several jobs.
	for range 10 {
		p.Enqueue(provider.PushJob{
			TenantID: "acme",
			Platform: "web",
			Body:     "drain-test",
		})
	}

	// Stop should drain remaining jobs and exit cleanly (no hang/panic).
	p.Stop()

	if got := prov.sentCount(); got != 10 {
		t.Errorf("expected 10 dispatched jobs after drain, got %d", got)
	}
}

func TestNewPoolValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     PoolConfig
		wantErr string
	}{
		{
			name:    "zero workers",
			cfg:     PoolConfig{WorkerCount: 0, QueueSize: 1, Providers: map[string]provider.Provider{"x": &mockProvider{}}, Repo: &mockRepo{}},
			wantErr: "worker count must be >= 1",
		},
		{
			name:    "zero queue size",
			cfg:     PoolConfig{WorkerCount: 1, QueueSize: 0, Providers: map[string]provider.Provider{"x": &mockProvider{}}, Repo: &mockRepo{}},
			wantErr: "queue size must be >= 1",
		},
		{
			name:    "no providers",
			cfg:     PoolConfig{WorkerCount: 1, QueueSize: 1, Providers: map[string]provider.Provider{}, Repo: &mockRepo{}},
			wantErr: "at least one provider is required",
		},
		{
			name:    "nil repo",
			cfg:     PoolConfig{WorkerCount: 1, QueueSize: 1, Providers: map[string]provider.Provider{"x": &mockProvider{}}, Repo: nil},
			wantErr: "subscription repository is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewPool(tt.cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("expected error %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}
