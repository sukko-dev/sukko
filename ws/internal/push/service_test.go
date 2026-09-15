package push

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/push/provider"
	"github.com/sukko-dev/sukko/internal/push/repository"
	"github.com/sukko-dev/sukko/internal/push/worker"
)

// mockConfigCache implements ConfigCache for testing.
type mockConfigCache struct {
	patterns map[string][]string // tenantID -> channel patterns
	topics   []string
}

func (m *mockConfigCache) GetPushPatterns(tenantID string) []string {
	return m.patterns[tenantID]
}

func (m *mockConfigCache) GetTopics() []string {
	return m.topics
}

func (m *mockConfigCache) GetCredential(_, _ string) (json.RawMessage, error) {
	return nil, nil
}

// mockRepo implements repository.SubscriptionRepository for testing.
type mockRepo struct {
	subs map[string][]repository.PushSubscription // tenantID -> subscriptions
}

func (r *mockRepo) Create(_ context.Context, _ *repository.PushSubscription) (int64, error) {
	return 0, nil
}

func (r *mockRepo) Delete(_ context.Context, _ int64, _ string) error { return nil }

func (r *mockRepo) DeleteByToken(_ context.Context, _, _ string) error { return nil }

func (r *mockRepo) FindByTenant(_ context.Context, tenantID string) ([]repository.PushSubscription, error) {
	return r.subs[tenantID], nil
}

func (r *mockRepo) UpdateLastSuccess(_ context.Context, _ int64) error               { return nil }
func (r *mockRepo) DeleteByJTI(_ context.Context, _, _ string) (int, error)          { return 0, nil }
func (r *mockRepo) DeleteBySub(_ context.Context, _, _ string, _ int64) (int, error) { return 0, nil }

func TestHandleMessage_Match(t *testing.T) {
	t.Parallel()

	cache := &mockConfigCache{
		patterns: map[string][]string{
			"acme": {"acme.alerts.*"},
		},
	}

	repo := &mockRepo{
		subs: map[string][]repository.PushSubscription{
			"acme": {
				{
					TenantID:  "acme",
					Principal: "user1",
					Platform:  "web",
					Endpoint:  "https://push.example.com/1",
					Channels:  []string{"acme.alerts.*"},
				},
				{
					TenantID:  "acme",
					Principal: "user2",
					Platform:  "web",
					Endpoint:  "https://push.example.com/2",
					Channels:  []string{"acme.alerts.*"},
				},
			},
		},
	}

	svc := &Service{
		repo:       repo,
		cache:      cache,
		defaultTTL: 60,
		defaultUrg: "normal",
		logger:     zerolog.Nop(),
		ctx:        context.Background(),
	}

	// Replace workerPool.Enqueue by calling handleMessage directly
	// and intercepting via a custom worker pool. Since workerPool is a concrete
	// type, we test the logic by calling handleMessage and using a real worker pool
	// with a capturing provider.

	// Instead, build a real worker pool with a capturing provider.
	capturingProv := &capturingProvider{}
	realWP, err := newRealWorkerPool(capturingProv, repo)
	if err != nil {
		t.Fatalf("newRealWorkerPool: %v", err)
	}
	svc.workerPool = realWP
	realWP.Start(context.Background())
	realWP.StartWorkers(1)

	svc.handleMessage("acme", "acme.alerts.BTC", []byte(`{"price":50000}`))

	realWP.Stop()

	if got := capturingProv.sentCount(); got != 2 {
		t.Errorf("expected 2 jobs dispatched (2 matching subscriptions), got %d", got)
	}

	// Verify job fields.
	capturingProv.mu.Lock()
	defer capturingProv.mu.Unlock()
	for _, job := range capturingProv.jobs {
		if job.TenantID != "acme" {
			t.Errorf("expected tenant 'acme', got %q", job.TenantID)
		}
		if job.Channel != "acme.alerts.BTC" {
			t.Errorf("expected channel 'acme.alerts.BTC', got %q", job.Channel)
		}
		if job.Body != `{"price":50000}` {
			t.Errorf("unexpected body: %q", job.Body)
		}
	}

}

func TestHandleMessage_NoMatch(t *testing.T) {
	t.Parallel()

	cache := &mockConfigCache{
		patterns: map[string][]string{
			"acme": {"acme.alerts.*"},
		},
	}

	repo := &mockRepo{
		subs: map[string][]repository.PushSubscription{
			"acme": {
				{
					TenantID: "acme",
					Platform: "web",
					Channels: []string{"acme.alerts.*"},
				},
			},
		},
	}

	capturingProv := &capturingProvider{}
	realWP, err := newRealWorkerPool(capturingProv, repo)
	if err != nil {
		t.Fatalf("newRealWorkerPool: %v", err)
	}

	svc := &Service{
		repo:       repo,
		cache:      cache,
		logger:     zerolog.Nop(),
		ctx:        context.Background(),
		workerPool: realWP,
	}
	realWP.Start(context.Background())
	realWP.StartWorkers(1)

	// Channel "acme.market.BTC" does not match pattern "acme.alerts.*"
	svc.handleMessage("acme", "acme.market.BTC", []byte(`{"price":50000}`))

	realWP.Stop()

	if got := capturingProv.sentCount(); got != 0 {
		t.Errorf("expected 0 jobs dispatched for non-matching channel, got %d", got)
	}
}

func TestHandleMessage_NoPatterns(t *testing.T) {
	t.Parallel()

	cache := &mockConfigCache{
		patterns: map[string][]string{}, // no patterns for any tenant
	}

	repo := &mockRepo{}

	capturingProv := &capturingProvider{}
	realWP, err := newRealWorkerPool(capturingProv, repo)
	if err != nil {
		t.Fatalf("newRealWorkerPool: %v", err)
	}

	svc := &Service{
		repo:       repo,
		cache:      cache,
		logger:     zerolog.Nop(),
		ctx:        context.Background(),
		workerPool: realWP,
	}
	realWP.Start(context.Background())
	realWP.StartWorkers(1)

	svc.handleMessage("unknown-tenant", "acme.alerts.BTC", []byte(`{"data":"test"}`))

	realWP.Stop()

	if got := capturingProv.sentCount(); got != 0 {
		t.Errorf("expected 0 jobs dispatched for tenant with no patterns, got %d", got)
	}
}

func TestHandleMessage_SubscriptionChannelFiltering(t *testing.T) {
	t.Parallel()

	// Tenant has push patterns for both alerts and market,
	// but subscription only subscribes to alerts.
	cache := &mockConfigCache{
		patterns: map[string][]string{
			"acme": {"acme.alerts.*", "acme.market.*"},
		},
	}

	repo := &mockRepo{
		subs: map[string][]repository.PushSubscription{
			"acme": {
				{
					TenantID:  "acme",
					Principal: "user1",
					Platform:  "web",
					Endpoint:  "https://push.example.com/1",
					Channels:  []string{"acme.alerts.*"}, // only alerts
				},
				{
					TenantID:  "acme",
					Principal: "user2",
					Platform:  "web",
					Endpoint:  "https://push.example.com/2",
					Channels:  []string{"acme.market.*"}, // only market
				},
			},
		},
	}

	capturingProv := &capturingProvider{}
	realWP, err := newRealWorkerPool(capturingProv, repo)
	if err != nil {
		t.Fatalf("newRealWorkerPool: %v", err)
	}

	svc := &Service{
		repo:       repo,
		cache:      cache,
		defaultTTL: 60,
		defaultUrg: "normal",
		logger:     zerolog.Nop(),
		ctx:        context.Background(),
		workerPool: realWP,
	}
	realWP.Start(context.Background())
	realWP.StartWorkers(1)

	// Send an alerts message — only user1's subscription should match.
	svc.handleMessage("acme", "acme.alerts.BTC", []byte(`{"alert":"BTC up"}`))

	realWP.Stop()

	if got := capturingProv.sentCount(); got != 1 {
		t.Errorf("expected 1 job dispatched (only alerts subscriber), got %d", got)
	}

	capturingProv.mu.Lock()
	defer capturingProv.mu.Unlock()
	if len(capturingProv.jobs) > 0 && capturingProv.jobs[0].Principal != "user1" {
		t.Errorf("expected job for user1, got %q", capturingProv.jobs[0].Principal)
	}
}

// --- helpers ---

// capturingProvider records all sent jobs for assertion.
type capturingProvider struct {
	mu   sync.Mutex
	jobs []provider.PushJob
}

func (c *capturingProvider) Send(_ context.Context, job provider.PushJob) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.jobs = append(c.jobs, job)
	return nil
}

func (c *capturingProvider) SendBatch(ctx context.Context, jobs []provider.PushJob) error {
	for _, j := range jobs {
		if err := c.Send(ctx, j); err != nil {
			return err
		}
	}
	return nil
}

func (c *capturingProvider) Name() string              { return "capturing" }
func (c *capturingProvider) Close() error              { return nil }
func (c *capturingProvider) InvalidateClient(_ string) {}

func (c *capturingProvider) sentCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.jobs)
}

func newRealWorkerPool(prov provider.Provider, repo repository.SubscriptionRepository) (*worker.Pool, error) {
	return worker.NewPool(worker.PoolConfig{
		WorkerCount: 1,
		QueueSize:   100,
		Providers:   map[string]provider.Provider{"web": prov},
		Repo:        repo,
		MaxRetries:  0,
		Logger:      zerolog.Nop(),
	})
}

// TestSetDryRunMode verifies the push_dry_run_mode gauge is set correctly.
//
//nolint:paralleltest // sub-tests share the package-level dryRunMode gauge — sequential is required.
func TestSetDryRunMode(t *testing.T) {
	t.Run("dry run enabled sets gauge to 1", func(t *testing.T) {
		SetDryRunMode(true)
		if got := testutil.ToFloat64(dryRunMode); got != 1.0 {
			t.Errorf("SetDryRunMode(true): got %v, want 1.0", got)
		}
	})

	t.Run("dry run disabled sets gauge to 0", func(t *testing.T) {
		SetDryRunMode(false)
		if got := testutil.ToFloat64(dryRunMode); got != 0.0 {
			t.Errorf("SetDryRunMode(false): got %v, want 0.0", got)
		}
	})
}
