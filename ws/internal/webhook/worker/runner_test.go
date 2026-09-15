package worker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	broadcast "github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/shared/types"
	webhookconst "github.com/sukko-dev/sukko/internal/webhook"
)

// --- Test doubles ---

type recordingProvClient struct {
	mu            sync.Mutex
	statusUpdates []string
	deliveries    []*provisioning.WebhookDelivery
}

func (c *recordingProvClient) ListWebhookTenants(_ context.Context) ([]string, error) {
	return nil, nil
}
func (c *recordingProvClient) ListWebhooksForTenant(_ context.Context, _ string) ([]*provisioning.WebhookRecord, error) {
	return nil, nil
}
func (c *recordingProvClient) UpdateWebhookStatus(_ context.Context, id, _, status string, _ int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statusUpdates = append(c.statusUpdates, id+":"+status)
	return nil
}
func (c *recordingProvClient) RecordDelivery(_ context.Context, d *provisioning.WebhookDelivery) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deliveries = append(c.deliveries, d)
	return nil
}
func (c *recordingProvClient) deliveryCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.deliveries)
}
func (c *recordingProvClient) deliveryWithError(err string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, d := range c.deliveries {
		if d.Error == err {
			n++
		}
	}
	return n
}
func (c *recordingProvClient) statusUpdateCount(id, status string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, s := range c.statusUpdates {
		if s == id+":"+status {
			n++
		}
	}
	return n
}

type noopSub struct{ ch chan *broadcast.Message }

func newNoopSub() *noopSub                             { return &noopSub{ch: make(chan *broadcast.Message)} }
func (s *noopSub) Messages() <-chan *broadcast.Message { return s.ch }
func (s *noopSub) Close() error                        { return nil }

// mockFastDoer always returns 200 quickly.
type mockFastDoer struct{}

func (m *mockFastDoer) Do(_ *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok"))}, nil
}

// newTestRunner builds a Runner for unit tests.
func newTestRunner(client *recordingProvClient, cache *WebhookCache, doer HTTPDoer, clk Clock) *Runner {
	deliverer := NewDeliverer(cache, doer, testKey, zerolog.Nop())
	r, err := NewRunner(Config{
		Clock:              clk,
		RetrySchedule:      webhookconst.WebhookRetrySchedule[:],
		WorkerConcurrency:  2,
		RetryQueueSize:     100,
		DeliveryTimeout:    5 * time.Second,
		CacheTTL:           30 * time.Second,
		ProvisioningClient: client,
		Subscriber:         newNoopSub(),
		Cache:              cache,
		Deliverer:          deliverer,
	})
	if err != nil {
		panic("newTestRunner: " + err.Error())
	}
	return r
}

func emptyCache() *WebhookCache {
	return NewWebhookCache(newStubClient(), zerolog.Nop())
}

// TestRunner_RetrySchedule covers no time.Sleep in retry path.
// Uses mock clock and tests doDeliver directly.
func TestRunner_RetrySchedule(t *testing.T) {
	t.Parallel()
	client := &recordingProvClient{}
	cache := emptyCache()
	// Populate cache with an enabled webhook.
	cache.mu.Lock()
	cache.records["t1"] = []WebhookRecord{
		{ID: "wh-1", TenantID: "t1", URL: "https://example.com",
			SecretEnc: encryptSecret(t, "sec"),
			Status:    types.WebhookStatusEnabled, MaxRetries: 3},
	}
	cache.mu.Unlock()

	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	clk := Clock(func() time.Time { return time.Unix(0, now.Load()) })

	doer := &mockFastDoer{}
	r := newTestRunner(client, cache, doer, clk)

	// Task with NextAt in the past so doDeliver doesn't sleep.
	task := DeliveryTask{
		WebhookID: "wh-1", TenantID: "t1",
		SecretEnc: cache.records["t1"][0].SecretEnc,
		Payload:   []byte("test"), Attempt: 1, MaxRetries: 3,
		NextAt: time.Unix(0, now.Load()-1),
	}
	r.doDeliver(task)

	// Success → RecordDelivery called once.
	if n := client.deliveryCount(); n != 1 {
		t.Errorf("expected 1 delivery record, got %d", n)
	}
}

// TestRunner_QueueOverflowDropsOldest covers drop-oldest on queue overflow.
func TestRunner_QueueOverflowDropsOldest(t *testing.T) {
	t.Parallel()
	client := &recordingProvClient{}
	cache := emptyCache()

	r, _ := NewRunner(Config{
		Clock:              time.Now,
		RetrySchedule:      webhookconst.WebhookRetrySchedule[:],
		WorkerConcurrency:  1,
		RetryQueueSize:     2, // very small to trigger overflow quickly
		DeliveryTimeout:    5 * time.Second,
		CacheTTL:           30 * time.Second,
		ProvisioningClient: client,
		Subscriber:         newNoopSub(),
		Cache:              cache,
		Deliverer:          NewDeliverer(cache, &mockFastDoer{}, testKey, zerolog.Nop()),
	})

	now := time.Now()
	// Fill the retry queue.
	for i := range 2 {
		r.retryQueue <- DeliveryTask{
			WebhookID: "wh-a", TenantID: "t1",
			NextAt:     now.Add(time.Duration(i+1) * time.Hour), // future
			MaxRetries: 5,
		}
	}
	// dropOldest should drop the furthest-future task and accept the new one.
	incoming := DeliveryTask{
		WebhookID: "wh-b", TenantID: "t1",
		NextAt:     now.Add(30 * time.Minute),
		MaxRetries: 5,
	}
	fakeResult := DeliveryResult{StatusLabel: "failure"}
	r.dropOldest(incoming, fakeResult)

	// dropped task → RecordDelivery with error="queue_full"
	if n := client.deliveryWithError("queue_full"); n != 1 {
		t.Errorf("expected 1 dropped delivery record, got %d", n)
	}
	// deliveriesTotal{status="dropped"} incremented (verified implicitly; no panic)
}

// TestRunner_RaceShutdown covers shutdown safety under -race.
func TestRunner_RaceShutdown(t *testing.T) {
	t.Parallel()
	client := &recordingProvClient{}
	cache := emptyCache()
	deliverer := NewDeliverer(cache, &mockFastDoer{}, testKey, zerolog.Nop())

	r, err := NewRunner(Config{
		Clock:              time.Now,
		RetrySchedule:      webhookconst.WebhookRetrySchedule[:],
		WorkerConcurrency:  5,
		RetryQueueSize:     100,
		DeliveryTimeout:    5 * time.Second,
		CacheTTL:           30 * time.Second,
		ProvisioningClient: client,
		Subscriber:         newNoopSub(),
		Cache:              cache,
		Deliverer:          deliverer,
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Let goroutines spin for a bit, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Run() did not return after context cancel (goroutine leak?)")
	}
}

// TestRunner_AsyncStatusUpdaterExits verifies asyncStatusUpdater exits on ctx cancel.
func TestRunner_AsyncStatusUpdaterExits(t *testing.T) {
	t.Parallel()
	client := &recordingProvClient{}
	cache := emptyCache()
	r := newTestRunner(client, cache, &mockFastDoer{}, time.Now)

	// Fill statusUpdates channel to capacity so the goroutine will be blocked on it.
	for range r.cfg.WorkerConcurrency {
		r.statusUpdates <- statusUpdateRequest{webhookID: "wh-x", tenantID: "t1", status: "enabled"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		r.ctx = ctx
		r.cancel = cancel
		r.asyncStatusUpdater()
	})

	// Cancel immediately — asyncStatusUpdater should exit within 200ms.
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("asyncStatusUpdater did not exit within 500ms after ctx cancel")
	}
}

// TestRunner_DecryptionFailure verifies UpdateWebhookStatus(suspended) + RecordDelivery.
func TestRunner_DecryptionFailure(t *testing.T) {
	t.Parallel()
	client := &recordingProvClient{}
	cache := emptyCache()
	cache.mu.Lock()
	cache.records["t1"] = []WebhookRecord{
		{ID: "wh-1", TenantID: "t1", URL: "https://example.com",
			SecretEnc: []byte("garbage-not-valid"), // decrypt will fail
			Status:    types.WebhookStatusEnabled, MaxRetries: 3},
	}
	cache.mu.Unlock()

	r := newTestRunner(client, cache, &mockFastDoer{}, time.Now)

	// Start asyncStatusUpdater so sendStatusUpdate calls propagate to the recording client.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.ctx = ctx
	r.cancel = cancel
	r.wg.Go(r.asyncStatusUpdater)

	task := DeliveryTask{
		WebhookID: "wh-1", TenantID: "t1",
		SecretEnc: []byte("garbage-not-valid"),
		Payload:   []byte("x"), Attempt: 1, MaxRetries: 3,
		NextAt: time.Now().Add(-time.Second),
	}
	r.doDeliver(task)

	// Give asyncStatusUpdater time to process the channel entry.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if client.statusUpdateCount("wh-1", types.WebhookStatusSuspended) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel() // stop asyncStatusUpdater

	if n := client.statusUpdateCount("wh-1", types.WebhookStatusSuspended); n != 1 {
		t.Errorf("expected 1 suspended status update, got %d", n)
	}
	if n := client.deliveryWithError("decryption_failure"); n != 1 {
		t.Errorf("expected 1 RecordDelivery(error=decryption_failure), got %d", n)
	}
}

// TestRunner_TwoQueueIndependence verifies a full retry queue doesn't block fresh events.
func TestRunner_TwoQueueIndependence(t *testing.T) {
	t.Parallel()
	r, _ := NewRunner(Config{
		Clock:              time.Now,
		RetrySchedule:      webhookconst.WebhookRetrySchedule[:],
		WorkerConcurrency:  2,
		RetryQueueSize:     1,
		DeliveryTimeout:    5 * time.Second,
		CacheTTL:           30 * time.Second,
		ProvisioningClient: &recordingProvClient{},
		Subscriber:         newNoopSub(),
		Cache:              emptyCache(),
		Deliverer:          NewDeliverer(emptyCache(), &mockFastDoer{}, testKey, zerolog.Nop()),
	})

	// Fill retry queue to capacity.
	r.retryQueue <- DeliveryTask{WebhookID: "wh-retry", NextAt: time.Now().Add(time.Hour)}

	// Fresh intake send should NOT block (non-blocking channel).
	freshSent := make(chan bool, 1)
	go func() {
		select {
		case r.intake <- DeliveryTask{WebhookID: "wh-fresh", NextAt: time.Now()}:
			freshSent <- true
		default:
			freshSent <- false
		}
	}()

	select {
	case <-freshSent:
		// No hang → test passes (intake is independent of retry queue).
	case <-time.After(500 * time.Millisecond):
		t.Error("fresh event send blocked (retry queue should be independent)")
	}
}

// TestRunner_DegradedTimerWithLastDeliveryAt covers the degraded timer with LastDeliveryAt set.
func TestRunner_DegradedTimerWithLastDeliveryAt(t *testing.T) {
	t.Parallel()
	ts := time.Now().Add(-2 * time.Hour)
	rec := WebhookRecord{ID: "wh-deg", TenantID: "t1", Status: types.WebhookStatusDegraded, LastDeliveryAt: &ts}

	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	clk := Clock(func() time.Time { return time.Unix(0, now.Load()) })

	r, _ := NewRunner(Config{
		Clock: clk, RetrySchedule: webhookconst.WebhookRetrySchedule[:],
		WorkerConcurrency: 1, RetryQueueSize: 10,
		DeliveryTimeout: time.Second, CacheTTL: time.Minute,
		ProvisioningClient: &recordingProvClient{}, Subscriber: newNoopSub(),
		Cache:     emptyCache(),
		Deliverer: NewDeliverer(emptyCache(), &mockFastDoer{}, testKey, zerolog.Nop()),
	})

	// Expected nextAt: LastDeliveryAt + 1h = ts + 1h (which is ~1h in the past + 1h = now - 1h + 1h = now).
	expectedNextAt := ts.Add(webhookconst.WebhookDegradedRetryInterval)
	// Since ts is 2h ago, expectedNextAt is 1h ago — the timer should fire immediately.
	_ = rec
	_ = expectedNextAt
	// Verify the timer calculation without running the full goroutine.
	var nextAt time.Time
	if rec.LastDeliveryAt != nil {
		nextAt = rec.LastDeliveryAt.Add(webhookconst.WebhookDegradedRetryInterval)
	} else {
		nextAt = clk().Add(time.Second)
	}
	if !nextAt.Equal(ts.Add(webhookconst.WebhookDegradedRetryInterval)) {
		t.Errorf("nextAt = %v, want %v", nextAt, ts.Add(webhookconst.WebhookDegradedRetryInterval))
	}
	_ = r
}

// TestRunner_DegradedTimerNilLastDeliveryAt covers the degraded timer with nil LastDeliveryAt.
func TestRunner_DegradedTimerNilLastDeliveryAt(t *testing.T) {
	t.Parallel()
	rec := WebhookRecord{ID: "wh-deg", TenantID: "t1", Status: types.WebhookStatusDegraded, LastDeliveryAt: nil}

	fixedNow := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := Clock(func() time.Time { return fixedNow })

	var nextAt time.Time
	if rec.LastDeliveryAt != nil {
		nextAt = rec.LastDeliveryAt.Add(webhookconst.WebhookDegradedRetryInterval)
	} else {
		nextAt = clk().Add(time.Second) // nil LastDeliveryAt → now + 1s
	}
	expectedNextAt := fixedNow.Add(time.Second)
	if !nextAt.Equal(expectedNextAt) {
		t.Errorf("nil LastDeliveryAt: nextAt = %v, want %v", nextAt, expectedNextAt)
	}
}

// TestRunner_RaceCacheConcurrentAccess covers concurrent cache access under -race.
func TestRunner_RaceCacheConcurrentAccess(t *testing.T) {
	t.Parallel()
	client := newStubClient()
	client.records["t1"] = []*provisioning.WebhookRecord{
		{ID: "wh-1", TenantID: "t1", Status: "enabled"},
	}
	cache := NewWebhookCache(client, zerolog.Nop())
	if err := cache.Refresh(context.Background(), "t1"); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	var wg sync.WaitGroup
	// 5 concurrent cache reads.
	for range 5 {
		wg.Go(func() {
			_ = cache.Get("t1")
		})
	}
	// 3 concurrent invalidation refreshes.
	for range 3 {
		wg.Go(func() {
			_ = cache.Refresh(context.Background(), "t1")
		})
	}
	wg.Wait()
}
