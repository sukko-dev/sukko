package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
)

// mockTestDeliveryClient implements testDeliveryClient for tests.
type mockTestDeliveryClient struct {
	resp *provisioningv1.TestDeliverResponse
	err  error
}

func (m *mockTestDeliveryClient) TestDeliver(_ context.Context, _ *provisioningv1.TestDeliverRequest, _ ...grpc.CallOption) (*provisioningv1.TestDeliverResponse, error) {
	return m.resp, m.err
}

// mockTestRateLimiter implements RateLimiter for tests.
type mockTestRateLimiter struct {
	allowed    bool
	retryAfter time.Duration
	err        error
}

func (m *mockTestRateLimiter) Allow(_ context.Context, _ string) (bool, time.Duration, error) {
	return m.allowed, m.retryAfter, m.err
}

// newTestHandlerRequest builds an http.Request pre-wired with chi URL params and tenant JWT claims.
func newTestHandlerRequest(webhookID, tenantID string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/slug/webhooks/"+webhookID+"/test", http.NoBody)
	// Inject chi URL params.
	chiCtx := chi.NewRouteContext()
	chiCtx.URLParams.Add("webhookID", webhookID)
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, chiCtx))
	// Inject tenant claims.
	return withTenantClaims(r, tenantID)
}

func TestWebhookTestHandler_HandleTestDeliver_OK(t *testing.T) {
	t.Parallel()
	client := &mockTestDeliveryClient{
		resp: &provisioningv1.TestDeliverResponse{
			StatusCode:          200,
			LatencyMs:           42,
			ResponseBodyPreview: `{"ok":true}`,
		},
	}
	rl := &mockTestRateLimiter{allowed: true}
	h, err := NewWebhookTestHandler(client, rl, rl, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewWebhookTestHandler() error = %v", err)
	}

	w := httptest.NewRecorder()
	h.HandleTestDeliver(w, newTestHandlerRequest("wh-1", "tenant-1"))

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	var result TestDeliveryResult
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200", result.StatusCode)
	}
	if result.LatencyMS != 42 {
		t.Errorf("LatencyMS = %d, want 42", result.LatencyMS)
	}
}

func TestWebhookTestHandler_HandleTestDeliver_NotFound(t *testing.T) {
	t.Parallel()
	client := &mockTestDeliveryClient{
		err: status.Error(codes.NotFound, "webhook not found"),
	}
	rl := &mockTestRateLimiter{allowed: true}
	h, _ := NewWebhookTestHandler(client, rl, rl, zerolog.Nop())

	w := httptest.NewRecorder()
	h.HandleTestDeliver(w, newTestHandlerRequest("wh-missing", "tenant-1"))

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), errCodeWebhookNotFound) {
		t.Errorf("body %q should contain %q", w.Body.String(), errCodeWebhookNotFound)
	}
}

func TestWebhookTestHandler_HandleTestDeliver_Unavailable(t *testing.T) {
	t.Parallel()
	client := &mockTestDeliveryClient{
		err: status.Error(codes.Unavailable, "worker not reachable"),
	}
	rl := &mockTestRateLimiter{allowed: true}
	h, _ := NewWebhookTestHandler(client, rl, rl, zerolog.Nop())

	w := httptest.NewRecorder()
	h.HandleTestDeliver(w, newTestHandlerRequest("wh-1", "tenant-1"))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
	if !strings.Contains(w.Body.String(), errCodeWebhookWorkerUnavailable) {
		t.Errorf("body %q should contain %q", w.Body.String(), errCodeWebhookWorkerUnavailable)
	}
}

func TestWebhookTestHandler_HandleTestDeliver_RateLimitExceeded(t *testing.T) {
	t.Parallel()
	client := &mockTestDeliveryClient{resp: &provisioningv1.TestDeliverResponse{StatusCode: 200}}
	rl := &mockTestRateLimiter{allowed: false, retryAfter: 30 * time.Second}
	h, _ := NewWebhookTestHandler(client, rl, rl, zerolog.Nop())

	w := httptest.NewRecorder()
	h.HandleTestDeliver(w, newTestHandlerRequest("wh-1", "tenant-1"))

	if w.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("expected Retry-After header on 429")
	}
}

// TestWebhookTestHandler_HandleTestDeliver_RateLimiterUnavailable covers the fail-open path:
// when the rate-limiter store (Valkey) is unavailable, the handler fails open — returns 200
// and logs a warning instead of returning 429.
func TestWebhookTestHandler_HandleTestDeliver_RateLimiterUnavailable(t *testing.T) {
	t.Parallel()
	client := &mockTestDeliveryClient{
		resp: &provisioningv1.TestDeliverResponse{
			StatusCode: 200,
			LatencyMs:  10,
		},
	}
	// Simulates Valkey being unavailable — RateLimiter returns an error.
	rl := &mockTestRateLimiter{err: errValkeyUnavailable}
	h, _ := NewWebhookTestHandler(client, rl, rl, zerolog.Nop())

	w := httptest.NewRecorder()
	h.HandleTestDeliver(w, newTestHandlerRequest("wh-1", "tenant-1"))

	// Must succeed (fail-open), not return 429.
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (fail-open on rate-limiter error)", w.Code)
	}
}

// errValkeyUnavailable is a sentinel for tests simulating Valkey being down.
var errValkeyUnavailable = errors.New("valkey unavailable")
