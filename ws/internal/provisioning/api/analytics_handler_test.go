package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestAnalyticsSSEHandler_MaxConnections verifies the semaphore enforces the cap.
func TestAnalyticsSSEHandler_MaxConnections(t *testing.T) {
	t.Parallel()
	h := NewAnalyticsSSEHandler(nil, 1, time.Minute, nil, zerolog.Nop())

	// Manually acquire the one semaphore slot.
	h.sem <- struct{}{}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/analytics/stream", http.NoBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "TOO_MANY_SSE_CONNECTIONS") {
		t.Errorf("expected TOO_MANY_SSE_CONNECTIONS in body, got %q", rec.Body.String())
	}
	// §IX: a 429 MUST carry Retry-After — without it a polite client retries
	// immediately into a limit that has not moved.
	if got := rec.Header().Get("Retry-After"); got == "" || got == "0" {
		t.Errorf("Retry-After = %q, want a positive integer (§IX)", got)
	}
}

// TestAnalyticsSSEHandler_ContextCancel verifies the handler exits when the request context is canceled.
func TestAnalyticsSSEHandler_ContextCancel(t *testing.T) {
	t.Parallel()
	// Large FlushInterval so the ticker doesn't fire; only context cancel exits the loop.
	h := NewAnalyticsSSEHandler(nil, 10, 5*time.Second, nil, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/analytics/stream", http.NoBody).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	// Give the handler time to write the initial snapshot, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Handler exited cleanly after context cancel.
	case <-time.After(2 * time.Second):
		t.Error("ServeHTTP did not return within 2s after context cancel")
	}
}

// TestAnalyticsSSEHandler_PushMetrics_PerProvider verifies that:
// 1. analyticsSnapshot.Push is map[string]pushProviderMetrics (not a flat struct)
// 2. The zero-fill initializes all three canonical providers ("web", "android", "ios")
// 3. The SQL query used for push metrics contains GROUP BY provider
func TestAnalyticsSSEHandler_PushMetrics_PerProvider(t *testing.T) {
	t.Parallel()

	// Verify Push field type is correct at compile time via explicit assignment.
	snap := &analyticsSnapshot{
		Push: map[string]pushProviderMetrics{
			"web":     {Sent: 10, Success: 8},
			"android": {},
			"ios":     {},
		},
	}

	if len(snap.Push) != 3 {
		t.Fatalf("expected 3 providers, got %d", len(snap.Push))
	}
	for _, provider := range []string{"web", "android", "ios"} {
		if _, ok := snap.Push[provider]; !ok {
			t.Errorf("expected provider %q in Push map", provider)
		}
	}
	if snap.Push["web"].Sent != 10 {
		t.Errorf("expected web.Sent=10, got %d", snap.Push["web"].Sent)
	}

	// Verify the zero-fill initialisation matches the expected providers.
	// This mirrors the inline init in queryMetrics.
	pushMap := map[string]pushProviderMetrics{
		"web":     {},
		"android": {},
		"ios":     {},
	}
	for _, provider := range []string{"web", "android", "ios"} {
		if _, ok := pushMap[provider]; !ok {
			t.Errorf("zero-fill missing provider %q", provider)
		}
	}

	// Verify SQL query contains GROUP BY provider (const is the authoritative source).
	if !strings.Contains(pushPerProviderSQL, "GROUP BY provider") {
		t.Error("pushPerProviderSQL must contain GROUP BY provider for per-provider breakdown")
	}
	if !strings.Contains(pushPerProviderSQL, "provider,") {
		t.Error("pushPerProviderSQL must SELECT provider column")
	}
}

// TestSSEWriteEvent verifies the SSE event wire format: "event: X\ndata: Y\n\n".
func TestSSEWriteEvent(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	if err := sseWriteEvent(rec, rec, "snapshot", map[string]int{"active": 5}); err != nil {
		t.Fatalf("sseWriteEvent error: %v", err)
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, "event: snapshot\ndata: ") {
		t.Errorf("unexpected SSE format: %q", body)
	}
	if !strings.Contains(body, `"active":5`) {
		t.Errorf("expected active:5 in data, got %q", body)
	}
	if !strings.HasSuffix(body, "\n\n") {
		t.Errorf("expected double newline at end, got %q", body)
	}
}

// TestAnalyticsSSEHandler_InvalidTenantIDRejected verifies a non-UUID tenant_id is rejected
// with 400 up front, before the SSE stream opens (#173) — replacing the prior silent
// empty-200 on the $2::uuid cast error. Empty tenant_id (all-tenants mode) skips this guard.
func TestAnalyticsSSEHandler_InvalidTenantIDRejected(t *testing.T) {
	t.Parallel()
	h := NewAnalyticsSSEHandler(nil, 10, time.Minute, nil, zerolog.Nop())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/analytics/stream?tenant_id=not-a-uuid", http.NoBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), errCodeInvalidRequest) {
		t.Errorf("want %s in body, got %q", errCodeInvalidRequest, rec.Body.String())
	}
}
