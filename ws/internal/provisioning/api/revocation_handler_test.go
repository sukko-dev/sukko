package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"

	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/revocation"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
)

func newTestRevocationHandler() (*RevocationHandler, *chi.Mux) {
	store := revocation.New(zerolog.Nop())
	bus := eventbus.New(zerolog.Nop())
	h := NewRevocationHandler(store, bus, 24*time.Hour, 30, 5, zerolog.Nop())

	r := chi.NewRouter()
	r.Post("/api/v1/tenants/{tenantSlug}/tokens/revoke", h.HandleRevoke)
	return h, r
}

func TestHandleRevoke_ByJTI(t *testing.T) {
	t.Parallel()
	h, router := newTestRevocationHandler()
	t.Cleanup(h.store.Close)

	body, _ := json.Marshal(revocationRequest{JTI: "tok-123"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/acme/tokens/revoke", bytes.NewReader(body))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body: %s", w.Code, w.Body.String())
	}

	var resp revocationResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Type != provapi.RevocationTypeToken {
		t.Errorf("type = %q, want %q", resp.Type, provapi.RevocationTypeToken)
	}
	if resp.TenantID != "acme" {
		t.Errorf("tenant_id = %q, want %q", resp.TenantID, "acme")
	}
}

func TestHandleRevoke_BySub(t *testing.T) {
	t.Parallel()
	h, router := newTestRevocationHandler()
	t.Cleanup(h.store.Close)

	body, _ := json.Marshal(revocationRequest{Sub: "alice"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/acme/tokens/revoke", bytes.NewReader(body))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body: %s", w.Code, w.Body.String())
	}

	var resp revocationResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Type != provapi.RevocationTypeUser {
		t.Errorf("type = %q, want %q", resp.Type, provapi.RevocationTypeUser)
	}
}

func TestHandleRevoke_BothSubAndJTI(t *testing.T) {
	t.Parallel()
	h, router := newTestRevocationHandler()
	t.Cleanup(h.store.Close)

	body, _ := json.Marshal(revocationRequest{Sub: "alice", JTI: "tok-123"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/acme/tokens/revoke", bytes.NewReader(body))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestHandleRevoke_MissingFields(t *testing.T) {
	t.Parallel()
	h, router := newTestRevocationHandler()
	t.Cleanup(h.store.Close)

	body, _ := json.Marshal(revocationRequest{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/acme/tokens/revoke", bytes.NewReader(body))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestHandleRevoke_InvalidJSON(t *testing.T) {
	t.Parallel()
	h, router := newTestRevocationHandler()
	t.Cleanup(h.store.Close)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/acme/tokens/revoke", bytes.NewReader([]byte("not json")))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestHandleRevoke_WithExplicitExp(t *testing.T) {
	t.Parallel()
	h, router := newTestRevocationHandler()
	t.Cleanup(h.store.Close)

	customExp := time.Now().Add(30 * time.Minute).Unix()
	body, _ := json.Marshal(revocationRequest{JTI: "tok-exp", Exp: &customExp})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/acme/tokens/revoke", bytes.NewReader(body))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp revocationResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	parsed, err := time.Parse(time.RFC3339, resp.ExpiresAt)
	if err != nil {
		t.Fatalf("parse expires_at: %v", err)
	}
	// Should be close to the custom exp, not 24h from now
	if parsed.Unix() != customExp {
		t.Errorf("expires_at = %d, want %d", parsed.Unix(), customExp)
	}
}

func TestHandleRevoke_ExpClampedToMax(t *testing.T) {
	t.Parallel()
	// Use a 1h max lifetime so the cap is easy to reason about.
	store := revocation.New(zerolog.Nop())
	bus := eventbus.New(zerolog.Nop())
	h := NewRevocationHandler(store, bus, time.Hour, 30, 5, zerolog.Nop())
	t.Cleanup(store.Close)

	r := chi.NewRouter()
	r.Post("/api/v1/tenants/{tenantSlug}/tokens/revoke", h.HandleRevoke)

	// Supply exp 48h from now — well beyond the 1h maxRevocationLifetime.
	farFutureExp := time.Now().Add(48 * time.Hour).Unix()
	body, _ := json.Marshal(revocationRequest{JTI: "tok-clamp", Exp: &farFutureExp})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/acme/tokens/revoke", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body: %s", w.Code, w.Body.String())
	}

	var resp revocationResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	parsed, err := time.Parse(time.RFC3339, resp.ExpiresAt)
	if err != nil {
		t.Fatalf("parse expires_at: %v", err)
	}
	// expires_at must not exceed now + 1h (the cap).
	maxAllowed := time.Now().Add(time.Hour).Add(5 * time.Second) // +5s for test execution slack
	if parsed.After(maxAllowed) {
		t.Errorf("expires_at %s exceeds maxRevocationLifetime cap (now+1h); client-supplied 48h was not clamped", resp.ExpiresAt)
	}
	// expires_at must not equal the far-future exp the client supplied.
	if parsed.Unix() == farFutureExp {
		t.Errorf("expires_at = client-supplied %d; clamping did not fire", farFutureExp)
	}
}

// TestNewRevocationHandler_RetryAfter covers the computed Retry-After seconds across the default,
// the boundary, and the >60/min floor case (the e2e override raises the rate to 100000) — §VIII.
func TestNewRevocationHandler_RetryAfter(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		perMin  int
		wantSec int
	}{
		{"default 30/min → 2s (historical rate.Every(2s))", 30, 2},
		{"60/min → 1s", 60, 1},
		{"120/min → floored to 1s (60/120=0)", 120, 1},
		{"e2e 100000/min → floored to 1s", 100000, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := revocation.New(zerolog.Nop())
			t.Cleanup(store.Close)
			h := NewRevocationHandler(store, eventbus.New(zerolog.Nop()), time.Hour, tc.perMin, 5, zerolog.Nop())
			if h.retryAfterSeconds != tc.wantSec {
				t.Errorf("perMin=%d: retryAfterSeconds = %d, want %d", tc.perMin, h.retryAfterSeconds, tc.wantSec)
			}
		})
	}
}

func TestHandleRevoke_RateLimited(t *testing.T) {
	t.Parallel()
	h, router := newTestRevocationHandler()
	t.Cleanup(h.store.Close)
	// Override limiter: burst=1 so the second request is rejected immediately.
	h.limiter = newIPRateLimiter(rate.Every(100*time.Second), 1)

	body := []byte(`{"sub":"user-rl"}`)
	send := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/acme/tokens/revoke", bytes.NewReader(body))
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	// First request consumes the burst token and succeeds.
	if w := send(); w.Code != http.StatusOK {
		t.Fatalf("first request: status = %d, want 200", w.Code)
	}
	// Second request exceeds burst and must be rate-limited.
	w := send()
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: status = %d, want 429", w.Code)
	}
	// Retry-After header is required by Constitution §IX.
	if ra := w.Header().Get("Retry-After"); ra == "" {
		t.Error("Retry-After header not set on 429 response")
	}
	var errResp struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if errResp.Code != errCodeRateLimited {
		t.Errorf("code = %q, want %q", errResp.Code, errCodeRateLimited)
	}
}
