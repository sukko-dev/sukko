package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

func TestRequireFeature(t *testing.T) {
	t.Parallel()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	tests := []struct {
		name       string
		edition    license.Edition
		feature    license.Feature
		wantStatus int
		wantBody   string // substring match
	}{
		{
			name:       "Pro edition allows SSE",
			edition:    license.Pro,
			feature:    license.SSETransport,
			wantStatus: http.StatusOK,
			wantBody:   "OK",
		},
		{
			name:       "Enterprise edition allows SSE",
			edition:    license.Enterprise,
			feature:    license.SSETransport,
			wantStatus: http.StatusOK,
			wantBody:   "OK",
		},
		{
			name:       "Community edition blocks SSE",
			edition:    license.Community,
			feature:    license.SSETransport,
			wantStatus: http.StatusForbidden,
			wantBody:   "EDITION_LIMIT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mgr := license.NewTestManager(tt.edition)
			gate := RequireFeature(mgr, tt.feature)
			handler := gate(inner)

			req := httptest.NewRequest(http.MethodGet, "/sse", http.NoBody)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}

			body := rec.Body.String()
			if tt.wantBody != "" && !strings.Contains(body, tt.wantBody) {
				t.Errorf("body = %q, want to contain %q", body, tt.wantBody)
			}
		})
	}
}

func TestRequireFeature_NilManager(t *testing.T) {
	t.Parallel()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// nil manager should pass through (graceful degradation)
	gate := RequireFeature(nil, license.SSETransport)
	handler := gate(inner)

	req := httptest.NewRequest(http.MethodGet, "/sse", http.NoBody)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("nil manager should pass through, got status %d", rec.Code)
	}
}

// Verify error response format matches httputil.ErrorResponse
func TestRequireFeature_ErrorFormat(t *testing.T) {
	t.Parallel()

	mgr := license.NewTestManager(license.Community)
	gate := RequireFeature(mgr, license.SSETransport)
	handler := gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))

	req := httptest.NewRequest(http.MethodGet, "/sse", http.NoBody)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var errResp struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to parse error response: %v", err)
	}
	if errResp.Code != "EDITION_LIMIT" {
		t.Errorf("error code = %q, want EDITION_LIMIT", errResp.Code)
	}
	// The message must name the feature's actual required edition (pro here) —
	// not a hardcoded tier.
	if !strings.Contains(strings.ToLower(errResp.Message), "pro") {
		t.Errorf("message = %q, want it to name the required edition (pro)", errResp.Message)
	}
}

// The denial message must reflect the feature's own required edition — an
// Enterprise feature must say "enterprise", not a hardcoded "Pro".
func TestRequireFeature_ErrorNamesRequiredEdition(t *testing.T) {
	t.Parallel()

	mgr := license.NewTestManager(license.Community)
	gate := RequireFeature(mgr, license.MobilePush) // Enterprise feature
	handler := gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", http.NoBody)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var errResp struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to parse error response: %v", err)
	}
	if !strings.Contains(strings.ToLower(errResp.Message), "enterprise") {
		t.Errorf("message = %q, want it to name the required edition (enterprise)", errResp.Message)
	}
}
