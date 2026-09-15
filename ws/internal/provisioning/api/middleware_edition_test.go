package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sukko-dev/sukko/internal/provisioning/api"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

func TestRequireFeature(t *testing.T) {
	t.Parallel()

	passthrough := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	tests := []struct {
		name       string
		edition    license.Edition
		feature    license.Feature
		wantStatus int
		wantCode   string
	}{
		{
			name:       "community blocked from pro feature",
			edition:    license.Community,
			feature:    license.PerTenantConfigurableQuotas,
			wantStatus: http.StatusForbidden,
			wantCode:   api.ErrCodeEditionLimit,
		},
		{
			name:       "pro allowed for pro feature",
			edition:    license.Pro,
			feature:    license.PerTenantConfigurableQuotas,
			wantStatus: http.StatusOK,
		},
		{
			name:       "pro blocked from enterprise feature",
			edition:    license.Pro,
			feature:    license.AuditLogging,
			wantStatus: http.StatusForbidden,
			wantCode:   api.ErrCodeEditionLimit,
		},
		{
			name:       "enterprise allowed for enterprise feature",
			edition:    license.Enterprise,
			feature:    license.AuditLogging,
			wantStatus: http.StatusOK,
		},
		{
			name:       "enterprise allowed for pro feature",
			edition:    license.Enterprise,
			feature:    license.PerTenantConfigurableQuotas,
			wantStatus: http.StatusOK,
		},
		{
			name:       "community blocked from enterprise feature",
			edition:    license.Community,
			feature:    license.AuditLogging,
			wantStatus: http.StatusForbidden,
			wantCode:   api.ErrCodeEditionLimit,
		},
		// Webhook delivery — Pro edition feature gate (§XIII).
		{
			name:       "community blocked from webhooks",
			edition:    license.Community,
			feature:    license.Webhooks,
			wantStatus: http.StatusForbidden,
			wantCode:   api.ErrCodeEditionLimit,
		},
		{
			name:       "pro allowed for webhooks",
			edition:    license.Pro,
			feature:    license.Webhooks,
			wantStatus: http.StatusOK,
		},
		{
			name:       "enterprise allowed for webhooks",
			edition:    license.Enterprise,
			feature:    license.Webhooks,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mgr := license.NewTestManager(tt.edition)
			middleware := api.RequireFeature(mgr, tt.feature)
			handler := middleware(passthrough)

			req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tt.wantStatus)
			}

			if tt.wantCode != "" {
				var resp map[string]string
				if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
					t.Fatalf("unmarshal response: %v", err)
				}
				if resp["code"] != tt.wantCode {
					t.Errorf("code = %q, want %q", resp["code"], tt.wantCode)
				}
			}
		})
	}
}
