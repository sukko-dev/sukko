package runner

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/auth"
)

// setupSuiteRoutingRules is fail-closed on every edition: routing rules are ungated
// (ADR-0014), so no rejection is tolerated. A feature-gate 403 in particular MUST surface
// as an error — tolerating it would hide a regression of the ungating from every delivery
// suite that uses this setup.
func TestSetupSuiteRoutingRules(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr bool
	}{
		{
			name:    "rules accepted",
			status:  http.StatusOK,
			body:    `{}`,
			wantErr: false,
		},
		{
			name:    "feature-gate 403 surfaces as error (ungating regressed)",
			status:  http.StatusForbidden,
			body:    `{"code":"EDITION_LIMIT","message":"This feature requires pro edition or higher"}`,
			wantErr: true,
		},
		{
			name:    "rule count limit surfaces as error",
			status:  http.StatusForbidden,
			body:    `{"code":"EDITION_LIMIT_ROUTING_RULES_PER_TENANT","message":"routing_rules_per_tenant limit reached: 11/10 (community edition)"}`,
			wantErr: true,
		},
		{
			name:    "server error surfaces as error",
			status:  http.StatusInternalServerError,
			body:    `{"code":"INTERNAL_ERROR","message":"boom"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			client := auth.NewProvisioningClient(srv.URL, nil, zerolog.Nop())
			err := setupSuiteRoutingRules(context.Background(), client, "tenant-x", zerolog.Nop())
			if (err != nil) != tt.wantErr {
				t.Errorf("setupSuiteRoutingRules() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// deliveryCheck must name the publish-side cause when the message never left the
// publisher — a bare "missing: []" hides the actual failure (§III).
func TestDeliveryCheck_ReportsPublishError(t *testing.T) {
	t.Parallel()

	check := deliveryCheck("x", DeliveryResult{Delivered: false, PublishErr: errors.New("kafka publish to t: boom")})
	if check.Status != "fail" {
		t.Fatalf("status = %q, want fail", check.Status)
	}
	if !strings.Contains(check.Error, "publish failed: kafka publish to t: boom") {
		t.Fatalf("error = %q, want the publish cause surfaced", check.Error)
	}

	timeout := deliveryCheck("x", DeliveryResult{Delivered: false, Missing: []string{"sub-a"}})
	if !strings.Contains(timeout.Error, "missing: [sub-a]") {
		t.Fatalf("timeout error = %q, want missing list", timeout.Error)
	}
}
