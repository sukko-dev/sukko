package runner

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

// newTestProvClient builds a ProvisioningClient pointing at handler, with a nil auth provider
// (setHeaders nil-guards the provider, so admin requests go out unsigned — fine for the fake).
func newTestProvClient(handler http.Handler) (client *auth.ProvisioningClient, closeFn func()) {
	srv := httptest.NewServer(handler)
	client = auth.NewProvisioningClient(srv.URL, nil, zerolog.Nop())
	return client, srv.Close
}

func TestResetRoutingRulesToFixture(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantErr    bool
		wantErrSub string
	}{
		{
			name: "happy path — PUT 200 then GET echoes fixture suffix",
			handler: func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPut:
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"items":[{"pattern":"**","topics":["default"],"priority":100}],"total":1}`))
				case http.MethodGet:
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"items":[{"pattern":"**","topics":["default"],"priority":100}],"total":1,"limit":50,"offset":0}`))
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			},
			wantErr: false,
		},
		{
			name: "PUT failure surfaces error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(`{"code":"ROUTING_RULE_VALIDATION_ERROR","message":"bad"}`))
					return
				}
				w.WriteHeader(http.StatusOK)
			},
			wantErr:    true,
			wantErrSub: "put fixture",
		},
		{
			name: "verify mismatch — GET lacks the fixture suffix surfaces error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPut:
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"items":[],"total":0}`))
				case http.MethodGet:
					// No "default" suffix present → verify must fail.
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"items":[{"pattern":"**","topics":["other"],"priority":100}],"total":1,"limit":50,"offset":0}`))
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			},
			wantErr:    true,
			wantErrSub: "not present after PUT",
		},
		{
			name: "GET failure surfaces error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPut:
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"total":1}`))
				case http.MethodGet:
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"code":"GET_ROUTING_RULES_FAILED"}`))
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			},
			wantErr:    true,
			wantErrSub: "verify",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, closeFn := newTestProvClient(tc.handler)
			defer closeFn()

			err := resetRoutingRulesToFixture(context.Background(), client, "tenant-a")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErrSub)
				}
				if tc.wantErrSub != "" && !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestProvRoutingCheck_Classification(t *testing.T) {
	t.Parallel()

	// The error text mirrors the readError convention: "<op>: HTTP <status>: <json body>".
	gateErr := errors.New(`set routing rules: HTTP 403: {"code":"EDITION_LIMIT","message":"This feature requires pro edition or higher"}`)
	roleErr := errors.New(`set routing rules: HTTP 403: {"code":"INSUFFICIENT_ROLE","message":"admin role required"}`)
	validationErr := errors.New(`set routing rules: HTTP 400: {"code":"ROUTING_RULE_VALIDATION_ERROR","message":"bad"}`)

	tests := []struct {
		name       string
		fn         func() error
		wantStatus string
	}{
		{
			name:       "2xx (nil error) → pass",
			fn:         func() error { return nil },
			wantStatus: metrics.CheckStatusPass,
		},
		{
			name:       "gate 403 EDITION_LIMIT → skip",
			fn:         func() error { return gateErr },
			wantStatus: metrics.CheckStatusSkip,
		},
		{
			name:       "other 403 (INSUFFICIENT_ROLE) → fail",
			fn:         func() error { return roleErr },
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "400 validation → fail",
			fn:         func() error { return validationErr },
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "opaque error (no code) → fail",
			fn:         func() error { return errors.New("connection refused") },
			wantStatus: metrics.CheckStatusFail,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := provRoutingCheck("probe", tc.fn)
			if got.Status != tc.wantStatus {
				t.Fatalf("provRoutingCheck status = %q, want %q (result=%+v)", got.Status, tc.wantStatus, got)
			}
			if got.Name != "probe" {
				t.Fatalf("provRoutingCheck name = %q, want %q", got.Name, "probe")
			}
			// A skip MUST never be reported as a pass (skip-never-pass invariant).
			if tc.wantStatus == metrics.CheckStatusSkip && got.Status == metrics.CheckStatusPass {
				t.Fatal("gate rejection classified as pass — skip-never-pass invariant violated")
			}
		})
	}
}

func TestExpectReject(t *testing.T) {
	t.Parallel()

	rejErr := fmt.Errorf("op: HTTP 409: %s", `{"code":"ROUTING_RULE_DUPLICATE_PATTERN","message":"dup"}`)

	tests := []struct {
		name       string
		status     int
		err        error
		wantStatus int
		wantCode   string
		wantOK     bool
	}{
		{
			name:       "exact status and code → nil",
			status:     http.StatusConflict,
			err:        rejErr,
			wantStatus: http.StatusConflict,
			wantCode:   errCodeRoutingRuleDuplicatePattern,
			wantOK:     true,
		},
		{
			name:       "right status wrong code → error",
			status:     http.StatusConflict,
			err:        fmt.Errorf("op: HTTP 409: %s", `{"code":"ROUTING_RULE_DUPLICATE_PRIORITY"}`),
			wantStatus: http.StatusConflict,
			wantCode:   errCodeRoutingRuleDuplicatePattern,
			wantOK:     false,
		},
		{
			name:       "wrong status right code → error",
			status:     http.StatusBadRequest,
			err:        rejErr,
			wantStatus: http.StatusConflict,
			wantCode:   errCodeRoutingRuleDuplicatePattern,
			wantOK:     false,
		},
		{
			name:       "no error (2xx) → error (not a rejection)",
			status:     http.StatusCreated,
			err:        nil,
			wantStatus: http.StatusConflict,
			wantCode:   errCodeRoutingRuleDuplicatePattern,
			wantOK:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := expectReject(tc.name, tc.status, tc.err, tc.wantStatus, tc.wantCode)
			if tc.wantOK && got != nil {
				t.Fatalf("expected nil, got %v", got)
			}
			if !tc.wantOK && got == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}
