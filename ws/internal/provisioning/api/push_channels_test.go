package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
)

func TestHandleCreateChannelConfig_ValidationErrors(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	bus := eventbus.New(logger)

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "invalid JSON",
			body:       `{invalid`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_REQUEST",
		},
		{
			name:       "missing tenant_id",
			body:       `{"patterns":["acme.alerts.*"]}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "MISSING_TENANT_ID",
		},
		{
			name:       "empty patterns",
			body:       `{"tenant_id":"acme","patterns":[]}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "MISSING_PATTERNS",
		},
		{
			name:       "missing patterns field",
			body:       `{"tenant_id":"acme"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "MISSING_PATTERNS",
		},
		{
			name:       "pattern wrong tenant prefix",
			body:       `{"tenant_id":"acme","patterns":["other.alerts.*"]}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_PATTERN",
		},
		{
			name:       "pattern no dot separator",
			body:       `{"tenant_id":"acme","patterns":["acme"]}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_PATTERN",
		},
		{
			name:       "pattern similar prefix but no match",
			body:       `{"tenant_id":"acme","patterns":["acmex.alerts.*"]}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_PATTERN",
		},
		{
			name:       "invalid urgency",
			body:       `{"tenant_id":"acme","patterns":["acme.alerts.*"],"default_urgency":"critical"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_URGENCY",
		},
		{
			name:       "one valid one invalid pattern",
			body:       `{"tenant_id":"acme","patterns":["acme.alerts.*","other.data.*"]}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_PATTERN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := &PushChannelHandler{
				channelConfigRepo: nil, // validation tests don't reach repo
				eventBus:          bus,
				logger:            logger,
			}

			req := httptest.NewRequest(http.MethodPost, "/api/v1/push/channels", bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			h.HandleCreateChannelConfig(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body = %s", w.Code, tt.wantStatus, w.Body.String())
			}

			var resp map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("unmarshal response: %v", err)
			}
			if resp["code"] != tt.wantCode {
				t.Errorf("code = %q, want %q; message = %q", resp["code"], tt.wantCode, resp["message"])
			}
		})
	}
}

func TestHandleGetChannelConfig_MissingTenantID(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	bus := eventbus.New(logger)

	h := &PushChannelHandler{
		channelConfigRepo: nil,
		eventBus:          bus,
		logger:            logger,
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/push/channels", http.NoBody)
	w := httptest.NewRecorder()

	h.HandleGetChannelConfig(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp["code"] != "MISSING_TENANT_ID" {
		t.Errorf("code = %q, want MISSING_TENANT_ID", resp["code"])
	}
}

func TestHandleDeleteChannelConfig_ValidationErrors(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	bus := eventbus.New(logger)

	tests := []struct {
		name       string
		query      string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "missing tenant_id",
			query:      "",
			wantStatus: http.StatusBadRequest,
			wantCode:   "MISSING_TENANT_ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := &PushChannelHandler{
				channelConfigRepo: nil,
				eventBus:          bus,
				logger:            logger,
			}

			url := "/api/v1/push/channels"
			if tt.query != "" {
				url += "?" + tt.query
			}

			req := httptest.NewRequest(http.MethodDelete, url, http.NoBody)
			w := httptest.NewRecorder()

			h.HandleDeleteChannelConfig(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body = %s", w.Code, tt.wantStatus, w.Body.String())
			}

			var resp map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("unmarshal response: %v", err)
			}
			if resp["code"] != tt.wantCode {
				t.Errorf("code = %q, want %q", resp["code"], tt.wantCode)
			}
		})
	}
}

func TestValidUrgencyValues(t *testing.T) {
	t.Parallel()

	// Verify all valid urgency values are in the validUrgencies map.
	expected := []string{"very-low", "low", "normal", "high"}
	for _, u := range expected {
		if !validUrgencies[u] {
			t.Errorf("urgency %q should be valid but is not in validUrgencies map", u)
		}
	}

	// Verify invalid values are rejected.
	invalid := []string{"critical", "urgent", "", "NORMAL"}
	for _, u := range invalid {
		if validUrgencies[u] {
			t.Errorf("urgency %q should be invalid but is in validUrgencies map", u)
		}
	}
}

func TestDefaultPushTTL(t *testing.T) {
	t.Parallel()

	// 28 days in seconds
	if defaultPushTTL != 2419200 {
		t.Errorf("defaultPushTTL = %d, want 2419200", defaultPushTTL)
	}
}

func TestNewPushChannelHandler_Validation(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	bus := eventbus.New(logger)

	t.Run("nil channel config repo", func(t *testing.T) {
		t.Parallel()
		_, err := NewPushChannelHandler(nil, nil, bus, logger)
		if err == nil {
			t.Error("expected error for nil channel config repo")
		}
	})

	// nil resolver / nil event bus need a non-nil repo (DB) to reach; nil-repo fires first.
	// The nil-resolver path is asserted in push_channels_integration_test.go (real repo).
}
