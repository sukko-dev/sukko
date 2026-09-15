package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

func TestHandleUploadCredentials_ValidationErrors(t *testing.T) {
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
			body:       `{"provider":"fcm","credential_data":"{}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "MISSING_TENANT_ID",
		},
		{
			name:       "missing provider",
			body:       `{"tenant_id":"acme","credential_data":"{}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "MISSING_PROVIDER",
		},
		{
			name:       "invalid provider",
			body:       `{"tenant_id":"acme","provider":"invalid","credential_data":"{}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_PROVIDER",
		},
		{
			name:       "missing credential_data",
			body:       `{"tenant_id":"acme","provider":"fcm"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "MISSING_CREDENTIAL_DATA",
		},
		{
			name:       "credential_data not valid JSON",
			body:       `{"tenant_id":"acme","provider":"fcm","credential_data":"not-json"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
		{
			name:       "fcm missing project_id",
			body:       `{"tenant_id":"acme","provider":"fcm","credential_data":"{\"private_key\":\"pk\",\"client_email\":\"a@b.com\"}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
		{
			name:       "fcm missing private_key",
			body:       `{"tenant_id":"acme","provider":"fcm","credential_data":"{\"project_id\":\"p\",\"client_email\":\"a@b.com\"}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
		{
			name:       "fcm missing client_email",
			body:       `{"tenant_id":"acme","provider":"fcm","credential_data":"{\"project_id\":\"p\",\"private_key\":\"pk\"}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
		{
			name:       "apns missing key_id",
			body:       `{"tenant_id":"acme","provider":"apns","credential_data":"{\"team_id\":\"t\",\"bundle_id\":\"b\",\"p8_key\":\"k\"}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
		{
			name:       "apns missing team_id",
			body:       `{"tenant_id":"acme","provider":"apns","credential_data":"{\"key_id\":\"k\",\"bundle_id\":\"b\",\"p8_key\":\"k\"}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
		{
			name:       "apns missing bundle_id",
			body:       `{"tenant_id":"acme","provider":"apns","credential_data":"{\"key_id\":\"k\",\"team_id\":\"t\",\"p8_key\":\"k\"}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
		{
			name:       "apns missing p8_key",
			body:       `{"tenant_id":"acme","provider":"apns","credential_data":"{\"key_id\":\"k\",\"team_id\":\"t\",\"bundle_id\":\"b\"}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
		{
			name:       "vapid missing public_key",
			body:       `{"tenant_id":"acme","provider":"vapid","credential_data":"{\"private_key\":\"pk\"}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
		{
			name:       "vapid missing private_key",
			body:       `{"tenant_id":"acme","provider":"vapid","credential_data":"{\"public_key\":\"pub\"}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
		{
			name:       "fcm invalid type",
			body:       `{"tenant_id":"acme","provider":"fcm","credential_data":"{\"project_id\":\"p\",\"private_key\":\"pk\",\"client_email\":\"a@b.com\",\"type\":\"not_service_account\"}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "FCM_CONNECTIVITY_FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// We create a handler with nil credentialsRepo. Validation errors
			// are caught before the repo is called, so this is safe for
			// validation-only tests.
			h := &PushCredentialHandler{
				credentialsRepo: nil, // validation tests don't reach repo
				eventBus:        bus,
				logger:          logger,
			}

			req := httptest.NewRequest(http.MethodPost, "/api/v1/push/credentials", bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			h.HandleUploadCredentials(w, req)

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

func TestHandleDeleteCredentials_ValidationErrors(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	bus := eventbus.New(logger)

	tests := []struct {
		name       string
		query      string
		body       string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "missing tenant_id",
			query:      "provider=fcm",
			wantStatus: http.StatusBadRequest,
			wantCode:   "MISSING_TENANT_ID",
		},
		{
			name:       "missing provider",
			query:      "tenant_id=acme",
			wantStatus: http.StatusBadRequest,
			wantCode:   "MISSING_PROVIDER",
		},
		{
			name:       "invalid provider",
			query:      "tenant_id=acme&provider=invalid",
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_PROVIDER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := &PushCredentialHandler{
				credentialsRepo: nil,
				eventBus:        bus,
				logger:          logger,
			}

			url := "/api/v1/push/credentials"
			if tt.query != "" {
				url += "?" + tt.query
			}

			req := httptest.NewRequest(http.MethodDelete, url, http.NoBody)
			w := httptest.NewRecorder()

			h.HandleDeleteCredentials(w, req)

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

func TestValidateCredentialData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		provider string
		data     string
		wantErr  bool
	}{
		{
			name:     "valid fcm",
			provider: "fcm",
			data:     `{"project_id":"p","private_key":"pk","client_email":"a@b.com"}`,
			wantErr:  false,
		},
		{
			name:     "valid apns",
			provider: "apns",
			data:     `{"key_id":"k","team_id":"t","bundle_id":"b","p8_key":"pk"}`,
			wantErr:  false,
		},
		{
			name:     "valid vapid",
			provider: "vapid",
			data:     `{"public_key":"pub","private_key":"priv"}`,
			wantErr:  false,
		},
		{
			name:     "invalid json",
			provider: "fcm",
			data:     `not-json`,
			wantErr:  true,
		},
		{
			name:     "fcm empty project_id",
			provider: "fcm",
			data:     `{"project_id":"","private_key":"pk","client_email":"a@b.com"}`,
			wantErr:  true,
		},
		{
			name:     "vapid numeric private_key",
			provider: "vapid",
			data:     `{"public_key":"pub","private_key":123}`,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateCredentialData(tt.provider, tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateCredentialData(%q, ...) error = %v, wantErr = %v", tt.provider, err, tt.wantErr)
			}
		})
	}
}

func TestTestFCMConnectivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{
			name:    "valid service account",
			data:    `{"type":"service_account","project_id":"p","private_key":"pk","client_email":"a@b.com"}`,
			wantErr: false,
		},
		{
			name:    "missing type is ok",
			data:    `{"project_id":"p","private_key":"pk","client_email":"a@b.com"}`,
			wantErr: false,
		},
		{
			name:    "wrong type",
			data:    `{"type":"authorized_user","project_id":"p","private_key":"pk","client_email":"a@b.com"}`,
			wantErr: true,
		},
		{
			name:    "invalid json",
			data:    `not-json`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := testFCMConnectivity(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("testFCMConnectivity() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewPushCredentialHandler_Validation(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	bus := eventbus.New(logger)

	t.Run("nil credentials repo", func(t *testing.T) {
		t.Parallel()
		_, err := NewPushCredentialHandler(nil, nil, bus, logger, nil)
		if err == nil {
			t.Error("expected error for nil credentials repo")
		}
	})

	// nil resolver / nil event bus require a non-nil repo (needs a DB) to reach — the nil-repo
	// check fires first here. The nil-resolver path is asserted in the integration test
	// (push_credentials_integration_test.go) where a real repo is available.
}

// Per-provider edition gate (ADR-0009): vapid needs WebPush (Pro); fcm/apns
// need MobilePush (Enterprise). The gate fires after provider validation and
// before credential-data validation.
func TestHandleUploadCredentials_PerProviderEditionGate(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	bus := eventbus.New(logger)

	tests := []struct {
		name       string
		edition    license.Edition
		body       string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "pro uploading fcm is denied",
			edition:    license.Pro,
			body:       `{"tenant_id":"acme","provider":"fcm","credential_data":"{}"}`,
			wantStatus: http.StatusForbidden,
			wantCode:   "EDITION_LIMIT",
		},
		{
			name:       "pro uploading apns is denied",
			edition:    license.Pro,
			body:       `{"tenant_id":"acme","provider":"apns","credential_data":"{}"}`,
			wantStatus: http.StatusForbidden,
			wantCode:   "EDITION_LIMIT",
		},
		{
			// Gate passes for vapid on Pro — request proceeds to credential
			// validation (invalid data here, proving we got past the gate).
			name:       "pro uploading vapid passes the gate",
			edition:    license.Pro,
			body:       `{"tenant_id":"acme","provider":"vapid","credential_data":"{}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
		{
			// Enterprise passes the gate for fcm — proceeds to validation.
			name:       "enterprise uploading fcm passes the gate",
			edition:    license.Enterprise,
			body:       `{"tenant_id":"acme","provider":"fcm","credential_data":"{}"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_CREDENTIAL_DATA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := &PushCredentialHandler{
				credentialsRepo: nil, // gate + validation fire before repo
				eventBus:        bus,
				logger:          logger,
				editionManager:  license.NewTestManager(tt.edition),
			}

			req := httptest.NewRequest(http.MethodPost, "/api/v1/push/credentials", bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			h.HandleUploadCredentials(w, req)

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
