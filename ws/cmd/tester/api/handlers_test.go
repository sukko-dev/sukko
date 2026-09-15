package api

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/runner"
)

func newTestRouter() (http.Handler, *runner.Runner) {
	r := runner.New(runner.Config{
		GatewayURL:      "ws://localhost:3000",
		ProvisioningURL: "http://localhost:8080",
	}, zerolog.Nop())
	return NewRouter(r, "test-auth", "", zerolog.Nop()), r
}

func TestHealth(t *testing.T) {
	t.Parallel()

	handler, _ := newTestRouter()
	req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestVersion(t *testing.T) {
	t.Parallel()

	handler, _ := newTestRouter()
	req := httptest.NewRequest(http.MethodGet, "/version", http.NoBody)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["service"] != "sukko-tester" {
		t.Errorf("service = %v, want sukko-tester", resp["service"])
	}
}

func TestStartTest_NoAuth(t *testing.T) {
	t.Parallel()

	handler, _ := newTestRouter()
	body := `{"type":"smoke"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestStartTest_InvalidType(t *testing.T) {
	t.Parallel()

	handler, _ := newTestRouter()
	body := `{"type":"invalid"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestStartTest_MissingType(t *testing.T) {
	t.Parallel()

	handler, _ := newTestRouter()
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestStartTest_InvalidBody(t *testing.T) {
	t.Parallel()

	handler, _ := newTestRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString("not json"))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestStartTest_Success(t *testing.T) {
	t.Parallel()

	handler, r := newTestRouter()
	body := `{"type":"smoke"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["id"] == nil || resp["id"] == "" {
		t.Error("expected non-empty id")
	}
	if resp["status"] != "running" {
		t.Errorf("status = %v, want running", resp["status"])
	}

	// Cleanup
	id := resp["id"].(string)
	_ = r.Stop(id)
	r.Wait()
}

func TestGetTest_NotFound(t *testing.T) {
	t.Parallel()

	handler, _ := newTestRouter()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tests/nonexistent", http.NoBody)
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestStopTest_NotFound(t *testing.T) {
	t.Parallel()

	handler, _ := newTestRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests/nonexistent/stop", http.NoBody)
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestAuthMiddleware_EmptyToken(t *testing.T) {
	t.Parallel()

	// Router with empty auth token — all requests should pass through
	r := runner.New(runner.Config{
		GatewayURL: "ws://localhost:3000",
	}, zerolog.Nop())
	handler := NewRouter(r, "", "", zerolog.Nop()) // no auth token

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tests/any", http.NoBody)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should reach the handler (404 because test doesn't exist, not 401)
	if w.Code == http.StatusUnauthorized {
		t.Error("expected no auth enforcement when token is empty")
	}
}

func TestAuthMiddleware_WrongToken(t *testing.T) {
	t.Parallel()

	handler, _ := newTestRouter()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tests/any", http.NoBody)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestStartTest_TenantIDPassthrough(t *testing.T) {
	t.Parallel()

	handler, r := newTestRouter()
	body := `{"type":"smoke","tenant_id":"my-tenant"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusCreated)
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	id, _ := resp["id"].(string)

	run, err := r.Get(id)
	if err != nil {
		t.Fatalf("Get(%q): %v", id, err)
	}

	if run.Config.TenantID != "my-tenant" {
		t.Errorf("TenantID = %q, want %q", run.Config.TenantID, "my-tenant")
	}

	_ = r.Stop(id)
	r.Wait()
}

func TestStartTest_AllTypes(t *testing.T) {
	t.Parallel()

	validTypes := []string{"smoke", "load", "stress", "soak", "validate"}
	for _, typ := range validTypes {
		t.Run(typ, func(t *testing.T) {
			t.Parallel()

			handler, r := newTestRouter()
			body := `{"type":"` + typ + `"}`
			req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
			req.Header.Set("Authorization", "Bearer test-auth")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != http.StatusCreated {
				t.Errorf("status = %d, want %d for type %q", w.Code, http.StatusCreated, typ)
			}

			var resp map[string]any
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if id, ok := resp["id"].(string); ok {
				_ = r.Stop(id)
			}
			r.Wait()
		})
	}
}

// signingKeyP256PEM returns a valid P-256 PKCS#8 PEM as a JSON-safe single-line
// string (newlines escaped) so it can be embedded in a JSON request body.
func signingKeyP256PEM(t *testing.T) string {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate P-256 key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal PKCS#8: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

// jsonString marshals s into a JSON string literal (quotes + escaping) so raw
// PEM text (with newlines) can be embedded safely in a request body.
func jsonString(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal json string: %v", err)
	}
	return string(b)
}

// TestStartTest_SigningKey_ValidPEM proves signing_key is consumed as raw PEM:
// a valid P-256 PKCS#8 PEM string is accepted (201).
func TestStartTest_SigningKey_ValidPEM(t *testing.T) {
	t.Parallel()
	pemStr := signingKeyP256PEM(t)

	handler, r := newTestRouter()
	body := fmt.Sprintf(`{"type":"validate","suite":"license-reload","signing_key":%s}`, jsonString(t, pemStr))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if id, ok := resp["id"].(string); ok {
		_ = r.Stop(id)
	}
	r.Wait()
}

// TestStartTest_SigningKey_NotPEM asserts a non-PEM value is rejected with the
// new PEM-parse error code and message.
func TestStartTest_SigningKey_NotPEM(t *testing.T) {
	t.Parallel()
	handler, _ := newTestRouter()
	body := `{"type":"validate","suite":"license-reload","signing_key":"not-a-pem-key"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["code"] != errCodeInvalidSignKey {
		t.Errorf("code = %v, want %s", resp["code"], errCodeInvalidSignKey)
	}
}

// TestStartTest_SigningKey_Base64OfPEMRejected proves the base64-decode step is
// gone: base64-encoding a valid PEM (the OLD wire format) is now rejected,
// because the field is parsed as raw PEM text.
func TestStartTest_SigningKey_Base64OfPEMRejected(t *testing.T) {
	t.Parallel()
	pemStr := signingKeyP256PEM(t)
	base64OfPEM := base64.StdEncoding.EncodeToString([]byte(pemStr))

	handler, _ := newTestRouter()
	body := fmt.Sprintf(`{"type":"validate","suite":"license-reload","signing_key":%s}`, jsonString(t, base64OfPEM))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d (base64-of-PEM must be rejected)", w.Code, http.StatusBadRequest)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["code"] != errCodeInvalidSignKey {
		t.Errorf("code = %v, want %s", resp["code"], errCodeInvalidSignKey)
	}
}

func TestStartTest_SigningKey_NotProvided(t *testing.T) {
	t.Parallel()
	handler, r := newTestRouter()
	body := `{"type":"smoke"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should succeed — signing_key is optional, only needed for license-reload suite
	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d", w.Code, http.StatusCreated)
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if id, ok := resp["id"].(string); ok {
		_ = r.Stop(id)
	}
	r.Wait()
}

func TestStartTest_AdminKey_OverridesConfigured(t *testing.T) {
	t.Parallel()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(priv)

	handler, r := newTestRouter()
	body := fmt.Sprintf(`{"type":"smoke","admin_key":"%s"}`, encoded) //nolint:gocritic // sprintfQuotedString: %s is correct — value is inside a raw JSON template
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	id, _ := resp["id"].(string)

	run, err := r.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(run.Config.AdminKeyBytes, []byte(priv)) {
		t.Error("AdminKeyBytes does not match decoded admin_key")
	}

	_ = r.Stop(id)
	r.Wait()
}

func TestStartTest_AdminKey_Empty(t *testing.T) {
	t.Parallel()

	handler, r := newTestRouter()
	body := `{"type":"smoke"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	id, _ := resp["id"].(string)

	run, err := r.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if run.Config.AdminKeyBytes != nil {
		t.Errorf("AdminKeyBytes = %x, want nil", run.Config.AdminKeyBytes)
	}

	_ = r.Stop(id)
	r.Wait()
}

func TestStartTest_AdminKey_LocalDevMode(t *testing.T) {
	t.Parallel()

	// Router without TESTER_ADMIN_KEY_FILE (local dev mode) — per-request admin_key still works.
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(priv)

	handler, r := newTestRouter()                                     // newTestRouter passes empty adminKeyID → local dev mode
	body := fmt.Sprintf(`{"type":"smoke","admin_key":"%s"}`, encoded) //nolint:gocritic // sprintfQuotedString: %s is correct
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	id, _ := resp["id"].(string)

	run, err := r.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(run.Config.AdminKeyBytes, []byte(priv)) {
		t.Error("AdminKeyBytes does not match body key in local dev mode")
	}

	_ = r.Stop(id)
	r.Wait()
}

func TestStartTest_AdminKeyID_OverridesDefault(t *testing.T) {
	t.Parallel()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(priv)

	rr := runner.New(runner.Config{GatewayURL: "ws://localhost:3000"}, zerolog.Nop())
	h := NewRouter(rr, "test-auth", "default-key-id", zerolog.Nop())

	body := fmt.Sprintf(`{"type":"smoke","admin_key":"%s","admin_key_id":"override-kid"}`, encoded) //nolint:gocritic // sprintfQuotedString: %s is correct — value is inside a raw JSON template, %q would double-escape
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	id, _ := resp["id"].(string)

	run, getErr := rr.Get(id)
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if run.Config.AdminKeyID != "override-kid" {
		t.Errorf("AdminKeyID = %q, want %q", run.Config.AdminKeyID, "override-kid")
	}

	_ = rr.Stop(id)
	rr.Wait()
}

func TestStartTest_AdminKey_DefaultKID(t *testing.T) {
	t.Parallel()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(priv)

	rr := runner.New(runner.Config{GatewayURL: "ws://localhost:3000"}, zerolog.Nop())
	h := NewRouter(rr, "test-auth", "configured-key-id", zerolog.Nop())

	body := fmt.Sprintf(`{"type":"smoke","admin_key":"%s"}`, encoded) //nolint:gocritic // sprintfQuotedString
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	id, _ := resp["id"].(string)

	run, getErr := rr.Get(id)
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	// When admin_key is provided without admin_key_id, effective kid = h.adminKeyID
	if run.Config.AdminKeyID != "configured-key-id" {
		t.Errorf("AdminKeyID = %q, want %q", run.Config.AdminKeyID, "configured-key-id")
	}

	_ = rr.Stop(id)
	rr.Wait()
}

func TestStartTest_AdminKeyPath_DeprecationWarned(t *testing.T) {
	t.Parallel()

	// Use a log buffer to capture Warn output.
	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf)

	rr := runner.New(runner.Config{GatewayURL: "ws://localhost:3000"}, zerolog.Nop())
	h := NewRouter(rr, "test-auth", "", logger)

	body := `{"type":"smoke","context":{"gateway_url":"ws://gw","provisioning_url":"http://prov","environment":"test","admin_key_path":"/some/path.bin"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "admin_key_path") {
		t.Errorf("expected deprecation warning containing 'admin_key_path' in log, got: %s", logOutput)
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	id, _ := resp["id"].(string)

	run, getErr := rr.Get(id)
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if run.Config.AdminKeyBytes != nil {
		t.Errorf("AdminKeyBytes = %x, want nil (admin_key_path is deprecated, not decoded)", run.Config.AdminKeyBytes)
	}
	if run.Config.AdminKeyID != "" {
		t.Errorf("AdminKeyID = %q, want empty string", run.Config.AdminKeyID)
	}

	_ = rr.Stop(id)
	rr.Wait()
}

func TestStartTest_AdminKeyID_WithoutAdminKey_Returns400(t *testing.T) {
	t.Parallel()

	rr := runner.New(runner.Config{GatewayURL: "ws://localhost:3000"}, zerolog.Nop())
	h := NewRouter(rr, "", "", zerolog.Nop())

	body := `{"type":"smoke","admin_key_id":"bootstrap-0"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if got, _ := resp["code"].(string); got != "INVALID_ADMIN_KEY_ID" {
		t.Errorf("code = %q, want %q", got, "INVALID_ADMIN_KEY_ID")
	}
}

func TestStartTest_AuthModeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			// mixed mode not valid for validate
			name:       "auth_mode=mixed + type=validate → 400",
			body:       `{"type":"validate","auth_mode":"mixed"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			// api-key mode not valid for load (only validate allowed)
			name:       "auth_mode=api-key + type=load → 400",
			body:       `{"type":"load","auth_mode":"api-key"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			// upgrade mode not valid for load (only validate allowed)
			name:       "auth_mode=upgrade + type=load → 400",
			body:       `{"type":"load","auth_mode":"upgrade"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			// api-key + validate + suite=api-key + tenant_id → 201
			name:       "auth_mode=api-key + type=validate + suite=api-key + tenant_id → 201",
			body:       `{"type":"validate","auth_mode":"api-key","suite":"api-key","tenant_id":"test-tenant"}`,
			wantStatus: http.StatusCreated,
		},
		{
			// api-key + validate + suite=auth is not in the allowlist (api-key, rest-publish)
			name:       "auth_mode=api-key + type=validate + suite=auth → 400",
			body:       `{"type":"validate","auth_mode":"api-key","suite":"auth","tenant_id":"test-tenant"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			// upgrade mode only supports suite=upgrade; suite=channels is rejected
			name:       "auth_mode=upgrade + type=validate + suite=channels → 400",
			body:       `{"type":"validate","auth_mode":"upgrade","suite":"channels","tenant_id":"test-tenant"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			// unrecognized auth_mode value → 400
			name:       "auth_mode=invalid → 400",
			body:       `{"type":"smoke","auth_mode":"invalid"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			// auth_mix_ratio outside [0,1] (non-zero) → 400
			name:       "auth_mix_ratio=1.5 → 400",
			body:       `{"type":"load","auth_mix_ratio":1.5}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			// auth_mix_ratio=0.0 is treated as "omitted" (float64 zero value) → uses default; valid for load
			name:       "auth_mix_ratio=0.0 → 201",
			body:       `{"type":"load","auth_mix_ratio":0.0}`,
			wantStatus: http.StatusCreated,
		},
		{
			// mixed mode is valid for load
			name:       "auth_mode=mixed + type=load → 201",
			body:       `{"type":"load","auth_mode":"mixed"}`,
			wantStatus: http.StatusCreated,
		},
		{
			// api-key + validate + empty tenant_id is now accepted: the runner self-provisions a
			// throwaway tenant. Guard removed in lockstep with the runner.
			name:       "auth_mode=api-key + type=validate + suite=api-key + empty tenant_id → 201",
			body:       `{"type":"validate","auth_mode":"api-key","suite":"api-key"}`,
			wantStatus: http.StatusCreated,
		},
		{
			// upgrade + validate + empty tenant_id is now accepted: auth.Setup self-provisions a
			// throwaway tenant. Guard removed in lockstep with the runner.
			name:       "auth_mode=upgrade + type=validate + suite=upgrade + empty tenant_id → 201",
			body:       `{"type":"validate","auth_mode":"upgrade","suite":"upgrade"}`,
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, r := newTestRouter()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(tt.body))
			req.Header.Set("Authorization", "Bearer test-auth")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body: %s", w.Code, tt.wantStatus, w.Body.String())
			}

			if tt.wantStatus == http.StatusCreated {
				var resp map[string]any
				_ = json.Unmarshal(w.Body.Bytes(), &resp)
				if id, ok := resp["id"].(string); ok {
					_ = r.Stop(id)
				}
				r.Wait()
			}
		})
	}
}

func TestStartTest_MissingAuthMode_DefaultsToJWT(t *testing.T) {
	t.Parallel()

	handler, r := newTestRouter()
	body := `{"type":"smoke"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-auth")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	id, _ := resp["id"].(string)

	run, err := r.Get(id)
	if err != nil {
		t.Fatalf("Get(%q): %v", id, err)
	}
	if run.Config.AuthMode != runner.AuthModeJWT {
		t.Errorf("AuthMode = %q, want %q", run.Config.AuthMode, runner.AuthModeJWT)
	}

	_ = r.Stop(id)
	r.Wait()
}

// TestStartTest_InferAuthModeFromSuite verifies the auth-mode inference: an absent auth_mode
// resolves to api-key for a validate run of the api-key suite (completing the mandatory 1:1
// mapping), and MUST NOT re-mode any other suite or non-validate type.
func TestStartTest_InferAuthModeFromSuite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want runner.AuthMode
	}{
		{
			name: "validate + suite=api-key → inferred api-key",
			body: `{"type":"validate","suite":"api-key"}`,
			want: runner.AuthModeAPIKey,
		},
		{
			name: "validate + suite=upgrade → inferred upgrade",
			body: `{"type":"validate","suite":"upgrade"}`,
			want: runner.AuthModeUpgrade,
		},
		{
			name: "validate + suite=rest-publish → jwt (not re-moded)",
			body: `{"type":"validate","suite":"rest-publish"}`,
			want: runner.AuthModeJWT,
		},
		{
			name: "load + suite=api-key → jwt (inference is validate-only)",
			body: `{"type":"load","suite":"api-key"}`,
			want: runner.AuthModeJWT,
		},
		{
			name: "load + suite=upgrade → jwt (inference is validate-only)",
			body: `{"type":"load","suite":"upgrade"}`,
			want: runner.AuthModeJWT,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, r := newTestRouter()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/tests", bytes.NewBufferString(tt.body))
			req.Header.Set("Authorization", "Bearer test-auth")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != http.StatusCreated {
				t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusCreated, w.Body.String())
			}
			var resp map[string]any
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			id, _ := resp["id"].(string)

			run, err := r.Get(id)
			if err != nil {
				t.Fatalf("Get(%q): %v", id, err)
			}
			if run.Config.AuthMode != tt.want {
				t.Errorf("resolved AuthMode = %q, want %q", run.Config.AuthMode, tt.want)
			}

			_ = r.Stop(id)
			r.Wait()
		})
	}
}
