package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pushv1 "github.com/sukko-dev/sukko/gen/proto/sukko/push/v1"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// mockPushForwarder implements PushForwarder for testing.
type mockPushForwarder struct {
	registerResp   *pushv1.RegisterDeviceResponse
	registerErr    error
	unregisterResp *pushv1.UnregisterDeviceResponse
	unregisterErr  error
	vapidResp      *pushv1.GetVAPIDKeyResponse
	vapidErr       error

	// Captured requests for assertion
	lastRegisterReq   *pushv1.RegisterDeviceRequest
	lastUnregisterReq *pushv1.UnregisterDeviceRequest
	lastVAPIDReq      *pushv1.GetVAPIDKeyRequest
}

func (m *mockPushForwarder) RegisterDevice(_ context.Context, req *pushv1.RegisterDeviceRequest) (*pushv1.RegisterDeviceResponse, error) {
	m.lastRegisterReq = req
	return m.registerResp, m.registerErr
}

func (m *mockPushForwarder) UnregisterDevice(_ context.Context, req *pushv1.UnregisterDeviceRequest) (*pushv1.UnregisterDeviceResponse, error) {
	m.lastUnregisterReq = req
	return m.unregisterResp, m.unregisterErr
}

func (m *mockPushForwarder) GetVAPIDKey(_ context.Context, req *pushv1.GetVAPIDKeyRequest) (*pushv1.GetVAPIDKeyResponse, error) {
	m.lastVAPIDReq = req
	return m.vapidResp, m.vapidErr
}

// pushTestGatewayWithJWT creates a Gateway with JWT auth for push handler testing.
// Returns the gateway and a valid Bearer token for tenant "test-tenant".
func pushTestGatewayWithJWT(t *testing.T, mock PushForwarder) (gw *Gateway, token string) {
	t.Helper()

	registry := auth.NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKeyForGateway(t)

	key := &auth.KeyInfo{
		KeyID:        "push-test-key",
		TenantID:     "test-tenant",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey: %v", err)
	}

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:     registry,
		RequireTenantID: true,
		TenantResolver:  identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator: %v", err)
	}

	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-1",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "test-tenant",
	}
	tokenString := createTestTokenForGateway(t, key, privateKey, claims)

	gw = &Gateway{
		config: &platform.GatewayConfig{
			AuthConfig:     platform.AuthConfig{AuthMode: "required"},
			MaxPublishSize: 65536,
		},
		validator:  validator,
		logger:     testLogger(),
		pushClient: mock,
		// Permissive rules for tenant "test-tenant" — provisioning-only
		// authorization requires a checker; tests override for deny scenarios.
		tenantPermChecker: testTenantChecker("test-tenant", &types.ChannelRules{
			Public:        []string{"*"},
			PublishPublic: []string{"*"},
		}),
	}

	return gw, tokenString
}

func TestHandlePushSubscribe_WebSuccess(t *testing.T) {
	t.Parallel()

	mock := &mockPushForwarder{
		registerResp: &pushv1.RegisterDeviceResponse{DeviceId: 42},
	}
	gw, token := pushTestGatewayWithJWT(t, mock)

	body := `{
		"platform": "web",
		"endpoint": "https://fcm.googleapis.com/fcm/send/abc123",
		"p256dh_key": "BNcRdreALRFXTkOOUHK1EtK2w...",
		"auth_secret": "tBHItJI5svbpC7htfGg...",
		"channels": ["test-tenant.alerts"]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d; body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if deviceID, ok := resp["device_id"].(float64); !ok || int64(deviceID) != 42 {
		t.Errorf("device_id = %v, want 42", resp["device_id"])
	}

	// Verify forwarded request
	if mock.lastRegisterReq == nil {
		t.Fatal("RegisterDevice was not called")
	}
	if mock.lastRegisterReq.GetPlatform() != "web" {
		t.Errorf("platform = %q, want %q", mock.lastRegisterReq.GetPlatform(), "web")
	}
	if mock.lastRegisterReq.GetTenantSlug() != "test-tenant" {
		t.Errorf("tenant_id = %q, want %q", mock.lastRegisterReq.GetTenantSlug(), "test-tenant")
	}
}

func TestHandlePushSubscribe_AndroidSuccess(t *testing.T) {
	t.Parallel()

	mock := &mockPushForwarder{
		registerResp: &pushv1.RegisterDeviceResponse{DeviceId: 99},
	}
	gw, token := pushTestGatewayWithJWT(t, mock)

	body := `{
		"platform": "android",
		"token": "fcm-token-abc123",
		"channels": ["test-tenant.notifications"]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d; body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if deviceID, ok := resp["device_id"].(float64); !ok || int64(deviceID) != 99 {
		t.Errorf("device_id = %v, want 99", resp["device_id"])
	}
}

func TestHandlePushSubscribe_IOSSuccess(t *testing.T) {
	t.Parallel()

	mock := &mockPushForwarder{
		registerResp: &pushv1.RegisterDeviceResponse{DeviceId: 77},
	}
	gw, token := pushTestGatewayWithJWT(t, mock)

	body := `{
		"platform": "ios",
		"token": "apns-device-token-xyz",
		"channels": ["test-tenant.updates"]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d; body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func TestHandlePushSubscribe_InvalidPlatform(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, nil)

	body := `{"platform": "blackberry", "channels": ["test-tenant.ch"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, "INVALID_REQUEST")
}

func TestHandlePushSubscribe_MissingChannels(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, nil)

	body := `{"platform": "web", "endpoint": "https://example.com", "p256dh_key": "key", "auth_secret": "sec", "channels": []}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, "INVALID_REQUEST")
}

func TestHandlePushSubscribe_InvalidTenantPrefix(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, nil)
	// Rules allow *.alerts — the channel below uses a wrong tenant prefix,
	// so it is filtered before rules even apply.
	gw.tenantPermChecker = testTenantChecker("acme", &types.ChannelRules{Public: []string{"*.alerts"}})

	body := `{
		"platform": "web",
		"endpoint": "https://example.com",
		"p256dh_key": "key",
		"auth_secret": "sec",
		"channels": ["wrong-tenant.alerts"]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, "INVALID_REQUEST")
}

func TestHandlePushSubscribe_MissingWebFields(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, nil)

	// Web platform without endpoint
	body := `{"platform": "web", "p256dh_key": "key", "auth_secret": "sec", "channels": ["test-tenant.ch"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, "INVALID_REQUEST")
}

func TestHandlePushSubscribe_MissingAndroidToken(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, nil)

	body := `{"platform": "android", "channels": ["test-tenant.ch"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, "INVALID_REQUEST")
}

func TestHandlePushSubscribe_NoPushClient(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, nil)
	gw.pushClient = nil

	body := `{
		"platform": "web",
		"endpoint": "https://example.com",
		"p256dh_key": "key",
		"auth_secret": "sec",
		"channels": ["test-tenant.ch"]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHandlePushSubscribe_InvalidJSON(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, "INVALID_REQUEST")
}

func TestHandlePushUnsubscribe_Success(t *testing.T) {
	t.Parallel()

	mock := &mockPushForwarder{
		unregisterResp: &pushv1.UnregisterDeviceResponse{Success: true},
	}
	gw, token := pushTestGatewayWithJWT(t, mock)

	body := `{"device_id": 42}`
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushUnsubscribe(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if success, ok := resp["success"].(bool); !ok || !success {
		t.Errorf("success = %v, want true", resp["success"])
	}

	// Verify forwarded request
	if mock.lastUnregisterReq == nil {
		t.Fatal("UnregisterDevice was not called")
	}
	if mock.lastUnregisterReq.GetTenantSlug() != "test-tenant" {
		t.Errorf("tenant_id = %q, want %q", mock.lastUnregisterReq.GetTenantSlug(), "test-tenant")
	}
	if mock.lastUnregisterReq.GetDeviceId() != 42 {
		t.Errorf("device_id = %d, want 42", mock.lastUnregisterReq.GetDeviceId())
	}
}

func TestHandlePushUnsubscribe_MissingDeviceID(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, nil)

	body := `{}`
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushUnsubscribe(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, "INVALID_REQUEST")
}

func TestHandlePushUnsubscribe_NoPushClient(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, nil)
	gw.pushClient = nil

	body := `{"device_id": 1}`
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushUnsubscribe(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHandlePushSubscribe_EditionGate_Community(t *testing.T) {
	t.Parallel()

	// Community edition should block Web Push
	mgr := license.NewTestManager(license.Community)
	pushGate := RequireFeature(mgr, license.WebPush)

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := pushGate(inner)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", http.NoBody)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	assertErrorCode(t, rec, "EDITION_LIMIT")
}

func TestHandlePushSubscribe_EditionGate_Pro(t *testing.T) {
	t.Parallel()

	// Pro edition allows Web Push (ADR-0009: WebPush is Pro).
	mgr := license.NewTestManager(license.Pro)
	pushGate := RequireFeature(mgr, license.WebPush)

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := pushGate(inner)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", http.NoBody)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d (Pro includes WebPush per ADR-0009)", rec.Code, http.StatusOK)
	}
}

func TestHandlePushSubscribe_EditionGate_Enterprise(t *testing.T) {
	t.Parallel()

	// Enterprise edition should allow Web Push
	mgr := license.NewTestManager(license.Enterprise)
	pushGate := RequireFeature(mgr, license.WebPush)

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := pushGate(inner)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", http.NoBody)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHandlePushUnsubscribe_APIKeyOnly_Forbidden(t *testing.T) {
	t.Parallel()

	mock := &mockPushForwarder{}
	gw, _ := pushTestGatewayWithJWT(t, mock)
	gw.apiKeyRegistry = &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"test-key": {KeyID: "k1", TenantID: "test-tenant", IsActive: true},
	}}
	gw.tenantSlugResolver = &mockTenantSlugResolver{slugs: map[string]string{"test-tenant": "test-tenant"}, present: true}

	body := `{"device_id":42}`
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("X-API-Key", "test-key")
	rec := httptest.NewRecorder()

	gw.HandlePushUnsubscribe(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

// ---------------------------------------------------------------------------
// gRPC status mapping (§XII): the gateway must translate push-service gRPC
// status codes into their HTTP equivalents — PermissionDenied (edition gate)
// → 403 EDITION_LIMIT with the upstream message preserved; Unavailable
// (service not deployed / unreachable) → 503 SERVICE_UNAVAILABLE. Flattening
// everything to 500 hides the edition gate the OpenAPI contract promises.
// ---------------------------------------------------------------------------

func TestHandlePushSubscribe_GRPCPermissionDenied_Maps403(t *testing.T) {
	t.Parallel()

	// Wrapped like production PushClient does ("push RegisterDevice: %w") —
	// the mapper must still classify it AND surface the clean status message,
	// not the "rpc error: code = … desc = …" framing.
	mock := &mockPushForwarder{
		registerErr: fmt.Errorf("push RegisterDevice: %w", status.Error(codes.PermissionDenied,
			"mobile push (FCM/APNs) requires Enterprise edition (current: pro)")),
	}
	gw, token := pushTestGatewayWithJWT(t, mock)

	body := `{"platform":"android","token":"fcm-tok","channels":["test-tenant.alerts"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (PermissionDenied must map to edition denial); body=%s", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "EDITION_LIMIT")
	if !strings.Contains(rec.Body.String(), "Enterprise") {
		t.Errorf("body = %q, want upstream message preserved (contains Enterprise)", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "rpc error") {
		t.Errorf("body = %q, must not leak the gRPC error framing", rec.Body.String())
	}
}

func TestHandlePushSubscribe_GRPCUnavailable_Maps503(t *testing.T) {
	t.Parallel()

	mock := &mockPushForwarder{
		registerErr: status.Error(codes.Unavailable, "connection refused"),
	}
	gw, token := pushTestGatewayWithJWT(t, mock)

	body := `{"platform":"web","endpoint":"https://p.example.com/s","p256dh_key":"k","auth_secret":"a","channels":["test-tenant.alerts"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	gw.HandlePushSubscribe(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (Unavailable = push service not deployed); body=%s", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "SERVICE_UNAVAILABLE")
}

func TestHandlePushVAPIDKey_GRPCUnavailable_Maps503(t *testing.T) {
	t.Parallel()

	mock := &mockPushForwarder{
		vapidErr: status.Error(codes.Unavailable, "name resolver error"),
	}
	gw, token := pushTestGatewayWithJWT(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/push/vapid-key", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	gw.HandlePushVAPIDKey(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "SERVICE_UNAVAILABLE")
}

func TestHandlePushUnsubscribe_GRPCUnavailable_Maps503(t *testing.T) {
	t.Parallel()

	mock := &mockPushForwarder{
		unregisterErr: status.Error(codes.Unavailable, "connection refused"),
	}
	gw, token := pushTestGatewayWithJWT(t, mock)

	body := `{"device_id":42}`
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/push/subscribe", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	gw.HandlePushUnsubscribe(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "SERVICE_UNAVAILABLE")
}

// GATEWAY_PUSH_ENABLED gates route REGISTRATION (explicit deployment mode):
// when false, the push routes do not exist (404) regardless of license; when
// true they are registered behind the edition gate as before.
func TestPushRoutes_DisabledByFlag_NotRegistered(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, &mockPushForwarder{})
	gw.config.PushEnabled = false
	srv := gw.NewServer()

	for _, rt := range []struct{ method, path string }{
		{http.MethodPost, "/api/v1/push/subscribe"},
		{http.MethodDelete, "/api/v1/push/subscribe"},
		{http.MethodGet, "/api/v1/push/vapid-key"},
	} {
		req := httptest.NewRequest(rt.method, rt.path, http.NoBody)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s with GATEWAY_PUSH_ENABLED=false: status = %d, want 404 (route absent)", rt.method, rt.path, rec.Code)
		}
	}
}

func TestPushRoutes_EnabledByFlag_Registered(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, &mockPushForwarder{
		vapidResp: &pushv1.GetVAPIDKeyResponse{PublicKey: "pk"},
	})
	gw.config.PushEnabled = true
	srv := gw.NewServer()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/push/vapid-key", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("vapid-key with GATEWAY_PUSH_ENABLED=true: status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
}
