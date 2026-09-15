package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	pushv1 "github.com/sukko-dev/sukko/gen/proto/sukko/push/v1"
)

func TestHandlePushVAPIDKey_Success(t *testing.T) {
	t.Parallel()

	mock := &mockPushForwarder{
		vapidResp: &pushv1.GetVAPIDKeyResponse{PublicKey: "BNcRdreALRFXTkOOUHK1EtK2w"},
	}
	gw, token := pushTestGatewayWithJWT(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/push/vapid-key", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushVAPIDKey(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp["public_key"] != "BNcRdreALRFXTkOOUHK1EtK2w" {
		t.Errorf("public_key = %q, want %q", resp["public_key"], "BNcRdreALRFXTkOOUHK1EtK2w")
	}

	// Verify forwarded request
	if mock.lastVAPIDReq == nil {
		t.Fatal("GetVAPIDKey was not called")
	}
	if mock.lastVAPIDReq.GetTenantSlug() != "test-tenant" {
		t.Errorf("tenant_id = %q, want %q", mock.lastVAPIDReq.GetTenantSlug(), "test-tenant")
	}
}

func TestHandlePushVAPIDKey_AuthRequired(t *testing.T) {
	t.Parallel()

	gw, _ := pushTestGatewayWithJWT(t, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/push/vapid-key", http.NoBody)
	// No credentials provided
	rec := httptest.NewRecorder()

	gw.HandlePushVAPIDKey(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	assertErrorCode(t, rec, "UNAUTHORIZED")
}

func TestHandlePushVAPIDKey_NoPushClient(t *testing.T) {
	t.Parallel()

	gw, token := pushTestGatewayWithJWT(t, nil)
	gw.pushClient = nil

	req := httptest.NewRequest(http.MethodGet, "/api/v1/push/vapid-key", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePushVAPIDKey(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}
