package worker

import (
	"context"
	"maps"
	"testing"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/provisioning"
)

func newTestInternalServer(t *testing.T, deliverFn func(context.Context, string, string) DeliveryResult, records map[string][]*provisioning.WebhookRecord) *InternalServer {
	t.Helper()
	client := newStubClient()
	maps.Copy(client.records, records)
	cache := NewWebhookCache(client, zerolog.Nop())
	for tid := range records {
		if err := cache.Refresh(context.Background(), tid); err != nil {
			t.Fatalf("cache.Refresh: %v", err)
		}
	}
	srv, err := NewInternalServer(InternalServerConfig{
		Cache:         cache,
		TestDeliverFn: deliverFn,
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewInternalServer: %v", err)
	}
	return srv
}

// TestInternalServer_TestDeliver_OK covers the happy-path case.
func TestInternalServer_TestDeliver_OK(t *testing.T) {
	t.Parallel()
	srv := newTestInternalServer(t, func(_ context.Context, _, _ string) DeliveryResult {
		return DeliveryResult{StatusCode: 200, LatencyMS: 42, BodyPreview: "ok", StatusLabel: "success"}
	}, map[string][]*provisioning.WebhookRecord{
		"t1": {{ID: "wh-1", TenantID: "t1", Status: "enabled"}},
	})

	resp, err := srv.TestDeliver(context.Background(), &provisioningv1.TestDeliverRequest{
		WebhookId: "wh-1", TenantUuid: "t1",
	})
	if err != nil {
		t.Fatalf("TestDeliver() error = %v", err)
	}
	if resp.GetStatusCode() != 200 {
		t.Errorf("StatusCode = %d, want 200", resp.GetStatusCode())
	}
}

// TestInternalServer_TestDeliver_NotFound verifies gRPC NOT_FOUND on absent webhook.
func TestInternalServer_TestDeliver_NotFound(t *testing.T) {
	t.Parallel()
	srv := newTestInternalServer(t, func(_ context.Context, _, _ string) DeliveryResult {
		return DeliveryResult{}
	}, map[string][]*provisioning.WebhookRecord{})

	_, err := srv.TestDeliver(context.Background(), &provisioningv1.TestDeliverRequest{
		WebhookId: "wh-missing", TenantUuid: "t1",
	})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Errorf("expected NOT_FOUND, got %v", err)
	}
}

// TestInternalServer_TestDeliver_SSRFBlock covers the SSRF block via RPC.
func TestInternalServer_TestDeliver_SSRFBlock(t *testing.T) {
	t.Parallel()
	srv := newTestInternalServer(t, func(_ context.Context, _, _ string) DeliveryResult {
		return DeliveryResult{Error: "ssrf_blocked: private IP", StatusLabel: "ssrf_blocked"}
	}, map[string][]*provisioning.WebhookRecord{
		"t1": {{ID: "wh-1", TenantID: "t1", Status: "enabled"}},
	})

	resp, err := srv.TestDeliver(context.Background(), &provisioningv1.TestDeliverRequest{
		WebhookId: "wh-1", TenantUuid: "t1",
	})
	if err != nil {
		t.Fatalf("TestDeliver() error = %v (should return result in resp, not gRPC error)", err)
	}
	if resp.GetError() == "" {
		t.Error("expected error in response body for ssrf_blocked")
	}
}

// TestInternalServer_TestDeliver_BodyPreviewTruncated verifies 512-byte cap.
func TestInternalServer_TestDeliver_BodyPreviewTruncated(t *testing.T) {
	t.Parallel()
	bigBody := make([]byte, 1024)
	for i := range bigBody {
		bigBody[i] = 'x'
	}
	truncated := string(bigBody[:bodyPreviewBytes])

	srv := newTestInternalServer(t, func(_ context.Context, _, _ string) DeliveryResult {
		return DeliveryResult{StatusCode: 200, BodyPreview: truncated, StatusLabel: "success"}
	}, map[string][]*provisioning.WebhookRecord{
		"t1": {{ID: "wh-1", TenantID: "t1", Status: "enabled"}},
	})

	resp, err := srv.TestDeliver(context.Background(), &provisioningv1.TestDeliverRequest{
		WebhookId: "wh-1", TenantUuid: "t1",
	})
	if err != nil {
		t.Fatalf("TestDeliver() error = %v", err)
	}
	if got := len(resp.GetResponseBodyPreview()); got != bodyPreviewBytes {
		t.Errorf("ResponseBodyPreview length = %d, want %d", got, bodyPreviewBytes)
	}
}
