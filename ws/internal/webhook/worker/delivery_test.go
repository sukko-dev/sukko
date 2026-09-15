package worker

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/crypto"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// testKey is a valid 32-byte AES-256 encryption key for tests.
var testKey = []byte("0123456789abcdef0123456789abcdef")

// encryptSecret encrypts plaintext with testKey and returns raw ciphertext bytes.
func encryptSecret(t *testing.T, plaintext string) []byte {
	t.Helper()
	ct, err := crypto.EncryptCredential(plaintext, testKey)
	if err != nil {
		t.Fatalf("encrypt secret: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(ct)
	if err != nil {
		t.Fatalf("decode b64 ciphertext: %v", err)
	}
	return raw
}

// --- mock HTTPDoer ---

type mockHTTPDoer struct {
	resp *http.Response
	err  error
}

func (m *mockHTTPDoer) Do(_ *http.Request) (*http.Response, error) {
	return m.resp, m.err
}

// callCheckDoer wraps an HTTPDoer and records whether Do was called.
type callCheckDoer struct {
	t      *testing.T
	called *bool
	inner  HTTPDoer
}

func (c *callCheckDoer) Do(req *http.Request) (*http.Response, error) {
	*c.called = true
	return c.inner.Do(req)
}

// cacheWithWebhook builds a WebhookCache populated with one webhook.
func cacheWithWebhook(t *testing.T, status string, secretEnc []byte) (*WebhookCache, string) { //nolint:gocritic // unnamed returns are clearer for this small test helper
	t.Helper()
	client := newStubClient()
	client.records["t1"] = []*provisioning.WebhookRecord{
		{
			ID:         "wh-1",
			TenantID:   "t1",
			URL:        "https://example.com/hook",
			Status:     status,
			SecretEnc:  secretEnc,
			MaxRetries: 5,
		},
	}
	cache := NewWebhookCache(client, zerolog.Nop())
	if err := cache.Refresh(context.Background(), "t1"); err != nil {
		t.Fatalf("cache.Refresh: %v", err)
	}
	return cache, "t1"
}

func mockOKResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// TestDeliver_HappyPath verifies HMAC signing and successful delivery.
func TestDeliver_HappyPath(t *testing.T) {
	t.Parallel()
	secret := encryptSecret(t, "my-webhook-secret")
	cache, _ := cacheWithWebhook(t, types.WebhookStatusEnabled, secret)
	doer := &mockHTTPDoer{resp: mockOKResponse(`{"ok":true}`)} //nolint:bodyclose // closed by Deliver() via defer resp.Body.Close()

	d := NewDeliverer(cache, doer, testKey, zerolog.Nop())
	result := d.Deliver(context.Background(), DeliveryTask{
		WebhookID: "wh-1", TenantID: "t1",
		SecretEnc: secret, // runner populates from cache record
		Payload:   []byte(`{"event":"test"}`), Attempt: 1,
	})
	if result.StatusLabel != "success" {
		t.Errorf("StatusLabel = %q, want success; error = %q", result.StatusLabel, result.Error)
	}
	if result.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200", result.StatusCode)
	}
}

// TestDeliver_DegradedWebhookAllowed verifies degraded webhooks pass the status gate
// so the degradedScheduler can achieve recovery via a successful HTTP delivery.
func TestDeliver_DegradedWebhookAllowed(t *testing.T) {
	t.Parallel()
	secret := encryptSecret(t, "sec")
	cache, _ := cacheWithWebhook(t, types.WebhookStatusDegraded, secret)
	var httpCalled bool
	innerDoer := &mockHTTPDoer{resp: mockOKResponse("ok")} //nolint:bodyclose // closed by Deliver() via defer resp.Body.Close()
	doer := &callCheckDoer{t: t, called: &httpCalled, inner: innerDoer}

	d := NewDeliverer(cache, doer, testKey, zerolog.Nop())
	result := d.Deliver(context.Background(), DeliveryTask{
		WebhookID: "wh-1", TenantID: "t1", SecretEnc: secret, Payload: []byte("x"), Attempt: 1,
	})
	if result.StatusLabel == "skipped" {
		t.Error("degraded webhook must not be skipped by the status gate — it needs to recover")
	}
	if !httpCalled {
		t.Error("HTTP call must be made for degraded webhook (required for recovery)")
	}
}

// TestDeliver_SuspendedWebhookCancelled covers cancellation of suspended webhooks.
func TestDeliver_SuspendedWebhookCancelled(t *testing.T) {
	t.Parallel()
	secret := encryptSecret(t, "sec")
	cache, _ := cacheWithWebhook(t, types.WebhookStatusSuspended, secret)
	var httpCalled bool
	innerDoer := &mockHTTPDoer{resp: mockOKResponse("")} //nolint:bodyclose // body is never read (Deliver() skips dispatch for suspended webhooks)
	doer := &callCheckDoer{t: t, called: &httpCalled, inner: innerDoer}

	d := NewDeliverer(cache, doer, testKey, zerolog.Nop())
	result := d.Deliver(context.Background(), DeliveryTask{
		WebhookID: "wh-1", TenantID: "t1", Payload: []byte("x"), Attempt: 1,
	})
	if result.StatusLabel != "skipped" {
		t.Errorf("suspended webhook should be skipped, got %q", result.StatusLabel)
	}
	if httpCalled {
		t.Error("HTTP call must not be made for suspended webhook")
	}
}

// TestDeliver_AbsentWebhookCancelled covers cancellation when the webhook is absent from cache.
func TestDeliver_AbsentWebhookCancelled(t *testing.T) {
	t.Parallel()
	client := newStubClient() // empty cache
	cache := NewWebhookCache(client, zerolog.Nop())
	var httpCalled bool
	doer := &callCheckDoer{t: t, called: &httpCalled, inner: &mockHTTPDoer{}}

	d := NewDeliverer(cache, doer, testKey, zerolog.Nop())
	result := d.Deliver(context.Background(), DeliveryTask{
		WebhookID: "wh-missing", TenantID: "t1", Payload: []byte("x"), Attempt: 1,
	})
	if result.StatusLabel != "skipped" {
		t.Errorf("absent webhook should be skipped, got %q", result.StatusLabel)
	}
	if httpCalled {
		t.Error("HTTP call must not be made for absent webhook")
	}
}

// TestDeliver_DecryptionFailure verifies StatusLabel and Error on decrypt failure.
// The test also verifies no HTTP call is made (behavior in delivery layer).
func TestDeliver_DecryptionFailure(t *testing.T) {
	t.Parallel()
	client := newStubClient()
	client.records["t1"] = []*provisioning.WebhookRecord{
		{ID: "wh-1", TenantID: "t1", URL: "https://example.com",
			SecretEnc: []byte("not-valid-ciphertext"), Status: "enabled"},
	}
	cache := NewWebhookCache(client, zerolog.Nop())
	_ = cache.Refresh(context.Background(), "t1")

	var httpCalled bool
	doer := &callCheckDoer{t: t, called: &httpCalled, inner: &mockHTTPDoer{}}
	d := NewDeliverer(cache, doer, testKey, zerolog.Nop())

	result := d.Deliver(context.Background(), DeliveryTask{
		WebhookID: "wh-1", TenantID: "t1", Payload: []byte("x"), Attempt: 1,
	})
	if result.StatusLabel != "decrypt_failed" {
		t.Errorf("StatusLabel = %q, want decrypt_failed", result.StatusLabel)
	}
	if result.Error != "decryption_failure" {
		t.Errorf("Error = %q, want decryption_failure", result.Error)
	}
	if httpCalled {
		t.Error("HTTP call must not be made on decryption failure")
	}
}

// TestDeliver_BodyPreviewTruncated covers body truncation at 512 bytes.
func TestDeliver_BodyPreviewTruncated(t *testing.T) {
	t.Parallel()
	secret := encryptSecret(t, "sec")
	cache, _ := cacheWithWebhook(t, types.WebhookStatusEnabled, secret)
	bigBody := strings.Repeat("x", 1024)
	doer := &mockHTTPDoer{resp: &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(bigBody)),
	}}
	d := NewDeliverer(cache, doer, testKey, zerolog.Nop())
	result := d.Deliver(context.Background(), DeliveryTask{
		WebhookID: "wh-1", TenantID: "t1", SecretEnc: secret, Payload: []byte("x"), Attempt: 1,
	})
	if len(result.BodyPreview) != bodyPreviewBytes {
		t.Errorf("BodyPreview length = %d, want %d", len(result.BodyPreview), bodyPreviewBytes)
	}
}

// TestTestDeliver_SSRFBlocked covers the SSRF block in the TestDeliver path.
func TestTestDeliver_SSRFBlocked(t *testing.T) {
	t.Parallel()
	secret := encryptSecret(t, "sec")
	client := newStubClient()
	client.records["t1"] = []*provisioning.WebhookRecord{
		{ID: "wh-ssrf", TenantID: "t1", URL: "http://10.0.0.1/hook",
			SecretEnc: secret, Status: "enabled", MaxRetries: 3},
	}
	cache := NewWebhookCache(client, zerolog.Nop())
	_ = cache.Refresh(context.Background(), "t1")

	doer := &mockHTTPDoer{err: errors.New("ssrf_blocked: resolved to private IP")}
	d := NewDeliverer(cache, doer, testKey, zerolog.Nop())

	result := d.TestDeliver(context.Background(), "wh-ssrf", "t1")
	if result.StatusLabel != "ssrf_blocked" {
		t.Errorf("StatusLabel = %q, want ssrf_blocked", result.StatusLabel)
	}
}

// TestTestDeliver_SideEffectFreeOnDecryptFailure covers the side-effect-free decryption-failure semantics.
func TestTestDeliver_SideEffectFreeOnDecryptFailure(t *testing.T) {
	t.Parallel()
	client := newStubClient()
	client.records["t1"] = []*provisioning.WebhookRecord{
		{ID: "wh-1", TenantID: "t1", URL: "https://example.com",
			SecretEnc: []byte("garbage"), Status: "suspended"},
	}
	cache := NewWebhookCache(client, zerolog.Nop())
	_ = cache.Refresh(context.Background(), "t1")

	var httpCalled bool
	doer := &callCheckDoer{t: t, called: &httpCalled, inner: &mockHTTPDoer{}}
	d := NewDeliverer(cache, doer, testKey, zerolog.Nop())

	result := d.TestDeliver(context.Background(), "wh-1", "t1")
	if result.Error != "decryption_failure" {
		t.Errorf("Error = %q, want decryption_failure", result.Error)
	}
	if httpCalled {
		t.Error("no HTTP call on TestDeliver decryption failure")
	}
}

// captureDoer records the outgoing request for header assertions.
type captureDoer struct {
	resp *http.Response
	req  *http.Request
}

func (c *captureDoer) Do(req *http.Request) (*http.Response, error) {
	c.req = req
	return c.resp, nil
}

// The stable message identity must ride as a header (mirrors Stripe/GitHub
// event-id headers): the body is the raw payload, so a header is the only
// identity channel — it lets consumers dedup across retry attempts, which the
// per-attempt X-Sukko-Delivery id cannot.
func TestDeliver_MessageIDHeader(t *testing.T) {
	t.Parallel()
	secret := encryptSecret(t, "my-webhook-secret")
	cache, _ := cacheWithWebhook(t, types.WebhookStatusEnabled, secret)
	doer := &captureDoer{resp: mockOKResponse(`{"ok":true}`)} //nolint:bodyclose // closed by Deliver() via defer resp.Body.Close()

	d := NewDeliverer(cache, doer, testKey, zerolog.Nop())
	result := d.Deliver(context.Background(), DeliveryTask{
		WebhookID: "wh-1", TenantID: "t1",
		SecretEnc: secret,
		Payload:   []byte(`{"event":"test"}`), Attempt: 1,
		Mid: "beefcafe-2-99",
	})
	if result.StatusLabel != "success" {
		t.Fatalf("StatusLabel = %q, want success; error = %q", result.StatusLabel, result.Error)
	}
	if got := doer.req.Header.Get("X-Sukko-Message-Id"); got != "beefcafe-2-99" {
		t.Errorf("X-Sukko-Message-Id = %q, want %q", got, "beefcafe-2-99")
	}
}

// Mid-less tasks (synthetic test/degraded-retry pings, pre-mid bus messages)
// must not send an empty header.
func TestDeliver_NoMidOmitsHeader(t *testing.T) {
	t.Parallel()
	secret := encryptSecret(t, "my-webhook-secret")
	cache, _ := cacheWithWebhook(t, types.WebhookStatusEnabled, secret)
	doer := &captureDoer{resp: mockOKResponse(`{"ok":true}`)} //nolint:bodyclose // closed by Deliver() via defer resp.Body.Close()

	d := NewDeliverer(cache, doer, testKey, zerolog.Nop())
	result := d.Deliver(context.Background(), DeliveryTask{
		WebhookID: "wh-1", TenantID: "t1",
		SecretEnc: secret,
		Payload:   []byte(`{"event":"test"}`), Attempt: 1,
	})
	if result.StatusLabel != "success" {
		t.Fatalf("StatusLabel = %q, want success; error = %q", result.StatusLabel, result.Error)
	}
	if vals, present := doer.req.Header["X-Sukko-Message-Id"]; present {
		t.Errorf("X-Sukko-Message-Id present (%v), want absent for mid-less task", vals)
	}
}
