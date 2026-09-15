package worker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/crypto"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/types"
	webhookconst "github.com/sukko-dev/sukko/internal/webhook"
)

const (
	// deliveryIDHeader identifies the delivery attempt uniquely.
	deliveryIDHeader = "X-Sukko-Delivery"
	// messageIDHeader carries the stable message identity of the delivered
	// event (same mid as the live/replay/history copies) so consumers can
	// dedup across retry attempts. Mirrors Stripe/GitHub event-id headers.
	messageIDHeader = "X-Sukko-Message-Id"
	// bodyPreviewBytes is the maximum response body bytes captured (§VI).
	bodyPreviewBytes = webhookconst.WebhookTestResponsePreviewBytes
)

// StatusLabelNotFound is the DeliveryResult.StatusLabel when the webhook is absent from cache.
// Referenced by grpcserver.go to map the result to gRPC NOT_FOUND (§I: shared symbolic string).
const StatusLabelNotFound = "not_found"

// StatusLabel* constants are DeliveryResult.StatusLabel values used as Prometheus metric labels.
// Named constants per §I — these values appear in switch cases in runner.go and assignments here.
const (
	StatusLabelSkipped     = "skipped"
	StatusLabelDecryptFail = "decrypt_failed"
	StatusLabelSSRFBlocked = ssrfBlockedPrefix // same value; reuse to keep them in sync
	StatusLabelSuccess     = "success"
	StatusLabelFailure     = "failure"
	StatusLabelTimeout     = "timeout"
	StatusLabelDropped     = "dropped"
)

// DeliveryTask describes a single pending HTTP delivery attempt.
type DeliveryTask struct {
	WebhookID  string
	TenantID   string
	URL        string
	SecretEnc  []byte    // raw AES-256-GCM ciphertext
	Payload    []byte    // raw HTTP POST body
	Attempt    int       // 1-indexed; attempt=1 = initial delivery
	MaxRetries int       // max_retries from webhook registration
	NextAt     time.Time // when to dispatch this task (zero = now)
	Mid        string    // stable message identity of the delivered event; empty for synthetic tasks (test/degraded pings)
}

// DeliveryResult summarizes a completed delivery attempt.
type DeliveryResult struct {
	StatusCode  int
	LatencyMS   int64
	BodyPreview string // first bodyPreviewBytes of response body
	Error       string // non-empty on connection/SSRF/decrypt error
	StatusLabel string // metric label: "success"|"failure"|"timeout"|"ssrf_blocked"|"decrypt_failed"|"dropped"
}

// Deliverer performs HTTP webhook deliveries and test deliveries.
// Inject via the delivery layer; the runner calls Deliver and TestDeliver.
type Deliverer struct {
	cache  *WebhookCache
	doer   HTTPDoer
	key    []byte
	logger zerolog.Logger
}

// NewDeliverer creates a Deliverer with the given dependencies.
func NewDeliverer(cache *WebhookCache, doer HTTPDoer, key []byte, logger zerolog.Logger) *Deliverer {
	return &Deliverer{
		cache:  cache,
		doer:   doer,
		key:    key,
		logger: logger.With().Str("component", "deliverer").Logger(),
	}
}

// Deliver executes a single HTTP POST for the given task.
//
// Pre-dispatch status check: if the webhook is absent or status ≠ "enabled",
// the task is canceled without HTTP call or RecordDelivery (caller skips side-effects).
//
// Decryption failure: returns DeliveryResult{StatusLabel:"decrypt_failed"}.
// The CALLER (doDeliver in runner.go) must call UpdateWebhookStatus(suspended) +
// RecordDelivery(error="decryption_failure") — Deliver() itself has no side-effects on
// decryption failure so that TestDeliver can share this code path without transitions.
func (d *Deliverer) Deliver(ctx context.Context, task DeliveryTask) DeliveryResult {
	// pre-dispatch status gate. Does NOT apply to TestDeliver.
	// Cancel if: webhook absent (deleted) OR status == suspended.
	// "degraded" webhooks are allowed through — the degradedScheduler dispatches them
	// specifically so they can recover to "enabled" via a successful HTTP delivery.
	// Blocking degraded here would make auto-recovery unreachable.
	rec := d.cache.GetByID(task.TenantID, task.WebhookID)
	if rec == nil || rec.Status == types.WebhookStatusSuspended {
		return DeliveryResult{StatusLabel: StatusLabelSkipped}
	}
	return d.dispatch(ctx, task)
}

// TestDeliver performs a test delivery, bypassing the status gate.
// On cache miss, the caller must do on-demand hydration before calling this.
// Decryption failure returns {Error:"decryption_failure"} with no state transitions.
func (d *Deliverer) TestDeliver(ctx context.Context, webhookID, tenantID string) DeliveryResult {
	rec := d.cache.GetByID(tenantID, webhookID)
	if rec == nil {
		return DeliveryResult{Error: StatusLabelNotFound, StatusLabel: StatusLabelNotFound}
	}
	// Synthesize a minimal task for the test dispatch (no real payload — just a ping).
	task := DeliveryTask{
		WebhookID:  rec.ID,
		TenantID:   rec.TenantID,
		URL:        rec.URL,
		SecretEnc:  rec.SecretEnc,
		Payload:    []byte(`{"type":"webhook.test"}`),
		Attempt:    1,
		MaxRetries: rec.MaxRetries,
	}
	return d.dispatch(ctx, task)
}

// dispatch performs the actual HTTP POST. Shared by Deliver and TestDeliver.
func (d *Deliverer) dispatch(ctx context.Context, task DeliveryTask) DeliveryResult {
	// Decrypt webhook secret.
	secretBytes, err := crypto.DecryptRawToBytes(task.SecretEnc, d.key)
	if err != nil {
		d.logger.Warn().Err(err).
			Str("webhook_id", task.WebhookID).
			Str(logging.LogKeyTenantUUID, task.TenantID).
			Msg("webhook secret decryption failed")
		// §IX: secrets must not appear in errors. No secret content logged.
		return DeliveryResult{Error: "decryption_failure", StatusLabel: StatusLabelDecryptFail}
	}
	defer clear(secretBytes) // §IX best-effort in-memory zeroing

	// Compute HMAC-SHA256 signature.
	mac := hmac.New(sha256.New, secretBytes)
	mac.Write(task.Payload)
	sig := webhookconst.WebhookSignaturePrefix + hex.EncodeToString(mac.Sum(nil))

	deliveryID := uuid.NewString()

	// bytes.NewReader auto-sets ContentLength and GetBody (enabling transparent retries).
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, task.URL, bytes.NewReader(task.Payload))
	if err != nil {
		return DeliveryResult{Error: fmt.Sprintf("build request: %v", err), StatusLabel: StatusLabelFailure}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(webhookconst.WebhookSignatureHeader, sig)
	req.Header.Set(deliveryIDHeader, deliveryID)
	if task.Mid != "" {
		req.Header.Set(messageIDHeader, task.Mid)
	}

	start := time.Now()
	resp, err := d.doer.Do(req)
	latencyMS := time.Since(start).Milliseconds()

	if err != nil {
		label := StatusLabelFailure
		if ctx.Err() != nil {
			label = StatusLabelTimeout
		}
		// strings.Contains handles both raw SSRFDialer errors (ssrfBlockedPrefix+": ...") and
		// Go's *url.Error wrapping ("Post \"https://...\": ssrf_blocked: ...").
		if strings.Contains(err.Error(), ssrfBlockedPrefix) {
			label = StatusLabelSSRFBlocked
		}
		return DeliveryResult{LatencyMS: latencyMS, Error: err.Error(), StatusLabel: label}
	}
	defer func() { _ = resp.Body.Close() }()

	// Cap response body preview at bodyPreviewBytes.
	preview, _ := readCapped(resp.Body, bodyPreviewBytes)

	statusLabel := StatusLabelSuccess
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		statusLabel = StatusLabelFailure
	}
	return DeliveryResult{
		StatusCode:  resp.StatusCode,
		LatencyMS:   latencyMS,
		BodyPreview: string(preview),
		StatusLabel: statusLabel,
	}
}

// readCapped reads at most limit bytes from r.
// io.ReadFull reads exactly len(buf) bytes; io.ErrUnexpectedEOF means the body was shorter.
func readCapped(r io.Reader, limit int) ([]byte, error) {
	buf := make([]byte, limit)
	n, err := io.ReadFull(r, buf)
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return buf[:n], nil
	}
	if err != nil {
		return buf[:n], fmt.Errorf("read capped: %w", err)
	}
	return buf[:n], nil
}
