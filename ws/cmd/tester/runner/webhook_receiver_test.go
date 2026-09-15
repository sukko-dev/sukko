package runner

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	webhookconst "github.com/sukko-dev/sukko/internal/webhook"
)

func signedHeader(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return webhookconst.WebhookSignaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

func TestWebhookReceiveHandler_UnknownRunID(t *testing.T) {
	t.Parallel()

	store := newWebhookStore()
	h := webhookReceiveHandler(store)

	r := httptest.NewRequest(http.MethodPost, "/webhook-receive/no-such-run", strings.NewReader("{}"))
	r.SetPathValue("runID", "no-such-run")
	w := httptest.NewRecorder()
	h(w, r)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestWebhookReceiveHandler_ValidDelivery(t *testing.T) {
	t.Parallel()

	secret := []byte("super-secret")
	store := newWebhookStore()
	store.register("run1", secret, 0)
	h := webhookReceiveHandler(store)

	body := []byte(`{"event":"test"}`)
	r := httptest.NewRequest(http.MethodPost, "/webhook-receive/run1", strings.NewReader(string(body)))
	r.SetPathValue("runID", "run1")
	r.Header.Set(webhookconst.WebhookSignatureHeader, signedHeader(secret, body))
	w := httptest.NewRecorder()
	h(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	entry := store.get("run1")
	if entry == nil {
		t.Fatal("entry missing after delivery")
	}
	deliveries := func() []WebhookDelivery {
		entry.mu.RLock()
		defer entry.mu.RUnlock()
		return entry.deliveries
	}()

	if len(deliveries) != 1 {
		t.Fatalf("got %d deliveries, want 1", len(deliveries))
	}
	if !deliveries[0].SignatureOK {
		t.Error("delivery.SignatureOK = false, want true")
	}
	if deliveries[0].StatusCode != http.StatusOK {
		t.Errorf("delivery.StatusCode = %d, want 200", deliveries[0].StatusCode)
	}
	if deliveries[0].Sequence != 1 {
		t.Errorf("delivery.Sequence = %d, want 1", deliveries[0].Sequence)
	}
}

func TestWebhookReceiveHandler_InvalidSignature(t *testing.T) {
	t.Parallel()

	secret := []byte("correct-secret")
	store := newWebhookStore()
	store.register("run2", secret, 0)
	h := webhookReceiveHandler(store)

	body := []byte(`{"event":"test"}`)
	r := httptest.NewRequest(http.MethodPost, "/webhook-receive/run2", strings.NewReader(string(body)))
	r.SetPathValue("runID", "run2")
	r.Header.Set(webhookconst.WebhookSignatureHeader, signedHeader([]byte("wrong-secret"), body))
	w := httptest.NewRecorder()
	h(w, r)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}

	entry := store.get("run2")
	n := func() int {
		entry.mu.RLock()
		defer entry.mu.RUnlock()
		return len(entry.deliveries)
	}()
	if n != 1 {
		t.Errorf("deliveries recorded = %d, want 1 on signature failure (SignatureOK=false)", n)
	}
	if d := func() WebhookDelivery {
		entry.mu.RLock()
		defer entry.mu.RUnlock()
		return entry.deliveries[0]
	}(); d.SignatureOK {
		t.Error("delivery.SignatureOK = true, want false for rejected signature")
	}
}

func TestWebhookReceiveHandler_FailFirstN(t *testing.T) {
	t.Parallel()

	secret := []byte("secret")
	store := newWebhookStore()
	store.register("run3", secret, 2) // first 2 → 500, then 200
	h := webhookReceiveHandler(store)

	for i := range 3 {
		body := fmt.Appendf(nil, `{"seq":%d}`, i+1)
		r := httptest.NewRequest(http.MethodPost, "/webhook-receive/run3", strings.NewReader(string(body)))
		r.SetPathValue("runID", "run3")
		r.Header.Set(webhookconst.WebhookSignatureHeader, signedHeader(secret, body))
		w := httptest.NewRecorder()
		h(w, r)

		wantStatus := http.StatusInternalServerError
		if i >= 2 {
			wantStatus = http.StatusOK
		}
		if w.Code != wantStatus {
			t.Errorf("delivery %d: status = %d, want %d", i+1, w.Code, wantStatus)
		}
	}

	entry := store.get("run3")
	deliveries := func() []WebhookDelivery {
		entry.mu.RLock()
		defer entry.mu.RUnlock()
		return entry.deliveries
	}()
	if len(deliveries) != 3 {
		t.Fatalf("got %d deliveries, want 3", len(deliveries))
	}
	for i, d := range deliveries {
		wantCode := http.StatusInternalServerError
		if i >= 2 {
			wantCode = http.StatusOK
		}
		if d.StatusCode != wantCode {
			t.Errorf("delivery[%d].StatusCode = %d, want %d", i, d.StatusCode, wantCode)
		}
	}
}

func TestWebhookReceiveHandler_AlwaysFail(t *testing.T) {
	t.Parallel()

	secret := []byte("secret")
	store := newWebhookStore()
	store.register("run4", secret, -1) // always 500
	h := webhookReceiveHandler(store)

	for i := range 3 {
		body := fmt.Appendf(nil, `{"seq":%d}`, i+1)
		r := httptest.NewRequest(http.MethodPost, "/webhook-receive/run4", strings.NewReader(string(body)))
		r.SetPathValue("runID", "run4")
		r.Header.Set(webhookconst.WebhookSignatureHeader, signedHeader(secret, body))
		w := httptest.NewRecorder()
		h(w, r)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("delivery %d: status = %d, want 500", i+1, w.Code)
		}
	}
}

func TestWebhookReceiveHandler_DeleteZeroesSecret(t *testing.T) {
	t.Parallel()

	secret := []byte("to-be-zeroed")
	store := newWebhookStore()
	store.register("run5", secret, 0)

	entry := store.get("run5")
	if entry == nil {
		t.Fatal("entry missing before delete")
	}

	store.delete("run5")

	// Entry is removed from the map.
	if store.get("run5") != nil {
		t.Error("entry still in store after delete")
	}
	// Secret bytes are zeroed.
	func() {
		entry.mu.RLock()
		defer entry.mu.RUnlock()
		for i, b := range entry.secret {
			if b != 0 {
				t.Errorf("secret byte[%d] = %d, want 0 after delete", i, b)
				break
			}
		}
	}()
}

func TestWebhookReceiveHandler_ConcurrentDeliveries(t *testing.T) {
	t.Parallel()

	const concurrency = 20
	secret := []byte("concurrent-secret")
	store := newWebhookStore()
	store.register("run6", secret, 0)
	h := webhookReceiveHandler(store)

	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("goroutine %d panicked: %v", i, r)
				}
			}()
			body := fmt.Appendf(nil, `{"i":%d}`, i)
			r := httptest.NewRequest(http.MethodPost, "/webhook-receive/run6", strings.NewReader(string(body)))
			r.SetPathValue("runID", "run6")
			r.Header.Set(webhookconst.WebhookSignatureHeader, signedHeader(secret, body))
			w := httptest.NewRecorder()
			h(w, r)
			if w.Code != http.StatusOK {
				t.Errorf("goroutine %d: status = %d, want 200", i, w.Code)
			}
		})
	}
	wg.Wait()

	entry := store.get("run6")
	if entry == nil {
		t.Fatal("entry missing after concurrent deliveries")
	}
	n := func() int {
		entry.mu.RLock()
		defer entry.mu.RUnlock()
		return len(entry.deliveries)
	}()
	if n != concurrency {
		t.Errorf("got %d deliveries, want %d", n, concurrency)
	}
}
