package runner

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sukko-dev/sukko/internal/shared/httputil"
	webhookconst "github.com/sukko-dev/sukko/internal/webhook"
)

// maxWebhookBodySize caps the body read for HMAC verification at 512 KiB.
const maxWebhookBodySize = 1 << 19

// WebhookDelivery records a single received webhook delivery attempt.
type WebhookDelivery struct {
	Sequence    int
	ReceivedAt  time.Time
	Headers     http.Header
	Body        []byte
	SignatureOK bool
	StatusCode  int
}

// webhookRunState holds per-run receiver state, protected by its own mutex.
// Owned by WebhookStore; the outer store lock is NEVER held while accessing Secret.
type webhookRunState struct {
	mu         sync.RWMutex
	deleted    bool // set by delete(); handler checks under same RLock as secret copy
	secret     []byte
	failFirstN int // 0=always 200; N>0=first N fail; -1=always 500
	seenCount  int
	deliveries []WebhookDelivery
}

// WebhookStore maps runIDs to their per-run state.
// The store mutex protects the map; individual entry mutexes protect entry fields.
type WebhookStore struct {
	mu    sync.RWMutex
	store map[string]*webhookRunState
}

func newWebhookStore() *WebhookStore {
	return &WebhookStore{store: make(map[string]*webhookRunState)}
}

func (s *WebhookStore) register(runID string, secret []byte, failFirstN int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[runID] = &webhookRunState{secret: secret, failFirstN: failFirstN}
}

func (s *WebhookStore) get(runID string) *webhookRunState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store[runID]
}

// delete removes an entry from the store and zeroes its secret bytes.
// Two-step locking: store lock released before clearing secret so concurrent
// store.get() calls are not blocked while waiting for entry.mu.
func (s *WebhookStore) delete(runID string) {
	entry := func() *webhookRunState {
		s.mu.Lock()
		defer s.mu.Unlock()
		e := s.store[runID]
		delete(s.store, runID)
		return e
	}()

	if entry != nil {
		entry.mu.Lock()
		defer entry.mu.Unlock()
		entry.deleted = true
		clear(entry.secret)
	}
}

// webhookReceiveHandler returns an http.HandlerFunc that records incoming deliveries
// for the runID encoded in the URL path parameter {runID}.
func webhookReceiveHandler(store *WebhookStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("runID")
		entry := store.get(runID)
		if entry == nil {
			httputil.WriteError(w, http.StatusNotFound, "UNKNOWN_RUN", "unknown runID")
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBodySize))
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "READ_ERROR", "failed to read body")
			return
		}

		// Copy secret under RLock; also check deleted flag atomically — delete()
		// sets deleted=true and clears secret under the same write lock.
		secret, ok := func() ([]byte, bool) {
			entry.mu.RLock()
			defer entry.mu.RUnlock()
			if entry.deleted {
				return nil, false
			}
			s := make([]byte, len(entry.secret))
			copy(s, entry.secret)
			return s, true
		}()
		if !ok {
			httputil.WriteError(w, http.StatusNotFound, "UNKNOWN_RUN", "unknown runID")
			return
		}

		sigHeader := r.Header.Get(webhookconst.WebhookSignatureHeader)
		if !verifyWebhookSignature(secret, body, sigHeader) {
			// Record rejected delivery so assertDeliveries can surface signing bugs as a named
			// "signatures" check failure rather than an opaque delivery-count timeout.
			func() {
				entry.mu.Lock()
				defer entry.mu.Unlock()
				if !entry.deleted {
					entry.seenCount++
					entry.deliveries = append(entry.deliveries, WebhookDelivery{
						Sequence:    entry.seenCount,
						ReceivedAt:  time.Now(),
						SignatureOK: false,
						StatusCode:  http.StatusForbidden,
					})
				}
			}()
			httputil.WriteError(w, http.StatusForbidden, "INVALID_SIGNATURE", "signature verification failed")
			return
		}

		// Record delivery under Lock; closure releases lock before WriteHeader (no I/O under lock).
		statusCode := func() int {
			entry.mu.Lock()
			defer entry.mu.Unlock()
			entry.seenCount++
			seq := entry.seenCount
			code := http.StatusOK
			if entry.failFirstN < 0 || (entry.failFirstN > 0 && seq <= entry.failFirstN) {
				code = http.StatusInternalServerError
			}
			entry.deliveries = append(entry.deliveries, WebhookDelivery{
				Sequence:    seq,
				ReceivedAt:  time.Now(),
				Headers:     r.Header.Clone(),
				Body:        body,
				SignatureOK: true,
				StatusCode:  code,
			})
			return code
		}()

		w.WriteHeader(statusCode)
	}
}

// verifyWebhookSignature checks that header equals "sha256=<HMAC-SHA256(secret, body)>".
func verifyWebhookSignature(secret, body []byte, header string) bool {
	if !strings.HasPrefix(header, webhookconst.WebhookSignaturePrefix) {
		return false
	}
	sigHex := strings.TrimPrefix(header, webhookconst.WebhookSignaturePrefix)
	sigBytes, err := hex.DecodeString(sigHex)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hmac.Equal(mac.Sum(nil), sigBytes)
}
