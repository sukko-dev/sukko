package provider

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
)

// generateTestVAPIDKeys creates a real ECDSA P-256 key pair and returns
// the base64 raw URL-encoded public and private keys suitable for VAPID.
func generateTestVAPIDKeys(t *testing.T) (publicKey, privateKey string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating ECDSA key: %v", err)
	}

	pubBytes := elliptic.Marshal(elliptic.P256(), priv.X, priv.Y)
	publicKey = base64.RawURLEncoding.EncodeToString(pubBytes)
	privateKey = base64.RawURLEncoding.EncodeToString(priv.D.Bytes())
	return publicKey, privateKey
}

// generateTestSubscriptionKeys creates an ECDH P-256 key pair for the push
// subscription (p256dh) and a 16-byte auth secret, both base64 raw URL-encoded.
func generateTestSubscriptionKeys(t *testing.T) (p256dh, auth string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating subscription key: %v", err)
	}

	pubBytes := elliptic.Marshal(elliptic.P256(), priv.X, priv.Y)
	p256dh = base64.RawURLEncoding.EncodeToString(pubBytes)

	authBytes := make([]byte, 16)
	if _, err := rand.Read(authBytes); err != nil {
		t.Fatalf("generating auth secret: %v", err)
	}
	auth = base64.RawURLEncoding.EncodeToString(authBytes)
	return p256dh, auth
}

// makeVAPIDCredsJSON builds the JSON credential payload for WebPushProvider.
func makeVAPIDCredsJSON(t *testing.T, publicKey, privateKey string) json.RawMessage {
	t.Helper()
	creds := map[string]string{
		"vapidPublicKey":  publicKey,
		"vapidPrivateKey": privateKey,
		"vapidContact":    "test@example.com",
	}
	raw, err := json.Marshal(creds)
	if err != nil {
		t.Fatalf("marshaling VAPID creds: %v", err)
	}
	return raw
}

func TestWebPushProvider_Name(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestVAPIDKeys(t)
	credsJSON := makeVAPIDCredsJSON(t, pub, priv)

	p, err := NewWebPushProvider(zerolog.Nop(), func(string) (json.RawMessage, error) {
		return credsJSON, nil
	})
	if err != nil {
		t.Fatalf("NewWebPushProvider: %v", err)
	}
	if got := p.Name(); got != "webpush" {
		t.Fatalf("Name() = %q, want %q", got, "webpush")
	}
}

func TestNewWebPushProvider_NilLookup(t *testing.T) {
	t.Parallel()

	_, err := NewWebPushProvider(zerolog.Nop(), nil)
	if err == nil {
		t.Fatal("expected error for nil credential lookup, got nil")
	}
}

func TestWebPushProvider_Send(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestVAPIDKeys(t)
	credsJSON := makeVAPIDCredsJSON(t, pub, priv)
	p256dh, auth := generateTestSubscriptionKeys(t)

	tests := []struct {
		name       string
		statusCode int
		wantErr    bool
		sentinel   error
	}{
		{
			name:       "success_201",
			statusCode: http.StatusCreated,
			wantErr:    false,
		},
		{
			name:       "success_200",
			statusCode: http.StatusOK,
			wantErr:    false,
		},
		{
			name:       "gone_410",
			statusCode: http.StatusGone,
			wantErr:    true,
			sentinel:   ErrSubscriptionExpired,
		},
		{
			name:       "rate_limited_429",
			statusCode: http.StatusTooManyRequests,
			wantErr:    true,
			sentinel:   ErrRateLimited,
		},
		{
			name:       "server_error_500",
			statusCode: http.StatusInternalServerError,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			}))
			defer srv.Close()

			p, err := NewWebPushProvider(zerolog.Nop(), func(string) (json.RawMessage, error) {
				return credsJSON, nil
			})
			if err != nil {
				t.Fatalf("NewWebPushProvider: %v", err)
			}

			job := PushJob{
				TenantID:   "tenant-1",
				Principal:  "user-1",
				Platform:   "web",
				Endpoint:   srv.URL,
				P256dhKey:  p256dh,
				AuthSecret: auth,
				Title:      "Test",
				Body:       "Hello",
				TTL:        60,
				Urgency:    "normal",
			}

			err = p.Send(context.Background(), job)

			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.sentinel != nil && !errors.Is(err, tt.sentinel) {
				t.Fatalf("expected error wrapping %v, got: %v", tt.sentinel, err)
			}
		})
	}
}

func TestWebPushProvider_SendBatch(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestVAPIDKeys(t)
	credsJSON := makeVAPIDCredsJSON(t, pub, priv)
	p256dh, auth := generateTestSubscriptionKeys(t)

	var requestCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	p, err := NewWebPushProvider(zerolog.Nop(), func(string) (json.RawMessage, error) {
		return credsJSON, nil
	})
	if err != nil {
		t.Fatalf("NewWebPushProvider: %v", err)
	}

	jobs := make([]PushJob, 3)
	for i := range jobs {
		jobs[i] = PushJob{
			TenantID:   "tenant-1",
			Principal:  fmt.Sprintf("user-%d", i),
			Platform:   "web",
			Endpoint:   srv.URL,
			P256dhKey:  p256dh,
			AuthSecret: auth,
			Title:      "Batch",
			Body:       fmt.Sprintf("Message %d", i),
			TTL:        60,
		}
	}

	if err := p.SendBatch(context.Background(), jobs); err != nil {
		t.Fatalf("SendBatch() returned unexpected error: %v", err)
	}

	if got := requestCount.Load(); got != 3 {
		t.Fatalf("expected 3 requests to mock server, got %d", got)
	}
}

func TestWebPushProvider_SendBatch_PartialFailure(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestVAPIDKeys(t)
	credsJSON := makeVAPIDCredsJSON(t, pub, priv)
	p256dh, auth := generateTestSubscriptionKeys(t)

	var requestCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requestCount.Add(1)
		// Second request returns 410 (gone).
		if n == 2 {
			w.WriteHeader(http.StatusGone)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	p, err := NewWebPushProvider(zerolog.Nop(), func(string) (json.RawMessage, error) {
		return credsJSON, nil
	})
	if err != nil {
		t.Fatalf("NewWebPushProvider: %v", err)
	}

	jobs := make([]PushJob, 3)
	for i := range jobs {
		jobs[i] = PushJob{
			TenantID:   "tenant-1",
			Principal:  fmt.Sprintf("user-%d", i),
			Platform:   "web",
			Endpoint:   srv.URL,
			P256dhKey:  p256dh,
			AuthSecret: auth,
			Title:      "Batch",
			Body:       fmt.Sprintf("Message %d", i),
			TTL:        60,
		}
	}

	err = p.SendBatch(context.Background(), jobs)
	if err == nil {
		t.Fatal("expected batch error due to partial failure, got nil")
	}
	if !errors.Is(err, ErrSubscriptionExpired) {
		t.Fatalf("expected error wrapping ErrSubscriptionExpired, got: %v", err)
	}

	// All 3 requests should still be attempted (failures are collected, not short-circuited).
	if got := requestCount.Load(); got != 3 {
		t.Fatalf("expected 3 requests to mock server, got %d", got)
	}
}

func TestWebPushProvider_Send_InvalidCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		creds   string
		wantMsg string
	}{
		{
			name:    "invalid_json",
			creds:   `{not json}`,
			wantMsg: "parsing credentials",
		},
		{
			name:    "missing_keys",
			creds:   `{"vapidContact": "test@example.com"}`,
			wantMsg: "missing VAPID keys",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := NewWebPushProvider(zerolog.Nop(), func(string) (json.RawMessage, error) {
				return json.RawMessage(tt.creds), nil
			})
			if err != nil {
				t.Fatalf("NewWebPushProvider: %v", err)
			}

			job := PushJob{
				TenantID:  "tenant-1",
				Principal: "user-1",
				Platform:  "web",
				Endpoint:  "https://example.com/push",
			}

			err = p.Send(context.Background(), job)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestWebPushProvider_Send_LookupError(t *testing.T) {
	t.Parallel()

	lookupErr := errors.New("credentials not found")
	p, err := NewWebPushProvider(zerolog.Nop(), func(string) (json.RawMessage, error) {
		return nil, lookupErr
	})
	if err != nil {
		t.Fatalf("NewWebPushProvider: %v", err)
	}

	job := PushJob{
		TenantID:  "tenant-1",
		Principal: "user-1",
		Platform:  "web",
		Endpoint:  "https://example.com/push",
	}

	err = p.Send(context.Background(), job)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, lookupErr) {
		t.Fatalf("expected error wrapping lookupErr, got: %v", err)
	}
}

func TestWebPushProvider_Close(t *testing.T) {
	t.Parallel()

	p, err := NewWebPushProvider(zerolog.Nop(), func(string) (json.RawMessage, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("NewWebPushProvider: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() returned unexpected error: %v", err)
	}
}
