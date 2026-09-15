package runner

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

func TestPushReceiver_RecordsDeliveries(t *testing.T) {
	t.Parallel()

	r, err := startPushReceiver(context.Background(), 0, zerolog.Nop()) // ephemeral port
	if err != nil {
		t.Fatalf("startPushReceiver: %v", err)
	}
	defer func() { _ = r.Close() }()

	url := fmt.Sprintf("http://127.0.0.1:%d/push/abc123", r.Port())
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte("encrypted-payload")))
	req.Header.Set("Content-Encoding", "aes128gcm")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()

	// RFC 8030: push resource creation responds 201.
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	reqs := r.Requests()
	if len(reqs) != 1 {
		t.Fatalf("recorded %d requests, want 1", len(reqs))
	}
	if reqs[0].Path != "/push/abc123" {
		t.Errorf("path = %q, want /push/abc123", reqs[0].Path)
	}
	if reqs[0].BodyLen != len("encrypted-payload") {
		t.Errorf("body len = %d, want %d", reqs[0].BodyLen, len("encrypted-payload"))
	}
	if reqs[0].ContentEncoding != "aes128gcm" {
		t.Errorf("content encoding = %q, want aes128gcm", reqs[0].ContentEncoding)
	}
}

func TestPushReceiver_RejectsNonPost(t *testing.T) {
	t.Parallel()

	r, err := startPushReceiver(context.Background(), 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("startPushReceiver: %v", err)
	}
	defer func() { _ = r.Close() }()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/push/x", r.Port()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}
	if len(r.Requests()) != 0 {
		t.Errorf("GET must not be recorded as a delivery")
	}
}

// TestPushReceiver_ConcurrentDeliveries exercises the recorder under
// concurrent POSTs — the -race run is the §VII/§VIII enforcement.
func TestPushReceiver_ConcurrentDeliveries(t *testing.T) {
	t.Parallel()

	r, err := startPushReceiver(context.Background(), 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("startPushReceiver: %v", err)
	}
	defer func() { _ = r.Close() }()

	const n = 20
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			url := fmt.Sprintf("http://127.0.0.1:%d/push/c%d", r.Port(), i)
			resp, postErr := http.Post(url, "application/octet-stream", bytes.NewReader([]byte("x")))
			if postErr == nil {
				_ = resp.Body.Close()
			}
		})
	}
	wg.Wait()

	if got := len(r.Requests()); got != n {
		t.Errorf("recorded %d requests, want %d", got, n)
	}
}

func TestPushReceiver_CloseStopsServing(t *testing.T) {
	t.Parallel()

	r, err := startPushReceiver(context.Background(), 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("startPushReceiver: %v", err)
	}
	port := r.Port()
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/push/x", port),
		"application/octet-stream", bytes.NewReader([]byte("x")))
	if err == nil {
		_ = resp.Body.Close()
		t.Error("POST after Close should fail")
	}
}
