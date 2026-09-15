package sse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestClient_ParseMessageEvent(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "id: 1\nevent: message\ndata: {\"msg\":\"hello\"}\n\n")
	}))
	defer srv.Close()

	client, status, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: srv.URL,
		Channels:   []string{"test.ch"},
		Token:      "test-token",
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v (status=%d)", err, status)
	}
	defer client.Close()

	event, err := client.ReadEvent(context.Background())
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}

	if event.ID != "1" {
		t.Errorf("ID = %q, want %q", event.ID, "1")
	}
	if event.Type != "message" {
		t.Errorf("Type = %q, want %q", event.Type, "message")
	}
	if event.Data != `{"msg":"hello"}` {
		t.Errorf("Data = %q, want %q", event.Data, `{"msg":"hello"}`)
	}
}

func TestClient_ParseKeepaliveSkipped(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// Keepalive comment followed by a real event
		fmt.Fprint(w, ": keepalive\n\nid: 42\nevent: message\ndata: after-keepalive\n\n")
	}))
	defer srv.Close()

	client, _, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: srv.URL,
		Channels:   []string{"test.ch"},
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	event, err := client.ReadEvent(context.Background())
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}

	// Should skip the keepalive and return the real event
	if event.ID != "42" {
		t.Errorf("ID = %q, want %q", event.ID, "42")
	}
	if event.Data != "after-keepalive" {
		t.Errorf("Data = %q, want %q", event.Data, "after-keepalive")
	}
}

func TestClient_ParseMultiLineData(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "id: 5\nevent: message\ndata: line1\ndata: line2\ndata: line3\n\n")
	}))
	defer srv.Close()

	client, _, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: srv.URL,
		Channels:   []string{"test.ch"},
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	event, err := client.ReadEvent(context.Background())
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}

	want := "line1\nline2\nline3"
	if event.Data != want {
		t.Errorf("Data = %q, want %q", event.Data, want)
	}
}

func TestClient_DefaultEventType(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// No "event:" line — should default to "message"
		fmt.Fprint(w, "id: 1\ndata: no-type\n\n")
	}))
	defer srv.Close()

	client, _, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: srv.URL,
		Channels:   []string{"test.ch"},
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	event, err := client.ReadEvent(context.Background())
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}

	if event.Type != "message" {
		t.Errorf("Type = %q, want %q (default)", event.Type, "message")
	}
}

func TestClient_ConnectError(t *testing.T) {
	t.Parallel()

	_, status, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: "http://127.0.0.1:1", // invalid port — connection refused
		Channels:   []string{"test.ch"},
		Logger:     zerolog.Nop(),
	})
	if err == nil {
		t.Fatal("expected error for bad URL")
	}
	if status != 0 {
		t.Errorf("status = %d, want 0 for transport error", status)
	}
}

func TestClient_ConnectHTTPError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	_, status, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: srv.URL,
		Channels:   []string{"test.ch"},
		Logger:     zerolog.Nop(),
	})
	if err == nil {
		t.Fatal("expected error for 401")
	}
	if status != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", status, http.StatusUnauthorized)
	}
}

func TestClient_CleanClose(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "id: 1\ndata: test\n\n")
	}))
	defer srv.Close()

	client, _, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: srv.URL,
		Channels:   []string{"test.ch"},
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Close should succeed
	if err := client.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	// Second close should be safe (sync.Once)
	if err := client.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestClient_ReadEventEOF(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// No events — body closes immediately
	}))
	defer srv.Close()

	client, _, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: srv.URL,
		Channels:   []string{"test.ch"},
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	_, err = client.ReadEvent(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Errorf("err = %v, want io.EOF", err)
	}
}

func TestClient_ReadEventContextTimeout(t *testing.T) {
	t.Parallel()

	// Server that sends headers but no events — ReadEvent blocks until ctx timeout.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Block until the client disconnects
		<-r.Context().Done()
	}))
	defer srv.Close()

	client, _, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: srv.URL,
		Channels:   []string{"test.ch"},
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = client.ReadEvent(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from canceled context")
	}

	// Should return promptly (< 1s), not hang forever
	if elapsed > 1*time.Second {
		t.Errorf("ReadEvent took %v, expected < 1s (context timeout mechanism failed)", elapsed)
	}
}

// TestConnectRaw_SurfacesErrorBody pins the load-bearing behavior of ConnectRaw: on a non-200
// response it returns the response body so callers can extract the error `code` (e.g. to tell a
// feature-gate EDITION_LIMIT from a setup FORBIDDEN). On success the body is nil (owned by *Client).
func TestConnectRaw_SurfacesErrorBody(t *testing.T) {
	t.Parallel()

	t.Run("non-200 returns body and nil client", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"code":"EDITION_LIMIT","message":"This feature requires pro edition or higher"}`)
		}))
		defer srv.Close()

		client, status, body, err := ConnectRaw(context.Background(), ConnectConfig{
			GatewayURL: srv.URL,
			Channels:   []string{"test.ch"},
			Logger:     zerolog.Nop(),
		})
		if err == nil {
			t.Fatal("expected error on 403")
		}
		if client != nil {
			t.Error("expected nil client on 403")
		}
		if status != http.StatusForbidden {
			t.Errorf("status = %d, want 403", status)
		}
		if !strings.Contains(string(body), `"code":"EDITION_LIMIT"`) {
			t.Errorf("body = %q, want it to contain the JSON code field", string(body))
		}
	})

	t.Run("200 returns client and nil body", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		client, status, body, err := ConnectRaw(context.Background(), ConnectConfig{
			GatewayURL: srv.URL,
			Channels:   []string{"test.ch"},
			Logger:     zerolog.Nop(),
		})
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer client.Close()
		if status != 0 {
			t.Errorf("status = %d, want 0 on success", status)
		}
		if body != nil {
			t.Errorf("body = %q, want nil on success (owned by *Client)", string(body))
		}
	})
}

func TestClient_APIKeyHeader(t *testing.T) {
	t.Parallel()

	var gotAPIKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("X-API-Key")
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client, _, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: srv.URL,
		Channels:   []string{"test.ch"},
		APIKey:     "test-api-key-123",
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if gotAPIKey != "test-api-key-123" {
		t.Errorf("X-API-Key = %q, want %q", gotAPIKey, "test-api-key-123")
	}
}
