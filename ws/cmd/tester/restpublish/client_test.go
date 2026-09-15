package restpublish

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_PublishSuccess(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/publish" {
			t.Errorf("path = %s, want /api/v1/publish", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-jwt" {
			t.Errorf("Authorization = %q, want %q", r.Header.Get("Authorization"), "Bearer test-jwt")
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q, want %q", r.Header.Get("Content-Type"), "application/json")
		}

		body, _ := io.ReadAll(r.Body)
		var req Request
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("unmarshal request: %v", err)
		}
		if req.Channel != "general.test" {
			t.Errorf("channel = %q, want %q", req.Channel, "general.test")
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"accepted","channel":"general.test"}`)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := client.Publish(context.Background(), Request{
		Channel: "general.test",
		Data:    json.RawMessage(`{"msg":"hello"}`),
	}, AuthConfig{Token: "test-jwt"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if resp.Status != "accepted" {
		t.Errorf("Status = %q, want %q", resp.Status, "accepted")
	}
	if resp.Channel != "general.test" {
		t.Errorf("Channel = %q, want %q", resp.Channel, "general.test")
	}
}

func TestClient_PublishAPIKey(t *testing.T) {
	t.Parallel()

	var gotAPIKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("X-API-Key")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"accepted","channel":"test.ch"}`)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.Publish(context.Background(), Request{
		Channel: "test.ch",
		Data:    json.RawMessage(`{}`),
	}, AuthConfig{APIKey: "key-123"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if gotAPIKey != "key-123" {
		t.Errorf("X-API-Key = %q, want %q", gotAPIKey, "key-123")
	}
}

func TestClient_PublishHTTPErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
	}{
		{"missing channel", http.StatusBadRequest},
		{"no auth", http.StatusUnauthorized},
		{"body too large", http.StatusRequestEntityTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.statusCode)
				fmt.Fprintf(w, `{"code":"ERROR","message":"%s"}`, tt.name)
			}))
			defer srv.Close()

			client := NewClient(srv.URL)
			_, err := client.Publish(context.Background(), Request{
				Channel: "test.ch",
				Data:    json.RawMessage(`{}`),
			}, AuthConfig{Token: "jwt"})
			if err == nil {
				t.Fatalf("expected error for HTTP %d", tt.statusCode)
			}
		})
	}
}

func TestClient_PublishRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        []byte
		contentType string
		wantStatus  int
	}{
		{
			name:        "invalid JSON",
			body:        []byte(`not json`),
			contentType: "application/json",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "wrong content type",
			body:        []byte(`{"channel":"x","data":{}}`),
			contentType: "text/plain",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ct := r.Header.Get("Content-Type")
				if ct != "application/json" {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, `{"code":"INVALID_REQUEST","message":"invalid content type"}`)
					return
				}
				body, _ := io.ReadAll(r.Body)
				var req Request
				if err := json.Unmarshal(body, &req); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, `{"code":"INVALID_REQUEST","message":"invalid JSON"}`)
					return
				}
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, `{"status":"accepted","channel":"x"}`)
			}))
			defer srv.Close()

			client := NewClient(srv.URL)
			status, _, err := client.PublishRaw(context.Background(), tt.body, AuthConfig{Token: "jwt"}, tt.contentType)
			if err != nil {
				t.Fatalf("PublishRaw: %v", err)
			}
			if status != tt.wantStatus {
				t.Errorf("status = %d, want %d", status, tt.wantStatus)
			}
		})
	}
}

func TestClient_PublishRawNoAuth(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" && r.Header.Get("X-API-Key") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"code":"UNAUTHORIZED","message":"no credentials"}`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	status, _, err := client.PublishRaw(context.Background(), []byte(`{"channel":"x","data":{}}`), AuthConfig{}, "application/json")
	if err != nil {
		t.Fatalf("PublishRaw: %v", err)
	}
	if status != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", status, http.StatusUnauthorized)
	}
}
