package version

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGet(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		serviceName string
	}{
		{
			name:        "ws-server service",
			serviceName: "ws-server",
		},
		{
			name:        "auth-service",
			serviceName: "auth-service",
		},
		{
			name:        "empty service name",
			serviceName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			info := Get(tt.serviceName)

			if info.Service != tt.serviceName {
				t.Errorf("Get(%q).Service = %q, want %q", tt.serviceName, info.Service, tt.serviceName)
			}
			if info.Version != Version {
				t.Errorf("Get(%q).Version = %q, want %q", tt.serviceName, info.Version, Version)
			}
			if info.CommitHash != CommitHash {
				t.Errorf("Get(%q).CommitHash = %q, want %q", tt.serviceName, info.CommitHash, CommitHash)
			}
			if info.BuildTime != BuildTime {
				t.Errorf("Get(%q).BuildTime = %q, want %q", tt.serviceName, info.BuildTime, BuildTime)
			}
		})
	}
}

func TestString(t *testing.T) {
	t.Parallel()
	expected := Version + " (" + CommitHash + ")"
	got := String()

	if got != expected {
		t.Errorf("String() = %q, want %q", got, expected)
	}
}

func TestString_DefaultValues(t *testing.T) {
	t.Parallel()
	// With default values, should return "dev (unknown)"
	expected := "dev (unknown)"
	got := String()

	if got != expected {
		t.Errorf("String() with defaults = %q, want %q", got, expected)
	}
}

func TestHandler(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		serviceName string
	}{
		{
			name:        "ws-server handler",
			serviceName: "ws-server",
		},
		{
			name:        "auth-service handler",
			serviceName: "auth-service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler := Handler(tt.serviceName)

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/version", nil)
			rec := httptest.NewRecorder()

			handler(rec, req)

			// Check status code
			if rec.Code != http.StatusOK {
				t.Errorf("Handler returned status %d, want %d", rec.Code, http.StatusOK)
			}

			// Check Content-Type header
			contentType := rec.Header().Get("Content-Type")
			if contentType != "application/json" {
				t.Errorf("Handler Content-Type = %q, want %q", contentType, "application/json")
			}

			// Check response body
			var info Info
			if err := json.NewDecoder(rec.Body).Decode(&info); err != nil {
				t.Fatalf("Failed to decode response body: %v", err)
			}

			if info.Service != tt.serviceName {
				t.Errorf("Response Service = %q, want %q", info.Service, tt.serviceName)
			}
			if info.Version != Version {
				t.Errorf("Response Version = %q, want %q", info.Version, Version)
			}
			if info.CommitHash != CommitHash {
				t.Errorf("Response CommitHash = %q, want %q", info.CommitHash, CommitHash)
			}
			if info.BuildTime != BuildTime {
				t.Errorf("Response BuildTime = %q, want %q", info.BuildTime, BuildTime)
			}
		})
	}
}

func TestInfo_JSONSerialization(t *testing.T) {
	t.Parallel()
	info := Info{
		Version:    "v1.0.0",
		CommitHash: "abc123",
		BuildTime:  "2024-01-15T10:30:00Z",
		Service:    "test-service",
	}

	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Failed to marshal Info: %v", err)
	}

	// Verify JSON field names (snake_case)
	expected := `{"version":"v1.0.0","commit_hash":"abc123","build_time":"2024-01-15T10:30:00Z","service":"test-service"}`
	if string(data) != expected {
		t.Errorf("JSON marshal = %s, want %s", string(data), expected)
	}

	// Verify round-trip
	var decoded Info
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal Info: %v", err)
	}

	if decoded != info {
		t.Errorf("Round-trip failed: got %+v, want %+v", decoded, info)
	}
}
