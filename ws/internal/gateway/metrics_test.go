package gateway

import (
	"testing"
	"time"
)

func TestRecordConnection(t *testing.T) {
	t.Parallel()
	// Verify counter increments without panicking
	RecordConnection()
}

func TestRecordDisconnection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		reason   string
		duration time.Duration
	}{
		{"normal disconnect", "normal", 30 * time.Second},
		{"no token", "no_token", 100 * time.Millisecond},
		{"invalid token", "invalid_token", 50 * time.Millisecond},
		{"backend unavailable", "backend_unavailable", 2 * time.Second},
		{"upgrade failed", "upgrade_failed", 10 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			RecordDisconnection(tt.reason, tt.duration)
		})
	}
}

func TestRecordAuthValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		status  string
		method  string
		latency time.Duration
	}{
		{"success jwt", "success", "jwt", 10 * time.Millisecond},
		{"failed api_key", "failed", "api_key", 5 * time.Millisecond},
		{"skipped none", "skipped", "none", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			RecordAuthValidation(tt.status, tt.method, tt.latency)
		})
	}
}

func TestRecordChannelCheck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		result string
	}{
		{"allowed", "allowed"},
		{"denied", "denied"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			RecordChannelCheck(tt.result)
		})
	}
}

func TestRecordMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		direction string
		bytes     int
	}{
		{"client to backend small", "client_to_backend", 128},
		{"client to backend large", "client_to_backend", 65536},
		{"backend to client small", "backend_to_client", 256},
		{"backend to client large", "backend_to_client", 131072},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			RecordMessage(tt.direction, tt.bytes)
		})
	}
}

func TestRecordProxyError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		errorType string
	}{
		{"client read error", "client_read_error"},
		{"client write error", "client_write_error"},
		{"backend read error", "backend_read_error"},
		{"backend write error", "backend_write_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			RecordProxyError(tt.errorType)
		})
	}
}

func TestRecordBackendConnect(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		status  string
		latency time.Duration
	}{
		{"success fast", "success", 50 * time.Millisecond},
		{"success slow", "success", 500 * time.Millisecond},
		{"failed timeout", "failed", time.Second},
		{"failed connection refused", "failed", 10 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			RecordBackendConnect(tt.status, tt.latency)
		})
	}
}
