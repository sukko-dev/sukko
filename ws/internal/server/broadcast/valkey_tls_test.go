package broadcast

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// =============================================================================
// Valkey TLS Configuration Tests
// =============================================================================
//
// These tests verify the TLS config building paths in newValkeyBus().
// Since newValkeyBus() attempts a live Valkey connection (Ping), we can only
// test error paths that fail before the connection attempt (CA file read, PEM parse).

func TestNewValkeyBus_TLS_MissingCAFile(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Type:            "valkey",
		BufferSize:      1024,
		ShutdownTimeout: 5 * time.Second,
		Valkey: ValkeyConfig{
			Addrs:      []string{"localhost:6379"},
			Channel:    "ws.broadcast",
			TLSEnabled: true,
			TLSCAPath:  "/nonexistent/path/ca.pem",
		},
	}

	_, err := newValkeyBus(cfg, zerolog.Nop())
	if err == nil {
		t.Fatal("expected error for missing CA file, got nil")
	}
	if !strings.Contains(err.Error(), "read CA cert") {
		t.Errorf("error = %q, want containing 'read CA cert'", err.Error())
	}
}

func TestNewValkeyBus_TLS_InvalidPEM(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/invalid-ca.pem"
	if err := os.WriteFile(tmpFile, []byte("not-a-valid-pem-certificate"), 0o600); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	cfg := Config{
		Type:            "valkey",
		BufferSize:      1024,
		ShutdownTimeout: 5 * time.Second,
		Valkey: ValkeyConfig{
			Addrs:      []string{"localhost:6379"},
			Channel:    "ws.broadcast",
			TLSEnabled: true,
			TLSCAPath:  tmpFile,
		},
	}

	_, err := newValkeyBus(cfg, zerolog.Nop())
	if err == nil {
		t.Fatal("expected error for invalid PEM, got nil")
	}
	if !strings.Contains(err.Error(), "parse CA cert") {
		t.Errorf("error = %q, want containing 'parse CA cert'", err.Error())
	}
}

func TestNewValkeyBus_TLS_ValidCAFile(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/valid-ca.pem"
	if err := os.WriteFile(tmpFile, tlsTestCACert, 0o600); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	cfg := Config{
		Type:            "valkey",
		BufferSize:      1024,
		ShutdownTimeout: 5 * time.Second,
		Valkey: ValkeyConfig{
			Addrs:      []string{"localhost:6379"},
			Channel:    "ws.broadcast",
			TLSEnabled: true,
			TLSCAPath:  tmpFile,
		},
	}

	// CA file loads and parses successfully. The connection itself will fail
	// (no Valkey server), but TLS config building is validated.
	_, err := newValkeyBus(cfg, zerolog.Nop())
	if err == nil {
		t.Skip("Valkey server available — connection succeeded (unexpected in CI)")
		return
	}
	// The error should be a connection failure, not a TLS config failure
	if strings.Contains(err.Error(), "CA cert") {
		t.Errorf("error = %q, should not contain CA cert error (TLS config should succeed)", err.Error())
	}
}

func TestNewValkeyBus_TLS_InsecureSkipVerify(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Type:            "valkey",
		BufferSize:      1024,
		ShutdownTimeout: 5 * time.Second,
		Valkey: ValkeyConfig{
			Addrs:       []string{"localhost:6379"},
			Channel:     "ws.broadcast",
			TLSEnabled:  true,
			TLSInsecure: true,
			// No CA path — TLS with InsecureSkipVerify only
		},
	}

	// TLS config building should succeed (no CA to load).
	// Connection will fail (no Valkey server), but the error should be connection-related.
	_, err := newValkeyBus(cfg, zerolog.Nop())
	if err == nil {
		t.Skip("Valkey server available — connection succeeded (unexpected in CI)")
		return
	}
	// Should be a connection error, not a TLS config error
	if strings.Contains(err.Error(), "CA cert") || strings.Contains(err.Error(), "parse") {
		t.Errorf("error = %q, should not be a TLS config error", err.Error())
	}
}

func TestNewValkeyBus_MissingAddrs(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Type:            "valkey",
		BufferSize:      1024,
		ShutdownTimeout: 5 * time.Second,
		Valkey: ValkeyConfig{
			Addrs: nil,
		},
	}

	_, err := newValkeyBus(cfg, zerolog.Nop())
	if err == nil {
		t.Fatal("expected error for missing addrs, got nil")
	}
	if !strings.Contains(err.Error(), "at least one address") {
		t.Errorf("error = %q, want containing 'at least one address'", err.Error())
	}
}

// Test CA cert defined in tls_test_helpers_test.go (shared across Valkey TLS tests).
