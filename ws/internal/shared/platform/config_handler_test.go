package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type testConfig struct {
	Port     int    `env:"PORT"`
	Host     string `env:"HOST"`
	Password string `env:"PASSWORD" redact:"true"`
	Token    string `env:"TOKEN" redact:"true"`
	NoEnvTag string
	private  string //nolint:unused // test unexported field handling
}

func TestRedactConfig_RedactsSensitiveFields(t *testing.T) {
	t.Parallel()
	cfg := testConfig{
		Port:     8080,
		Host:     "localhost",
		Password: "secret123",
		Token:    "tok-abc",
	}

	result := RedactConfig(cfg)

	if result["PASSWORD"] != "[REDACTED]" {
		t.Errorf("PASSWORD = %v, want [REDACTED]", result["PASSWORD"])
	}
	if result["TOKEN"] != "[REDACTED]" {
		t.Errorf("TOKEN = %v, want [REDACTED]", result["TOKEN"])
	}
}

func TestRedactConfig_PreservesNonSensitiveFields(t *testing.T) {
	t.Parallel()
	cfg := testConfig{
		Port: 8080,
		Host: "localhost",
	}

	result := RedactConfig(cfg)

	if result["PORT"] != 8080 {
		t.Errorf("PORT = %v, want 8080", result["PORT"])
	}
	if result["HOST"] != "localhost" {
		t.Errorf("HOST = %v, want localhost", result["HOST"])
	}
}

func TestRedactConfig_EmptySecretStaysEmpty(t *testing.T) {
	t.Parallel()
	cfg := testConfig{
		Port:     8080,
		Password: "",
		Token:    "",
	}

	result := RedactConfig(cfg)

	if result["PASSWORD"] != "" {
		t.Errorf("PASSWORD = %v, want empty string", result["PASSWORD"])
	}
	if result["TOKEN"] != "" {
		t.Errorf("TOKEN = %v, want empty string", result["TOKEN"])
	}
}

func TestRedactConfig_FallsBackToFieldName(t *testing.T) {
	t.Parallel()
	cfg := testConfig{NoEnvTag: "value"}

	result := RedactConfig(cfg)

	if result["NoEnvTag"] != "value" {
		t.Errorf("NoEnvTag = %v, want value", result["NoEnvTag"])
	}
	if _, ok := result["private"]; ok {
		t.Error("unexported field should not be in result")
	}
}

func TestRedactConfig_PointerStruct(t *testing.T) {
	t.Parallel()
	cfg := &testConfig{
		Port:     9090,
		Password: "hidden",
	}

	result := RedactConfig(cfg)

	if result["PORT"] != 9090 {
		t.Errorf("PORT = %v, want 9090", result["PORT"])
	}
	if result["PASSWORD"] != "[REDACTED]" {
		t.Errorf("PASSWORD = %v, want [REDACTED]", result["PASSWORD"])
	}
}

func TestRedactConfig_NonStruct(t *testing.T) {
	t.Parallel()

	result := RedactConfig("not a struct")
	if len(result) != 0 {
		t.Errorf("expected empty map for non-struct, got %v", result)
	}
}

type nestedInner struct {
	Secret string `env:"INNER_SECRET" redact:"true"`
	Host   string `env:"INNER_HOST"`
}

type nestedConfig struct {
	Outer string      `env:"OUTER"`
	Inner nestedInner `env:"INNER"`
}

func TestRedactConfig_NestedStruct(t *testing.T) {
	t.Parallel()
	cfg := nestedConfig{
		Outer: "visible",
		Inner: nestedInner{
			Secret: "hidden",
			Host:   "db.example.com",
		},
	}

	result := RedactConfig(cfg)

	if result["OUTER"] != "visible" {
		t.Errorf("OUTER = %v, want visible", result["OUTER"])
	}
	if result["INNER_SECRET"] != "[REDACTED]" {
		t.Errorf("INNER_SECRET = %v, want [REDACTED]", result["INNER_SECRET"])
	}
	if result["INNER_HOST"] != "db.example.com" {
		t.Errorf("INNER_HOST = %v, want db.example.com", result["INNER_HOST"])
	}
	// Nested struct itself should NOT appear as a key
	if _, ok := result["INNER"]; ok {
		t.Error("nested struct should be flattened, not stored as a key")
	}
}

func TestConfigHandler_Returns200JSON(t *testing.T) {
	t.Parallel()
	cfg := testConfig{
		Port:     8080,
		Host:     "localhost",
		Password: "secret",
	}

	handler := ConfigHandler(cfg)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/config", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}

	if body["PASSWORD"] != "[REDACTED]" {
		t.Errorf("PASSWORD = %v, want [REDACTED]", body["PASSWORD"])
	}
	if body["HOST"] != "localhost" {
		t.Errorf("HOST = %v, want localhost", body["HOST"])
	}
}
