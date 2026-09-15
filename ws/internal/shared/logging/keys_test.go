package logging

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
)

// TestTenantLogKeyValues pins the shared tenant log-key constant values. The legacy
// ambiguous "tenant_id" key is forbidden for tenant identifiers (#161) — a regression that
// reintroduced it would defeat slug-vs-UUID log correlation.
func TestTenantLogKeyValues(t *testing.T) {
	t.Parallel()
	if LogKeyTenantSlug != "tenant_slug" {
		t.Errorf("LogKeyTenantSlug = %q, want %q", LogKeyTenantSlug, "tenant_slug")
	}
	if LogKeyTenantUUID != "tenant_uuid" {
		t.Errorf("LogKeyTenantUUID = %q, want %q", LogKeyTenantUUID, "tenant_uuid")
	}
	if LogKeyTenantSlug == "tenant_id" || LogKeyTenantUUID == "tenant_id" {
		t.Error("tenant log keys must not use the legacy ambiguous 'tenant_id' key")
	}
}

// TestTenantLogKeysEmitDistinctFields verifies zerolog emits the slug and UUID under
// distinct, explicit keys and never under "tenant_id".
func TestTenantLogKeysEmitDistinctFields(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	logger.Info().
		Str(LogKeyTenantSlug, "acme").
		Str(LogKeyTenantUUID, "11111111-2222-3333-4444-555555555555").
		Msg("test")

	var fields map[string]any
	if err := json.Unmarshal(buf.Bytes(), &fields); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}
	if got := fields["tenant_slug"]; got != "acme" {
		t.Errorf("tenant_slug = %v, want acme", got)
	}
	if got := fields["tenant_uuid"]; got != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("tenant_uuid = %v, want the UUID", got)
	}
	if _, found := fields["tenant_id"]; found {
		t.Error("no tenant identifier may be emitted under the legacy 'tenant_id' key")
	}
}
