package revocation

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestRevoke_LogsTenantSlug verifies the revocation store (data plane) logs the tenant
// under the explicit tenant_slug key, never the legacy tenant_id (#161 log-key hygiene).
func TestRevoke_LogsTenantSlug(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	s := New(zerolog.New(&buf))
	defer s.Close()

	if err := s.Revoke(Entry{
		TenantID:  "acme",
		Type:      "token",
		JTI:       "tok-1",
		RevokedAt: time.Now().Unix(),
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("Revoke() error = %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, `"tenant_slug":"acme"`) {
		t.Errorf("expected tenant_slug key with slug value; got: %s", out)
	}
	if strings.Contains(out, `"tenant_id"`) {
		t.Errorf("log must not use the legacy tenant_id key; got: %s", out)
	}
}

func TestRevoke_ByJTI(t *testing.T) {
	t.Parallel()
	s := New(zerolog.Nop())
	defer s.Close()

	err := s.Revoke(Entry{
		TenantID:  "acme",
		Type:      "token",
		JTI:       "tok-123",
		RevokedAt: time.Now().Unix(),
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("Revoke() error = %v", err)
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1", s.Len())
	}
}

func TestRevoke_BySub(t *testing.T) {
	t.Parallel()
	s := New(zerolog.Nop())
	defer s.Close()

	err := s.Revoke(Entry{
		TenantID:  "acme",
		Type:      "user",
		Sub:       "alice",
		RevokedAt: time.Now().Unix(),
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("Revoke() error = %v", err)
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1", s.Len())
	}
}

func TestRevoke_Validation(t *testing.T) {
	t.Parallel()
	s := New(zerolog.Nop())
	defer s.Close()

	tests := []struct {
		name  string
		entry Entry
	}{
		{"missing tenant_id", Entry{Type: "token", JTI: "x"}},
		{"invalid type", Entry{TenantID: "a", Type: "bad"}},
		{"user missing sub", Entry{TenantID: "a", Type: "user"}},
		{"token missing jti", Entry{TenantID: "a", Type: "token"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := s.Revoke(tt.entry); err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestRevoke_Idempotent(t *testing.T) {
	t.Parallel()
	s := New(zerolog.Nop())
	defer s.Close()

	entry := Entry{
		TenantID:  "acme",
		Type:      "token",
		JTI:       "tok-dup",
		RevokedAt: time.Now().Unix(),
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	}

	_ = s.Revoke(entry)
	_ = s.Revoke(entry)

	if s.Len() != 1 {
		t.Errorf("duplicate revocation created %d entries, want 1", s.Len())
	}
}

func TestPrune_RemovesExpired(t *testing.T) {
	t.Parallel()
	s := New(zerolog.Nop())
	defer s.Close()

	// Add an already-expired entry
	_ = s.Revoke(Entry{
		TenantID:  "acme",
		Type:      "token",
		JTI:       "expired",
		RevokedAt: time.Now().Add(-2 * time.Hour).Unix(),
		ExpiresAt: time.Now().Add(-1 * time.Hour).Unix(),
	})

	// Add a valid entry
	_ = s.Revoke(Entry{
		TenantID:  "acme",
		Type:      "token",
		JTI:       "valid",
		RevokedAt: time.Now().Unix(),
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	})

	pruned := s.Prune()
	if pruned != 1 {
		t.Errorf("Prune() = %d, want 1", pruned)
	}
	if s.Len() != 1 {
		t.Errorf("Len() after prune = %d, want 1", s.Len())
	}
}

func TestSnapshot_ExcludesExpired(t *testing.T) {
	t.Parallel()
	s := New(zerolog.Nop())
	defer s.Close()

	_ = s.Revoke(Entry{
		TenantID: "acme", Type: "token", JTI: "expired",
		RevokedAt: time.Now().Add(-2 * time.Hour).Unix(),
		ExpiresAt: time.Now().Add(-1 * time.Hour).Unix(),
	})
	_ = s.Revoke(Entry{
		TenantID: "acme", Type: "token", JTI: "valid",
		RevokedAt: time.Now().Unix(),
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	})

	snap := s.Snapshot()
	if len(snap) != 1 {
		t.Errorf("Snapshot() returned %d entries, want 1", len(snap))
	}
	if len(snap) > 0 && snap[0].JTI != "valid" {
		t.Errorf("Snapshot()[0].JTI = %q, want %q", snap[0].JTI, "valid")
	}
}

func TestRevoke_TenantIsolation(t *testing.T) {
	t.Parallel()
	s := New(zerolog.Nop())
	defer s.Close()

	_ = s.Revoke(Entry{
		TenantID: "acme", Type: "user", Sub: "alice",
		RevokedAt: time.Now().Unix(),
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	})
	_ = s.Revoke(Entry{
		TenantID: "globex", Type: "user", Sub: "alice",
		RevokedAt: time.Now().Unix(),
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	})

	// Same sub, different tenants — two separate entries
	if s.Len() != 2 {
		t.Errorf("Len() = %d, want 2 (different tenants)", s.Len())
	}
}
