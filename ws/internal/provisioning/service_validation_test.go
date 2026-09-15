package provisioning_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
)

// TestValidators_WrapSentinels pins that each validator branch wraps its per-class sentinel
// (so the API layer's errors.Is classification can map it to a distinct 400 code) while
// preserving the detail message.
func TestValidators_WrapSentinels(t *testing.T) {
	t.Parallel()

	longName := strings.Repeat("x", 257)
	validPEM := strings.Repeat("x", 60) // passes the >=50 length gate; only the field under test is bad

	tests := []struct {
		name         string
		err          error
		wantSentinel error
		wantDetail   string // substring the detail message must retain
	}{
		{"slug format", provisioning.ValidateSlug("Bad_Slug"), provisioning.ErrSlugInvalid, "3-63"},
		{"name empty", (&provisioning.Tenant{Slug: "ok-slug", Name: ""}).Validate(), provisioning.ErrNameInvalid, "required"},
		{"name too long", (&provisioning.Tenant{Slug: "ok-slug", Name: longName}).Validate(), provisioning.ErrNameInvalid, "256"},
		{"consumer type", (&provisioning.Tenant{Slug: "ok-slug", Name: "n", ConsumerType: provisioning.ConsumerType("exclusive")}).Validate(), provisioning.ErrConsumerTypeInvalid, "consumer type"},
		{"key id", provisioning.ValidateKeyInput("Bad_Key", provisioning.AlgorithmES256, validPEM), provisioning.ErrKeyInvalid, "key ID"},
		{"key algorithm", provisioning.ValidateKeyInput("ok-key", provisioning.Algorithm("ES512"), validPEM), provisioning.ErrKeyInvalid, "algorithm"},
		{"key empty pem", provisioning.ValidateKeyInput("ok-key", provisioning.AlgorithmES256, ""), provisioning.ErrKeyInvalid, "public key"},
		{"key short pem", provisioning.ValidateKeyInput("ok-key", provisioning.AlgorithmES256, "short"), provisioning.ErrKeyInvalid, "PEM"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.err == nil {
				t.Fatalf("expected an error, got nil")
			}
			if !errors.Is(tt.err, tt.wantSentinel) {
				t.Errorf("errors.Is(%v, %v) = false; want the sentinel to be wrapped", tt.err, tt.wantSentinel)
			}
			if !strings.Contains(tt.err.Error(), tt.wantDetail) {
				t.Errorf("detail message %q does not retain %q", tt.err.Error(), tt.wantDetail)
			}
		})
	}
}

// TestValidators_ValidInputPasses is the other side of the gate — valid input yields nil.
func TestValidators_ValidInputPasses(t *testing.T) {
	t.Parallel()

	if err := provisioning.ValidateSlug("acme-corp"); err != nil {
		t.Errorf("ValidateSlug(valid) = %v, want nil", err)
	}
	if err := (&provisioning.Tenant{Slug: "acme-corp", Name: "Acme", ConsumerType: provisioning.ConsumerShared}).Validate(); err != nil {
		t.Errorf("Tenant.Validate(valid) = %v, want nil", err)
	}
	if err := provisioning.ValidateKeyInput("acme-key-1", provisioning.AlgorithmES256, strings.Repeat("x", 60)); err != nil {
		t.Errorf("ValidateKeyInput(valid) = %v, want nil", err)
	}
}

// TestCreateTenant_InvalidKeyLeavesNoState pins that an invalid embedded public_key
// is rejected (ErrKeyInvalid) BEFORE any side effect, so no Kafka topics and no tenant record
// are created. Verified at unit level via the mock Kafka admin and tenant store.
func TestCreateTenant_InvalidKeyLeavesNoState(t *testing.T) {
	t.Parallel()

	kafkaAdmin := testutil.NewMockKafkaAdmin()
	ts := testutil.NewMockTenantStore()
	svc := newTestServiceWithKafka(kafkaAdmin, ts)

	_, err := svc.CreateTenant(context.Background(), provisioning.CreateTenantRequest{
		Slug: "acme-corp",
		Name: "Acme Corp",
		PublicKey: &provisioning.CreateKeyRequest{
			KeyID:     "acme-key-1",
			Algorithm: provisioning.AlgorithmES256,
			PublicKey: "short", // fails the PEM length gate → ErrKeyInvalid
		},
	})

	if err == nil {
		t.Fatal("expected an error for invalid public_key, got nil")
	}
	if !errors.Is(err, provisioning.ErrKeyInvalid) {
		t.Errorf("err = %v, want ErrKeyInvalid", err)
	}
	if topics := kafkaAdmin.GetTopics(); len(topics) != 0 {
		t.Errorf("expected zero topics created (validation before side effects), got %v", topics)
	}
	_, total, listErr := ts.List(context.Background(), provisioning.ListOptions{Limit: 10})
	if listErr != nil {
		t.Fatalf("list tenants: %v", listErr)
	}
	if total != 0 {
		t.Errorf("expected no tenant created, got %d", total)
	}
}
