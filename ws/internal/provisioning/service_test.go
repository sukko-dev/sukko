package provisioning_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
	sharedkafka "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

func newTestService() (*provisioning.Service, *testutil.MockTenantStore, *testutil.MockKeyStore, *testutil.MockKafkaAdmin) {
	tenantStore := testutil.NewMockTenantStore()
	keyStore := testutil.NewMockKeyStore()
	routingRulesStore := testutil.NewMockRoutingRulesStore()
	quotaStore := testutil.NewMockQuotaStore()
	auditStore := testutil.NewMockAuditStore()
	kafkaAdmin := testutil.NewMockKafkaAdmin()

	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 tenantStore,
		KeyStore:                    keyStore,
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           routingRulesStore,
		QuotaStore:                  quotaStore,
		AuditStore:                  auditStore,
		KafkaAdmin:                  kafkaAdmin,
		EventBus:                    eventbus.New(zerolog.Nop()),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          50,
		MaxRoutingRulesPerTenant:    5,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  86400000, // distinct from DefaultRetentionMs to catch wrong config wiring
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
	})
	if err != nil {
		panic("newTestService: " + err.Error())
	}

	return svc, tenantStore, keyStore, kafkaAdmin
}

func TestService_CreateTenant(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		req         provisioning.CreateTenantRequest
		setupMock   func(*testutil.MockTenantStore, *testutil.MockKeyStore)
		wantErr     bool
		errContains string
	}{
		{
			name: "valid tenant without key",
			req: provisioning.CreateTenantRequest{
				Slug: "acme-corp",
				Name: "Acme Corporation",
			},
			wantErr: false,
		},
		{
			name: "valid tenant with consumer type",
			req: provisioning.CreateTenantRequest{
				Slug:         "beta-test",
				Name:         "Beta Test Inc",
				ConsumerType: provisioning.ConsumerDedicated,
			},
			wantErr: false,
		},
		{
			name: "valid tenant with metadata",
			req: provisioning.CreateTenantRequest{
				Slug:     "gamma-labs",
				Name:     "Gamma Labs",
				Metadata: provisioning.Metadata{"env": "prod"},
			},
			wantErr: false,
		},
		{
			name: "invalid tenant ID - uppercase",
			req: provisioning.CreateTenantRequest{
				Slug: "INVALID",
				Name: "Invalid Tenant",
			},
			wantErr:     true,
			errContains: "invalid tenant",
		},
		{
			name: "invalid tenant ID - too short",
			req: provisioning.CreateTenantRequest{
				Slug: "ab",
				Name: "Too Short",
			},
			wantErr:     true,
			errContains: "invalid tenant",
		},
		{
			name: "duplicate tenant",
			req: provisioning.CreateTenantRequest{
				Slug: "existing",
				Name: "Existing Tenant",
			},
			setupMock: func(ts *testutil.MockTenantStore, _ *testutil.MockKeyStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("existing"))
			},
			wantErr:     true,
			errContains: "slug already taken",
		},
		{
			name: "store error",
			req: provisioning.CreateTenantRequest{
				Slug: "store-fail",
				Name: "Store Fail",
			},
			setupMock: func(ts *testutil.MockTenantStore, _ *testutil.MockKeyStore) {
				ts.CreateErr = errors.New("database connection failed")
			},
			wantErr:     true,
			errContains: "database connection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc, tenantStore, keyStore, _ := newTestService()

			if tt.setupMock != nil {
				tt.setupMock(tenantStore, keyStore)
			}

			resp, err := svc.CreateTenant(context.Background(), tt.req)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if resp == nil {
					t.Error("expected response, got nil")
				}
				if resp != nil && resp.Tenant.Slug != tt.req.Slug {
					t.Errorf("tenant slug mismatch: got %q, want %q", resp.Tenant.Slug, tt.req.Slug)
				}
			}
		})
	}
}

func TestService_GetTenantBySlug(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	// Setup: create a tenant
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	tests := []struct {
		name     string
		tenantID string
		wantErr  error
	}{
		{
			name:     "existing tenant",
			tenantID: "acme-corp",
		},
		{
			name:     "non-existing tenant",
			tenantID: "not-found",
			wantErr:  provisioning.ErrTenantNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := svc.GetTenantBySlug(context.Background(), tt.tenantID)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("error = %v, want %v", err, tt.wantErr)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if got == nil {
					t.Error("expected tenant, got nil")
				}
				if got != nil && got.Slug != tt.tenantID {
					t.Errorf("tenant slug mismatch: got %q, want %q", got.Slug, tt.tenantID)
				}
			}
		})
	}
}

func TestService_SuspendAndReactivateTenant(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	// Setup: create a tenant
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	// Test suspend
	err := svc.SuspendTenant(context.Background(), "acme-corp")
	if err != nil {
		t.Fatalf("failed to suspend tenant: %v", err)
	}

	// Verify suspended
	got, _ := svc.GetTenantBySlug(context.Background(), "acme-corp")
	if got.Status != provisioning.StatusSuspended {
		t.Errorf("expected status %q, got %q", provisioning.StatusSuspended, got.Status)
	}

	// Test reactivate
	err = svc.ReactivateTenant(context.Background(), "acme-corp")
	if err != nil {
		t.Fatalf("failed to reactivate tenant: %v", err)
	}

	// Verify active
	got, _ = svc.GetTenantBySlug(context.Background(), "acme-corp")
	if got.Status != provisioning.StatusActive {
		t.Errorf("expected status %q, got %q", provisioning.StatusActive, got.Status)
	}
}

func TestService_SuspendTenant_StateValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		status    provisioning.TenantStatus
		wantErr   error
		wantNoErr bool
	}{
		{
			name:      "suspend active tenant",
			status:    provisioning.StatusActive,
			wantNoErr: true,
		},
		{
			name:    "suspend deleted tenant",
			status:  provisioning.StatusDeleted,
			wantErr: provisioning.ErrTenantDeleted,
		},
		{
			name:    "suspend already-suspended tenant",
			status:  provisioning.StatusSuspended,
			wantErr: provisioning.ErrTenantNotActive,
		},
		{
			name:    "suspend deprovisioning tenant",
			status:  provisioning.StatusDeprovisioning,
			wantErr: provisioning.ErrTenantNotActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc, tenantStore, _, _ := newTestService()

			tenant := testutil.NewTestTenant("acme-corp")
			tenant.Status = tt.status
			_ = tenantStore.Create(context.Background(), tenant)

			err := svc.SuspendTenant(context.Background(), "acme-corp")
			if tt.wantNoErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("expected %v, got: %v", tt.wantErr, err)
			}
		})
	}
}

func TestService_ReactivateTenant_StateValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		status    provisioning.TenantStatus
		wantErr   error
		wantNoErr bool
	}{
		{
			name:      "reactivate suspended tenant",
			status:    provisioning.StatusSuspended,
			wantNoErr: true,
		},
		{
			name:    "reactivate deleted tenant",
			status:  provisioning.StatusDeleted,
			wantErr: provisioning.ErrTenantDeleted,
		},
		{
			name:    "reactivate active tenant",
			status:  provisioning.StatusActive,
			wantErr: provisioning.ErrTenantNotActive,
		},
		{
			name:    "reactivate deprovisioning tenant",
			status:  provisioning.StatusDeprovisioning,
			wantErr: provisioning.ErrTenantNotActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc, tenantStore, _, _ := newTestService()

			tenant := testutil.NewTestTenant("acme-corp")
			tenant.Status = tt.status
			_ = tenantStore.Create(context.Background(), tenant)

			err := svc.ReactivateTenant(context.Background(), "acme-corp")
			if tt.wantNoErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("expected %v, got: %v", tt.wantErr, err)
			}
		})
	}
}

func TestService_ReactivateTenant_TopicProvisioning(t *testing.T) {
	t.Parallel()

	errNonIdempotent := errors.New("kafka: unexpected error")

	tests := []struct {
		name       string
		setupKafka func(k *testutil.MockKafkaAdmin)
		wantErr    bool
	}{
		{
			name: "(a) both topics already exist → returns nil, no error",
			setupKafka: func(k *testutil.MockKafkaAdmin) {
				// Both CreateTopic calls return TopicAlreadyExists — idempotent.
				k.CreateTopicErrs = []error{kerr.TopicAlreadyExists, kerr.TopicAlreadyExists}
			},
			wantErr: false,
		},
		{
			name: "(b) DLQ already exists only → returns nil",
			setupKafka: func(k *testutil.MockKafkaAdmin) {
				k.CreateTopicErrs = []error{kerr.TopicAlreadyExists, nil}
			},
			wantErr: false,
		},
		{
			name: "(c) DLQ non-idempotency error → returns nil (graceful degradation)",
			setupKafka: func(k *testutil.MockKafkaAdmin) {
				k.CreateTopicErrs = []error{errNonIdempotent, nil}
			},
			wantErr: false, // §IV: reactivation continues regardless
		},
		{
			name: "(d) default topic non-idempotency error → returns nil (graceful degradation)",
			setupKafka: func(k *testutil.MockKafkaAdmin) {
				k.CreateTopicErrs = []error{nil, errNonIdempotent}
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kafka := testutil.NewMockKafkaAdmin()
			if tt.setupKafka != nil {
				tt.setupKafka(kafka)
			}

			// Put tenant in suspended state so ReactivateTenant proceeds.
			ts := testutil.NewMockTenantStore()
			tenant := testutil.NewTestTenant("acme-corp")
			tenant.Status = provisioning.StatusSuspended
			_ = ts.Create(context.Background(), tenant)

			svc := newTestServiceWithKafka(kafka, ts)

			err := svc.ReactivateTenant(context.Background(), "acme-corp")
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestService_ReactivateTenant_TopicConfig(t *testing.T) {
	t.Parallel()

	kafka := testutil.NewMockKafkaAdmin()
	ts := testutil.NewMockTenantStore()
	tenant := testutil.NewTestTenant("acme-corp")
	tenant.Status = provisioning.StatusSuspended
	_ = ts.Create(context.Background(), tenant)

	svc := newTestServiceWithKafka(kafka, ts)

	if err := svc.ReactivateTenant(context.Background(), "acme-corp"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dlq := dlqTopic("acme-corp")
	def := defaultTopic("acme-corp")

	if p := kafka.CreatedTopicParts[dlq]; p != 1 {
		t.Errorf("DLQ partitions = %d, want 1", p)
	}
	if p := kafka.CreatedTopicParts[def]; p != 3 {
		t.Errorf("default partitions = %d, want 3", p)
	}
	wantDLQRetention := strconv.FormatInt(testDLQRetentionMs, 10)
	wantDefaultRetention := strconv.FormatInt(testDefaultRetentionMs, 10)
	if v := kafka.CreatedTopicConfigs[dlq][sharedkafka.RetentionMsConfigKey]; v != wantDLQRetention {
		t.Errorf("DLQ retention.ms = %q, want %q", v, wantDLQRetention)
	}
	if v := kafka.CreatedTopicConfigs[def][sharedkafka.RetentionMsConfigKey]; v != wantDefaultRetention {
		t.Errorf("default retention.ms = %q, want %q", v, wantDefaultRetention)
	}
}

func TestService_DeprovisionTenant_StateValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		status    provisioning.TenantStatus
		wantErr   error
		wantNoErr bool
	}{
		{
			name:      "deprovision active tenant",
			status:    provisioning.StatusActive,
			wantNoErr: true,
		},
		{
			name:      "deprovision suspended tenant",
			status:    provisioning.StatusSuspended,
			wantNoErr: true,
		},
		{
			name:    "deprovision deleted tenant",
			status:  provisioning.StatusDeleted,
			wantErr: provisioning.ErrTenantDeleted,
		},
		{
			name:    "deprovision already-deprovisioning tenant",
			status:  provisioning.StatusDeprovisioning,
			wantErr: provisioning.ErrTenantNotActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc, tenantStore, _, _ := newTestService()

			tenant := testutil.NewTestTenant("acme-corp")
			tenant.Status = tt.status
			_ = tenantStore.Create(context.Background(), tenant)

			err := svc.DeprovisionTenant(context.Background(), "acme-corp", false)
			if tt.wantNoErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("expected %v, got: %v", tt.wantErr, err)
			}
		})
	}
}

func TestService_DeprovisionTenant(t *testing.T) {
	t.Parallel()
	svc, tenantStore, keyStore, _ := newTestService()

	// Setup: create a tenant with key
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)
	key := testutil.NewTestTenantKey("key-1", tenant.ID)
	_ = keyStore.Create(context.Background(), key)

	// Deprovision
	err := svc.DeprovisionTenant(context.Background(), "acme-corp", false)
	if err != nil {
		t.Fatalf("failed to deprovision tenant: %v", err)
	}

	// Verify status
	got, _ := svc.GetTenantBySlug(context.Background(), "acme-corp")
	if got.Status != provisioning.StatusDeprovisioning {
		t.Errorf("expected status %q, got %q", provisioning.StatusDeprovisioning, got.Status)
	}

	// Verify keys revoked
	keys, _, _ := svc.ListKeys(context.Background(), "acme-corp", provisioning.ListOptions{Limit: 100})
	for _, k := range keys {
		if k.IsActive {
			t.Errorf("key %q should be revoked", k.KeyID)
		}
	}
}

func TestService_DeprovisionTenant_WithRoutingRules(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, kafkaAdmin := newTestService()

	// Setup: create tenant and set routing rules
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	// Pre-seed Kafka topics that routing rules reference.
	_ = kafkaAdmin.CreateTopic(context.Background(), "test.acme-corp.trade", 1, 1, nil)
	_ = kafkaAdmin.CreateTopic(context.Background(), "test.acme-corp.orderbook", 1, 1, nil)

	_ = svc.ReplaceRoutingRules(context.Background(), "acme-corp", []provisioning.TopicRoutingRule{
		{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
		{Pattern: "**.orderbook", Topics: []string{"orderbook"}, Priority: 2},
	})

	// Deprovision (exercises the retention-update code path for routing rules)
	err := svc.DeprovisionTenant(context.Background(), "acme-corp", false)
	if err != nil {
		t.Fatalf("failed to deprovision tenant with routing rules: %v", err)
	}

	// Verify status
	got, _ := svc.GetTenantBySlug(context.Background(), "acme-corp")
	if got.Status != provisioning.StatusDeprovisioning {
		t.Errorf("expected status %q, got %q", provisioning.StatusDeprovisioning, got.Status)
	}

	// Routing rules should still exist (they're cleaned up later by lifecycle manager)
	rules, err := svc.GetRoutingRules(context.Background(), "acme-corp")
	if err != nil {
		t.Fatalf("expected routing rules to still exist after deprovision: %v", err)
	}
	if len(rules) != 2 {
		t.Errorf("expected 2 routing rules, got %d", len(rules))
	}
}

func TestService_CreateKey(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	// Setup: create a tenant
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	tests := []struct {
		name        string
		tenantID    string
		req         provisioning.CreateKeyRequest
		wantErr     bool
		errContains string
	}{
		{
			name:     "valid ES256 key",
			tenantID: "acme-corp",
			req: provisioning.CreateKeyRequest{
				KeyID:     "key-1",
				Algorithm: provisioning.AlgorithmES256,
				PublicKey: testutil.SampleES256PublicKeyPEM(),
			},
			wantErr: false,
		},
		{
			name:     "invalid key ID",
			tenantID: "acme-corp",
			req: provisioning.CreateKeyRequest{
				KeyID:     "INVALID_KEY",
				Algorithm: provisioning.AlgorithmES256,
				PublicKey: testutil.SampleES256PublicKeyPEM(),
			},
			wantErr:     true,
			errContains: "invalid key",
		},
		{
			name:     "non-existing tenant",
			tenantID: "not-found",
			req: provisioning.CreateKeyRequest{
				KeyID:     "key-2",
				Algorithm: provisioning.AlgorithmES256,
				PublicKey: testutil.SampleES256PublicKeyPEM(),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			key, err := svc.CreateKey(context.Background(), tt.tenantID, tt.req)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if key == nil {
					t.Error("expected key, got nil")
				}
				if key != nil && key.KeyID != tt.req.KeyID {
					t.Errorf("key ID mismatch: got %q, want %q", key.KeyID, tt.req.KeyID)
				}
			}
		})
	}
}

func TestService_RevokeKey(t *testing.T) {
	t.Parallel()
	svc, tenantStore, keyStore, _ := newTestService()

	// Setup: create tenant and key
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)
	key := testutil.NewTestTenantKey("key-1", tenant.ID)
	_ = keyStore.Create(context.Background(), key)

	// Revoke
	err := svc.RevokeKey(context.Background(), "acme-corp", "key-1")
	if err != nil {
		t.Fatalf("failed to revoke key: %v", err)
	}

	// Verify revoked
	activeKeys, _ := keyStore.GetActiveKeys(context.Background())
	for _, k := range activeKeys {
		if k.KeyID == "key-1" {
			t.Error("key should not be in active keys")
		}
	}
}

func TestService_RevokeKey_WrongTenant(t *testing.T) {
	t.Parallel()
	svc, tenantStore, keyStore, _ := newTestService()

	// Setup: create two tenants with keys
	tenant1 := testutil.NewTestTenant("acme-corp")
	tenant2 := testutil.NewTestTenant("beta-test")
	_ = tenantStore.Create(context.Background(), tenant1)
	_ = tenantStore.Create(context.Background(), tenant2)
	key := testutil.NewTestTenantKey("key-1", tenant1.ID)
	_ = keyStore.Create(context.Background(), key)

	// Try to revoke key with wrong tenant
	err := svc.RevokeKey(context.Background(), "beta-test", "key-1")
	if err == nil {
		t.Error("expected error when revoking key for wrong tenant")
	}
	if !strings.Contains(err.Error(), "does not belong") {
		t.Errorf("expected 'does not belong' error, got: %v", err)
	}
}

func TestService_SetRoutingRules(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, kafkaAdmin := newTestService()

	// Setup: create a tenant
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	// Pre-seed Kafka topics that routing rules reference.
	_ = kafkaAdmin.CreateTopic(context.Background(), "test.acme-corp.trade", 1, 1, nil)
	_ = kafkaAdmin.CreateTopic(context.Background(), "test.acme-corp.orderbook", 1, 1, nil)

	// Set routing rules
	rules := []provisioning.TopicRoutingRule{
		{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
		{Pattern: "**.orderbook", Topics: []string{"orderbook"}, Priority: 2},
	}
	err := svc.ReplaceRoutingRules(context.Background(), "acme-corp", rules)
	if err != nil {
		t.Fatalf("failed to set routing rules: %v", err)
	}

	// Verify rules stored
	got, err := svc.GetRoutingRules(context.Background(), "acme-corp")
	if err != nil {
		t.Fatalf("failed to get routing rules: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 rules, got %d", len(got))
	}
}

func TestService_SetRoutingRules_SuspendedTenant(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	// Setup: create and suspend a tenant
	tenant := testutil.NewTestTenant("acme-corp")
	tenant.Status = provisioning.StatusSuspended
	_ = tenantStore.Create(context.Background(), tenant)

	// Try to set routing rules
	rules := []provisioning.TopicRoutingRule{
		{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
	}
	err := svc.ReplaceRoutingRules(context.Background(), "acme-corp", rules)
	if err == nil {
		t.Error("expected error for suspended tenant")
	}
	if !strings.Contains(err.Error(), "not active") {
		t.Errorf("expected 'not active' error, got: %v", err)
	}
}

func TestService_DeleteRoutingRules(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	// Setup: create tenant and set rules
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)
	_ = svc.ReplaceRoutingRules(context.Background(), "acme-corp", []provisioning.TopicRoutingRule{
		{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
	})

	// Delete rules
	err := svc.DeleteRoutingRules(context.Background(), "acme-corp")
	if err != nil {
		t.Fatalf("failed to delete routing rules: %v", err)
	}

	// Verify rules gone — empty slice, not an error
	rules, err := svc.GetRoutingRules(context.Background(), "acme-corp")
	if err != nil {
		t.Errorf("unexpected error after deleting routing rules: %v", err)
	}
	if len(rules) != 0 {
		t.Errorf("expected 0 rules after delete, got %d", len(rules))
	}
}

func TestService_Ready(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	// Test healthy
	err := svc.Ready(context.Background())
	if err != nil {
		t.Errorf("expected Ready to succeed, got: %v", err)
	}

	// Test unhealthy
	tenantStore.PingErr = errors.New("connection refused")
	err = svc.Ready(context.Background())
	if err == nil {
		t.Error("expected Ready to fail with PingErr")
	}
}

func TestService_GetActiveKeys(t *testing.T) {
	t.Parallel()
	svc, tenantStore, keyStore, _ := newTestService()

	// Setup: create tenants and keys
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	key1 := testutil.NewTestTenantKey("key-1", tenant.ID)
	key2 := testutil.NewTestTenantKey("key-2", tenant.ID)
	key3 := testutil.NewTestTenantKey("key-3", tenant.ID)
	_ = keyStore.Create(context.Background(), key1)
	_ = keyStore.Create(context.Background(), key2)
	_ = keyStore.Create(context.Background(), key3)

	// Revoke one key
	_ = keyStore.Revoke(context.Background(), "key-2")

	// Get active keys
	activeKeys, err := svc.GetActiveKeys(context.Background())
	if err != nil {
		t.Fatalf("failed to get active keys: %v", err)
	}

	if len(activeKeys) != 2 {
		t.Errorf("expected 2 active keys, got %d", len(activeKeys))
	}
}

func TestService_UpdateQuota(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, kafkaAdmin := newTestService()

	// Setup: create tenant and quota
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)
	_, _ = svc.CreateTenant(context.Background(), provisioning.CreateTenantRequest{
		Slug: "quota-test",
		Name: "Quota Test",
	})

	// Update quota
	newMaxTopics := 100
	newProducerRate := int64(20 * 1024 * 1024)
	req := provisioning.UpdateQuotaRequest{
		MaxTopics:        &newMaxTopics,
		ProducerByteRate: &newProducerRate,
	}

	quota, err := svc.UpdateQuota(context.Background(), "quota-test", req)
	if err != nil {
		t.Fatalf("failed to update quota: %v", err)
	}

	if quota.MaxTopics != newMaxTopics {
		t.Errorf("MaxTopics mismatch: got %d, want %d", quota.MaxTopics, newMaxTopics)
	}
	if quota.ProducerByteRate != newProducerRate {
		t.Errorf("ProducerByteRate mismatch: got %d, want %d", quota.ProducerByteRate, newProducerRate)
	}

	// Verify Kafka quota was set (no-op check for mock)
	_ = kafkaAdmin.GetACLs()
}

func TestService_ListTenants(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	// Setup: create multiple tenants
	for i, id := range []string{"acme-corp", "beta-test", "gamma-labs"} {
		tenant := testutil.NewTestTenant(id)
		if i == 2 {
			tenant.Status = provisioning.StatusSuspended
		}
		_ = tenantStore.Create(context.Background(), tenant)
	}

	tests := []struct {
		name      string
		opts      provisioning.ListOptions
		wantCount int
		wantTotal int
	}{
		{
			name:      "all tenants",
			opts:      provisioning.ListOptions{Limit: 10, Offset: 0},
			wantCount: 3,
			wantTotal: 3,
		},
		{
			name:      "with limit",
			opts:      provisioning.ListOptions{Limit: 2, Offset: 0},
			wantCount: 2,
			wantTotal: 3,
		},
		{
			name:      "with offset",
			opts:      provisioning.ListOptions{Limit: 10, Offset: 2},
			wantCount: 1,
			wantTotal: 3,
		},
		{
			name: "filter by status",
			opts: func() provisioning.ListOptions {
				status := provisioning.StatusSuspended
				return provisioning.ListOptions{Limit: 10, Offset: 0, Status: &status}
			}(),
			wantCount: 1,
			wantTotal: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tenants, total, err := svc.ListTenants(context.Background(), tt.opts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(tenants) != tt.wantCount {
				t.Errorf("tenant count mismatch: got %d, want %d", len(tenants), tt.wantCount)
			}
			if total != tt.wantTotal {
				t.Errorf("total mismatch: got %d, want %d", total, tt.wantTotal)
			}
		})
	}
}

func TestService_GetAuditLog(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	// Create a tenant (generates audit entries)
	_, _ = svc.CreateTenant(context.Background(), provisioning.CreateTenantRequest{
		Slug: "acme-corp",
		Name: "Acme Corporation",
	})

	// Suspend the tenant (generates another audit entry)
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)
	_ = svc.SuspendTenant(context.Background(), "acme-corp")

	// Get audit log
	entries, total, err := svc.GetAuditLog(context.Background(), "acme-corp", provisioning.ListOptions{
		Limit:  10,
		Offset: 0,
	})
	if err != nil {
		t.Fatalf("failed to get audit log: %v", err)
	}

	if total < 1 {
		t.Error("expected at least one audit entry")
	}
	if len(entries) < 1 {
		t.Error("expected at least one audit entry in results")
	}
}

func TestService_UpdateTenant(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	// Setup: create a tenant
	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	// Update
	newName := "Acme Corp Updated"
	newConsumerType := provisioning.ConsumerDedicated
	updated, err := svc.UpdateTenant(context.Background(), "acme-corp", provisioning.UpdateTenantRequest{
		Name:         &newName,
		ConsumerType: &newConsumerType,
	})
	if err != nil {
		t.Fatalf("failed to update tenant: %v", err)
	}

	if updated.Name != newName {
		t.Errorf("name mismatch: got %q, want %q", updated.Name, newName)
	}
	if updated.ConsumerType != newConsumerType {
		t.Errorf("consumer type mismatch: got %q, want %q", updated.ConsumerType, newConsumerType)
	}
}

func TestService_CreateTenantWithKey(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTestService()

	// Create tenant with initial key
	expiresAt := time.Now().Add(365 * 24 * time.Hour)
	req := provisioning.CreateTenantRequest{
		Slug: "acme-corp",
		Name: "Acme Corporation",
		PublicKey: &provisioning.CreateKeyRequest{
			KeyID:     "initial-key",
			Algorithm: provisioning.AlgorithmES256,
			PublicKey: testutil.SampleES256PublicKeyPEM(),
			ExpiresAt: &expiresAt,
		},
	}

	resp, err := svc.CreateTenant(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	if resp.Key == nil {
		t.Error("expected key in response")
	}
	if resp.Key != nil && resp.Key.KeyID != "initial-key" {
		t.Errorf("key ID mismatch: got %q, want %q", resp.Key.KeyID, "initial-key")
	}
}

// =============================================================================
// Routing Rules Edge Case Tests (W5)
// =============================================================================

func TestService_SetRoutingRules_ExceedsMaxLimit(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	// MaxRoutingRulesPerTenant is set to 5 in newTestService — create 6 rules to exceed it
	rules := make([]provisioning.TopicRoutingRule, 6)
	for i := range rules {
		rules[i] = provisioning.TopicRoutingRule{
			Pattern:  fmt.Sprintf("**.suffix%d", i),
			Topics:   []string{fmt.Sprintf("topic%d", i)},
			Priority: i + 1,
		}
	}

	err := svc.ReplaceRoutingRules(context.Background(), "acme-corp", rules)
	if err == nil {
		t.Fatal("expected error when exceeding MaxRoutingRules, got nil")
	}
	if !errors.Is(err, provisioning.ErrTooManyRoutingRules) {
		t.Errorf("expected ErrTooManyRoutingRules, got: %v", err)
	}
}

func TestService_SetRoutingRules_InvalidRulesPropagated(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	// Empty pattern should fail validation
	rules := []provisioning.TopicRoutingRule{
		{Pattern: "", Topics: []string{"trade"}, Priority: 1},
	}

	err := svc.ReplaceRoutingRules(context.Background(), "acme-corp", rules)
	if err == nil {
		t.Fatal("expected error for invalid rules, got nil")
	}
	if !strings.Contains(err.Error(), "invalid routing rules") {
		t.Errorf("expected 'invalid routing rules' in error, got: %v", err)
	}
}

func TestService_SetRoutingRules_NilStore(t *testing.T) {
	t.Parallel()

	// Service without routing rules store
	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:              testutil.NewMockTenantStore(),
		KeyStore:                 testutil.NewMockKeyStore(),
		APIKeyStore:              testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:        nil, // not configured
		QuotaStore:               testutil.NewMockQuotaStore(),
		AuditStore:               testutil.NewMockAuditStore(),
		KafkaAdmin:               testutil.NewMockKafkaAdmin(),
		EventBus:                 eventbus.New(zerolog.Nop()),
		TopicNamespace:           "test",
		DefaultPartitions:        3,
		DefaultRetentionMs:       604800000,
		MaxTopicsPerTenant:       50,
		MaxRoutingRulesPerTenant: 100,
		DeprovisionGraceDays:     30,
		Logger:                   zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	rules := []provisioning.TopicRoutingRule{
		{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
	}

	setErr := svc.ReplaceRoutingRules(context.Background(), "acme-corp", rules)
	if setErr == nil {
		t.Fatal("expected error for nil routing rules store, got nil")
	}
	if !errors.Is(setErr, provisioning.ErrRoutingRulesNotConfigured) {
		t.Errorf("expected ErrRoutingRulesNotConfigured, got: %v", setErr)
	}

	// Get should also fail gracefully
	_, getErr := svc.GetRoutingRules(context.Background(), "acme-corp")
	if getErr == nil {
		t.Fatal("expected error for nil routing rules store on Get, got nil")
	}

	// Delete should also fail gracefully
	delErr := svc.DeleteRoutingRules(context.Background(), "acme-corp")
	if delErr == nil {
		t.Fatal("expected error for nil routing rules store on Delete, got nil")
	}
}

func TestService_DeleteRoutingRules_Nonexistent(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	// Delete routing rules that don't exist — idempotent, no error
	err := svc.DeleteRoutingRules(context.Background(), "acme-corp")
	if err != nil {
		t.Errorf("expected nil error when deleting nonexistent routing rules, got: %v", err)
	}
}

func TestService_SetRoutingRules_NonexistentTenant(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTestService()

	rules := []provisioning.TopicRoutingRule{
		{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
	}

	err := svc.ReplaceRoutingRules(context.Background(), "nonexistent", rules)
	if err == nil {
		t.Fatal("expected error for nonexistent tenant, got nil")
	}
}

func TestService_AddRoutingRule_TopicExistsError(t *testing.T) {
	t.Parallel()

	kafka := testutil.NewMockKafkaAdmin()
	kafka.TopicExistsErr = errors.New("kafka: topic check failed")

	ts := testutil.NewMockTenantStore()
	_ = ts.Create(context.Background(), testutil.NewTestTenant("acme-corp"))
	svc := newTestServiceWithKafka(kafka, ts)

	err := svc.AddRoutingRule(context.Background(), "acme-corp", provisioning.TopicRoutingRule{
		Pattern:  "**.trade",
		Topics:   []string{"trade"},
		Priority: 1,
	})
	if err == nil {
		t.Fatal("expected error when TopicExists fails, got nil")
	}
}

func TestService_CreateTenant_DotsInID(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTestService()

	// "acme.corp" is rejected by tenant.Validate() before the dot check
	// because dots are not valid in tenant IDs (lowercase alphanumeric + hyphens only).
	_, err := svc.CreateTenant(context.Background(), provisioning.CreateTenantRequest{
		Slug: "acme.corp",
		Name: "Acme Corp",
	})
	if err == nil {
		t.Fatal("expected error for dots in tenant ID, got nil")
	}
	if !strings.Contains(err.Error(), "invalid tenant") {
		t.Errorf("expected 'invalid tenant' error, got: %v", err)
	}
}

func TestService_CreateTenant_UnderscorePrefix(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTestService()

	// "_system" is rejected by tenant.Validate() before the underscore-prefix check
	// because underscores/leading underscores are not valid in tenant IDs.
	_, err := svc.CreateTenant(context.Background(), provisioning.CreateTenantRequest{
		Slug: "_system",
		Name: "System Tenant",
	})
	if err == nil {
		t.Fatal("expected error for underscore prefix, got nil")
	}
	if !strings.Contains(err.Error(), "invalid tenant") {
		t.Errorf("expected 'invalid tenant' error, got: %v", err)
	}
}

// =============================================================================
// API Key Tests
// =============================================================================

func newTestServiceWithAPIKeys() (*provisioning.Service, *testutil.MockTenantStore, *testutil.MockKeyStore, *testutil.MockKafkaAdmin, *testutil.MockAPIKeyStore) {
	tenantStore := testutil.NewMockTenantStore()
	keyStore := testutil.NewMockKeyStore()
	apiKeyStore := testutil.NewMockAPIKeyStore()
	routingRulesStore := testutil.NewMockRoutingRulesStore()
	quotaStore := testutil.NewMockQuotaStore()
	auditStore := testutil.NewMockAuditStore()
	kafkaAdmin := testutil.NewMockKafkaAdmin()

	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 tenantStore,
		KeyStore:                    keyStore,
		APIKeyStore:                 apiKeyStore,
		RoutingRulesStore:           routingRulesStore,
		QuotaStore:                  quotaStore,
		AuditStore:                  auditStore,
		KafkaAdmin:                  kafkaAdmin,
		EventBus:                    eventbus.New(zerolog.Nop()),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          50,
		MaxRoutingRulesPerTenant:    5,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  86400000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
	})
	if err != nil {
		panic("newTestServiceWithAPIKeys: " + err.Error())
	}

	return svc, tenantStore, keyStore, kafkaAdmin, apiKeyStore
}

func TestService_CreateAPIKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		tenantID    string
		req         provisioning.CreateAPIKeyRequest
		setupMock   func(*testutil.MockTenantStore)
		wantErr     bool
		errContains string
	}{
		{
			name:     "happy path",
			tenantID: "acme-corp",
			req:      provisioning.CreateAPIKeyRequest{Name: "Production Key"},
			setupMock: func(ts *testutil.MockTenantStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("acme-corp"))
			},
			wantErr: false,
		},
		{
			name:     "tenant not active",
			tenantID: "suspended-corp",
			req:      provisioning.CreateAPIKeyRequest{Name: "Suspended Key"},
			setupMock: func(ts *testutil.MockTenantStore) {
				tenant := testutil.NewTestTenant("suspended-corp")
				_ = ts.Create(context.Background(), tenant)
				tenant.Status = provisioning.StatusSuspended
				_ = ts.Update(context.Background(), tenant)
			},
			wantErr:     true,
			errContains: "not active",
		},
		{
			name:        "tenant not found",
			tenantID:    "nonexistent",
			req:         provisioning.CreateAPIKeyRequest{Name: "Ghost Key"},
			wantErr:     true,
			errContains: "get tenant",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc, tenantStore, _, _, _ := newTestServiceWithAPIKeys()

			if tt.setupMock != nil {
				tt.setupMock(tenantStore)
			}

			key, err := svc.CreateAPIKey(context.Background(), tt.tenantID, tt.req)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if key == nil {
					t.Fatal("expected key, got nil")
				}
				// key.TenantID is the tenant UUID (not slug); assert UUID format only
				if len(key.TenantID) != 36 || strings.Count(key.TenantID, "-") != 4 {
					t.Errorf("key.TenantID = %q, want UUID format (36 chars, 4 dashes)", key.TenantID)
				}
				if key.Name != tt.req.Name {
					t.Errorf("name mismatch: got %q, want %q", key.Name, tt.req.Name)
				}
				if key.KeyID == "" {
					t.Error("expected non-empty key ID")
				}
			}
		})
	}
}

func TestService_ListAPIKeys(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _, _ := newTestServiceWithAPIKeys()

	// Setup: create tenant
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("acme-corp"))

	// Create 2 API keys
	_, err := svc.CreateAPIKey(context.Background(), "acme-corp", provisioning.CreateAPIKeyRequest{Name: "Key One"})
	if err != nil {
		t.Fatalf("failed to create first API key: %v", err)
	}
	_, err = svc.CreateAPIKey(context.Background(), "acme-corp", provisioning.CreateAPIKeyRequest{Name: "Key Two"})
	if err != nil {
		t.Fatalf("failed to create second API key: %v", err)
	}

	// List with default pagination
	keys, total, err := svc.ListAPIKeys(context.Background(), "acme-corp", provisioning.ListOptions{Limit: 50, Offset: 0})
	if err != nil {
		t.Fatalf("failed to list API keys: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("expected 2 API keys, got %d", len(keys))
	}
	if total != 2 {
		t.Errorf("expected total 2, got %d", total)
	}
}

func TestService_RevokeAPIKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		tenantID    string
		keyID       string
		setupMock   func(*testutil.MockTenantStore, *testutil.MockAPIKeyStore) string
		wantErr     bool
		errContains string
	}{
		{
			name:     "happy path",
			tenantID: "acme-corp",
			setupMock: func(ts *testutil.MockTenantStore, aks *testutil.MockAPIKeyStore) string {
				tenant := testutil.NewTestTenant("acme-corp")
				_ = ts.Create(context.Background(), tenant)
				key := &provisioning.APIKey{
					KeyID:    "pk_live_test123",
					TenantID: tenant.ID,
					Name:     "Test Key",
					IsActive: true,
				}
				_ = aks.Create(context.Background(), key)
				return key.KeyID
			},
			wantErr: false,
		},
		{
			name:     "key not found",
			tenantID: "acme-corp",
			keyID:    "pk_live_nonexistent",
			setupMock: func(ts *testutil.MockTenantStore, _ *testutil.MockAPIKeyStore) string {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("acme-corp"))
				return ""
			},
			wantErr:     true,
			errContains: "api key not found",
		},
		{
			name:     "tenant mismatch",
			tenantID: "other-corp",
			setupMock: func(ts *testutil.MockTenantStore, aks *testutil.MockAPIKeyStore) string {
				acmeTenant := testutil.NewTestTenant("acme-corp")
				_ = ts.Create(context.Background(), acmeTenant)
				_ = ts.Create(context.Background(), testutil.NewTestTenant("other-corp"))
				key := &provisioning.APIKey{
					KeyID:    "pk_live_owned_by_acme",
					TenantID: acmeTenant.ID,
					Name:     "Acme Key",
					IsActive: true,
				}
				_ = aks.Create(context.Background(), key)
				return key.KeyID
			},
			wantErr:     true,
			errContains: "does not belong",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc, tenantStore, _, _, apiKeyStore := newTestServiceWithAPIKeys()

			keyID := tt.keyID
			if tt.setupMock != nil {
				if id := tt.setupMock(tenantStore, apiKeyStore); id != "" {
					keyID = id
				}
			}

			err := svc.RevokeAPIKey(context.Background(), tt.tenantID, keyID)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestService_GetActiveAPIKeys(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _, _ := newTestServiceWithAPIKeys()

	// Setup: create tenant
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("acme-corp"))

	// Create 3 API keys
	for i, name := range []string{"Key A", "Key B", "Key C"} {
		key, err := svc.CreateAPIKey(context.Background(), "acme-corp", provisioning.CreateAPIKeyRequest{Name: name})
		if err != nil {
			t.Fatalf("failed to create API key %d: %v", i, err)
		}
		// Revoke the second key to verify it's excluded from active list
		if i == 1 {
			if err := svc.RevokeAPIKey(context.Background(), "acme-corp", key.KeyID); err != nil {
				t.Fatalf("failed to revoke API key: %v", err)
			}
		}
	}

	// Get active keys
	activeKeys, err := svc.GetActiveAPIKeys(context.Background())
	if err != nil {
		t.Fatalf("failed to get active API keys: %v", err)
	}

	if len(activeKeys) != 2 {
		t.Errorf("expected 2 active API keys, got %d", len(activeKeys))
	}
}

// --- Force-delete tests ---

func TestService_DeprovisionTenant_Force_Active(t *testing.T) {
	t.Parallel()
	svc, tenantStore, keyStore, _ := newTestService()

	// Setup: create tenant with key
	tenant := testutil.NewTestTenant("force-test")
	_ = tenantStore.Create(context.Background(), tenant)
	_ = keyStore.Create(context.Background(), &provisioning.TenantKey{
		KeyID:    "k1",
		TenantID: tenant.ID,
	})

	// Force-delete
	err := svc.DeprovisionTenant(context.Background(), "force-test", true)
	if err != nil {
		t.Fatalf("force-delete failed: %v", err)
	}

	// Verify status is deleted (not deprovisioning)
	got, err := tenantStore.GetBySlug(context.Background(), "force-test")
	if err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	if got.Status != provisioning.StatusDeleted {
		t.Errorf("status = %q, want %q", got.Status, provisioning.StatusDeleted)
	}
}

func TestService_DeprovisionTenant_Force_Deprovisioning(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	// Setup: tenant already in deprovisioning state
	tenant := testutil.NewTestTenant("deprovisioning-test")
	tenant.Status = provisioning.StatusDeprovisioning
	_ = tenantStore.Create(context.Background(), tenant)

	// Force-delete should succeed even on deprovisioning tenant
	err := svc.DeprovisionTenant(context.Background(), "deprovisioning-test", true)
	if err != nil {
		t.Fatalf("force-delete of deprovisioning tenant failed: %v", err)
	}

	got, err := tenantStore.GetBySlug(context.Background(), "deprovisioning-test")
	if err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	if got.Status != provisioning.StatusDeleted {
		t.Errorf("status = %q, want %q", got.Status, provisioning.StatusDeleted)
	}
}

func TestService_DeprovisionTenant_Force_RevokesAPIKeys(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _, apiKeyStore := newTestServiceWithAPIKeys()

	// Setup: tenant with active API key
	tenant := testutil.NewTestTenant("apikey-test")
	_ = tenantStore.Create(context.Background(), tenant)
	_ = apiKeyStore.Create(context.Background(), &provisioning.APIKey{
		KeyID:    "ak1",
		TenantID: tenant.ID,
		Name:     "test-key",
		IsActive: true,
	})

	// Force-delete
	err := svc.DeprovisionTenant(context.Background(), "apikey-test", true)
	if err != nil {
		t.Fatalf("force-delete failed: %v", err)
	}

	// Verify API key was revoked
	key, err := apiKeyStore.Get(context.Background(), "ak1")
	if err != nil {
		t.Fatalf("get API key: %v", err)
	}
	if key.IsActive {
		t.Error("API key should be revoked after force-delete")
	}
}

func TestService_DeprovisionTenant_Force_DeletedTenantRejected(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _ := newTestService()

	// Setup: already deleted tenant
	tenant := testutil.NewTestTenant("deleted-test")
	tenant.Status = provisioning.StatusDeleted
	_ = tenantStore.Create(context.Background(), tenant)

	// Force-delete of already-deleted should return error
	err := svc.DeprovisionTenant(context.Background(), "deleted-test", true)
	if !errors.Is(err, provisioning.ErrTenantDeleted) {
		t.Errorf("expected ErrTenantDeleted, got: %v", err)
	}
}

// testWebhookEncryptionKey is a valid 32-byte AES-256 key for use in service webhook tests.
var testWebhookEncryptionKey = func() []byte {
	key, _ := hex.DecodeString("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	return key
}()

func newTestServiceWithWebhooks(store *testutil.MockWebhookStore) *provisioning.Service {
	webhookStore := provisioning.WebhookStore(store)
	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 testutil.NewMockTenantStore(),
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           testutil.NewMockRoutingRulesStore(),
		QuotaStore:                  testutil.NewMockQuotaStore(),
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  testutil.NewMockKafkaAdmin(),
		EventBus:                    eventbus.New(zerolog.Nop()),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          50,
		MaxRoutingRulesPerTenant:    5,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  86400000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
		WebhookStore:                webhookStore,
		EncryptionKey:               testWebhookEncryptionKey,
		MaxWebhooksPerTenant:        10,
		WebhookAllowHTTP:            false,
	})
	if err != nil {
		panic("newTestServiceWithWebhooks: " + err.Error())
	}
	return svc
}

// --- Webhook service tests ---

func TestService_CreateWebhook_HappyPath(t *testing.T) {
	t.Parallel()
	store := testutil.NewMockWebhookStore()
	svc := newTestServiceWithWebhooks(store)
	ctx := context.Background()

	w, err := svc.CreateWebhook(ctx, provisioning.CreateWebhookRequest{
		TenantID:       "tenant-1",
		URL:            "https://example.com/hook",
		ChannelPattern: "orders.*",
		Secret:         "my-secret",
		MaxRetries:     3,
	})
	if err != nil {
		t.Fatalf("CreateWebhook() error = %v", err)
	}
	if w.ID == "" {
		t.Error("CreateWebhook() returned empty ID")
	}
	if w.SecretEnc == "" {
		t.Error("CreateWebhook() returned empty SecretEnc (secret not encrypted)")
	}
	if w.SecretEnc == "my-secret" {
		t.Error("CreateWebhook() returned plaintext secret")
	}
	if w.Status != types.WebhookStatusEnabled {
		t.Errorf("Status = %q, want %q", w.Status, types.WebhookStatusEnabled)
	}
}

func TestService_CreateWebhook_NilStore(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTestService() // no WebhookStore configured
	_, err := svc.CreateWebhook(context.Background(), provisioning.CreateWebhookRequest{
		TenantID: "t1", URL: "https://example.com", ChannelPattern: "a.*", Secret: "s",
	})
	if !errors.Is(err, provisioning.ErrWebhookStoreNotConfigured) {
		t.Errorf("CreateWebhook() with nil store error = %v, want ErrWebhookStoreNotConfigured", err)
	}
}

func TestService_CreateWebhook_InvalidURL(t *testing.T) {
	t.Parallel()
	store := testutil.NewMockWebhookStore()
	svc := newTestServiceWithWebhooks(store)
	ctx := context.Background()

	tests := []struct {
		name string
		url  string
	}{
		{"empty URL", ""},
		{"http URL when not allowed", "http://example.com/hook"},
		{"no host", "/relative/path"},
		{"private loopback", "https://127.0.0.1/hook"},
		{"private RFC-1918", "https://10.0.0.1/hook"},
		{"link-local", "https://169.254.169.254/metadata"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := svc.CreateWebhook(ctx, provisioning.CreateWebhookRequest{
				TenantID: "t1", URL: tt.url, ChannelPattern: "a.*", Secret: "s",
			})
			if !errors.Is(err, provisioning.ErrWebhookInvalidInput) {
				t.Errorf("CreateWebhook(%q) error = %v, want ErrWebhookInvalidInput", tt.url, err)
			}
		})
	}
}

func TestService_CreateWebhook_MaxRetriesOutOfRange(t *testing.T) {
	t.Parallel()
	store := testutil.NewMockWebhookStore()
	svc := newTestServiceWithWebhooks(store)
	ctx := context.Background()

	for _, retries := range []int{-1, 11} {
		_, err := svc.CreateWebhook(ctx, provisioning.CreateWebhookRequest{
			TenantID: "t1", URL: "https://example.com", ChannelPattern: "a.*", Secret: "s", MaxRetries: retries,
		})
		if !errors.Is(err, provisioning.ErrWebhookInvalidInput) {
			t.Errorf("CreateWebhook(MaxRetries=%d) error = %v, want ErrWebhookInvalidInput", retries, err)
		}
	}
}

func TestService_CreateWebhook_QuotaExceeded(t *testing.T) {
	t.Parallel()
	store := testutil.NewMockWebhookStore()
	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 testutil.NewMockTenantStore(),
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           testutil.NewMockRoutingRulesStore(),
		QuotaStore:                  testutil.NewMockQuotaStore(),
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  testutil.NewMockKafkaAdmin(),
		EventBus:                    eventbus.New(zerolog.Nop()),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          50,
		MaxRoutingRulesPerTenant:    5,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  86400000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
		WebhookStore:                store,
		EncryptionKey:               testWebhookEncryptionKey,
		MaxWebhooksPerTenant:        1, // cap at 1
		WebhookAllowHTTP:            false,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	ctx := context.Background()

	// First webhook — succeeds.
	if _, err := svc.CreateWebhook(ctx, provisioning.CreateWebhookRequest{
		TenantID: "t1", URL: "https://example.com", ChannelPattern: "a.*", Secret: "s",
	}); err != nil {
		t.Fatalf("first CreateWebhook() error = %v", err)
	}

	// Second webhook — must be rejected.
	_, err = svc.CreateWebhook(ctx, provisioning.CreateWebhookRequest{
		TenantID: "t1", URL: "https://example.com/2", ChannelPattern: "b.*", Secret: "s2",
	})
	if !errors.Is(err, provisioning.ErrWebhookQuotaExceeded) {
		t.Errorf("CreateWebhook() over quota error = %v, want ErrWebhookQuotaExceeded", err)
	}
}

func TestService_GetWebhook_HappyPath(t *testing.T) {
	t.Parallel()
	store := testutil.NewMockWebhookStore()
	svc := newTestServiceWithWebhooks(store)
	ctx := context.Background()

	// Pre-seed the store directly.
	_ = store.Create(ctx, &provisioning.Webhook{
		ID: "wh_gettest1", TenantID: "t1", URL: "https://example.com",
		ChannelPattern: "a.*", SecretEnc: "enc", Status: types.WebhookStatusEnabled, MaxRetries: 5,
	})

	got, err := svc.GetWebhook(ctx, "wh_gettest1", "t1")
	if err != nil {
		t.Fatalf("GetWebhook() error = %v", err)
	}
	if got.ID != "wh_gettest1" {
		t.Errorf("ID = %q, want %q", got.ID, "wh_gettest1")
	}
}

func TestService_GetWebhook_NilStore(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTestService()
	_, err := svc.GetWebhook(context.Background(), "wh_x", "t1")
	if !errors.Is(err, provisioning.ErrWebhookStoreNotConfigured) {
		t.Errorf("GetWebhook() nil store error = %v, want ErrWebhookStoreNotConfigured", err)
	}
}

func TestService_ListWebhooks_NilStore(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTestService()
	_, _, err := svc.ListWebhooks(context.Background(), "t1", provisioning.ListOptions{Limit: 10})
	if !errors.Is(err, provisioning.ErrWebhookStoreNotConfigured) {
		t.Errorf("ListWebhooks() nil store error = %v, want ErrWebhookStoreNotConfigured", err)
	}
}

func TestService_UpdateWebhook_EmptyPatchRejected(t *testing.T) {
	t.Parallel()
	store := testutil.NewMockWebhookStore()
	svc := newTestServiceWithWebhooks(store)
	ctx := context.Background()

	_, err := svc.UpdateWebhook(ctx, provisioning.UpdateWebhookRequest{
		ID: "wh_x", TenantID: "t1",
		// all optional fields nil
	})
	if !errors.Is(err, provisioning.ErrWebhookInvalidInput) {
		t.Errorf("UpdateWebhook() empty PATCH error = %v, want ErrWebhookInvalidInput", err)
	}
}

func TestService_UpdateWebhook_DegradedStatusRejected(t *testing.T) {
	t.Parallel()
	store := testutil.NewMockWebhookStore()
	svc := newTestServiceWithWebhooks(store)
	ctx := context.Background()

	degraded := types.WebhookStatusDegraded
	_, err := svc.UpdateWebhook(ctx, provisioning.UpdateWebhookRequest{
		ID: "wh_x", TenantID: "t1",
		Status: &degraded,
	})
	if !errors.Is(err, provisioning.ErrWebhookInvalidInput) {
		t.Errorf("UpdateWebhook(status=degraded) error = %v, want ErrWebhookInvalidInput", err)
	}
}

func TestService_UpdateWebhook_SSRFURLRejected(t *testing.T) {
	t.Parallel()
	store := testutil.NewMockWebhookStore()
	svc := newTestServiceWithWebhooks(store)
	ctx := context.Background()

	ssrfURL := "https://169.254.169.254/latest/meta-data"
	_, err := svc.UpdateWebhook(ctx, provisioning.UpdateWebhookRequest{
		ID: "wh_x", TenantID: "t1",
		URL: &ssrfURL,
	})
	if !errors.Is(err, provisioning.ErrWebhookInvalidInput) {
		t.Errorf("UpdateWebhook(SSRF URL) error = %v, want ErrWebhookInvalidInput", err)
	}
}

func TestService_DeleteWebhook_HappyPath(t *testing.T) {
	t.Parallel()
	store := testutil.NewMockWebhookStore()
	svc := newTestServiceWithWebhooks(store)
	ctx := context.Background()

	_ = store.Create(ctx, &provisioning.Webhook{
		ID: "wh_del001", TenantID: "t1", URL: "https://example.com",
		ChannelPattern: "a.*", SecretEnc: "enc", Status: types.WebhookStatusEnabled, MaxRetries: 5,
	})

	if err := svc.DeleteWebhook(ctx, "wh_del001", "t1"); err != nil {
		t.Fatalf("DeleteWebhook() error = %v", err)
	}

	_, err := svc.GetWebhook(ctx, "wh_del001", "t1")
	if !errors.Is(err, provisioning.ErrWebhookNotFound) {
		t.Errorf("GetWebhook() after delete error = %v, want ErrWebhookNotFound", err)
	}
}

func TestService_DeleteWebhook_NilStore(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTestService()
	err := svc.DeleteWebhook(context.Background(), "wh_x", "t1")
	if !errors.Is(err, provisioning.ErrWebhookStoreNotConfigured) {
		t.Errorf("DeleteWebhook() nil store error = %v, want ErrWebhookStoreNotConfigured", err)
	}
}

// --- provisioning_active_tenants gauge side-effect tests ---
// These tests verify that each service method correctly updates the gauge.
// NOT parallel: they share the process-wide Prometheus default registry.

//nolint:paralleltest // shares Prometheus default registry — concurrent gauge mutations across tests would make the assertion flaky.
func TestGauge_SuspendTenant_Decrements(t *testing.T) {
	svc, ts, _, _ := newTestService()
	active := testutil.NewTestTenant("gauge-suspend")
	_ = ts.Create(context.Background(), active)
	provisioning.SetActiveTenants(1)

	if err := svc.SuspendTenant(context.Background(), "gauge-suspend"); err != nil {
		t.Fatalf("SuspendTenant: %v", err)
	}
	if got := activeTenantsGaugeValue(t); got != 0.0 {
		t.Errorf("after SuspendTenant: gauge = %v, want 0.0", got)
	}
}

//nolint:paralleltest // shares Prometheus default registry.
func TestGauge_ReactivateTenant_Increments(t *testing.T) {
	svc, ts, _, _ := newTestService()
	susp := testutil.NewTestTenant("gauge-reactivate")
	susp.Status = provisioning.StatusSuspended
	_ = ts.Create(context.Background(), susp)
	provisioning.SetActiveTenants(0)

	if err := svc.ReactivateTenant(context.Background(), "gauge-reactivate"); err != nil {
		t.Fatalf("ReactivateTenant: %v", err)
	}
	if got := activeTenantsGaugeValue(t); got != 1.0 {
		t.Errorf("after ReactivateTenant: gauge = %v, want 1.0", got)
	}
}

//nolint:paralleltest // shares Prometheus default registry.
func TestGauge_DeprovisionTenant_Active_Decrements(t *testing.T) {
	svc, ts, _, _ := newTestService()
	active := testutil.NewTestTenant("gauge-deprovision-active")
	_ = ts.Create(context.Background(), active)
	provisioning.SetActiveTenants(1)

	if err := svc.DeprovisionTenant(context.Background(), "gauge-deprovision-active", false); err != nil {
		t.Fatalf("DeprovisionTenant: %v", err)
	}
	if got := activeTenantsGaugeValue(t); got != 0.0 {
		t.Errorf("after DeprovisionTenant(active): gauge = %v, want 0.0", got)
	}
}

//nolint:paralleltest // shares Prometheus default registry.
func TestGauge_CreateTenant_Increments(t *testing.T) {
	svc, _, _, _ := newTestService()
	provisioning.SetActiveTenants(0)

	_, err := svc.CreateTenant(context.Background(), provisioning.CreateTenantRequest{
		Slug: "gauge-create",
		Name: "Gauge Create",
	})
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if got := activeTenantsGaugeValue(t); got != 1.0 {
		t.Errorf("after CreateTenant: gauge = %v, want 1.0", got)
	}
}

//nolint:paralleltest // shares Prometheus default registry.
func TestGauge_DeprovisionTenant_Active_Force_Decrements(t *testing.T) {
	svc, ts, _, _ := newTestService()
	active := testutil.NewTestTenant("gauge-force-deprovision")
	_ = ts.Create(context.Background(), active)
	provisioning.SetActiveTenants(1)

	if err := svc.DeprovisionTenant(context.Background(), "gauge-force-deprovision", true); err != nil {
		t.Fatalf("DeprovisionTenant(force=true, active): %v", err)
	}
	if got := activeTenantsGaugeValue(t); got != 0.0 {
		t.Errorf("after DeprovisionTenant(force=true, active): gauge = %v, want 0.0", got)
	}
}

//nolint:paralleltest // shares Prometheus default registry.
func TestGauge_DeprovisionTenant_Suspended_NoChange(t *testing.T) {
	// A suspended tenant was already Dec'd by SuspendTenant.
	// Force-deprovisioning it must NOT decrement the gauge again.
	svc, ts, _, _ := newTestService()
	susp := testutil.NewTestTenant("gauge-deprovision-susp")
	susp.Status = provisioning.StatusSuspended
	_ = ts.Create(context.Background(), susp)
	provisioning.SetActiveTenants(0)

	if err := svc.DeprovisionTenant(context.Background(), "gauge-deprovision-susp", true); err != nil {
		t.Fatalf("DeprovisionTenant(force=true, suspended): %v", err)
	}
	if got := activeTenantsGaugeValue(t); got != 0.0 {
		t.Errorf("after DeprovisionTenant(force=true, suspended): gauge = %v, want 0.0 (must not double-decrement)", got)
	}
}

//nolint:paralleltest // shares Prometheus default registry.
func TestGauge_DeprovisionTenant_Suspended_GracePeriod_NoChange(t *testing.T) {
	// Grace-period (force=false) deprovision of a suspended tenant:
	// the gauge guard at service.go line 613 must prevent a double-decrement.
	svc, ts, _, _ := newTestService()
	susp := testutil.NewTestTenant("gauge-deprovision-susp-grace")
	susp.Status = provisioning.StatusSuspended
	_ = ts.Create(context.Background(), susp)
	provisioning.SetActiveTenants(0)

	if err := svc.DeprovisionTenant(context.Background(), "gauge-deprovision-susp-grace", false); err != nil {
		t.Fatalf("DeprovisionTenant(force=false, suspended): %v", err)
	}
	if got := activeTenantsGaugeValue(t); got != 0.0 {
		t.Errorf("after DeprovisionTenant(force=false, suspended): gauge = %v, want 0.0 (must not double-decrement)", got)
	}
}
