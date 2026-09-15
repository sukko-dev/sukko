package provisioning_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
	sharedkafka "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// testDefaultRetentionMs and testDLQRetentionMs use intentionally distinct values so that
// assertions on created topic configs can prove each topic uses its own retention setting.
const (
	testDefaultRetentionMs int64 = 604800000 // 7 days
	testDLQRetentionMs     int64 = 86400000  // 1 day — distinct from default to catch wrong wiring
)

// newTestServiceWithKafka returns a service built with the shared test config.
// Pass an optional TenantStore to override the default mock (nil = use a fresh mock).
func newTestServiceWithKafka(kafkaAdmin *testutil.MockKafkaAdmin, ts ...provisioning.TenantStore) *provisioning.Service {
	var tenantStore provisioning.TenantStore
	if len(ts) > 0 && ts[0] != nil {
		tenantStore = ts[0]
	} else {
		tenantStore = testutil.NewMockTenantStore()
	}
	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 tenantStore,
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           testutil.NewMockRoutingRulesStore(),
		QuotaStore:                  testutil.NewMockQuotaStore(),
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  kafkaAdmin,
		EventBus:                    eventbus.New(zerolog.Nop()),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          testDefaultRetentionMs,
		MaxTopicsPerTenant:          50,
		MaxRoutingRulesPerTenant:    5,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  testDLQRetentionMs,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
	})
	if err != nil {
		panic("newTestServiceWithKafka: " + err.Error())
	}
	return svc
}

func dlqTopic(tenantID string) string {
	return sharedkafka.BuildTopicName("test", tenantID, routing.DeadLetterTopicSuffix)
}

func defaultTopic(tenantID string) string {
	return sharedkafka.BuildTopicName("test", tenantID, routing.DefaultTopicSuffix)
}

func TestCreateTenant_SagaRollback(t *testing.T) {
	t.Parallel()

	const tenantID = "acme"
	errCreate := errors.New("kafka: create failed")
	errDB := errors.New("db: insert failed")

	tests := []struct {
		name              string
		setupKafka        func(k *testutil.MockKafkaAdmin)
		setupDB           func(ts *testutil.MockTenantStore)
		wantErr           bool
		wantDeletedTopics []string // expected deletion order
		wantTopics        []string // expected topics present after success
	}{
		{
			name: "(a) DLQ created, default fails → only DLQ deleted",
			setupKafka: func(k *testutil.MockKafkaAdmin) {
				// First CreateTopic (DLQ) succeeds, second (default) fails.
				k.CreateTopicErrs = []error{nil, errCreate}
			},
			wantErr:           true,
			wantDeletedTopics: []string{dlqTopic(tenantID)},
		},
		{
			name: "(b) both created, DB insert fails → both deleted in reverse order",
			setupDB: func(ts *testutil.MockTenantStore) {
				ts.CreateErr = errDB
			},
			wantErr:           true,
			wantDeletedTopics: []string{defaultTopic(tenantID), dlqTopic(tenantID)},
		},
		{
			name: "(c) DLQ creation fails → zero deletions",
			setupKafka: func(k *testutil.MockKafkaAdmin) {
				k.CreateTopicErrs = []error{errCreate}
			},
			wantErr:           true,
			wantDeletedTopics: []string{},
		},
		{
			name:       "(d) happy path → both topics present",
			wantErr:    false,
			wantTopics: []string{dlqTopic(tenantID), defaultTopic(tenantID)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kafka := testutil.NewMockKafkaAdmin()
			if tt.setupKafka != nil {
				tt.setupKafka(kafka)
			}

			var svc *provisioning.Service
			if tt.setupDB != nil {
				ts := testutil.NewMockTenantStore()
				tt.setupDB(ts)
				svc = newTestServiceWithKafka(kafka, ts)
			} else {
				svc = newTestServiceWithKafka(kafka)
			}

			resp, err := svc.CreateTenant(context.Background(), provisioning.CreateTenantRequest{
				Slug: tenantID,
				Name: "Acme Corp",
			})

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				// Happy path: assert returned tenant has UUID primary key and correct slug.
				if resp == nil || resp.Tenant == nil {
					t.Fatal("expected non-nil tenant in response")
				}
				if len(resp.Tenant.ID) != 36 || resp.Tenant.ID == tenantID {
					t.Errorf("tenant.ID = %q; want UUID format (len 36, not the slug)", resp.Tenant.ID)
				}
				if resp.Tenant.Slug != tenantID {
					t.Errorf("tenant.Slug = %q, want %q", resp.Tenant.Slug, tenantID)
				}
			}

			if tt.wantDeletedTopics != nil {
				if len(kafka.DeletedTopics) != len(tt.wantDeletedTopics) {
					t.Errorf("deleted topics: want %v, got %v", tt.wantDeletedTopics, kafka.DeletedTopics)
				} else {
					for i, want := range tt.wantDeletedTopics {
						if kafka.DeletedTopics[i] != want {
							t.Errorf("deleted topic[%d]: want %q, got %q", i, want, kafka.DeletedTopics[i])
						}
					}
				}
			}

			for _, want := range tt.wantTopics {
				found := slices.Contains(kafka.GetTopics(), want)
				if !found {
					t.Errorf("expected topic %q to be present; got %v", want, kafka.GetTopics())
				}
			}

			// Verify correct partition counts and retention for each infrastructure topic.
			// Using distinct test values (DLQ=1 day, default=7 days) ensures the assertion
			// actually catches wrong config wiring — if both had 604800000, a swap would pass.
			if len(tt.wantTopics) > 0 {
				dlq := dlqTopic(tenantID)
				def := defaultTopic(tenantID)
				if p := kafka.CreatedTopicParts[dlq]; p != 1 {
					t.Errorf("DLQ topic partitions: want 1, got %d", p)
				}
				if p := kafka.CreatedTopicParts[def]; p != 3 {
					t.Errorf("default topic partitions: want 3, got %d", p)
				}
				wantDLQRetention := strconv.FormatInt(testDLQRetentionMs, 10)
				wantDefaultRetention := strconv.FormatInt(testDefaultRetentionMs, 10)
				if v := kafka.CreatedTopicConfigs[dlq][sharedkafka.RetentionMsConfigKey]; v != wantDLQRetention {
					t.Errorf("DLQ topic retention.ms: want %q, got %q", wantDLQRetention, v)
				}
				if v := kafka.CreatedTopicConfigs[def][sharedkafka.RetentionMsConfigKey]; v != wantDefaultRetention {
					t.Errorf("default topic retention.ms: want %q, got %q", wantDefaultRetention, v)
				}
			}
		})
	}
}

func TestCreateTenant_SlugValidation(t *testing.T) {
	t.Parallel()

	kafka := testutil.NewMockKafkaAdmin()
	svc := newTestServiceWithKafka(kafka)

	tests := []struct {
		name        string
		slug        string
		wantErr     bool
		errContains string
	}{
		{name: "valid simple", slug: "acme", wantErr: false},
		{name: "valid with digits", slug: "acme-123", wantErr: false},
		{name: "valid long", slug: "a" + "b" + "c" + "d" + "e" + "f" + "g-corp", wantErr: false},
		{name: "empty slug", slug: "", wantErr: true},
		{name: "uppercase rejected", slug: "Acme", wantErr: true},
		{name: "underscore rejected", slug: "acme_corp", wantErr: true},
		{name: "dot rejected", slug: "acme.corp", wantErr: true},
		{name: "starts with digit", slug: "1acme", wantErr: true},
		{name: "too short (2 chars)", slug: "ab", wantErr: true},
		{name: "reserved: admin", slug: "admin", wantErr: true, errContains: "reserved"},
		{name: "reserved: api", slug: "api", wantErr: true, errContains: "reserved"},
		{name: "reserved: keys", slug: "keys", wantErr: true, errContains: "reserved"},
		{name: "reserved: rename", slug: "rename", wantErr: true, errContains: "reserved"},
		{name: "reserved: health", slug: "health", wantErr: true, errContains: "reserved"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := svc.CreateTenant(context.Background(), provisioning.CreateTenantRequest{
				Slug: tt.slug,
				Name: "Test Tenant",
			})
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for slug %q, got nil", tt.slug)
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error = %q, want contains %q", err.Error(), tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for slug %q: %v", tt.slug, err)
				}
			}
		})
	}
}
