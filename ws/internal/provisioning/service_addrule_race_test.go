package provisioning_test

// Concurrency-correctness tests for the routing-rule write paths (ADR-0018
// ingress/egress disjointness). These wire the REAL Postgres-backed
// RoutingRulesRepository (testcontainer) into the Service — everything else
// mocked — so AddRoutingRule executes the genuine lock+read+validate+insert
// transaction rather than a mock. No t.Parallel(): each test owns a container,
// but the race rounds inside share one pool (§VIII).

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
	sharedkafka "github.com/sukko-dev/sukko/internal/shared/kafka"
	sharedtestutil "github.com/sukko-dev/sukko/internal/shared/testutil"
)

// addRaceMetricSeq produces unique Prometheus metric prefixes per repository
// construction (the default registerer is process-global and panics on
// duplicates — see repository/routing_rules_test.go:metricSeq).
var addRaceMetricSeq atomic.Int64

const raceTestNamespace = "test"

// routingRaceHarness bundles a Service backed by a real RoutingRulesRepository.
type routingRaceHarness struct {
	svc   *provisioning.Service
	repo  *repository.RoutingRulesRepository
	pool  *pgxpool.Pool
	uuids map[string]string // slug → tenant UUID (DB primary key)
}

// newRoutingRaceHarness boots an isolated Postgres container, runs migrations,
// seeds one tenant per slug (mock tenant store + real tenants row for the FK),
// provisions topics t1/t2/t3 in the mock Kafka admin, and wires the Service.
func newRoutingRaceHarness(t *testing.T, slugs ...string) *routingRaceHarness {
	t.Helper()
	ctx := context.Background()
	pool := sharedtestutil.NewTestPool(t)

	tenantStore := testutil.NewMockTenantStore()
	kafkaAdmin := testutil.NewMockKafkaAdmin()
	uuids := make(map[string]string, len(slugs))

	for _, slug := range slugs {
		tenant := testutil.NewTestTenant(slug)
		if err := tenantStore.Create(ctx, tenant); err != nil {
			t.Fatalf("seed mock tenant %q: %v", slug, err)
		}
		// The tenant_routing_rules FK requires a matching tenants row with the SAME UUID.
		if _, err := pool.Exec(ctx,
			`INSERT INTO tenants (id, slug, name, status, consumer_type, metadata)
			 VALUES ($1, $2, $3, 'active', 'shared', '{}')`,
			tenant.ID, slug, slug+"-name",
		); err != nil {
			t.Fatalf("seed tenants row %q: %v", slug, err)
		}
		uuids[slug] = tenant.ID

		for _, suffix := range []string{"t1", "t2", "t3"} {
			name := sharedkafka.BuildTopicName(raceTestNamespace, slug, suffix)
			if err := kafkaAdmin.CreateTopic(ctx, name, 1, 1, nil); err != nil {
				t.Fatalf("seed topic %q: %v", name, err)
			}
		}
	}

	repo := repository.NewRoutingRulesRepository(pool, zerolog.Nop(),
		fmt.Sprintf("addrace%d", addRaceMetricSeq.Add(1)))

	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 tenantStore,
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           repo,
		TopicStore:                  testutil.NewMockTopicStore(),
		QuotaStore:                  testutil.NewMockQuotaStore(),
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  kafkaAdmin,
		EventBus:                    eventbus.New(zerolog.Nop()),
		TopicNamespace:              raceTestNamespace,
		DefaultPartitions:           1,
		DefaultRetentionMs:          3600000,
		MaxRoutingRulesPerTenant:    10,
		MaxTopicsPerRule:            5,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	return &routingRaceHarness{svc: svc, repo: repo, pool: pool, uuids: uuids}
}

// TestAddRoutingRule_ConcurrentDisjointness_SameTenant races two Adds for the
// SAME tenant whose rules are individually valid but jointly violate ADR-0018
// disjointness: one rule writes egress copies to t2, the other consumes t2 as
// ingress. Under the per-tenant advisory lock exactly one must commit per
// round; the loser must see the winner's committed row and fail with
// ErrEgressIngressOverlap. Without the lock, both Adds can pass the
// read-validate step before either inserts and BOTH commit — the overlap that
// causes duplicate, un-dedupable delivery.
func TestAddRoutingRule_ConcurrentDisjointness_SameTenant(t *testing.T) {
	t.Parallel() // each test owns an isolated testcontainer (sibling convention, §XVIII)
	h := newRoutingRaceHarness(t, "race-tenant")
	ctx := context.Background()
	slug := "race-tenant"
	tenantUUID := h.uuids[slug]

	ruleEgress := provisioning.TopicRoutingRule{
		Pattern: "a.**", IngressTopic: "t1", EgressTopics: []string{"t2"}, Priority: 10,
	}
	ruleIngress := provisioning.TopicRoutingRule{
		Pattern: "b.**", IngressTopic: "t2", Priority: 20,
	}

	const rounds = 40
	for round := range rounds {
		if err := h.repo.DeleteAll(ctx, tenantUUID); err != nil {
			t.Fatalf("round %d: DeleteAll: %v", round, err)
		}

		start := make(chan struct{})
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i, rule := range []provisioning.TopicRoutingRule{ruleEgress, ruleIngress} {
			wg.Go(func() {
				<-start
				errs[i] = h.svc.AddRoutingRule(ctx, slug, rule)
			})
		}
		close(start)
		wg.Wait()

		failures := 0
		for _, err := range errs {
			if err == nil {
				continue
			}
			failures++
			if !errors.Is(err, provisioning.ErrEgressIngressOverlap) {
				t.Fatalf("round %d: loser returned wrong error: %v", round, err)
			}
		}
		if failures != 1 {
			t.Fatalf("round %d: want exactly one loser with ErrEgressIngressOverlap, got %d failures (errs=%v)",
				round, failures, errs)
		}

		stored, err := h.repo.GetAll(ctx, tenantUUID)
		if err != nil {
			t.Fatalf("round %d: GetAll: %v", round, err)
		}
		if len(stored) != 1 {
			t.Fatalf("round %d: want 1 stored rule, got %d: %+v", round, len(stored), stored)
		}
		if err := provisioning.ValidateEgressDisjoint(stored); err != nil {
			t.Fatalf("round %d: stored rules violate ADR-0018 disjointness: %v", round, err)
		}
	}
}

// TestRoutingRuleWrites_PerTenantLockScoping pins the lock's scope
// deterministically, with no timing races:
//
//  1. While tenant A's advisory lock is held by a foreign transaction, a
//     routing-rule write for tenant B completes — different tenants derive
//     different lock keys and are NOT serialized against each other.
//  2. Add and Replace for tenant A block until their context deadline —
//     proving both write paths acquire the per-tenant lock inside their
//     transaction (this is also the deterministic mutation kill: with the
//     lock removed, the writes return instantly and these cases fail).
//  3. After the foreign transaction releases the lock, tenant A's Add
//     succeeds — the lock was transaction-scoped, nothing leaked.
func TestRoutingRuleWrites_PerTenantLockScoping(t *testing.T) {
	t.Parallel() // each test owns an isolated testcontainer (sibling convention, §XVIII)
	h := newRoutingRaceHarness(t, "tenant-a", "tenant-b")
	ctx := context.Background()

	ruleA := provisioning.TopicRoutingRule{Pattern: "a.**", IngressTopic: "t1", Priority: 10}
	ruleB := provisioning.TopicRoutingRule{Pattern: "b.**", IngressTopic: "t1", Priority: 10}

	// Hold tenant A's routing-rules lock in a raw transaction, exactly as the
	// write paths take it (same exported query — same key derivation).
	lockTx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock-holding tx: %v", err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	if _, err := lockTx.Exec(ctx, repository.TenantRoutingRulesLockQuery, h.uuids["tenant-a"]); err != nil {
		t.Fatalf("acquire tenant A lock: %v", err)
	}

	// (1) Different tenant is not serialized: B's Add must complete while A's
	// lock is held. The generous deadline only bounds a wrongly-serialized hang.
	ctxB, cancelB := context.WithTimeout(ctx, 10*time.Second)
	defer cancelB()
	if err := h.svc.AddRoutingRule(ctxB, "tenant-b", ruleB); err != nil {
		t.Fatalf("tenant B Add failed (or blocked on tenant A's lock): %v", err)
	}

	// (2) Same tenant blocks on the held lock, for BOTH write paths.
	blocked := []struct {
		name  string
		write func(ctx context.Context) error
	}{
		{"Add", func(ctx context.Context) error {
			return h.svc.AddRoutingRule(ctx, "tenant-a", ruleA)
		}},
		{"Replace", func(ctx context.Context) error {
			return h.svc.ReplaceRoutingRules(ctx, "tenant-a", []provisioning.TopicRoutingRule{ruleA})
		}},
	}
	// These subtests MUST run while lockTx (above) still holds tenant A's lock;
	// step (3) below releases it. Parallelizing would defer them until after the
	// release, so they would no longer block and the assertion would be void.
	//nolint:paralleltest // subtests share the parent's held lockTx (see above)
	for _, tc := range blocked {
		t.Run(tc.name+"BlocksWhileLockHeld", func(t *testing.T) {
			opCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			err := tc.write(opCtx)
			if err == nil {
				t.Fatalf("%s for tenant A succeeded while its advisory lock was held — lock not acquired in the write transaction", tc.name)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s: want context.DeadlineExceeded from blocking on the lock, got: %v", tc.name, err)
			}
		})
	}

	// (3) Release the lock; tenant A's Add now goes through and the stored
	// state is what the winner wrote — nothing from the canceled attempts.
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release tenant A lock: %v", err)
	}
	if err := h.svc.AddRoutingRule(ctx, "tenant-a", ruleA); err != nil {
		t.Fatalf("tenant A Add after lock release: %v", err)
	}
	stored, err := h.repo.GetAll(ctx, h.uuids["tenant-a"])
	if err != nil {
		t.Fatalf("GetAll tenant A: %v", err)
	}
	if len(stored) != 1 || stored[0].Pattern != "a.**" {
		t.Fatalf("tenant A stored rules = %+v, want exactly the post-release rule", stored)
	}

	// No leaked pool connections. The two blocked writes above each timed out
	// inside pg_advisory_xact_lock — the exact path a conditional/ shadowed-err
	// rollback abandons, leaking a checked-out connection permanently. With the
	// unconditional deferred rollback every write returns its connection, so once
	// lockTx is released and the final Add commits the pool is fully idle.
	// (Poll briefly: pgxpool returns connections synchronously on Commit/Rollback,
	// but the lock-holding lockTx.Rollback above may settle a beat later.)
	// pgxpool returns a connection synchronously on Commit/Rollback, so once the
	// lock-holding lockTx.Rollback (step 3) and the final Add's Commit have both
	// returned, every connection is back in the pool. A nonzero count here means a
	// blocked write above returned without rolling back — the leak this guards.
	if acquired := h.pool.Stat().AcquiredConns(); acquired != 0 {
		t.Fatalf("pool has %d acquired connections after all writes settled, want 0 — a blocked write leaked its transaction", acquired)
	}
}
