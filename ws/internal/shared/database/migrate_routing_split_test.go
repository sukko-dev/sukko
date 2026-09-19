package database

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/migrations"
)

// preSplitMigrationFS returns a migration FS containing only 001 and 002 — the
// schema as it stood before the routing-rule ingress/egress split — so a test
// can seed legacy `topics TEXT[]` rows and then apply 003 on top.
func preSplitMigrationFS(t *testing.T) fs.FS {
	t.Helper()
	m := fstest.MapFS{}
	for _, name := range []string{"001_initial.sql", "002_analytics_partition.sql"} {
		data, err := fs.ReadFile(migrations.Postgres, "postgres/"+name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		m["postgres/"+name] = &fstest.MapFile{Data: data}
	}
	return m
}

// TestMigration003_SplitsLegacyTopicsRows pins the data effect of
// 003_routing_rule_ingress_egress.sql on pre-existing rows (ADR-0018):
//   - topics[1] becomes ingress_topic (matching first-match-wins semantics),
//   - the remainder becomes egress_topics, deduplicated and with any copy of
//     the ingress topic removed,
//   - a single-topic row gets an EMPTY egress_topics array (never NULL),
//   - a degenerate zero-topic row is deleted rather than migrated into a NULL
//     ingress topic,
//   - the legacy topics column is gone.
func TestMigration003_SplitsLegacyTopicsRows(t *testing.T) {
	t.Parallel()
	connStr := startTestPostgres(t)
	ctx := context.Background()
	logger := zerolog.Nop()

	// 1. Bring the schema to its pre-split state (001 + 002 only).
	if err := RunMigrations(ctx, connStr, preSplitMigrationFS(t), logger); err != nil {
		t.Fatalf("RunMigrations(pre-split): %v", err)
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// 2. Seed a tenant and legacy multi-topic rows in the old shape.
	var tenantID string
	if err := db.QueryRowContext(ctx,
		`INSERT INTO tenants (slug, name) VALUES ('acme', 'Acme') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	seed := func(pattern string, topics []string, priority int) {
		t.Helper()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO tenant_routing_rules (tenant_id, pattern, topics, priority) VALUES ($1, $2, $3, $4)`,
			tenantID, pattern, topics, priority); err != nil {
			t.Fatalf("seed rule %s: %v", pattern, err)
		}
	}
	seed("**.trade", []string{"trades", "audit", "audit", "trades", "analytics"}, 1)
	seed("**.order", []string{"orders"}, 2)
	seed("**.dead", []string{}, 3) // degenerate: never valid at write time — must be deleted

	// 3. Apply the full migration set — only 003 is pending.
	if err := RunMigrations(ctx, connStr, migrations.Postgres, logger); err != nil {
		t.Fatalf("RunMigrations(full): %v", err)
	}

	// 4. Multi-topic row: head → ingress, tail → egress (deduped, ingress removed).
	var ingress string
	var egress []byte // TEXT[] scanned raw; compare its textual form
	if err := db.QueryRowContext(ctx,
		`SELECT ingress_topic, egress_topics FROM tenant_routing_rules WHERE pattern = '**.trade'`).
		Scan(&ingress, &egress); err != nil {
		t.Fatalf("query migrated multi-topic row: %v", err)
	}
	if ingress != "trades" {
		t.Errorf("ingress_topic = %q, want %q (topics[1])", ingress, "trades")
	}
	// array_agg(DISTINCT ...) sorts: expect {analytics,audit} — no trades, no dup audit.
	if got := string(egress); got != "{analytics,audit}" {
		t.Errorf("egress_topics = %q, want %q", got, "{analytics,audit}")
	}

	// 5. Single-topic row: empty (non-NULL) egress array.
	if err := db.QueryRowContext(ctx,
		`SELECT ingress_topic, egress_topics FROM tenant_routing_rules WHERE pattern = '**.order'`).
		Scan(&ingress, &egress); err != nil {
		t.Fatalf("query migrated single-topic row: %v", err)
	}
	if ingress != "orders" {
		t.Errorf("ingress_topic = %q, want %q", ingress, "orders")
	}
	if got := string(egress); got != "{}" {
		t.Errorf("egress_topics = %q, want empty array {}", got)
	}

	// 6. Zero-topic row deleted; legacy column gone.
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tenant_routing_rules WHERE pattern = '**.dead'`).Scan(&count); err != nil {
		t.Fatalf("count degenerate rows: %v", err)
	}
	if count != 0 {
		t.Errorf("zero-topic row survived migration, want deleted")
	}
	var hasTopics bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name = 'tenant_routing_rules' AND column_name = 'topics')`).Scan(&hasTopics); err != nil {
		t.Fatalf("check topics column: %v", err)
	}
	if hasTopics {
		t.Error("legacy topics column still present after migration")
	}
}

// TestMigration003_SanitizesReservedAndCrossRule pins that migration 003 does not
// merely reshape legacy rows but SANITIZES them. Pre-split validation had no
// reserved-suffix or cross-rule checks, so a stored row may hold shapes the new
// write path rejects; carrying them across verbatim would reinstate the two
// defects ADR-0018 removes — DLQ-to-subscriber delivery and duplicate delivery.
func TestMigration003_SanitizesReservedAndCrossRule(t *testing.T) {
	t.Parallel()
	connStr := startTestPostgres(t)
	ctx := context.Background()
	logger := zerolog.Nop()

	if err := RunMigrations(ctx, connStr, preSplitMigrationFS(t), logger); err != nil {
		t.Fatalf("RunMigrations(pre-split): %v", err)
	}
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var tenantID string
	if err := db.QueryRowContext(ctx,
		`INSERT INTO tenants (slug, name) VALUES ('beta', 'Beta') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	seed := func(pattern string, topics []string, priority int) {
		t.Helper()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO tenant_routing_rules (tenant_id, pattern, topics, priority) VALUES ($1, $2, $3, $4)`,
			tenantID, pattern, topics, priority); err != nil {
			t.Fatalf("seed rule %s: %v", pattern, err)
		}
	}
	// (a) all topics reserved → no legitimate ingress → row deleted.
	seed("**.a", []string{"dead-letter"}, 1)
	// (b) dead-letter is skipped as ingress; the real topic becomes ingress, egress empty.
	seed("**.b", []string{"dead-letter", "real"}, 2)
	// (c) default must never become an egress topic (it is always consumed).
	seed("**.c", []string{"audit", "default"}, 3)
	// (d) cross-rule: t2 is rule-e's ingress, so it must be stripped from rule-d's egress.
	seed("**.d", []string{"t1", "t2"}, 4)
	seed("**.e", []string{"t2", "t3"}, 5)

	if err := RunMigrations(ctx, connStr, migrations.Postgres, logger); err != nil {
		t.Fatalf("RunMigrations(full): %v", err)
	}

	get := func(pattern string) (ingress string, egress string, found bool) {
		t.Helper()
		var raw []byte
		err := db.QueryRowContext(ctx,
			`SELECT ingress_topic, egress_topics FROM tenant_routing_rules WHERE tenant_id = $1 AND pattern = $2`,
			tenantID, pattern).Scan(&ingress, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", false
		}
		if err != nil {
			t.Fatalf("query %s: %v", pattern, err)
		}
		return ingress, string(raw), true
	}

	// (a) deleted.
	if _, _, found := get("**.a"); found {
		t.Error("all-reserved row survived migration, want deleted")
	}
	// (b) ingress=real, egress={}.
	if ing, eg, found := get("**.b"); !found || ing != "real" || eg != "{}" {
		t.Errorf("**.b => ingress=%q egress=%q found=%v, want real / {} / true", ing, eg, found)
	}
	// (c) ingress=audit, egress={} — default never egress.
	if ing, eg, found := get("**.c"); !found || ing != "audit" || eg != "{}" {
		t.Errorf("**.c => ingress=%q egress=%q found=%v, want audit / {} / true (default must not be egress)", ing, eg, found)
	}
	// (d) ingress=t1, egress={} — t2 stripped because it is rule-e's ingress.
	if ing, eg, found := get("**.d"); !found || ing != "t1" || eg != "{}" {
		t.Errorf("**.d => ingress=%q egress=%q found=%v, want t1 / {} / true (t2 is another rule's ingress)", ing, eg, found)
	}

	// The CHECK constraint forbids re-introducing dead-letter as an ingress topic.
	_, err = db.ExecContext(ctx,
		`INSERT INTO tenant_routing_rules (tenant_id, pattern, priority, ingress_topic, egress_topics)
		 VALUES ($1, '**.reserved', 99, 'dead-letter', '{}')`, tenantID)
	if err == nil {
		t.Error("insert with ingress_topic='dead-letter' succeeded, want CHECK violation")
	}
}
