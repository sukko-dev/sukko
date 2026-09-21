package database

import (
	"context"
	"database/sql"
	"io/fs"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/sukko-dev/sukko/internal/shared/migrations"
)

func startTestPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("migrate_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get connection string: %v", err)
	}
	return connStr
}

func TestRunMigrations_FreshDatabase(t *testing.T) {
	t.Parallel()
	connStr := startTestPostgres(t)
	ctx := context.Background()
	logger := zerolog.Nop()

	err := RunMigrations(ctx, connStr, migrations.Postgres, logger)
	if err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	// Verify tables exist
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	tables := []string{
		"tenants", "tenant_keys", "api_keys", "tenant_routing_rules",
		"push_credentials", "push_channel_configs", "push_subscriptions",
		"webhooks", "webhook_deliveries",
	}
	for _, table := range tables {
		var exists bool
		err := db.QueryRowContext(ctx,
			"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)", table).Scan(&exists)
		if err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("table %s does not exist after migration", table)
		}
	}

	// Verify migration tracking table
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM _migrations").Scan(&count)
	if err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	// Expect one recorded migration per embedded .sql file (RunMigrations applies
	// every *.sql and skips atlas.sum) — derived, so adding a migration doesn't
	// require touching this assertion.
	entries, err := fs.ReadDir(migrations.Postgres, "postgres")
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	want := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			want++
		}
	}
	if count != want {
		t.Errorf("expected %d recorded migrations (one per .sql file), got %d", want, count)
	}
}

func TestRunMigrations_Idempotent(t *testing.T) {
	t.Parallel()
	connStr := startTestPostgres(t)
	ctx := context.Background()
	logger := zerolog.Nop()

	// First run
	if err := RunMigrations(ctx, connStr, migrations.Postgres, logger); err != nil {
		t.Fatalf("first RunMigrations: %v", err)
	}

	// Second run — should succeed with 0 pending
	if err := RunMigrations(ctx, connStr, migrations.Postgres, logger); err != nil {
		t.Fatalf("second RunMigrations: %v", err)
	}
}

func TestRunMigrations_InvalidURL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := zerolog.Nop()

	err := RunMigrations(ctx, "postgres://bad:bad@localhost:1/nonexistent?sslmode=disable", migrations.Postgres, logger)
	if err == nil {
		t.Fatal("expected error for invalid URL, got nil")
	}
}
