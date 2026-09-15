package database

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql

	"github.com/rs/zerolog"
)

// RunMigrations applies pending database migrations from an embedded filesystem.
// Opens a short-lived *sql.DB connection, executes pending migrations in order,
// tracks applied versions in a _migrations table, and closes the connection.
// The caller should open the connection pool after this returns.
func RunMigrations(ctx context.Context, databaseURL string, migrationFS fs.FS, logger zerolog.Logger) error {
	// Open short-lived connection for migration only
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open migration connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping migration connection: %w", err)
	}

	// Acquire advisory lock — only one process runs migrations at a time.
	// Other processes wait until the lock is released. Prevents concurrent
	// replicas from racing on migration apply.
	const migrationLockID = 0x73756B6B6F // "sukko" in hex
	if _, err := db.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() { //nolint:contextcheck // unlock must succeed even if parent ctx is canceled
		_, _ = db.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockID)
	}()

	// Read migration files from embedded FS
	entries, err := fs.ReadDir(migrationFS, "postgres")
	if err != nil {
		return fmt.Errorf("read migration directory: %w", err)
	}

	// Ensure tracking table exists
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS _migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		return fmt.Errorf("create migration tracking table: %w", err)
	}

	// Get already-applied migrations
	applied := make(map[string]bool)
	rows, err := db.QueryContext(ctx, "SELECT version FROM _migrations")
	if err != nil {
		return fmt.Errorf("query applied migrations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("scan migration version: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate applied migrations: %w", err)
	}

	// Collect and sort pending SQL files
	var pending []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		if applied[name] {
			continue
		}
		pending = append(pending, name)
	}
	sort.Strings(pending)

	if len(pending) == 0 {
		logger.Info().Int("applied", len(applied)).Msg("Database schema up to date")
		return nil
	}

	// Apply pending migrations in a transaction per file
	for _, name := range pending {
		data, err := fs.ReadFile(migrationFS, "postgres/"+name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin transaction for %s: %w", name, err)
		}

		if _, err := tx.ExecContext(ctx, string(data)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("execute migration %s: %w", name, err)
		}

		if _, err := tx.ExecContext(ctx, "INSERT INTO _migrations (version) VALUES ($1)", name); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s: %w", name, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}

		logger.Info().Str("migration", name).Msg("Migration applied")
	}

	logger.Info().Int("applied", len(pending)).Int("total", len(applied)+len(pending)).Msg("Database migrations complete")
	return nil
}
