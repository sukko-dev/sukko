package repository_test

import (
	"context"
	"testing"

	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

func TestOpenDatabase_Connectivity(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)

	ctx := context.Background()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	// Verify migrations created the tenants table
	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_name = 'tenants'
		)`).Scan(&exists)
	if err != nil {
		t.Fatalf("query information_schema: %v", err)
	}
	if !exists {
		t.Fatal("tenants table should exist after migrations")
	}
}
