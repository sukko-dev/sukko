package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sukko-dev/sukko/internal/shared/database"
)

// OpenDatabase creates a pgxpool connection pool for the push service.
// Pool tuning is configured via DATABASE_URL query params (e.g., ?pool_max_conns=25).
func OpenDatabase(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := database.OpenPool(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open push database: %w", err)
	}
	return pool, nil
}
