package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sukko-dev/sukko/internal/shared/database"
)

// OpenDatabase creates a pgxpool connection to PostgreSQL.
func OpenDatabase(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := database.OpenPool(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open provisioning database: %w", err)
	}
	return pool, nil
}
