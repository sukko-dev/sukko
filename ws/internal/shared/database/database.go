// Package database provides PostgreSQL connection management via pgxpool.
package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OpenPool creates a pgxpool connection pool from a DATABASE_URL.
// pgxpool handles connection pooling internally — no manual MaxOpenConns needed.
// Pool tuning via URL query params: ?pool_max_conns=25&pool_min_conns=5
func OpenPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}
