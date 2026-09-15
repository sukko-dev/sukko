package auth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// KeyRegistryConfig configures the KeyRegistry.
type KeyRegistryConfig struct {
	// Pool is the pgx connection pool.
	Pool *pgxpool.Pool

	// RefreshInterval is how often to refresh the cache (default: 1 minute).
	RefreshInterval time.Duration

	// QueryTimeout is the timeout for database queries (default: 5 seconds).
	QueryTimeout time.Duration

	// Logger for structured logging.
	Logger zerolog.Logger

	// Metrics is an optional callback for reporting cache metrics.
	// If nil, metrics are not reported (but still tracked internally via Stats()).
	Metrics pkgmetrics.CacheMetrics
}

// DBKeyRegistry implements KeyRegistryWithRefresh by fetching keys from the provisioning database.
// It maintains an in-memory cache that is refreshed periodically.
type DBKeyRegistry struct {
	pool            *pgxpool.Pool
	refreshInterval time.Duration
	queryTimeout    time.Duration
	logger          zerolog.Logger
	metrics         pkgmetrics.CacheMetrics

	// Cache
	cacheMu      sync.RWMutex
	keysByID     map[string]*KeyInfo
	keysByTenant map[string][]*KeyInfo
	lastRefresh  time.Time

	// Stats
	cacheHits     atomic.Int64
	cacheMisses   atomic.Int64
	refreshErrors atomic.Int64

	// Lifecycle
	closeOnce sync.Once
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewKeyRegistry creates a new DBKeyRegistry.
func NewKeyRegistry(cfg KeyRegistryConfig) (*DBKeyRegistry, error) {
	if cfg.Pool == nil {
		return nil, errors.New("database connection pool is required")
	}
	if cfg.RefreshInterval <= 0 {
		return nil, errors.New("RefreshInterval must be > 0")
	}
	if cfg.QueryTimeout <= 0 {
		return nil, errors.New("QueryTimeout must be > 0")
	}

	r := &DBKeyRegistry{
		pool:            cfg.Pool,
		refreshInterval: cfg.RefreshInterval,
		queryTimeout:    cfg.QueryTimeout,
		logger:          cfg.Logger,
		metrics:         cfg.Metrics,
		keysByID:        make(map[string]*KeyInfo),
		keysByTenant:    make(map[string][]*KeyInfo),
		stopCh:          make(chan struct{}),
	}

	// Initial load
	ctx, cancel := context.WithTimeout(context.Background(), cfg.QueryTimeout)
	defer cancel()

	if err := r.refreshCache(ctx); err != nil {
		r.logger.Warn().Err(err).Msg("Initial key cache load failed, will retry")
	}

	// Start background refresh
	r.wg.Go(r.backgroundRefresh)

	return r, nil
}

// GetKey retrieves a key by ID from the cache.
func (r *DBKeyRegistry) GetKey(ctx context.Context, keyID string) (*KeyInfo, error) {
	r.cacheMu.RLock()
	key, ok := r.keysByID[keyID]
	r.cacheMu.RUnlock()

	if !ok {
		r.cacheMisses.Add(1)
		if r.metrics != nil {
			r.metrics.OnCacheMiss()
		}

		// Try fetching directly from DB on cache miss
		key, err := r.fetchKeyFromDB(ctx, keyID)
		if err != nil {
			return nil, err
		}

		// Add to cache
		r.cacheMu.Lock()
		r.keysByID[key.KeyID] = key
		r.cacheMu.Unlock()

		return key, nil
	}

	r.cacheHits.Add(1)
	if r.metrics != nil {
		r.metrics.OnCacheHit()
	}

	// Check validity
	if key.RevokedAt != nil {
		return nil, ErrKeyRevoked
	}
	if key.ExpiresAt != nil && key.ExpiresAt.Before(time.Now()) {
		return nil, ErrKeyExpired
	}
	if !key.IsActive {
		return nil, ErrKeyNotFound
	}

	return key, nil
}

// GetKeysByTenantUUID retrieves all active keys for a tenant UUID. keysByTenant
// is keyed by KeyInfo.TenantID (the tenant UUID), and the fallthrough DB query
// filters WHERE t.id = $1 — so both the cache lookup and the DB path key on the
// same tenant UUID (identity of record).
func (r *DBKeyRegistry) GetKeysByTenantUUID(ctx context.Context, tenantUUID string) ([]*KeyInfo, error) {
	r.cacheMu.RLock()
	keys := r.keysByTenant[tenantUUID]
	r.cacheMu.RUnlock()

	if len(keys) == 0 {
		// Try fetching from DB
		return r.fetchKeysByTenantFromDB(ctx, tenantUUID)
	}

	// Filter to only valid keys
	var validKeys []*KeyInfo
	for _, key := range keys {
		if key.IsValid() {
			validKeys = append(validKeys, key)
		}
	}

	return validKeys, nil
}

// Refresh forces a refresh of the key cache.
func (r *DBKeyRegistry) Refresh(ctx context.Context) error {
	return r.refreshCache(ctx)
}

// Stats returns cache statistics.
func (r *DBKeyRegistry) Stats() KeyRegistryStats {
	r.cacheMu.RLock()
	defer r.cacheMu.RUnlock()

	activeCount := 0
	for _, key := range r.keysByID {
		if key.IsValid() {
			activeCount++
		}
	}

	return KeyRegistryStats{
		TotalKeys:     len(r.keysByID),
		ActiveKeys:    activeCount,
		LastRefresh:   r.lastRefresh,
		RefreshErrors: r.refreshErrors.Load(),
		CacheHits:     r.cacheHits.Load(),
		CacheMisses:   r.cacheMisses.Load(),
	}
}

// Close stops background refresh and releases resources.
func (r *DBKeyRegistry) Close() error {
	r.closeOnce.Do(func() {
		close(r.stopCh)
	})
	r.wg.Wait()
	return nil
}

// backgroundRefresh periodically refreshes the key cache.
func (r *DBKeyRegistry) backgroundRefresh() {
	defer logging.RecoverPanic(r.logger, "backgroundRefresh", nil)

	ticker := time.NewTicker(r.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), r.queryTimeout)
			if err := r.refreshCache(ctx); err != nil {
				r.refreshErrors.Add(1)
				r.logger.Error().Err(err).Msg("Failed to refresh key cache")
				if r.metrics != nil {
					r.metrics.OnCacheRefresh(false, 0)
				}
			}
			cancel()
		}
	}
}

// refreshCache loads all active keys from the database.
func (r *DBKeyRegistry) refreshCache(ctx context.Context) error {
	query := `
		SELECT
			tk.key_id,
			t.id,
			tk.algorithm,
			tk.public_key,
			tk.is_active,
			tk.expires_at,
			tk.revoked_at
		FROM tenant_keys tk
		JOIN tenants t ON tk.tenant_id = t.id
		WHERE tk.is_active = true
		  AND tk.revoked_at IS NULL
		  AND (tk.expires_at IS NULL OR tk.expires_at > $1)
		  AND t.status = 'active'
	`

	rows, err := r.pool.Query(ctx, query, time.Now())
	if err != nil {
		return fmt.Errorf("query keys: %w", err)
	}
	defer rows.Close()

	newKeysByID := make(map[string]*KeyInfo)
	newKeysByTenant := make(map[string][]*KeyInfo)

	for rows.Next() {
		var key KeyInfo
		var expiresAt, revokedAt *time.Time

		if err := rows.Scan(
			&key.KeyID,
			&key.TenantID,
			&key.Algorithm,
			&key.PublicKeyPEM,
			&key.IsActive,
			&expiresAt,
			&revokedAt,
		); err != nil {
			r.logger.Warn().Err(err).Msg("Failed to scan key row")
			continue
		}

		key.ExpiresAt = expiresAt
		key.RevokedAt = revokedAt

		// Parse the public key
		pub, err := ParsePublicKey(key.PublicKeyPEM, key.Algorithm)
		if err != nil {
			r.logger.Warn().
				Str("key_id", key.KeyID).
				Str("algorithm", key.Algorithm).
				Err(err).
				Msg("Failed to parse public key")
			continue
		}
		key.PublicKey = pub

		newKeysByID[key.KeyID] = &key
		newKeysByTenant[key.TenantID] = append(newKeysByTenant[key.TenantID], &key)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}

	// Swap cache
	r.cacheMu.Lock()
	r.keysByID = newKeysByID
	r.keysByTenant = newKeysByTenant
	r.lastRefresh = time.Now()
	r.cacheMu.Unlock()

	r.logger.Debug().
		Int("key_count", len(newKeysByID)).
		Int("tenant_count", len(newKeysByTenant)).
		Msg("Key cache refreshed")

	if r.metrics != nil {
		r.metrics.OnCacheRefresh(true, len(newKeysByID))
	}

	return nil
}

// fetchKeyFromDB fetches a single key from the database.
func (r *DBKeyRegistry) fetchKeyFromDB(ctx context.Context, keyID string) (*KeyInfo, error) {
	query := `
		SELECT
			tk.key_id,
			t.id,
			tk.algorithm,
			tk.public_key,
			tk.is_active,
			tk.expires_at,
			tk.revoked_at
		FROM tenant_keys tk
		JOIN tenants t ON tk.tenant_id = t.id
		WHERE tk.key_id = $1
		  AND t.status = 'active'
	`

	var key KeyInfo
	var expiresAt, revokedAt *time.Time

	err := r.pool.QueryRow(ctx, query, keyID).Scan(
		&key.KeyID,
		&key.TenantID,
		&key.Algorithm,
		&key.PublicKeyPEM,
		&key.IsActive,
		&expiresAt,
		&revokedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query key: %w", err)
	}

	key.ExpiresAt = expiresAt
	key.RevokedAt = revokedAt

	// Check validity
	if !key.IsActive {
		return nil, ErrKeyNotFound
	}
	if key.RevokedAt != nil {
		return nil, ErrKeyRevoked
	}
	if key.ExpiresAt != nil && key.ExpiresAt.Before(time.Now()) {
		return nil, ErrKeyExpired
	}

	// Parse the public key
	pub, err := ParsePublicKey(key.PublicKeyPEM, key.Algorithm)
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}
	key.PublicKey = pub

	return &key, nil
}

// fetchKeysByTenantFromDB fetches all active keys for a tenant from the database.
func (r *DBKeyRegistry) fetchKeysByTenantFromDB(ctx context.Context, tenantUUID string) ([]*KeyInfo, error) {
	query := `
		SELECT
			tk.key_id,
			t.id,
			tk.algorithm,
			tk.public_key,
			tk.is_active,
			tk.expires_at,
			tk.revoked_at
		FROM tenant_keys tk
		JOIN tenants t ON tk.tenant_id = t.id
		WHERE t.id = $1
		  AND tk.is_active = true
		  AND tk.revoked_at IS NULL
		  AND (tk.expires_at IS NULL OR tk.expires_at > $2)
		  AND t.status = 'active'
	`

	rows, err := r.pool.Query(ctx, query, tenantUUID, time.Now())
	if err != nil {
		return nil, fmt.Errorf("query keys: %w", err)
	}
	defer rows.Close()

	var keys []*KeyInfo
	for rows.Next() {
		var key KeyInfo
		var expiresAt, revokedAt *time.Time

		if err := rows.Scan(
			&key.KeyID,
			&key.TenantID,
			&key.Algorithm,
			&key.PublicKeyPEM,
			&key.IsActive,
			&expiresAt,
			&revokedAt,
		); err != nil {
			r.logger.Warn().Err(err).Msg("Failed to scan tenant key row")
			continue
		}

		key.ExpiresAt = expiresAt
		key.RevokedAt = revokedAt

		// Parse the public key
		pub, err := ParsePublicKey(key.PublicKeyPEM, key.Algorithm)
		if err != nil {
			r.logger.Warn().
				Str("key_id", key.KeyID).
				Str("algorithm", key.Algorithm).
				Err(err).
				Msg("Failed to parse public key")
			continue
		}
		key.PublicKey = pub

		keys = append(keys, &key)
	}

	if err := rows.Err(); err != nil {
		return keys, fmt.Errorf("iterate tenant keys rows: %w", err)
	}
	return keys, nil
}

// Ensure DBKeyRegistry implements KeyRegistryWithRefresh.
var _ KeyRegistryWithRefresh = (*DBKeyRegistry)(nil)
