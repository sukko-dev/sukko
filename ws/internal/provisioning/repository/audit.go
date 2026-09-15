// Package repository provides database repositories for the provisioning system.
package repository

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sukko-dev/sukko/internal/provisioning"
)

// AuditRepository implements AuditStore using PostgreSQL via pgxpool.
type AuditRepository struct {
	pool *pgxpool.Pool
}

// NewAuditRepository creates an AuditRepository.
func NewAuditRepository(pool *pgxpool.Pool) *AuditRepository {
	return &AuditRepository{pool: pool}
}

// Log records an audit entry.
func (r *AuditRepository) Log(ctx context.Context, entry *provisioning.AuditEntry) error {
	query := `
		INSERT INTO provisioning_audit (tenant_id, action, actor, actor_type, ip_address, details, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`

	now := time.Now()
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = now
	}
	if entry.ActorType == "" {
		entry.ActorType = provisioning.ActorTypeUser
	}
	if entry.Details == nil {
		entry.Details = make(provisioning.Metadata)
	}

	// Convert empty tenant_id to NULL
	var tenantID any
	if entry.TenantID != "" {
		tenantID = entry.TenantID
	}

	ipAddress := normalizeAuditIP(entry.IPAddress)

	err := r.pool.QueryRow(ctx, query,
		tenantID,
		entry.Action,
		entry.Actor,
		entry.ActorType,
		ipAddress,
		entry.Details,
		entry.CreatedAt,
	).Scan(&entry.ID)
	if err != nil {
		return fmt.Errorf("insert audit entry: %w", err)
	}

	return nil
}

// ListByTenant returns audit entries for a tenant.
func (r *AuditRepository) ListByTenant(ctx context.Context, tenantID string, opts provisioning.ListOptions) ([]*provisioning.AuditEntry, int, error) {
	// Count total
	countQuery := `SELECT COUNT(*) FROM provisioning_audit WHERE tenant_id = $1`
	var total int
	if err := r.pool.QueryRow(ctx, countQuery, tenantID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count audit entries: %w", err)
	}

	// Get page
	limit := opts.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := max(opts.Offset, 0)

	query := `
		SELECT id, tenant_id, action, actor, actor_type, ip_address, details, created_at
		FROM provisioning_audit
		WHERE tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.pool.Query(ctx, query, tenantID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("query audit entries: %w", err)
	}
	defer rows.Close()

	entries := []*provisioning.AuditEntry{}
	for rows.Next() {
		entry := &provisioning.AuditEntry{}
		var tenantIDNull *string
		var ipAddressNull *string

		err := rows.Scan(
			&entry.ID,
			&tenantIDNull,
			&entry.Action,
			&entry.Actor,
			&entry.ActorType,
			&ipAddressNull,
			&entry.Details,
			&entry.CreatedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("scan audit entry: %w", err)
		}

		if tenantIDNull != nil {
			entry.TenantID = *tenantIDNull
		}
		if ipAddressNull != nil {
			entry.IPAddress = *ipAddressNull
		}

		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate audit entries: %w", err)
	}

	return entries, total, nil
}

var _ provisioning.AuditStore = (*AuditRepository)(nil)

// normalizeAuditIP prepares a caller-supplied IP for the INET ip_address column
// (§II backstop — the middleware strips ports, but this layer must never lose a
// whole audit record to a malformed IP): a bare IP passes through, a host:port
// form is stripped to its host, and anything that is not an IP at all becomes
// NULL — recording the action is §IX-mandatory, the IP is metadata.
func normalizeAuditIP(raw string) any {
	if raw == "" {
		return nil
	}
	if net.ParseIP(raw) != nil {
		return raw
	}
	if host, _, err := net.SplitHostPort(raw); err == nil {
		if net.ParseIP(host) != nil {
			return host
		}
	}
	return nil
}
