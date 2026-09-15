package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/sukko-dev/sukko/internal/server/registry"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// lazySREMTimeout is the maximum time for best-effort stale index cleanup operations.
const lazySREMTimeout = 3 * time.Second

// ConnectionDetail is the API representation of a live WebSocket connection.
// All fields except api_key_id, user_id, and channels_capped are always present.
type ConnectionDetail struct {
	ConnectionID   string   `json:"connection_id"`
	TenantID       string   `json:"tenant_id"`
	APIKeyID       string   `json:"api_key_id,omitempty"`
	UserID         string   `json:"user_id,omitempty"`
	PodID          string   `json:"pod_id"`
	ShardID        string   `json:"shard_id"`
	RemoteIP       string   `json:"remote_ip"`
	Transport      string   `json:"transport"`
	ConnectedAt    string   `json:"connected_at"`
	Channels       []string `json:"channels"`
	ChannelsCapped bool     `json:"channels_capped,omitempty"`
	RegistryStatus string   `json:"registry_status"`
}

// connectionFilters holds query parameters for connection list requests.
type connectionFilters struct {
	APIKeyID  string
	Channel   string
	Transport string
	PodID     string
	TenantID  string
	Limit     int
	Offset    int
}

// BuildConnectionsValkeyClient creates a dedicated Valkey client for the provisioning service's
// connections registry reader. Uses PROVISIONING_VALKEY_* config fields.
func BuildConnectionsValkeyClient(cfg platform.ProvisioningConfig) (valkey.Client, error) {
	opt := valkey.ClientOption{
		InitAddress: cfg.ValkeyConfig.Addrs,
		Password:    cfg.ValkeyConfig.Password,
		SelectDB:    platform.RegistryValkeyDB,
	}
	if cfg.ValkeyConfig.MasterName != "" {
		opt.Sentinel = valkey.SentinelOption{
			MasterSet: cfg.ValkeyConfig.MasterName,
		}
	}
	if cfg.ValkeyConfig.TLSEnabled {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfg.ValkeyConfig.TLSInsecure, //nolint:gosec // Controlled by ValkeyConfig.TLSInsecure config for dev/testing environments
			MinVersion:         tls.VersionTLS12,
		}
		if cfg.ValkeyConfig.TLSCAPath != "" {
			caCert, err := os.ReadFile(cfg.ValkeyConfig.TLSCAPath)
			if err != nil {
				return nil, fmt.Errorf("provisioning valkey: read CA cert: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("provisioning valkey: parse CA cert from %s", cfg.ValkeyConfig.TLSCAPath)
			}
			tlsCfg.RootCAs = pool
		}
		opt.TLSConfig = tlsCfg
	}
	client, err := valkey.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("provisioning valkey client: %w", err)
	}
	return client, nil
}

// registryReader wraps a Valkey client for reading the connections registry.
// Unexported — all construction is through NewConnectionsHandler.
type registryReader struct {
	client valkey.Client
	env    string
	logger zerolog.Logger
	lazyWg sync.WaitGroup // owns lazy SREM goroutines; independent from the caller's WaitGroup
}

func newRegistryReader(client valkey.Client, env string, logger zerolog.Logger, _ *sync.WaitGroup) *registryReader {
	return &registryReader{client: client, env: env, logger: logger}
}

// listTenantConnections returns all connections for a tenant, applying optional filters.
// Supports pagination via limit/offset applied after filtering.
func (r *registryReader) listTenantConnections(ctx context.Context, tenantID string, filters connectionFilters) ([]ConnectionDetail, int, error) {
	var idxKey string
	if filters.APIKeyID != "" {
		idxKey = registry.APIKeyIdxKey(r.env, tenantID, filters.APIKeyID)
	} else {
		idxKey = registry.TenantIdxKey(r.env, tenantID)
	}

	// SMEMBERS — load all connection IDs in this index set.
	members, err := r.client.Do(ctx, r.client.B().Smembers().Key(idxKey).Build()).AsStrSlice()
	if err != nil {
		return nil, 0, fmt.Errorf("SMEMBERS %s: %w", idxKey, err)
	}
	if len(members) == 0 {
		return []ConnectionDetail{}, 0, nil
	}

	// Pipeline HGETALL per connection ID.
	cmds := make([]valkey.Completed, 0, len(members))
	for _, connID := range members {
		cmds = append(cmds, r.client.B().Hgetall().Key(registry.ConnKey(r.env, connID)).Build())
	}
	results := r.client.DoMulti(ctx, cmds...)

	// Collect health keys to look up (one per unique pod).
	healthLookups := make(map[string]bool)
	type connWithID struct {
		connID string
		fields map[string]string
	}
	var valid []connWithID
	var staleIDs []string

	for i, res := range results {
		fields, err := res.AsStrMap()
		if err != nil || len(fields) == 0 {
			// Hash expired — stale index entry; lazy SREM below.
			staleIDs = append(staleIDs, members[i])
			continue
		}
		valid = append(valid, connWithID{connID: members[i], fields: fields})
		if podID := fields[registry.FieldPodID]; podID != "" {
			healthLookups[podID] = true
		}
	}

	// Lazy SREM stale entries (best-effort, non-blocking on error).
	// Uses r.lazyWg (not the server's main WaitGroup) to prevent wg.Go-after-Wait panics
	// if a re-query goroutine calls listTenantConnections near shutdown.
	if len(staleIDs) > 0 {
		r.lazyWg.Go(func() { //nolint:contextcheck // goroutine outlives the request ctx; uses context.Background() intentionally for cleanup
			defer logging.RecoverPanic(r.logger, "lazy_srem", nil)
			sremCtx, cancel := context.WithTimeout(context.Background(), lazySREMTimeout)
			defer cancel()
			args := make([]string, 0, len(staleIDs))
			args = append(args, staleIDs...)
			if err := r.client.Do(sremCtx, r.client.B().Srem().Key(idxKey).Member(args...).Build()).Error(); err != nil {
				r.logger.Warn().Err(err).Str("idx_key", idxKey).Msg("connections reader: lazy SREM failed")
			}
		})
	}

	// Fetch health keys for all pods found.
	healthMap := r.fetchHealthKeys(ctx, healthLookups)

	// Build and filter ConnectionDetail list.
	var matched []ConnectionDetail
	for _, cv := range valid {
		detail := r.fieldsToDetail(cv.connID, cv.fields, healthMap)
		// Apply filters.
		if filters.Transport != "" && detail.Transport != filters.Transport {
			continue
		}
		if filters.Channel != "" {
			found := slices.Contains(detail.Channels, filters.Channel)
			if !found {
				continue
			}
		}
		matched = append(matched, detail)
	}

	total := len(matched)
	// Pagination.
	if filters.Offset > total {
		return []ConnectionDetail{}, total, nil
	}
	end := min(filters.Offset+filters.Limit, total)
	return matched[filters.Offset:end], total, nil
}

// getConnection returns a single connection by ID, verifying tenant ownership.
// Returns nil, nil when the connection is not found or tenant doesn't match.
func (r *registryReader) getConnection(ctx context.Context, connID, tenantID string) (*ConnectionDetail, error) {
	fields, err := r.client.Do(ctx, r.client.B().Hgetall().Key(registry.ConnKey(r.env, connID)).Build()).AsStrMap()
	if err != nil {
		return nil, fmt.Errorf("HGETALL %s: %w", registry.ConnKey(r.env, connID), err)
	}
	if len(fields) == 0 {
		return nil, nil // not found
	}
	if fields[registry.FieldTenantID] != tenantID {
		return nil, nil // tenant mismatch — not found for this tenant
	}
	podID := fields[registry.FieldPodID]
	var healthMap map[string]map[string]string
	if podID != "" {
		healthMap = r.fetchHealthKeys(ctx, map[string]bool{podID: true})
	}
	if healthMap == nil {
		healthMap = map[string]map[string]string{}
	}
	detail := r.fieldsToDetail(connID, fields, healthMap)
	return &detail, nil
}

// publishDisconnect publishes an admin disconnect message to the pod's admin channel.
// Returns the number of Valkey subscribers (non-zero = at least one ws-server pod received it).
func (r *registryReader) publishDisconnect(ctx context.Context, podID, connID, tenantID, reason string) (int64, error) {
	msg := registry.AdminMsg{
		Type:         registry.AdminMsgTypeDisconnect,
		ConnectionID: connID,
		TenantID:     tenantID,
		Reason:       reason,
	}
	payload, _ := json.Marshal(msg) // AdminMsg contains only string fields; marshal cannot fail
	channelName := registry.AdminChannelName(r.env, podID)
	count, err := r.client.Do(ctx, r.client.B().Publish().Channel(channelName).Message(string(payload)).Build()).AsInt64()
	if err != nil {
		return 0, fmt.Errorf("PUBLISH %s: %w", channelName, err)
	}
	return count, nil
}

// fetchHealthKeys loads health metadata for a set of pod IDs. Returns a map of podID → fields.
// Individual HGETALL errors (e.g., key type mismatch) are silently skipped — the pod entry
// is absent from the result and callers treat the pod as possibly stale.
func (r *registryReader) fetchHealthKeys(ctx context.Context, pods map[string]bool) map[string]map[string]string {
	if len(pods) == 0 {
		return map[string]map[string]string{}
	}
	podIDs := make([]string, 0, len(pods))
	for id := range pods {
		podIDs = append(podIDs, id)
	}
	cmds := make([]valkey.Completed, 0, len(podIDs))
	for _, id := range podIDs {
		cmds = append(cmds, r.client.B().Hgetall().Key(registry.HealthKey(r.env, id)).Build())
	}
	results := r.client.DoMulti(ctx, cmds...)
	out := make(map[string]map[string]string, len(podIDs))
	for i, res := range results {
		fields, err := res.AsStrMap()
		if err != nil {
			continue
		}
		out[podIDs[i]] = fields
	}
	return out
}

// fieldsToDetail converts raw Valkey hash fields to a ConnectionDetail.
func (r *registryReader) fieldsToDetail(connID string, fields map[string]string, healthMap map[string]map[string]string) ConnectionDetail {
	var channels []string
	if raw := fields[registry.FieldChannels]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &channels); err != nil {
			r.logger.Warn().Err(err).Str("conn_id", connID).Msg("connections reader: malformed channels JSON, returning empty")
		}
	}
	if channels == nil {
		channels = []string{}
	}

	podID := fields[registry.FieldPodID]
	status := r.deriveStatus(podID, healthMap)

	return ConnectionDetail{
		ConnectionID:   connID,
		TenantID:       fields[registry.FieldTenantID],
		APIKeyID:       fields[registry.FieldAPIKeyID],
		UserID:         fields[registry.FieldUserID],
		PodID:          podID,
		ShardID:        fields[registry.FieldShardID],
		RemoteIP:       fields[registry.FieldRemoteIP],
		Transport:      fields[registry.FieldTransport],
		ConnectedAt:    fields[registry.FieldConnectedAt],
		Channels:       channels,
		ChannelsCapped: fields[registry.FieldChannelsCapped] == "1",
		RegistryStatus: status,
	}
}

// deriveStatus determines per-item registry_status from the pod's health data.
// Possibly stale when: drops > 0, last_heartbeat stale, health key absent, admin channel unhealthy.
func (r *registryReader) deriveStatus(podID string, healthMap map[string]map[string]string) string {
	hf, ok := healthMap[podID]
	if !ok || len(hf) == 0 {
		return registry.RegistryStatusPossiblyStale
	}
	// Check drop counter.
	if drops, _ := strconv.ParseInt(hf[registry.HealthFieldDrops], 10, 64); drops > 0 {
		return registry.RegistryStatusPossiblyStale
	}
	// Check last heartbeat timestamp.
	if ts := hf[registry.HealthFieldLastHeartbeat]; ts != "" {
		if t, err := time.Parse(time.RFC3339, ts); err == nil {
			staleThreshold := time.Duration(platform.RegistryStalenessDivisor) * platform.DefaultConnectionsRegistryHeartbeatInterval
			if time.Since(t) > staleThreshold {
				return registry.RegistryStatusPossiblyStale
			}
		}
	} else {
		return registry.RegistryStatusPossiblyStale
	}
	// Check admin channel health.
	if hf[registry.HealthFieldAdminChannelHealthy] != registry.HealthValueTrue {
		return registry.RegistryStatusPossiblyStale
	}
	return registry.RegistryStatusCurrent
}

// IsAdminHealthy returns true if the pod's admin channel is healthy according to health data.
func (r *registryReader) isAdminHealthy(ctx context.Context, podID string) bool {
	hf, err := r.client.Do(ctx, r.client.B().Hgetall().Key(registry.HealthKey(r.env, podID)).Build()).AsStrMap()
	if err != nil || len(hf) == 0 {
		return false
	}
	return hf[registry.HealthFieldAdminChannelHealthy] == registry.HealthValueTrue
}
