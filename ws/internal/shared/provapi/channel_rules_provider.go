package provapi

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/routing"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// 20s ping + 10s timeout = 30s worst-case detection, matching the channel-rules
// propagation budget (rule changes must reach clients within 30 seconds).
// Using 30s would allow 40s detection, violating that bound.
const (
	provAPIKeepaliveTime    = 20 * time.Second
	provAPIKeepaliveTimeout = 10 * time.Second
)

// TenantRoutingSnapshot is a point-in-time snapshot of a tenant's routing rules.
type TenantRoutingSnapshot struct {
	Rules []types.RoutingRule
}

// StreamChannelRulesProviderConfig configures the gRPC stream-backed channel rules provider.
type StreamChannelRulesProviderConfig struct {
	GRPCAddr          string
	ReconnectDelay    time.Duration
	ReconnectMaxDelay time.Duration
	MetricPrefix      string // "gateway" or "ws"
	Logger            zerolog.Logger
}

// StreamChannelRulesProvider provides per-tenant channel rules backed by a gRPC
// streaming connection to the provisioning service. Caches rules in memory.
type StreamChannelRulesProvider struct {
	mu           sync.RWMutex
	channelRules map[string]*types.ChannelRules // tenantID → rules

	// tenantUUIDBySlug maps a tenant slug (current, and previous slug during a
	// rename hold window) to the stable tenant UUID. Backs ResolveTenantUUID for
	// JWT tenant binding. Guarded by mu; rebuilt on each snapshot.
	tenantUUIDBySlug map[string]string

	// slugByUUID is the reverse of tenantUUIDBySlug, built from the CURRENT slug
	// only (never previous-slug aliases) so a UUID always resolves to its live
	// slug. Backs ResolveTenantSlug for API-key auth, which carries only the
	// tenant UUID but needs the slug for data-plane channel scoping. Guarded by mu.
	slugByUUID map[string]string

	snapshotMu       sync.Mutex   // guards the routing-snapshot COW read-modify-write; today watchLoop is the sole writer
	routingSnapshots atomic.Value // holds map[string]TenantRoutingSnapshot

	conn   *grpc.ClientConn
	config StreamChannelRulesProviderConfig
	logger zerolog.Logger

	streamState atomic.Int32
	reconnects  atomic.Int64

	// snapshotReceived flips true after the first snapshot has been successfully
	// applied to the caches. Readiness gates on it: a stream that is TCP-connected
	// but has not yet delivered its initial snapshot cannot answer authorization
	// queries, and the gateway must not report ready until it can.
	snapshotReceived atomic.Bool

	// tenantUUIDsPresent reports whether the stream is delivering tenant UUIDs
	// (DS-001). It is false when a non-empty snapshot arrives with no tenant_uuid
	// set — i.e. the provisioning peer predates the field during a rolling deploy
	// — so readiness can stay degraded rather than serving a fail-closed binding
	// that rejects all traffic. Cleared true on any batch carrying a UUID.
	tenantUUIDsPresent atomic.Bool

	cancel context.CancelFunc
	wg     sync.WaitGroup

	streamStateGauge       prometheus.Gauge
	reconnectsCounter      prometheus.Counter
	invalidPatternsCounter prometheus.Counter
}

// NewStreamChannelRulesProvider creates a new gRPC stream-backed channel rules provider.
func NewStreamChannelRulesProvider(cfg StreamChannelRulesProviderConfig) (*StreamChannelRulesProvider, error) {
	if cfg.GRPCAddr == "" {
		return nil, errors.New("stream channel rules provider: GRPCAddr is required")
	}
	if cfg.ReconnectDelay <= 0 {
		return nil, errors.New("stream channel rules provider: ReconnectDelay must be > 0")
	}
	if cfg.ReconnectMaxDelay < cfg.ReconnectDelay {
		return nil, errors.New("stream channel rules provider: ReconnectMaxDelay must be >= ReconnectDelay")
	}
	if cfg.MetricPrefix == "" {
		return nil, errors.New("stream channel rules provider: MetricPrefix is required")
	}

	conn, err := grpc.NewClient(cfg.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                provAPIKeepaliveTime,
			Timeout:             provAPIKeepaliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial provisioning gRPC: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &StreamChannelRulesProvider{
		channelRules:     make(map[string]*types.ChannelRules),
		tenantUUIDBySlug: make(map[string]string),
		slugByUUID:       make(map[string]string),
		conn:             conn,
		config:           cfg,
		logger:           cfg.Logger.With().Str("component", "stream_channel_rules_provider").Logger(),
		cancel:           cancel,
		streamStateGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: cfg.MetricPrefix + "_provisioning_config_stream_state",
			Help: "State of the provisioning config gRPC stream (0=disconnected, 1=connected)",
		}),
		reconnectsCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: cfg.MetricPrefix + "_provisioning_config_stream_reconnects_total",
			Help: "Total reconnection attempts for the provisioning config stream",
		}),
		invalidPatternsCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: cfg.MetricPrefix + "_" + routing.MetricInvalidPatternBase,
			Help: "Total routing patterns skipped due to invalid syntax",
		}),
	}
	r.routingSnapshots.Store(make(map[string]TenantRoutingSnapshot))

	r.wg.Go(func() {
		defer logging.RecoverPanic(r.logger, "channel_rules_stream", nil)
		r.streamLoop(ctx)
	})

	return r, nil
}

// GetChannelRules returns the channel rules for a tenant.
func (r *StreamChannelRulesProvider) GetChannelRules(_ context.Context, tenantID string) (*types.ChannelRules, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rules, ok := r.channelRules[tenantID]
	if !ok {
		return nil, types.ErrChannelRulesNotFound
	}
	return rules, nil
}

// GetRoutingSnapshot returns the routing snapshot for a tenant, or false if not found.
func (r *StreamChannelRulesProvider) GetRoutingSnapshot(tenantID string) (TenantRoutingSnapshot, bool) {
	m, _ := r.routingSnapshots.Load().(map[string]TenantRoutingSnapshot)
	snap, ok := m[tenantID]
	return snap, ok
}

// State returns the current stream state (0=disconnected, 1=connected).
func (r *StreamChannelRulesProvider) State() int32 {
	return r.streamState.Load()
}

// SnapshotReceived reports whether the initial rules snapshot has been
// successfully applied. False means rules are unknown (not "empty"): callers
// MUST fail closed and report degraded/not-ready rather than treating tenants
// as having no rules configured.
func (r *StreamChannelRulesProvider) SnapshotReceived() bool {
	return r.snapshotReceived.Load()
}

// TenantUUIDsPresent reports whether the tenant-config stream is delivering
// tenant UUIDs (DS-001). False during rolling-deploy skew against a provisioning
// peer that predates the tenant_uuid field; readiness stays degraded until true.
func (r *StreamChannelRulesProvider) TenantUUIDsPresent() bool {
	return r.tenantUUIDsPresent.Load()
}

// ResolveTenantUUID maps a tenant slug (current or previous-within-hold) to its
// stable tenant UUID, satisfying auth.TenantResolver for the gateway's JWT tenant
// binding. Returns auth.ErrTenantNotResolvable when the slug is unknown.
func (r *StreamChannelRulesProvider) ResolveTenantUUID(_ context.Context, slug string) (string, error) {
	r.mu.RLock()
	uuid, ok := r.tenantUUIDBySlug[slug]
	r.mu.RUnlock()
	if !ok || uuid == "" {
		return "", auth.ErrTenantNotResolvable
	}
	return uuid, nil
}

// ResolveTenantSlug maps a stable tenant UUID to its current slug. It backs
// API-key auth, which carries only the tenant UUID (API keys have no slug) but
// needs the slug for data-plane channel scoping. Returns the CURRENT slug — never
// a previous-slug alias — or auth.ErrTenantNotResolvable when the UUID is unknown.
func (r *StreamChannelRulesProvider) ResolveTenantSlug(_ context.Context, uuid string) (string, error) {
	r.mu.RLock()
	slug, ok := r.slugByUUID[uuid]
	r.mu.RUnlock()
	if !ok || slug == "" {
		return "", auth.ErrTenantNotResolvable
	}
	return slug, nil
}

// Close stops the stream and releases resources.
func (r *StreamChannelRulesProvider) Close() error {
	r.cancel()
	r.wg.Wait()
	if err := r.conn.Close(); err != nil {
		return fmt.Errorf("close channel rules provider gRPC connection: %w", err)
	}
	return nil
}

// streamLoop runs the gRPC stream with reconnection logic.
func (r *StreamChannelRulesProvider) streamLoop(ctx context.Context) {
	client := provisioningv1.NewProvisioningInternalServiceClient(r.conn)
	delay := r.config.ReconnectDelay

	for {
		select {
		case <-ctx.Done():
			r.streamState.Store(StreamStateDisconnected)
			r.streamStateGauge.Set(StreamStateDisconnected)
			return
		default:
		}

		stream, err := client.WatchTenantConfig(ctx, &provisioningv1.WatchTenantConfigRequest{})
		if err != nil {
			r.logger.Warn().Err(err).Dur("retry_in", delay).Msg("failed to start WatchTenantConfig stream")
			r.streamState.Store(StreamStateDisconnected)
			r.streamStateGauge.Set(StreamStateDisconnected)

			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}

			delay = backoff(delay, r.config.ReconnectMaxDelay)
			r.reconnects.Add(1)
			r.reconnectsCounter.Inc()
			continue
		}

		r.streamState.Store(StreamStateConnected)
		r.streamStateGauge.Set(StreamStateConnected)
		delay = r.config.ReconnectDelay

		r.logger.Info().Msg("WatchTenantConfig stream connected")

		for {
			resp, err := stream.Recv()
			if err != nil {
				r.logger.Warn().Err(err).Msg("WatchTenantConfig stream disconnected")
				r.streamState.Store(StreamStateDisconnected)
				r.streamStateGauge.Set(StreamStateDisconnected)
				break
			}

			r.updateTenantConfigs(resp)
		}
	}
}

// updateTenantConfigs processes a WatchTenantConfigResponse and updates caches.
func (r *StreamChannelRulesProvider) updateTenantConfigs(resp *provisioningv1.WatchTenantConfigResponse) {
	isSnapshot := resp.GetIsSnapshot()
	removedIDs := resp.GetRemovedTenantSlugs()
	tenants := resp.GetTenants()

	// Update channel rules + the slug->UUID resolver map under the mutex.
	r.mu.Lock()
	if isSnapshot {
		r.channelRules = make(map[string]*types.ChannelRules)
		r.tenantUUIDBySlug = make(map[string]string)
		r.slugByUUID = make(map[string]string)
	}
	for _, slug := range removedIDs {
		delete(r.channelRules, slug)
		// Prune the reverse map by UUID before dropping the forward entry — a
		// slug key cannot delete from a UUID-keyed map, and leaving a stale
		// uuid->slug would mis-scope a later API-key auth after tenant removal.
		if uuid, ok := r.tenantUUIDBySlug[slug]; ok {
			delete(r.slugByUUID, uuid)
		}
		delete(r.tenantUUIDBySlug, slug)
	}
	sawUUID := false
	for _, tc := range tenants {
		if tc.GetChannelRules() != nil {
			r.channelRules[tc.GetTenantSlug()] = protoToChannelRules(tc.GetChannelRules())
		}
		if uuid := tc.GetTenantUuid(); uuid != "" {
			sawUUID = true
			r.tenantUUIDBySlug[tc.GetTenantSlug()] = uuid
			// Reverse map uses the CURRENT slug only — a UUID must resolve to its
			// live slug, never a stale previous-slug alias.
			r.slugByUUID[uuid] = tc.GetTenantSlug()
			// Alias the previous slug during the rename hold window so old-slug
			// JWTs still resolve to this tenant's UUID (forward direction only).
			if prev := tc.GetPreviousSlug(); prev != "" {
				r.tenantUUIDBySlug[prev] = uuid
			}
		}
	}
	channelRulesLen := len(r.channelRules)
	r.mu.Unlock()

	// DS-001 readiness: a non-empty snapshot with no tenant_uuid means the
	// provisioning peer predates the field (rolling-deploy skew) — stay degraded
	// so the fail-closed binding doesn't reject all traffic. Any batch carrying a
	// UUID (snapshot or delta) marks UUIDs present (covers recovery via delta).
	// A zero-tenant snapshot is ready (nothing to resolve), never a permanent 503.
	switch {
	case sawUUID:
		r.tenantUUIDsPresent.Store(true)
	case isSnapshot && len(tenants) == 0:
		r.tenantUUIDsPresent.Store(true)
	case isSnapshot:
		r.tenantUUIDsPresent.Store(false)
	}

	// COW update for routing snapshots. snapshotMu makes the Load→build→Store sequence
	// atomic against any other writer; watchLoop is the only one today, so it is
	// uncontended — it is kept so a second writer cannot silently lose an update.
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	old, _ := r.routingSnapshots.Load().(map[string]TenantRoutingSnapshot)
	next := make(map[string]TenantRoutingSnapshot, len(old))
	if !isSnapshot {
		maps.Copy(next, old)
	}
	for _, tenantID := range removedIDs {
		delete(next, tenantID)
	}
	for _, tc := range tenants {
		protoRules := tc.GetRoutingRules()
		rules := make([]types.RoutingRule, 0, len(protoRules))
		for _, pr := range protoRules {
			pattern := routing.NormalizePattern(pr.GetPattern())
			if _, err := routing.MatchRoutingPattern(pattern, "x"); err != nil {
				r.invalidPatternsCounter.Inc()
				continue
			}
			if pr.GetIngressTopic() == "" {
				// A rule without a ingress topic cannot route (misconfigured, or
				// streamed by a pre-split provisioning peer during rolling deploy).
				// Skipping fails closed: publishes matching only this rule reject
				// with PUBLISH_NOT_ROUTABLE rather than routing implicitly (§XV).
				continue
			}
			rules = append(rules, types.RoutingRule{
				Pattern:      pattern,
				IngressTopic: pr.GetIngressTopic(),
				EgressTopics: pr.GetEgressTopics(),
				Priority:     int(pr.GetPriority()),
			})
		}
		next[tc.GetTenantSlug()] = TenantRoutingSnapshot{Rules: rules}
	}
	r.routingSnapshots.Store(next)

	// Flip only after the snapshot has been fully applied to both caches —
	// receipt alone must not mark the provider ready. Idempotent for
	// later snapshots (reconnects).
	if isSnapshot {
		r.snapshotReceived.Store(true)
	}

	r.logger.Debug().
		Bool("snapshot", isSnapshot).
		Int("channel_rules", channelRulesLen).
		Msg("tenant config cache updated")
}

// protoToChannelRules converts proto ChannelRules to types.ChannelRules.
func protoToChannelRules(cr *provisioningv1.ChannelRules) *types.ChannelRules {
	rules := &types.ChannelRules{
		Public:         cr.GetPublicChannels(),
		Default:        cr.GetDefaultChannels(),
		PublishPublic:  cr.GetPublishPublicChannels(),
		PublishDefault: cr.GetPublishDefaultChannels(),
	}

	if len(cr.GetGroupMappings()) > 0 {
		rules.GroupMappings = make(map[string][]string, len(cr.GetGroupMappings()))
		for group, gc := range cr.GetGroupMappings() {
			rules.GroupMappings[group] = gc.GetChannels()
		}
	}

	if len(cr.GetPublishGroupMappings()) > 0 {
		rules.PublishGroupMappings = make(map[string][]string, len(cr.GetPublishGroupMappings()))
		for group, gc := range cr.GetPublishGroupMappings() {
			rules.PublishGroupMappings[group] = gc.GetChannels()
		}
	}

	return rules
}
