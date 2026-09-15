package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
)

// Jitter constants for exponential backoff reconnection.
const (
	pushJitterBase  = 0.75 // Minimum jitter multiplier (75% of delay)
	pushJitterRange = 0.25 // Additional random jitter range (up to 25%)
)

// ConfigClientConfig configures the push config client.
type ConfigClientConfig struct {
	ProvisioningAddr  string
	Namespace         string
	ReconnectDelay    time.Duration
	ReconnectMaxDelay time.Duration
	Logger            zerolog.Logger
}

// ConfigClient connects to provisioning via gRPC and caches push config + topic data.
// It subscribes to two streams:
//  1. WatchPushConfig — push patterns + credentials per tenant
//  2. WatchTopics — Kafka topic names per tenant (via shared StreamTopicRegistry)
//
// Both are merged into a single config cache that satisfies the ConfigCache interface
// for both the push Service and the gRPC server.
//
// Thread Safety: All public methods are safe for concurrent use. Push config
// reads use atomic.Value for lock-free access on the hot path.
type ConfigClient struct {
	provConn   *grpc.ClientConn
	provClient provisioningv1.ProvisioningInternalServiceClient

	// topicRegistry handles WatchTopics — reuses the shared StreamTopicRegistry
	// which starts its own stream goroutine in the constructor.
	topicRegistry *provapi.StreamTopicRegistry

	// pushConfig stores a *pushConfigSnapshot for lock-free reads.
	pushConfig atomic.Value

	logger zerolog.Logger
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	// Config
	reconnectDelay    time.Duration
	reconnectMaxDelay time.Duration

	// Prometheus metrics
	streamStateGauge  prometheus.Gauge
	reconnectsCounter prometheus.Counter
}

// pushConfigSnapshot is the immutable snapshot stored in atomic.Value.
// Each update replaces the entire snapshot (copy-on-write).
type pushConfigSnapshot struct {
	// patterns maps tenantID -> channel patterns for push notifications.
	patterns map[string][]string

	// credentials maps tenantID -> provider -> credential JSON.
	credentials map[string]map[string]json.RawMessage

	// pushEnabledTenants is the set of tenant IDs with push config.
	// Used to filter topic results to only push-enabled tenants.
	pushEnabledTenants map[string]struct{}
}

// emptySnapshot returns an initialized empty snapshot.
func emptySnapshot() *pushConfigSnapshot {
	return &pushConfigSnapshot{
		patterns:           make(map[string][]string),
		credentials:        make(map[string]map[string]json.RawMessage),
		pushEnabledTenants: make(map[string]struct{}),
	}
}

// NewConfigClient creates a new push config client that subscribes to
// WatchPushConfig and WatchTopics from the provisioning service.
// The WatchTopics stream (via StreamTopicRegistry) starts immediately in the
// constructor. Call Start to begin the WatchPushConfig stream.
func NewConfigClient(cfg ConfigClientConfig) (*ConfigClient, error) {
	if cfg.ProvisioningAddr == "" {
		return nil, errors.New("config client: ProvisioningAddr is required")
	}
	if cfg.Namespace == "" {
		return nil, errors.New("config client: Namespace is required")
	}
	if cfg.ReconnectDelay <= 0 {
		return nil, errors.New("config client: ReconnectDelay must be > 0")
	}
	if cfg.ReconnectMaxDelay < cfg.ReconnectDelay {
		return nil, errors.New("config client: ReconnectMaxDelay must be >= ReconnectDelay")
	}

	conn, err := grpc.NewClient(cfg.ProvisioningAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("config client: dial provisioning gRPC: %w", err)
	}

	// Create StreamTopicRegistry for WatchTopics (starts its own stream goroutine).
	topicRegistry, err := provapi.NewStreamTopicRegistry(provapi.StreamTopicRegistryConfig{
		GRPCAddr:          cfg.ProvisioningAddr,
		Namespace:         cfg.Namespace,
		ReconnectDelay:    cfg.ReconnectDelay,
		ReconnectMaxDelay: cfg.ReconnectMaxDelay,
		MetricPrefix:      "push",
		Logger:            cfg.Logger,
	})
	if err != nil {
		// Close the connection we opened since we're failing.
		_ = conn.Close() // best-effort cleanup
		return nil, fmt.Errorf("config client: create topic registry: %w", err)
	}

	c := &ConfigClient{
		provConn:          conn,
		provClient:        provisioningv1.NewProvisioningInternalServiceClient(conn),
		topicRegistry:     topicRegistry,
		logger:            cfg.Logger.With().Str("component", "push_config_client").Logger(),
		reconnectDelay:    cfg.ReconnectDelay,
		reconnectMaxDelay: cfg.ReconnectMaxDelay,
		streamStateGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "push_config_stream_state",
			Help: "State of the push config gRPC stream (0=disconnected, 1=connected)",
		}),
		reconnectsCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: "push_config_stream_reconnects_total",
			Help: "Total reconnection attempts for the push config stream",
		}),
	}

	// Initialize with empty snapshot so reads never get nil.
	c.pushConfig.Store(emptySnapshot())

	return c, nil
}

// Start launches the WatchPushConfig stream goroutine. The WatchTopics stream
// is already running (started by StreamTopicRegistry constructor).
func (c *ConfigClient) Start(ctx context.Context) {
	c.ctx, c.cancel = context.WithCancel(ctx)

	c.wg.Go(func() {
		defer logging.RecoverPanic(c.logger, "push_config_stream", nil)
		c.pushConfigStreamLoop(c.ctx)
	})
}

// Stop cancels all goroutines, waits for them to exit, and closes connections.
// Shutdown order: cancel context -> wait goroutines -> close topic registry -> close connection.
func (c *ConfigClient) Stop() error {
	if c.cancel != nil {
		c.cancel()
	}

	// Wait for WatchPushConfig goroutine to exit.
	c.wg.Wait()

	// Close topic registry (stops WatchTopics stream + its connection).
	var errs []error
	if err := c.topicRegistry.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close topic registry: %w", err))
	}

	// Close our gRPC connection (used by WatchPushConfig).
	if err := c.provConn.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close provisioning gRPC connection: %w", err))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// TopicRegistry returns the underlying StreamTopicRegistry for use by callers
// that need direct access (e.g., wiring OnUpdate callbacks).
func (c *ConfigClient) TopicRegistry() *provapi.StreamTopicRegistry {
	return c.topicRegistry
}

// ProvisioningClient returns the gRPC provisioning client for callers that need
// direct access (e.g., StorePushCredentials in the gRPC server).
func (c *ConfigClient) ProvisioningClient() provisioningv1.ProvisioningInternalServiceClient {
	return c.provClient
}

// --------------------------------------------------------------------------
// ConfigCache interface implementation
// --------------------------------------------------------------------------

// GetPushPatterns returns the channel patterns for push notifications for a tenant.
// Returns nil if the tenant has no push patterns configured.
func (c *ConfigClient) GetPushPatterns(tenantID string) []string {
	val := c.pushConfig.Load()
	if val == nil {
		return nil
	}
	snap, ok := val.(*pushConfigSnapshot)
	if !ok {
		return nil
	}
	patterns := snap.patterns[tenantID]
	if len(patterns) == 0 {
		return nil
	}

	// Return a copy to avoid data races if the caller mutates the slice.
	result := make([]string, len(patterns))
	copy(result, patterns)
	return result
}

// GetTopics returns all Kafka topics for push-enabled tenants by merging
// shared and dedicated topics from the StreamTopicRegistry, filtered to
// only tenants that have push config.
func (c *ConfigClient) GetTopics() []string {
	val := c.pushConfig.Load()
	if val == nil {
		return nil
	}
	snap, ok := val.(*pushConfigSnapshot)
	if !ok {
		return nil
	}

	// If no tenants are push-enabled, return nil early.
	if len(snap.pushEnabledTenants) == 0 {
		return nil
	}

	// Collect all topics from the topic registry.
	// Shared topics apply to all tenants, so include them if any tenant is push-enabled.
	// Errors are logged but not propagated — serve stale/empty topics rather than
	// crashing the consumer refresh loop. The registry auto-recovers on next stream update.
	sharedTopics, err := c.topicRegistry.GetSharedTenantTopics(context.Background(), "")
	if err != nil {
		c.logger.Warn().Err(err).Msg("failed to get shared topics from registry")
	}
	dedicatedTenants, err := c.topicRegistry.GetDedicatedTenants(context.Background(), "")
	if err != nil {
		c.logger.Warn().Err(err).Msg("failed to get dedicated tenants from registry")
	}

	// Use a map for deduplication.
	topicSet := make(map[string]struct{}, len(sharedTopics))
	for _, t := range sharedTopics {
		topicSet[t] = struct{}{}
	}

	// Include dedicated topics only for push-enabled tenants.
	for _, dt := range dedicatedTenants {
		if _, ok := snap.pushEnabledTenants[dt.TenantID]; ok {
			for _, t := range dt.Topics {
				topicSet[t] = struct{}{}
			}
		}
	}

	if len(topicSet) == 0 {
		return nil
	}

	topics := make([]string, 0, len(topicSet))
	for t := range topicSet {
		topics = append(topics, t)
	}
	return topics
}

// GetCredential returns provider credentials for a tenant and provider type.
// Returns (nil, nil) when no credential exists for the tenant/provider pair.
func (c *ConfigClient) GetCredential(tenantID, provider string) (json.RawMessage, error) {
	val := c.pushConfig.Load()
	if val == nil {
		return nil, nil
	}
	snap, ok := val.(*pushConfigSnapshot)
	if !ok {
		return nil, nil
	}

	providerCreds, ok := snap.credentials[tenantID]
	if !ok {
		return nil, nil
	}

	cred, ok := providerCreds[provider]
	if !ok {
		return nil, nil
	}

	// Return a copy to prevent mutation of cached data.
	result := make(json.RawMessage, len(cred))
	copy(result, cred)
	return result, nil
}

// --------------------------------------------------------------------------
// WatchPushConfig stream loop
// --------------------------------------------------------------------------

// pushConfigStreamLoop runs the WatchPushConfig gRPC stream with reconnection.
func (c *ConfigClient) pushConfigStreamLoop(ctx context.Context) {
	delay := c.reconnectDelay

	for {
		select {
		case <-ctx.Done():
			c.streamStateGauge.Set(provapi.StreamStateDisconnected)
			return
		default:
		}

		stream, err := c.provClient.WatchPushConfig(ctx, &provisioningv1.WatchPushConfigRequest{})
		if err != nil {
			c.logger.Warn().Err(err).Dur("retry_in", delay).Msg("failed to start WatchPushConfig stream")
			c.streamStateGauge.Set(provapi.StreamStateDisconnected)

			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}

			delay = pushConfigBackoff(delay, c.reconnectMaxDelay)
			c.reconnectsCounter.Inc()
			continue
		}

		c.streamStateGauge.Set(provapi.StreamStateConnected)
		delay = c.reconnectDelay

		c.logger.Info().Msg("WatchPushConfig stream connected")

		for {
			resp, err := stream.Recv()
			if err != nil {
				c.logger.Warn().Err(err).Msg("WatchPushConfig stream disconnected")
				c.streamStateGauge.Set(provapi.StreamStateDisconnected)
				break
			}

			c.updatePushConfig(resp)
		}
	}
}

// updatePushConfig processes a WatchPushConfigResponse and updates the cached snapshot.
func (c *ConfigClient) updatePushConfig(resp *provisioningv1.WatchPushConfigResponse) {
	if resp.GetIsSnapshot() {
		c.applySnapshot(resp)
	} else {
		c.applyDelta(resp)
	}

	c.logger.Debug().
		Bool("snapshot", resp.GetIsSnapshot()).
		Int("credentials", len(resp.GetPushCredentials())).
		Int("channel_configs", len(resp.GetPushChannelConfigs())).
		Int("removed_credentials", len(resp.GetRemovedCredentialIds())).
		Int("removed_tenants", len(resp.GetRemovedConfigTenantSlugs())).
		Msg("push config cache updated")
}

// applySnapshot replaces the entire push config cache with a full snapshot.
func (c *ConfigClient) applySnapshot(resp *provisioningv1.WatchPushConfigResponse) {
	snap := emptySnapshot()

	for _, cred := range resp.GetPushCredentials() {
		tid := cred.GetTenantSlug()
		if _, ok := snap.credentials[tid]; !ok {
			snap.credentials[tid] = make(map[string]json.RawMessage)
		}
		snap.credentials[tid][cred.GetProvider()] = json.RawMessage(cred.GetCredentialData())
		snap.pushEnabledTenants[tid] = struct{}{}
	}

	for _, cfg := range resp.GetPushChannelConfigs() {
		tid := cfg.GetTenantSlug()
		patterns := make([]string, len(cfg.GetPatterns()))
		copy(patterns, cfg.GetPatterns())
		snap.patterns[tid] = patterns
		snap.pushEnabledTenants[tid] = struct{}{}
		// LOG-009: Per-tenant pattern update (snapshot)
		c.logger.Debug().Str(logging.LogKeyTenantSlug, tid).Int("pattern_count", len(patterns)).
			Str("action", "snapshot").Msg("push config updated for tenant")
	}

	c.pushConfig.Store(snap)
}

// applyDelta merges a delta update into the current push config snapshot.
// Creates a new snapshot (copy-on-write) to avoid races with concurrent readers.
func (c *ConfigClient) applyDelta(resp *provisioningv1.WatchPushConfigResponse) {
	val := c.pushConfig.Load()
	if val == nil {
		// No prior snapshot — treat delta as a snapshot.
		c.applySnapshot(resp)
		return
	}
	old, ok := val.(*pushConfigSnapshot)
	if !ok {
		c.applySnapshot(resp)
		return
	}

	// Deep copy the old snapshot.
	snap := &pushConfigSnapshot{
		patterns:           make(map[string][]string, len(old.patterns)),
		credentials:        make(map[string]map[string]json.RawMessage, len(old.credentials)),
		pushEnabledTenants: make(map[string]struct{}, len(old.pushEnabledTenants)),
	}

	for tid, pats := range old.patterns {
		cp := make([]string, len(pats))
		copy(cp, pats)
		snap.patterns[tid] = cp
	}
	for tid, provCreds := range old.credentials {
		cpMap := make(map[string]json.RawMessage, len(provCreds))
		for prov, data := range provCreds {
			cpData := make(json.RawMessage, len(data))
			copy(cpData, data)
			cpMap[prov] = cpData
		}
		snap.credentials[tid] = cpMap
	}
	for tid := range old.pushEnabledTenants {
		snap.pushEnabledTenants[tid] = struct{}{}
	}

	// Apply removals first.
	for _, tid := range resp.GetRemovedConfigTenantSlugs() {
		delete(snap.patterns, tid)
		delete(snap.credentials, tid)
		delete(snap.pushEnabledTenants, tid)
		// LOG-009: Per-tenant config removed (delta)
		c.logger.Debug().Str(logging.LogKeyTenantSlug, tid).Int("pattern_count", 0).
			Str("action", "removed").Msg("push config updated for tenant")
	}

	// RemovedCredentialIds are formatted as "tenantID:provider".
	// Parse and remove individual credentials.
	for _, id := range resp.GetRemovedCredentialIds() {
		tid, provider := parseCredentialID(id)
		if tid == "" {
			continue
		}
		if provCreds, ok := snap.credentials[tid]; ok {
			delete(provCreds, provider)
			if len(provCreds) == 0 {
				delete(snap.credentials, tid)
			}
		}
	}

	// Apply upserts.
	for _, cred := range resp.GetPushCredentials() {
		tid := cred.GetTenantSlug()
		if _, ok := snap.credentials[tid]; !ok {
			snap.credentials[tid] = make(map[string]json.RawMessage)
		}
		snap.credentials[tid][cred.GetProvider()] = json.RawMessage(cred.GetCredentialData())
		snap.pushEnabledTenants[tid] = struct{}{}
	}

	for _, cfg := range resp.GetPushChannelConfigs() {
		tid := cfg.GetTenantSlug()
		patterns := make([]string, len(cfg.GetPatterns()))
		copy(patterns, cfg.GetPatterns())
		snap.patterns[tid] = patterns
		snap.pushEnabledTenants[tid] = struct{}{}
		// LOG-009: Per-tenant pattern update (delta)
		c.logger.Debug().Str(logging.LogKeyTenantSlug, tid).Int("pattern_count", len(patterns)).
			Str("action", "updated").Msg("push config updated for tenant")
	}

	c.pushConfig.Store(snap)
}

// parseCredentialID splits a credential ID "tenantID:provider" into its parts.
// Returns ("", "") if the format is invalid.
func parseCredentialID(id string) (tenantID, provider string) {
	for i := range id {
		if id[i] == ':' {
			return id[:i], id[i+1:]
		}
	}
	return "", ""
}

// pushConfigBackoff calculates the next backoff delay with jitter, capped at maxDelay.
func pushConfigBackoff(current, maxDelay time.Duration) time.Duration {
	next := min(time.Duration(float64(current)*2), maxDelay)
	// Add jitter: 75%-100% of calculated delay
	jitter := pushJitterBase + rand.Float64()*pushJitterRange //nolint:gosec // G404: jitter for backoff delay does not need cryptographic randomness
	return time.Duration(math.Round(float64(next) * jitter))
}

// Compile-time interface satisfaction checks.
var (
	_ ConfigCache = (*ConfigClient)(nil)
)
