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

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// StreamTopicRegistryConfig configures the gRPC stream-backed topic registry.
type StreamTopicRegistryConfig struct {
	GRPCAddr          string
	Namespace         string // Topic namespace (e.g., "prod")
	ReconnectDelay    time.Duration
	ReconnectMaxDelay time.Duration
	MetricPrefix      string // "gateway" or "ws"
	OnUpdate          func() // Callback when topics change (e.g., pool.RefreshTopics)
	Logger            zerolog.Logger
}

// StreamTopicRegistry implements types.TenantRegistry backed by a gRPC
// streaming connection to the provisioning service. It caches topic data
// and notifies via callback when topics change.
type StreamTopicRegistry struct {
	mu               sync.RWMutex
	sharedTopics     []string
	dedicatedTenants []types.TenantTopics
	createOnlyTopics []string          // full topic names created on the broker but never consumed (routing-rule egress, ADR-0018)
	topicTenants     map[string]string // topic name → owning tenant slug (shared + dedicated), #179 P3
	namespace        string
	onUpdate         func()

	snapshotReceived atomic.Bool // true once the initial full snapshot has been applied (#179 P3 readiness)

	conn   *grpc.ClientConn
	config StreamTopicRegistryConfig
	logger zerolog.Logger

	streamState atomic.Int32
	reconnects  atomic.Int64

	cancel context.CancelFunc
	wg     sync.WaitGroup

	streamStateGauge  prometheus.Gauge
	reconnectsCounter prometheus.Counter
}

// NewStreamTopicRegistry creates a new gRPC stream-backed topic registry.
func NewStreamTopicRegistry(cfg StreamTopicRegistryConfig) (*StreamTopicRegistry, error) {
	if cfg.GRPCAddr == "" {
		return nil, errors.New("stream topic registry: GRPCAddr is required")
	}
	if cfg.ReconnectDelay <= 0 {
		return nil, errors.New("stream topic registry: ReconnectDelay must be > 0")
	}
	if cfg.ReconnectMaxDelay < cfg.ReconnectDelay {
		return nil, errors.New("stream topic registry: ReconnectMaxDelay must be >= ReconnectDelay")
	}
	if cfg.Namespace == "" {
		return nil, errors.New("stream topic registry: Namespace is required")
	}
	if cfg.MetricPrefix == "" {
		return nil, errors.New("stream topic registry: MetricPrefix is required")
	}

	conn, err := grpc.NewClient(cfg.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial provisioning gRPC: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &StreamTopicRegistry{
		namespace: cfg.Namespace,
		onUpdate:  cfg.OnUpdate,
		conn:      conn,
		config:    cfg,
		logger:    cfg.Logger.With().Str("component", "stream_topic_registry").Logger(),
		cancel:    cancel,
		streamStateGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: cfg.MetricPrefix + "_provisioning_topics_stream_state",
			Help: "State of the provisioning topics gRPC stream (0=disconnected, 1=connected)",
		}),
		reconnectsCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: cfg.MetricPrefix + "_provisioning_topics_stream_reconnects_total",
			Help: "Total reconnection attempts for the provisioning topics stream",
		}),
	}

	r.wg.Go(func() {
		defer logging.RecoverPanic(r.logger, "topic_registry_stream", nil)
		r.streamLoop(ctx)
	})

	return r, nil
}

// GetSharedTenantTopics returns all topics for shared-mode tenants.
// The namespace parameter is ignored since it was provided at construction time
// via the gRPC stream request.
func (r *StreamTopicRegistry) GetSharedTenantTopics(_ context.Context, _ string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy to avoid data races
	result := make([]string, len(r.sharedTopics))
	copy(result, r.sharedTopics)
	return result, nil
}

// GetDedicatedTenants returns tenants that require dedicated consumers.
func (r *StreamTopicRegistry) GetDedicatedTenants(_ context.Context, _ string) ([]types.TenantTopics, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy to avoid data races
	result := make([]types.TenantTopics, len(r.dedicatedTenants))
	for i, dt := range r.dedicatedTenants {
		topics := make([]string, len(dt.Topics))
		copy(topics, dt.Topics)
		result[i] = types.TenantTopics{
			TenantID: dt.TenantID,
			Topics:   topics,
		}
	}
	return result, nil
}

// CreateOnlyTopics returns the full topic names that must exist on the broker
// but are NEVER consumed — routing-rule egress topics (ADR-0018). Callers use
// this for topic creation (ensureTopicsExist) only; these names MUST NOT be
// added to any consumer's subscription.
func (r *StreamTopicRegistry) CreateOnlyTopics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]string, len(r.createOnlyTopics))
	copy(result, r.createOnlyTopics)
	return result
}

// SetOnUpdate sets the callback invoked when topics change.
// This allows wiring the callback after construction (e.g., when the consumer
// pool depends on the registry and vice versa).
func (r *StreamTopicRegistry) SetOnUpdate(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onUpdate = fn
}

// State returns the current stream state (0=disconnected, 1=connected).
func (r *StreamTopicRegistry) State() int32 {
	return r.streamState.Load()
}

// Close stops the stream and releases resources.
func (r *StreamTopicRegistry) Close() error {
	r.cancel()
	r.wg.Wait()
	if err := r.conn.Close(); err != nil {
		return fmt.Errorf("close topic registry gRPC connection: %w", err)
	}
	return nil
}

// streamLoop runs the gRPC stream with reconnection logic.
func (r *StreamTopicRegistry) streamLoop(ctx context.Context) {
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

		stream, err := client.WatchTopics(ctx, &provisioningv1.WatchTopicsRequest{
			Namespace: r.namespace,
		})
		if err != nil {
			r.logger.Warn().Err(err).Dur("retry_in", delay).Msg("failed to start WatchTopics stream")
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

		r.logger.Info().Str("namespace", r.namespace).Msg("WatchTopics stream connected")

		for {
			resp, err := stream.Recv()
			if err != nil {
				r.logger.Warn().Err(err).Msg("WatchTopics stream disconnected")
				r.streamState.Store(StreamStateDisconnected)
				r.streamStateGauge.Set(StreamStateDisconnected)
				break
			}

			r.updateTopics(resp)
		}
	}
}

// updateTopics processes a WatchTopicsResponse and updates the cache.
func (r *StreamTopicRegistry) updateTopics(resp *provisioningv1.WatchTopicsResponse) {
	// applyTopicUpdate holds the write lock via defer and returns the onUpdate
	// callback. The callback is called AFTER the lock is released, which is
	// critical: onUpdate → pool.RefreshTopics → GetSharedTenantTopics/
	// GetDedicatedTenants → r.mu.RLock(). Calling it under the write lock
	// would deadlock.
	onUpdate := r.applyTopicUpdate(resp)

	r.logger.Debug().
		Bool("snapshot", resp.GetIsSnapshot()).
		Int("shared_topics", len(resp.GetSharedTopics())).
		Int("dedicated_tenants", len(resp.GetDedicatedTenants())).
		Msg("topic cache updated")

	// Notify callback (e.g., consumer pool refresh)
	if onUpdate != nil {
		onUpdate()
	}
}

// applyTopicUpdate updates the topic cache under the write lock and returns
// the onUpdate callback for the caller to invoke after the lock is released.
func (r *StreamTopicRegistry) applyTopicUpdate(resp *provisioningv1.WatchTopicsResponse) func() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Copy proto response slices to avoid sharing backing arrays (defense in depth)
	r.sharedTopics = make([]string, len(resp.GetSharedTopics()))
	copy(r.sharedTopics, resp.GetSharedTopics())

	r.createOnlyTopics = make([]string, len(resp.GetCreateOnlyTopics()))
	copy(r.createOnlyTopics, resp.GetCreateOnlyTopics())

	r.dedicatedTenants = make([]types.TenantTopics, 0, len(resp.GetDedicatedTenants()))
	for _, dt := range resp.GetDedicatedTenants() {
		topics := make([]string, len(dt.GetTopics()))
		copy(topics, dt.GetTopics())
		r.dedicatedTenants = append(r.dedicatedTenants, types.TenantTopics{
			TenantID: dt.GetTenantSlug(),
			Topics:   topics,
		})
	}

	// Build the topic → tenant map (#179 P3): shared topics carry their tenant via shared_topic_tenants;
	// dedicated topics carry it via dedicated_tenants. This is the authoritative tenant source consumers
	// use instead of reverse-parsing topic names.
	r.topicTenants = make(map[string]string, len(resp.GetSharedTopicTenants()))
	for _, st := range resp.GetSharedTopicTenants() {
		r.topicTenants[st.GetTopic()] = st.GetTenantSlug()
	}
	for _, dt := range r.dedicatedTenants {
		for _, topic := range dt.Topics {
			r.topicTenants[topic] = dt.TenantID
		}
	}

	if resp.GetIsSnapshot() {
		r.snapshotReceived.Store(true)
	}

	return r.onUpdate
}

// SnapshotReceived reports whether the initial full topic snapshot has been applied. Used to gate
// ws-server readiness in kafka mode (#179 P3) — before the snapshot, topic→tenant cannot be resolved.
func (r *StreamTopicRegistry) SnapshotReceived() bool {
	return r.snapshotReceived.Load()
}

// TopicTenants returns a copy of the topic → tenant map. Consumers resolve a record's tenant from this
// map (#179 P3) instead of reverse-parsing the topic name. The namespace arg is ignored (set at
// construction). Returns an empty (non-nil) map before the first snapshot.
func (r *StreamTopicRegistry) TopicTenants(_ context.Context, _ string) (map[string]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]string, len(r.topicTenants))
	maps.Copy(out, r.topicTenants)
	return out, nil
}

// Ensure StreamTopicRegistry implements types.TenantRegistry.
var _ types.TenantRegistry = (*StreamTopicRegistry)(nil)
