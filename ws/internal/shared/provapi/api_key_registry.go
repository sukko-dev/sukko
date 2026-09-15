package provapi

import (
	"context"
	"errors"
	"fmt"
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
)

// APIKeyInfo holds the cached API key data received from the provisioning stream.
type APIKeyInfo struct {
	KeyID    string
	TenantID string
	Name     string
	IsActive bool
}

// StreamAPIKeyRegistryConfig configures the gRPC stream-backed API key registry.
type StreamAPIKeyRegistryConfig struct {
	GRPCAddr          string
	ReconnectDelay    time.Duration
	ReconnectMaxDelay time.Duration
	MetricPrefix      string // "gateway" or "ws"
	Logger            zerolog.Logger
}

// StreamAPIKeyRegistry provides O(1) API key lookup backed by a gRPC streaming
// connection to the provisioning service's WatchAPIKeys RPC.
type StreamAPIKeyRegistry struct {
	mu       sync.RWMutex
	keysByID map[string]*APIKeyInfo

	conn   *grpc.ClientConn
	config StreamAPIKeyRegistryConfig
	logger zerolog.Logger

	streamState atomic.Int32
	reconnects  atomic.Int64

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Prometheus metrics (registered with prefix)
	streamStateGauge  prometheus.Gauge
	reconnectsCounter prometheus.Counter
}

// NewStreamAPIKeyRegistry creates a new gRPC stream-backed API key registry.
// It dials the gRPC server and starts a background goroutine to receive updates.
func NewStreamAPIKeyRegistry(cfg StreamAPIKeyRegistryConfig) (*StreamAPIKeyRegistry, error) {
	if cfg.GRPCAddr == "" {
		return nil, errors.New("stream api key registry: GRPCAddr is required")
	}
	if cfg.ReconnectDelay <= 0 {
		return nil, errors.New("stream api key registry: ReconnectDelay must be > 0")
	}
	if cfg.ReconnectMaxDelay < cfg.ReconnectDelay {
		return nil, errors.New("stream api key registry: ReconnectMaxDelay must be >= ReconnectDelay")
	}
	if cfg.MetricPrefix == "" {
		return nil, errors.New("stream api key registry: MetricPrefix is required")
	}

	conn, err := grpc.NewClient(cfg.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial provisioning gRPC: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &StreamAPIKeyRegistry{
		keysByID: make(map[string]*APIKeyInfo),
		conn:     conn,
		config:   cfg,
		logger:   cfg.Logger.With().Str("component", "stream_api_key_registry").Logger(),
		cancel:   cancel,
		streamStateGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: cfg.MetricPrefix + "_provisioning_api_keys_stream_state",
			Help: "State of the provisioning API keys gRPC stream (0=disconnected, 1=connected)",
		}),
		reconnectsCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: cfg.MetricPrefix + "_provisioning_api_keys_stream_reconnects_total",
			Help: "Total reconnection attempts for the provisioning API keys stream",
		}),
	}

	r.wg.Go(func() {
		defer logging.RecoverPanic(r.logger, "api_key_registry_stream", nil)
		r.streamLoop(ctx)
	})

	return r, nil
}

// Lookup returns the API key info for the given key ID.
// Returns false if the key is not found or not active.
func (r *StreamAPIKeyRegistry) Lookup(apiKey string) (*APIKeyInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, ok := r.keysByID[apiKey]
	if !ok || !info.IsActive {
		return nil, false
	}

	return info, true
}

// State returns the current stream state (0=disconnected, 1=connected).
func (r *StreamAPIKeyRegistry) State() int32 {
	return r.streamState.Load()
}

// Close stops the stream and releases resources.
func (r *StreamAPIKeyRegistry) Close() error {
	r.cancel()
	r.wg.Wait()
	if err := r.conn.Close(); err != nil {
		return fmt.Errorf("close api key registry gRPC connection: %w", err)
	}
	return nil
}

// streamLoop runs the gRPC stream with reconnection logic.
func (r *StreamAPIKeyRegistry) streamLoop(ctx context.Context) {
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

		stream, err := client.WatchAPIKeys(ctx, &provisioningv1.WatchAPIKeysRequest{})
		if err != nil {
			r.logger.Warn().Err(err).Dur("retry_in", delay).Msg("failed to start WatchAPIKeys stream")
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
		delay = r.config.ReconnectDelay // Reset on successful connect

		r.logger.Info().Msg("WatchAPIKeys stream connected")

		// Receive loop
		for {
			resp, err := stream.Recv()
			if err != nil {
				r.logger.Warn().Err(err).Msg("WatchAPIKeys stream disconnected")
				r.streamState.Store(StreamStateDisconnected)
				r.streamStateGauge.Set(StreamStateDisconnected)
				break
			}

			r.updateKeys(resp)
		}
	}
}

// updateKeys processes a WatchAPIKeysResponse and updates the cache.
func (r *StreamAPIKeyRegistry) updateKeys(resp *provisioningv1.WatchAPIKeysResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if resp.GetIsSnapshot() {
		// Full replacement
		r.keysByID = make(map[string]*APIKeyInfo, len(resp.GetApiKeys()))
	}

	// Remove keys
	for _, keyID := range resp.GetRemovedKeyIds() {
		delete(r.keysByID, keyID)
	}

	// Add/update keys
	for _, ki := range resp.GetApiKeys() {
		r.keysByID[ki.GetKeyId()] = &APIKeyInfo{
			KeyID:    ki.GetKeyId(),
			TenantID: ki.GetTenantUuid(),
			Name:     ki.GetName(),
			IsActive: ki.GetIsActive(),
		}
	}

	r.logger.Debug().
		Bool("snapshot", resp.GetIsSnapshot()).
		Int("keys", len(r.keysByID)).
		Msg("api key cache updated")
}
