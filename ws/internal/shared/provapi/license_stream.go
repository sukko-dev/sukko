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
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// StreamLicenseWatcherConfig configures the StreamLicenseWatcher.
type StreamLicenseWatcherConfig struct {
	GRPCAddr          string
	ReconnectDelay    time.Duration
	ReconnectMaxDelay time.Duration
	MetricPrefix      string // "gateway", "ws", or "push"
	Manager           *license.Manager
	Logger            zerolog.Logger
}

// StreamLicenseWatcher subscribes to the WatchLicense gRPC stream and applies each
// update via Manager.ReloadNoDowngrade(): an upgrade or renewal takes effect live, a
// downgrade is retained until the next restart (ADR-0038). License EXPIRY is never
// actioned by a running server — an expired key is refused only at the next
// boot/reload, so the watcher has no shutdown or grace-period behavior. Reconnects
// with exponential backoff.
type StreamLicenseWatcher struct {
	conn    *grpc.ClientConn
	config  StreamLicenseWatcherConfig
	logger  zerolog.Logger
	manager *license.Manager

	streamState atomic.Int32
	reconnects  atomic.Int64

	cancel context.CancelFunc // cancels the root context (streamLoop) on Close()
	wg     sync.WaitGroup

	streamStateGauge  prometheus.Gauge
	reconnectsCounter prometheus.Counter
}

// NewStreamLicenseWatcher creates a watcher that subscribes to WatchLicense
// and reloads the Manager on each license key update.
func NewStreamLicenseWatcher(cfg StreamLicenseWatcherConfig) (*StreamLicenseWatcher, error) {
	if cfg.GRPCAddr == "" {
		return nil, errors.New("license stream watcher: GRPCAddr is required")
	}
	if cfg.Manager == nil {
		return nil, errors.New("license stream watcher: Manager is required")
	}
	if cfg.ReconnectDelay <= 0 {
		cfg.ReconnectDelay = 5 * time.Second
	}
	if cfg.ReconnectMaxDelay < cfg.ReconnectDelay {
		cfg.ReconnectMaxDelay = 2 * time.Minute
	}
	if cfg.MetricPrefix == "" {
		cfg.MetricPrefix = "unknown"
	}

	conn, err := grpc.NewClient(cfg.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial provisioning gRPC: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	w := &StreamLicenseWatcher{
		conn:    conn,
		config:  cfg,
		logger:  cfg.Logger.With().Str("component", "license_stream_watcher").Logger(),
		manager: cfg.Manager,
		cancel:  cancel,
		streamStateGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: cfg.MetricPrefix + "_provisioning_license_stream_state",
			Help: "State of the provisioning license gRPC stream (0=disconnected, 1=connected)",
		}),
		reconnectsCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: cfg.MetricPrefix + "_provisioning_license_stream_reconnects_total",
			Help: "Total reconnection attempts for the provisioning license stream",
		}),
	}

	w.wg.Go(func() {
		defer logging.RecoverPanic(w.logger, "license_stream_watcher", nil)
		w.streamLoop(ctx)
	})

	return w, nil
}

// Close stops the watcher and releases resources.
func (w *StreamLicenseWatcher) Close() error {
	w.cancel()
	w.wg.Wait()
	if err := w.conn.Close(); err != nil {
		return fmt.Errorf("close gRPC connection: %w", err)
	}
	return nil
}

// State returns the current stream state (0=disconnected, 1=connected).
func (w *StreamLicenseWatcher) State() int32 {
	return w.streamState.Load()
}

// streamLoop runs the gRPC stream with reconnection logic.
func (w *StreamLicenseWatcher) streamLoop(ctx context.Context) {
	client := provisioningv1.NewProvisioningInternalServiceClient(w.conn)
	delay := w.config.ReconnectDelay

	// The FIRST non-empty key this process receives is boot-time resolution: the
	// authoritative current edition from provisioning. It is applied unconditionally
	// (like reading config at startup), even when it is LOWER than the env-booted
	// edition — this is how "a downgrade takes effect at the next restart" converges
	// for services that boot from SUKKO_LICENSE_KEY and only then learn the DB key
	// (ADR-0038). Every subsequent update is a LIVE reload that never lowers. The flag
	// persists across reconnects — a reconnect snapshot is not a fresh boot.
	firstKeyApplied := false

	for {
		select {
		case <-ctx.Done():
			w.streamState.Store(StreamStateDisconnected)
			w.streamStateGauge.Set(StreamStateDisconnected)
			return
		default:
		}

		stream, err := client.WatchLicense(ctx, &provisioningv1.WatchLicenseRequest{})
		if err != nil {
			w.logger.Warn().Err(err).Dur("retry_in", delay).Msg("failed to start WatchLicense stream")
			w.streamState.Store(StreamStateDisconnected)
			w.streamStateGauge.Set(StreamStateDisconnected)

			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}

			delay = backoff(delay, w.config.ReconnectMaxDelay)
			w.reconnects.Add(1)
			w.reconnectsCounter.Inc()
			continue
		}

		w.streamState.Store(StreamStateConnected)
		w.streamStateGauge.Set(StreamStateConnected)
		delay = w.config.ReconnectDelay

		w.logger.Info().Msg("WatchLicense stream connected")

		// Receive loop
		for {
			resp, err := stream.Recv()
			if err != nil {
				w.logger.Warn().Err(err).Msg("WatchLicense stream disconnected")
				w.streamState.Store(StreamStateDisconnected)
				w.streamStateGauge.Set(StreamStateDisconnected)
				break
			}

			key := resp.GetLicenseKey()

			// Empty key = no key currently persisted in the provisioning DB. Skip the
			// reload and keep the current edition — an empty stream update never changes
			// a running server (ADR-0038: edition is set at boot, re-keyed at restart).
			if key == "" {
				w.logger.Debug().Msg("received empty license key from stream — keeping current edition")
				continue
			}

			// First non-empty key = boot-time resolution (see streamLoop header): adopt
			// the authoritative edition unconditionally, then switch to live mode.
			if !firstKeyApplied {
				firstKeyApplied = true
				if err := w.manager.Reload(key); err != nil {
					// Expected + benign when the snapshot equals this process's own boot
					// key — the iat replay check rejects it as a no-op. Anything else is a
					// genuine failure; keep the booted edition either way.
					if errors.Is(err, license.ErrReplayDetected) {
						w.logger.Debug().Msg("initial license snapshot matches booted key — no change")
					} else {
						w.logger.Warn().Err(err).Msg("initial license resolution from stream failed; keeping booted edition")
					}
					continue
				}
				w.logger.Info().
					Str("edition", w.manager.Edition().String()).
					Msg("initial license resolved from stream")
				continue
			}

			// Live update: apply only if it lowers neither edition nor capacity. A
			// downgrade is retained until the next restart (logged inside
			// ReloadNoDowngrade); provisioning has persisted it, so a restart boots it.
			applied, err := w.manager.ReloadNoDowngrade(key)
			if err != nil {
				w.logger.Warn().Err(err).Msg("license reload from stream failed")
				continue
			}
			if applied {
				w.logger.Info().
					Str("edition", w.manager.Edition().String()).
					Msg("license reloaded from stream")
			}
		}
	}
}
