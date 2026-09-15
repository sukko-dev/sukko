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

// DefaultDowngradeGracePeriod is the time allowed between downgrade detection and forced restart.
// It is a vendor-enforced constant — not operator-configurable.
const DefaultDowngradeGracePeriod = 14 * 24 * time.Hour

// StreamLicenseWatcherConfig configures the StreamLicenseWatcher.
type StreamLicenseWatcherConfig struct {
	GRPCAddr             string
	ReconnectDelay       time.Duration
	ReconnectMaxDelay    time.Duration
	MetricPrefix         string // "gateway", "ws", or "push"
	Manager              *license.Manager
	Logger               zerolog.Logger
	OnDowngrade          func(from, to license.Edition) // Optional callback invoked when the downgrade grace period expires. Typically calls cancel() to initiate graceful shutdown.
	DowngradeGracePeriod time.Duration                  // For testing only. When zero (production), DefaultDowngradeGracePeriod is used. Services MUST NOT set this field.
}

// StreamLicenseWatcher subscribes to the WatchLicense gRPC stream and calls
// Manager.Reload() on each update. Reconnects with exponential backoff.
type StreamLicenseWatcher struct {
	conn    *grpc.ClientConn
	config  StreamLicenseWatcherConfig
	logger  zerolog.Logger
	manager *license.Manager

	streamState atomic.Int32
	reconnects  atomic.Int64

	ctx    context.Context // root context; canceled by Close()
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Grace period drain state (timerMu protects timer and expiryCancel).
	timer        *time.Timer        // nil = no grace period running; non-nil = timer created (may be running or already fired; Stop() is always safe).
	expiryCancel context.CancelFunc // cancels the running expiry watcher goroutine; nil if none
	timerMu      sync.Mutex
	// Intentional: fires OnDowngrade at most once per process lifetime.
	// Not reset on reconnect or cancel — the process is expected to restart after a drain.
	once sync.Once

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
		ctx:     ctx,
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

	// Bootstrap expiry watcher for the key already loaded at startup via NewManager.
	// Stable deployments never receive a WatchLicense snapshot of their current key
	// (that would be ErrReplayDetected), so without this the expiry goroutine would
	// never be scheduled for keys that were loaded before the watcher started.
	if claims := cfg.Manager.Claims(); claims != nil && claims.Exp > 0 && !claims.IsExpired() {
		w.scheduleExpiryWatcher(claims.Exp)
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

// startGracePeriod starts the downgrade grace period timer. No-op if already running.
func (w *StreamLicenseWatcher) startGracePeriod(from, to license.Edition) {
	w.timerMu.Lock()
	if w.timer != nil {
		w.timerMu.Unlock()
		return // grace period already running
	}
	w.timerMu.Unlock()

	gracePeriod := w.config.DowngradeGracePeriod
	if gracePeriod == 0 {
		gracePeriod = DefaultDowngradeGracePeriod
	}

	// Called outside the lock; time.AfterFunc is non-blocking and avoids holding timerMu across a goroutine spawn.
	t := time.AfterFunc(gracePeriod, func() {
		// time.AfterFunc runs on a Go runtime goroutine, not under wg.Go —
		// RecoverPanic must be added explicitly per Constitution V.
		defer logging.RecoverPanic(w.logger, "downgrade_timer", nil)
		w.once.Do(func() {
			if w.config.OnDowngrade != nil {
				w.config.OnDowngrade(from, to)
			} else {
				w.logger.Warn().Msg("OnDowngrade is nil — grace period expired but no action taken")
			}
		})
	})

	w.timerMu.Lock()
	w.timer = t
	w.timerMu.Unlock()

	w.logger.Warn().
		Str("from_edition", from.String()).
		Str("to_edition", to.String()).
		Time("drain_at", time.Now().Add(gracePeriod)).
		Msg("Edition downgrade detected — grace period started")
}

// cancelGracePeriodLocked stops and nils the grace period timer without acquiring timerMu.
// Caller MUST hold timerMu.
func (w *StreamLicenseWatcher) cancelGracePeriodLocked() {
	if w.timer == nil {
		return
	}
	w.timer.Stop()
	w.timer = nil
}

// cancelGracePeriod cancels the running grace period timer. No-op if no timer is running.
func (w *StreamLicenseWatcher) cancelGracePeriod() {
	w.timerMu.Lock()
	if w.timer == nil {
		w.timerMu.Unlock()
		return
	}
	// Return value ignored — sync.Once in the callback handles the race where the timer already fired.
	w.timer.Stop()
	// time.AfterFunc timers have no .C channel — draining would deadlock.
	w.timer = nil
	w.timerMu.Unlock()

	w.logger.Info().
		Str("reason", "license renewed or upgraded").
		Str("current_edition", w.manager.Edition().String()).
		Msg("Edition downgrade grace period canceled")
}

// scheduleExpiryWatcher cancels any prior expiry goroutine and grace period timer,
// then (if exp > 0) launches a new expiry goroutine that starts the downgrade grace
// period when the license key expires. fromEdition is captured at call time.
func (w *StreamLicenseWatcher) scheduleExpiryWatcher(exp int64) {
	fromEdition := w.manager.Edition() // capture before locking

	w.timerMu.Lock()
	w.cancelGracePeriodLocked()
	if w.expiryCancel != nil {
		w.expiryCancel()
		w.expiryCancel = nil
	}
	var launchCtx context.Context
	if exp > 0 {
		ctx, cancel := context.WithDeadline(w.ctx, time.Unix(exp, 0))
		w.expiryCancel = cancel
		launchCtx = ctx
	}
	w.timerMu.Unlock() // MUST unlock before wg.Go (Constitution VII: no goroutine spawn while holding mutex)

	if launchCtx != nil {
		w.wg.Go(func() {
			defer logging.RecoverPanic(w.logger, "expiry_watcher", nil)
			<-launchCtx.Done()
			if launchCtx.Err() == context.DeadlineExceeded {
				w.startGracePeriod(fromEdition, license.Community)
			}
			// context.Canceled (renewal or Close()) — return without action
		})
	}
}

// streamLoop runs the gRPC stream with reconnection logic.
func (w *StreamLicenseWatcher) streamLoop(ctx context.Context) {
	client := provisioningv1.NewProvisioningInternalServiceClient(w.conn)
	delay := w.config.ReconnectDelay

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

			// Empty key = Community / no license — skip reload
			if resp.GetLicenseKey() == "" {
				w.logger.Debug().Msg("received empty license key (Community)")
				continue
			}

			oldEdition := w.manager.Edition()
			if err := w.manager.Reload(resp.GetLicenseKey()); err != nil {
				w.logger.Warn().Err(err).Msg("license reload from stream failed")
				continue
			}
			newEdition := w.manager.Edition()

			w.logger.Info().
				Str("edition", newEdition.String()).
				Msg("license reloaded from stream")

			// Reschedule expiry watcher for the renewed key's expiry timestamp.
			if reloadClaims := w.manager.Claims(); reloadClaims != nil {
				w.scheduleExpiryWatcher(reloadClaims.Exp) //nolint:contextcheck // scheduleExpiryWatcher uses w.ctx (struct root context), not the stream loop's ctx parameter
			}

			if !newEdition.IsAtLeast(oldEdition) {
				// newEdition < oldEdition — downgrade detected; start grace period timer if not already running
				w.startGracePeriod(oldEdition, newEdition)
			} else {
				// renewal or upgrade — cancel any running timer
				w.cancelGracePeriod()
			}
		}
	}
}
