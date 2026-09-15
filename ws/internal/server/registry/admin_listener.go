package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// ForceCloser is the interface AdminListener uses to force-disconnect a connection.
// Implemented by *server.Client — defined here to avoid circular imports between
// server and registry packages.
type ForceCloser interface {
	// ConnID returns the unique connection identifier assigned at upgrade.
	ConnID() string
	// TenantID returns the tenant this connection is authenticated to.
	TenantID() string
	// ForceDisconnect sends a WebSocket close frame and tears down the connection.
	// Safe to call concurrently and more than once — subsequent calls are no-ops.
	ForceDisconnect()
}

// adminListenerMetrics holds Prometheus metrics for an AdminListener instance.
// GaugeVec is per-shard (label "shard_id"); Counters are pod-level aggregates.
type adminListenerMetrics struct {
	ChannelHealthy *prometheus.GaugeVec
	DisconnectMiss prometheus.Counter
	TenantMismatch prometheus.Counter
	MsgDropped     prometheus.Counter
}

func newAdminListenerMetrics(reg prometheus.Registerer) *adminListenerMetrics {
	m := &adminListenerMetrics{
		ChannelHealthy: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ws_admin_channel_healthy",
			Help: "1 when the shard's admin Valkey channel subscription is active, 0 during reconnect.",
		}, []string{"shard_id"}),
		DisconnectMiss: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ws_admin_disconnect_miss_total",
			Help: "Number of admin disconnect messages for connection IDs not found in this shard.",
		}),
		TenantMismatch: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ws_admin_disconnect_tenant_mismatch_total",
			Help: "Number of admin disconnect messages where tenant_id did not match the connection's tenant.",
		}),
		MsgDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ws_admin_channel_msg_dropped_total",
			Help: "Admin disconnect messages dropped because the internal msgCh buffer was full.",
		}),
	}
	reg.MustRegister(m.ChannelHealthy, m.DisconnectMiss, m.TenantMismatch, m.MsgDropped)
	return m
}

// AdminListener subscribes to the per-pod admin Valkey channel and dispatches
// force-disconnect commands to connections on this shard.
// AdminListener is exported because server.Server holds it as a field cross-package.
type AdminListener struct {
	shardID      int
	cfg          *platform.ServerConfig
	valkeyClient valkey.Client
	healthWriter *HealthWriter
	logger       zerolog.Logger
	clients      *sync.Map
	channelName  string
	metrics      *adminListenerMetrics

	// dedicated and dedicatedCancel are set by Subscribe() and used in Run().
	dedicated       valkey.DedicatedClient
	dedicatedCancel func()
	msgCh           chan valkey.PubSubMessage
	disconnectCh    <-chan error
}

// NewAdminListener creates an AdminListener for the given shard.
// clients is a reference to Server.clients (sync.Map mapping int64 client IDs to ForceCloser).
// reg is the Prometheus registerer; pass prometheus.DefaultRegisterer in production.
func NewAdminListener(
	shardID int,
	cfg *platform.ServerConfig,
	valkeyClient valkey.Client,
	healthWriter *HealthWriter,
	logger zerolog.Logger,
	clients *sync.Map,
	reg prometheus.Registerer,
) *AdminListener {
	return &AdminListener{
		shardID:      shardID,
		cfg:          cfg,
		valkeyClient: valkeyClient,
		healthWriter: healthWriter,
		logger:       logger.With().Int("shard_id", shardID).Logger(),
		clients:      clients,
		channelName:  AdminChannelName(cfg.Environment, cfg.PodID),
		msgCh:        make(chan valkey.PubSubMessage, AdminMsgChanSize),
		metrics:      newAdminListenerMetrics(reg),
	}
}

// Subscribe performs a synchronous blocking SUBSCRIBE and blocks until Valkey confirms
// the subscription. Uses an independent context (NOT the caller's process context) so
// the startup timeout can be tuned independently from the SIGINT signal.
//
// On success: marks the shard healthy, returns nil. The dedicated client is stored for
// use by Run().
// On failure: propagated to Server.Start() which returns it to the orchestrator.
func (al *AdminListener) Subscribe(_ context.Context) error {
	// Independent deadline — prevents SIGINT from canceling the startup subscribe.
	subscribeCtx, cancel := context.WithTimeout(context.Background(), al.cfg.AdminChannelSubscribeTimeout)
	defer cancel()

	msgCh := al.msgCh
	dc, dcCancel := al.valkeyClient.Dedicate()
	disconnectCh := dc.SetPubSubHooks(valkey.PubSubHooks{
		OnMessage: func(m valkey.PubSubMessage) {
			select {
			case msgCh <- m:
			default:
				al.metrics.MsgDropped.Inc()
			}
		},
	})

	if err := dc.Do(subscribeCtx, dc.B().Subscribe().Channel(al.channelName).Build()).Error(); err != nil { //nolint:contextcheck // subscribeCtx derives from context.Background() intentionally — independent deadline from SIGINT
		dcCancel()
		al.healthWriter.SetAdminHealthy(al.shardID, false)
		al.metrics.ChannelHealthy.WithLabelValues(strconv.Itoa(al.shardID)).Set(0)
		return fmt.Errorf("admin listener shard %d: SUBSCRIBE %s: %w", al.shardID, al.channelName, err)
	}

	al.dedicated = dc
	al.dedicatedCancel = dcCancel
	al.disconnectCh = disconnectCh

	al.healthWriter.SetAdminHealthy(al.shardID, true)
	al.metrics.ChannelHealthy.WithLabelValues(strconv.Itoa(al.shardID)).Set(1)
	al.logger.Info().Str("channel", al.channelName).Msg("admin listener: subscribed")
	return nil
}

// Run is the message-receive loop. MUST be launched via wg.Go after Subscribe() returns successfully.
// defer logging.RecoverPanic is the FIRST defer per §VII.
func (al *AdminListener) Run(ctx context.Context) {
	defer logging.RecoverPanic(al.logger, "admin_listener", nil)

	currentBackoff := al.cfg.ConnectionsRegistryRestartInitialBackoff
	first := true

	for {
		if ctx.Err() != nil {
			if al.dedicatedCancel != nil {
				al.dedicatedCancel()
			}
			return
		}

		if !first {
			// Reconnect: release old client, acquire new.
			if al.dedicatedCancel != nil {
				al.dedicatedCancel()
			}

			dc, dcCancel := al.valkeyClient.Dedicate()
			msgCh := al.msgCh
			disconnectCh := dc.SetPubSubHooks(valkey.PubSubHooks{
				OnMessage: func(m valkey.PubSubMessage) {
					select {
					case msgCh <- m:
					default:
						al.metrics.MsgDropped.Inc()
					}
				},
			})

			// Reconnect SUBSCRIBE — bounded by process ctx so SIGINT unblocks it.
			resubCtx, resubCancel := context.WithTimeout(ctx, al.cfg.AdminChannelSubscribeTimeout)
			subErr := dc.Do(resubCtx, dc.B().Subscribe().Channel(al.channelName).Build()).Error()
			resubCancel()

			if subErr != nil {
				dcCancel()
				al.healthWriter.SetAdminHealthy(al.shardID, false)
				al.metrics.ChannelHealthy.WithLabelValues(strconv.Itoa(al.shardID)).Set(0)
				al.logger.Warn().Err(subErr).Str("channel", al.channelName).Msg("admin listener: SUBSCRIBE failed, retrying")

				t := time.NewTimer(currentBackoff)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					return
				}
				currentBackoff = min(currentBackoff*2, al.cfg.ConnectionsRegistryRestartMaxBackoff)
				continue
			}

			al.dedicated = dc
			al.dedicatedCancel = dcCancel
			al.disconnectCh = disconnectCh
			al.healthWriter.SetAdminHealthy(al.shardID, true)
			al.metrics.ChannelHealthy.WithLabelValues(strconv.Itoa(al.shardID)).Set(1)
			currentBackoff = al.cfg.ConnectionsRegistryRestartInitialBackoff
			al.logger.Info().Str("channel", al.channelName).Msg("admin listener: reconnected")
		}
		first = false

		// Message-receive loop for this connection lifetime.
	receiveLoop:
		for {
			select {
			case <-ctx.Done():
				if al.dedicatedCancel != nil {
					al.dedicatedCancel()
				}
				return

			case err := <-al.disconnectCh:
				al.healthWriter.SetAdminHealthy(al.shardID, false)
				al.metrics.ChannelHealthy.WithLabelValues(strconv.Itoa(al.shardID)).Set(0)
				if err != nil {
					al.logger.Warn().Err(err).Msg("admin listener: Valkey disconnect, reconnecting")
				}
				t := time.NewTimer(currentBackoff)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					if al.dedicatedCancel != nil {
						al.dedicatedCancel()
					}
					return
				}
				currentBackoff = min(currentBackoff*2, al.cfg.ConnectionsRegistryRestartMaxBackoff)
				break receiveLoop

			case m := <-al.msgCh:
				al.handleMessage(m)
			}
		}
	}
}

// handleMessage parses and dispatches an admin disconnect message.
func (al *AdminListener) handleMessage(m valkey.PubSubMessage) {
	var msg AdminMsg
	if err := json.Unmarshal([]byte(m.Message), &msg); err != nil {
		al.logger.Warn().Err(err).Str("channel", m.Channel).Msg("admin listener: malformed JSON, ignoring")
		return
	}

	if msg.Type != AdminMsgTypeDisconnect {
		return
	}

	found := false
	al.clients.Range(func(k, _ any) bool {
		c, ok := k.(ForceCloser)
		if !ok {
			return true
		}
		if c.ConnID() != msg.ConnectionID {
			return true
		}
		found = true
		// Verify tenant to prevent cross-tenant force-disconnect.
		if c.TenantID() != msg.TenantID {
			al.metrics.TenantMismatch.Inc()
			al.logger.Warn().
				Str("conn_id", msg.ConnectionID).
				Str("expected_tenant", msg.TenantID).
				Str("actual_tenant", c.TenantID()).
				Msg("admin listener: tenant mismatch on disconnect, ignoring")
			return false
		}
		c.ForceDisconnect()
		al.logger.Info().
			Str("conn_id", msg.ConnectionID).
			Str(logging.LogKeyTenantSlug, msg.TenantID).
			Str("reason", msg.Reason).
			Msg("admin listener: force-disconnected connection")
		return false
	})

	if !found {
		al.metrics.DisconnectMiss.Inc()
		al.logger.Debug().
			Str("conn_id", msg.ConnectionID).
			Str(logging.LogKeyTenantSlug, msg.TenantID).
			Msg("admin listener: disconnect target not on this shard (expected)")
	}
}
