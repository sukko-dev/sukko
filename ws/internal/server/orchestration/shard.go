package orchestration

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server"
	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/server/registry"
	"github.com/sukko-dev/sukko/internal/shared/alerting"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

const panicComponentTenantForwarder = "tenant_forwarder"

// tenantEntry tracks the per-tenant broadcast subscription and its goroutine lifecycle.
// ch is receive-only because bus.Subscribe returns <-chan *broadcast.Message.
type tenantEntry struct {
	ch       <-chan *broadcast.Message
	cancelFn context.CancelFunc
	refCount int
}

// drain discards buffered messages from a receive-only channel without blocking.
// Called when a per-tenant forwarder's context is canceled to prevent stale messages
// from being forwarded to broadcastChan after cancellation.
func drain(ch <-chan *broadcast.Message) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// Shard represents a single instance of the WebSocket server, running on its own core.
// It manages a subset of total connections and communicates via the central BroadcastBus.
// Capacity control is handled by the server's ResourceGuard.
type Shard struct {
	ID            int // Exported for external access
	server        *server.Server
	advertiseAddr string // Address advertised to LoadBalancer (e.g., localhost:3002)
	broadcastBus  broadcast.Bus

	// broadcastChan is a shard-owned fan-in channel written by per-tenant forwarder goroutines
	// and read by runBroadcastListener. Bidirectional because the shard creates it.
	// NOT closed on shutdown — forwarders exit via context cancellation; GC handles cleanup.
	broadcastChan chan *broadcast.Message

	// Per-tenant broadcast subscription registry. Protected by tenantMu.
	tenantMu      sync.Mutex
	tenantEntries map[string]tenantEntry

	logger         zerolog.Logger
	maxConnections int
	bufferSize     int // cfg.BroadcastBufferSize — used for OnTenantClientConnect forwarder

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// ShardConfig holds configuration for a single Shard
type ShardConfig struct {
	ID             int
	Addr           string // Address for this shard to bind/listen on (e.g., 0.0.0.0:3002)
	AdvertiseAddr  string // Address advertised to LoadBalancer (e.g., localhost:3002)
	Config         *platform.ServerConfig
	BroadcastBus   broadcast.Bus          // Reference to the central bus
	MessageBackend backend.MessageBackend // Pluggable message backend
	Alerter        alerting.Alerter       // Shared alerter instance
	Logger         zerolog.Logger
	MaxConnections int
	RegistryWriter *registry.Writer       // Pod-level registry writer; nil when registry disabled
	HealthWriter   *registry.HealthWriter // Pod-level health writer; nil when registry disabled
}

// NewShard creates a new Shard instance.
// The shard no longer calls bus.Subscribe at construction time — subscriptions are
// created on demand in OnTenantClientConnect when the first client for a tenant connects.
func NewShard(cfg ShardConfig) (*Shard, error) {
	ctx, cancel := context.WithCancel(context.Background())

	shard := &Shard{
		ID:             cfg.ID,
		advertiseAddr:  cfg.AdvertiseAddr,
		broadcastBus:   cfg.BroadcastBus,
		broadcastChan:  make(chan *broadcast.Message, cfg.Config.BroadcastBufferSize),
		tenantEntries:  make(map[string]tenantEntry),
		logger:         cfg.Logger.With().Int("shard_id", cfg.ID).Logger(),
		maxConnections: cfg.MaxConnections,
		bufferSize:     cfg.Config.BroadcastBufferSize,
		ctx:            ctx,
		cancel:         cancel,
	}

	shardServer, err := server.NewServer(server.Params{
		Config:         cfg.Config,
		Addr:           cfg.Addr,
		MaxConnections: cfg.MaxConnections,
		Backend:        cfg.MessageBackend,
		BroadcastBus:   cfg.BroadcastBus,
		TenantHooks:    shard, // shard implements server.TenantHooks
		RegistryWriter: cfg.RegistryWriter,
		HealthWriter:   cfg.HealthWriter,
		ShardID:        cfg.ID,
	}, cfg.Alerter)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create shared server for shard %d: %w", cfg.ID, err)
	}
	shard.server = shardServer

	shard.logger.Info().
		Str("bind_addr", cfg.Addr).
		Str("advertise_addr", cfg.AdvertiseAddr).
		Int("max_connections", cfg.MaxConnections).
		Msg("Shard created with separate bind/advertise addresses")

	return shard, nil
}

// OnTenantClientConnect is called when a WebSocket client belonging to tenantID connects.
// It ensures exactly one Valkey SUBSCRIBE (and one forwarder goroutine) exists per tenant
// per shard, regardless of how many concurrent clients that tenant has on this shard.
// Implements server.TenantHooks.
func (s *Shard) OnTenantClientConnect(tenantID string) error {
	s.tenantMu.Lock()

	// Fast path: entry exists — increment ref count only, no new subscription needed.
	if entry, ok := s.tenantEntries[tenantID]; ok {
		entry.refCount++
		s.tenantEntries[tenantID] = entry
		s.tenantMu.Unlock()
		return nil
	}
	s.tenantMu.Unlock()

	// Slow path: first client for this tenant — subscribe to bus and launch forwarder.
	ch, err := s.broadcastBus.Subscribe(tenantID)
	if err != nil {
		return fmt.Errorf("shard %d: subscribe tenant %s: %w", s.ID, tenantID, err)
	}

	tenantCtx, tenantCancel := context.WithCancel(s.ctx)

	s.tenantMu.Lock()
	if existing, ok := s.tenantEntries[tenantID]; ok {
		// Concurrent first-connect race: another goroutine created the entry while we
		// were outside the lock calling bus.Subscribe. Roll back our subscription.
		// Rollback invariant: bus.subRefCounts[tenantID]==1, shard.refCount==2.
		existing.refCount++
		s.tenantEntries[tenantID] = existing
		s.tenantMu.Unlock()
		tenantCancel()
		if uerr := s.broadcastBus.Unsubscribe(tenantID, ch); uerr != nil {
			s.logger.Warn().
				Err(uerr).
				Str(logging.LogKeyTenantSlug, tenantID).
				Msg("shard: rollback Unsubscribe error (non-fatal, channel will GC)")
		}
		return nil
	}

	entry := tenantEntry{
		ch:       ch,
		cancelFn: tenantCancel,
		refCount: 1,
	}
	s.tenantEntries[tenantID] = entry
	s.tenantMu.Unlock()

	s.wg.Go(func() {
		defer logging.RecoverPanic(s.logger, panicComponentTenantForwarder, map[string]any{"tenant_id": tenantID})
		s.runTenantForwarder(tenantCtx, ch)
	})
	return nil
}

// OnTenantClientDisconnect is called when a WebSocket client belonging to tenantID disconnects.
// When the last client for a tenant disconnects, it cancels the forwarder goroutine and
// unsubscribes from the bus. Implements server.TenantHooks.
func (s *Shard) OnTenantClientDisconnect(tenantID string) {
	s.tenantMu.Lock()
	entry, ok := s.tenantEntries[tenantID]
	if !ok {
		s.tenantMu.Unlock()
		return
	}
	entry.refCount--
	if entry.refCount > 0 {
		s.tenantEntries[tenantID] = entry
		s.tenantMu.Unlock()
		return
	}
	// Last client for this tenant: cancel forwarder and remove entry.
	entry.cancelFn()
	delete(s.tenantEntries, tenantID)
	ch := entry.ch
	s.tenantMu.Unlock()

	// Unsubscribe outside the lock — bus I/O must not be held under tenantMu.
	if err := s.broadcastBus.Unsubscribe(tenantID, ch); err != nil {
		s.logger.Warn().
			Err(err).
			Str(logging.LogKeyTenantSlug, tenantID).
			Msg("shard: Unsubscribe error on disconnect")
	}
	// tenantEntry.ch MUST NOT be closed: bus fanOut (write-side) owns the channel lifecycle.
	// The forwarder goroutine exits via context cancellation; the channel is GC'd.
}

// runTenantForwarder reads messages from a per-tenant bus channel and writes them to
// broadcastChan (the shard's internal fan-in channel read by runBroadcastListener).
// Drains the channel before returning to prevent stale messages from leaking through.
func (s *Shard) runTenantForwarder(ctx context.Context, ch <-chan *broadcast.Message) {
	for {
		select {
		case msg := <-ch:
			select {
			case s.broadcastChan <- msg:
			case <-ctx.Done():
				drain(ch)
				return
			}
		case <-ctx.Done():
			drain(ch)
			return
		}
	}
}

// Start begins the shard's operation.
func (s *Shard) Start() error {
	s.logger.Info().Msg("Starting shard")

	if err := s.server.Start(); err != nil {
		return fmt.Errorf("failed to start shared server for shard %d: %w", s.ID, err)
	}

	s.wg.Go(s.runBroadcastListener)

	s.logger.Info().Msg("Shard started")
	return nil
}

// Shutdown gracefully stops the shard.
// Ordering: cancel context → shutdown server → wg.Wait (all forwarders + listener).
// s.cancel() cascades to all per-tenant contexts (derived from s.ctx); forwarders exit via drain+ctx.Done.
func (s *Shard) Shutdown() error {
	s.logger.Info().Msg("Shutting down shard")

	// Cancel shard context — cascades to all tenant forwarder contexts.
	s.cancel()

	// Stop accepting new connections and drain in-progress handlers.
	if err := s.server.Shutdown(); err != nil {
		s.logger.Error().Err(err).Msg("Error during shared server shutdown")
	}

	// Wait for runBroadcastListener + all tenant forwarder goroutines.
	s.wg.Wait()

	s.logger.Info().Msg("Shard shut down")
	return nil
}

// runBroadcastListener reads from the shard-owned broadcastChan and calls server.Broadcast.
// All per-tenant forwarder goroutines write to broadcastChan; this goroutine reads from it.
// The channel is never closed — this loop exits via ctx.Done().
func (s *Shard) runBroadcastListener() {
	defer logging.RecoverPanic(s.logger, "runBroadcastListener", map[string]any{
		"shard_id": s.ID,
	})

	s.logger.Info().Msg("Broadcast listener started")

	for {
		select {
		case msg := <-s.broadcastChan:
			s.server.Broadcast(msg.Subject, msg.Payload, msg.Pos, msg.Mid)
		case <-s.ctx.Done():
			s.logger.Info().Msg("Broadcast listener stopped")
			return
		}
	}
}

// Server returns the underlying server instance for gRPC service integration.
func (s *Shard) Server() *server.Server {
	return s.server
}

// GetCurrentConnections returns the current number of active connections for this shard.
func (s *Shard) GetCurrentConnections() int64 {
	return s.server.GetStats().CurrentConnections.Load()
}

// GetMaxConnections returns the maximum number of connections for this shard.
func (s *Shard) GetMaxConnections() int {
	return s.maxConnections
}

// GetAddr returns the address this shard is listening on.
func (s *Shard) GetAddr() string {
	return s.advertiseAddr
}

// GetSystemStats returns system-wide CPU and memory metrics.
// Since all shards run in the same process, these metrics are shared.
func (s *Shard) GetSystemStats() (cpuPercent, memoryMB, memoryPercent float64) {
	systemMonitor := metrics.GetSystemMonitor(s.logger)
	sysMetrics := systemMonitor.GetMetrics()
	return sysMetrics.CPUPercent, sysMetrics.MemoryMB, sysMetrics.MemoryPercent
}
