package ws

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	ws "github.com/gobwas/ws"
	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// defaultPoolRampRate is the fallback connections-per-second rate when not specified.
const defaultPoolRampRate = 100

// Pool manages a set of WebSocket client connections.
// Single-lifecycle: do NOT call RampUp or FillSlot after Drain() — Pool.wg is not safe
// to reuse after Wait() returns (§VII).
type Pool struct {
	mu             sync.Mutex
	clients        []*Client
	drained        atomic.Bool
	active         atomic.Int64
	lastDrainCount atomic.Int64
	wg             sync.WaitGroup
	logger         zerolog.Logger
}

// NewPool creates a Pool with the given logger.
func NewPool(logger zerolog.Logger) *Pool {
	return &Pool{logger: logger}
}

// PoolConfig configures connections created during ramp-up.
type PoolConfig struct {
	GatewayURL string
	Token      string                     // static token (backward compat)
	TokenFunc  func(connIndex int) string // per-connection token (takes precedence over Token)
	APIKey     string
	Channels   []string
	// ChannelsFunc returns per-connection channels (takes precedence over
	// Channels). Needed for user-scoped channels, where each connection
	// subscribes to its own resolved name (e.g. "tenant.dm.<subject>").
	ChannelsFunc func(connIndex int) []string
	OnMessage    func(Message)
	// OnClose is called when a connection's ReadLoop exits, with the slot index,
	// WebSocket close code, and the time of closure. May be nil.
	OnClose func(connIndex int, code ws.StatusCode, t time.Time)
}

// RampUp creates `count` connections at the given rate (connections per second).
// p.clients is pre-sized to count so that slot indices are stable; failed slots are nil.
func (p *Pool) RampUp(ctx context.Context, cfg PoolConfig, count, rate int) error {
	if cfg.GatewayURL == "" {
		return errors.New("ramp up: gateway URL is required")
	}
	if rate <= 0 {
		rate = defaultPoolRampRate
	}
	interval := time.Second / time.Duration(rate)

	// Pre-size so p.clients[i] maps 1:1 to loop index — CloseAt and FillSlot depend on this.
	p.mu.Lock()
	p.clients = make([]*Client, count)
	p.mu.Unlock()

	for i := range count {
		select {
		case <-ctx.Done():
			return fmt.Errorf("ramp up: %w", ctx.Err())
		default:
		}

		token := cfg.Token
		if cfg.TokenFunc != nil {
			token = cfg.TokenFunc(i)
		}

		client, err := Connect(ctx, ConnectConfig{
			GatewayURL: cfg.GatewayURL,
			Token:      token,
			APIKey:     cfg.APIKey,
			Logger:     p.logger.With().Int("client", i).Logger(),
			OnMessage:  cfg.OnMessage,
		})
		if err != nil {
			p.logger.Warn().Err(err).Int("client", i).Msg("connection failed")
			p.clients[i] = nil
			continue
		}

		if channels := poolChannels(cfg, i); len(channels) > 0 {
			if err := client.Subscribe(channels); err != nil {
				p.logger.Warn().Err(err).Msg("subscribe failed")
				_ = client.Close() // best-effort: subscribe already failed
				p.clients[i] = nil
				continue
			}
		}

		// Write slot and launch goroutine inside the same lock — Drain sets drained=true
		// inside this same lock, so wg.Go either happens before Drain's lock (counted,
		// wg.Wait will include it) or sees drained=true and aborts without calling wg.Go.
		connIndex := i
		p.mu.Lock()
		if p.drained.Load() {
			p.mu.Unlock()
			_ = client.Close()
			continue
		}
		p.clients[i] = client
		p.wg.Go(func() {
			defer logging.RecoverPanic(p.logger, "pool-read-loop", nil)
			code, _ := client.ReadLoop(ctx)
			if cfg.OnClose != nil {
				cfg.OnClose(connIndex, code, time.Now())
			}
		})
		p.mu.Unlock()
		p.active.Add(1)

		if i < count-1 {
			time.Sleep(interval)
		}
	}

	return nil
}

// Active returns the number of currently active connections.
func (p *Pool) Active() int64 {
	return p.active.Load()
}

// CloseAt closes the connection at the given index and marks its slot as nil.
// Safe to call from the soak runner when revoking individual connections.
// No-ops if the pool has already been drained (p.clients is nil).
func (p *Pool) CloseAt(index int) {
	p.mu.Lock()
	if p.clients == nil {
		p.mu.Unlock()
		return
	}
	c := p.clients[index]
	p.clients[index] = nil
	p.mu.Unlock()

	if c != nil {
		_ = c.Close() // best-effort
		p.active.Add(-1)
	}
}

// FillSlot reconnects a single slot by index, replacing a previously closed or failed
// connection. Uses context.Background() so soak connections outlive per-cycle contexts.
// Returns an error if the pool has already been drained.
func (p *Pool) FillSlot(index int, cfg PoolConfig) error {
	if p.drained.Load() {
		return errors.New("fill slot: pool already drained")
	}

	token := cfg.Token
	if cfg.TokenFunc != nil {
		token = cfg.TokenFunc(index)
	}

	// context.Background() is intentional — soak connections must survive per-cycle
	// context cancellations and are closed explicitly via CloseAt or Drain.
	client, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: cfg.GatewayURL,
		Token:      token,
		APIKey:     cfg.APIKey,
		Logger:     p.logger.With().Int("client", index).Logger(),
		OnMessage:  cfg.OnMessage,
	})
	if err != nil {
		return fmt.Errorf("fill slot %d: connect: %w", index, err)
	}

	if channels := poolChannels(cfg, index); len(channels) > 0 {
		if err := client.Subscribe(channels); err != nil {
			_ = client.Close()
			return fmt.Errorf("fill slot %d: subscribe: %w", index, err)
		}
	}

	// Write the slot and launch the goroutine inside the same lock. Drain also
	// writes drained=true inside the same lock, guaranteeing mutual exclusion:
	// wg.Go either happens before Drain's lock (counted, wg.Wait will include it)
	// or not at all (drained=true is seen below, we bail before wg.Go).
	p.mu.Lock()
	if p.drained.Load() {
		p.mu.Unlock()
		_ = client.Close()
		return errors.New("fill slot: pool already drained")
	}
	p.clients[index] = client
	p.wg.Go(func() {
		defer logging.RecoverPanic(p.logger, "pool-read-loop", nil)
		code, _ := client.ReadLoop(context.Background())
		if cfg.OnClose != nil {
			cfg.OnClose(index, code, time.Now())
		}
	})
	p.mu.Unlock()
	p.active.Add(1)

	return nil
}

// Drain closes all non-nil connections and waits for read loops to finish.
// Idempotent — concurrent or repeated calls are no-ops after the first.
func (p *Pool) Drain() {
	// Set drained=true inside the lock — the same lock used by FillSlot and RampUp
	// to call wg.Go. This guarantees that after we release the lock, no new wg.Go
	// calls can occur, making wg.Wait() safe (no concurrent Add racing with Wait).
	p.mu.Lock()
	if p.drained.Load() {
		p.mu.Unlock()
		return // already draining or drained
	}
	p.drained.Store(true)
	clients := p.clients
	p.clients = nil
	p.mu.Unlock()

	// Count non-nil slots — pre-sized slices may have nil entries for failed connections.
	var nonNilCount int64
	var closeWg sync.WaitGroup
	for _, c := range clients {
		if c == nil {
			continue
		}
		nonNilCount++
		closeWg.Go(func() {
			defer logging.RecoverPanic(p.logger, "pool-drain-close", nil)
			_ = c.Close() // best-effort: multi-step drain continues on individual close failure
		})
	}
	closeWg.Wait()
	p.lastDrainCount.Store(nonNilCount)
	p.active.Add(-nonNilCount)

	// Wait for all read loop goroutines to finish.
	p.wg.Wait()
}

// RefreshAll sends auth refresh messages to all active connections with freshly
// minted tokens. Returns the count of successful and failed refreshes.
// Auth refresh is fire-and-forget at the send level — gateway responses
// (auth_ack/auth_error) arrive asynchronously via the ReadLoop's onMsg callback.
func (p *Pool) RefreshAll(tokenFunc func(connIndex int) string) (refreshed, failed int) {
	p.mu.Lock()
	clients := slices.Clone(p.clients)
	p.mu.Unlock()

	for i, c := range clients {
		if c == nil {
			continue
		}
		token := tokenFunc(i)
		if err := c.RefreshToken(token); err != nil {
			p.logger.Warn().Err(err).Int("client", i).Msg("auth refresh failed")
			failed++
		} else {
			refreshed++
		}
	}
	return refreshed, failed
}

// DrainSummary returns a human-readable summary of the last drain operation.
func (p *Pool) DrainSummary() string {
	return fmt.Sprintf("drained %d connections", p.lastDrainCount.Load())
}

// poolChannels resolves the subscribe channels for a connection slot:
// ChannelsFunc (per-connection) takes precedence over the static Channels.
func poolChannels(cfg PoolConfig, connIndex int) []string {
	if cfg.ChannelsFunc != nil {
		return cfg.ChannelsFunc(connIndex)
	}
	return cfg.Channels
}
