package broadcast

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// --- Constants ---

const (
	valkeyChannelSeparator = ":"
	valkeyChannelWildcard  = "*"

	retryInitialBackoff   = 100 * time.Millisecond
	retryMaxBackoff       = 5 * time.Second
	subCommandTimeout     = 5 * time.Second
	reconcileTickInterval = 30 * time.Second

	panicComponentSubscriptionMgr = "subscription_management_goroutine"
)

// tenantChannel constructs the full per-tenant Valkey channel name.
// All callers MUST use this function — no inline string concatenation.
func tenantChannel(prefix, tenantID string) string {
	return prefix + valkeyChannelSeparator + tenantID
}

// tenantChannelPattern constructs the PSUBSCRIBE pattern for all tenant channels.
func tenantChannelPattern(prefix string) string {
	return prefix + valkeyChannelSeparator + valkeyChannelWildcard
}

// --- Struct ---

// valkeyBus implements Bus using Valkey/Redis Pub/Sub with per-tenant channel isolation.
type valkeyBus struct {
	client valkey.Client

	channelPrefix string // cfg.Valkey.Channel (e.g. "ws.broadcast") — used as prefix
	bufferSize    int    // per-tenant subscriber channel capacity (cfg.BroadcastBufferSize)
	limits        license.Limits

	// Per-tenant subscriber registry. Protected by subMu.
	tenantSubscribers map[string][]subscriberEntry
	allSubscribers    []subscriberEntry
	subMu             sync.RWMutex

	// Pod-level ref counts for Valkey subscription lifecycle — the authoritative
	// DESIRED subscription state (ADR-0016). Protected by subRefMu.
	// desiredGen is bumped under subRefMu whenever the desired set changes, so a
	// convergence pass can snapshot (generation, desired set) atomically. It is
	// also bumped by reinitDedicatedClient when the confirmed set is invalidated,
	// which forces convergence to read false until a full pass completes.
	// Lock ordering: subRefMu and subMu are never held together (see GetMetrics).
	// No I/O is ever performed while holding subRefMu.
	subRefCounts         map[string]int
	subscribeAllRefCount int
	subRefMu             sync.Mutex
	desiredGen           atomic.Uint64

	// Subscription wakeup channel — capacity 1, read exclusively by
	// subscriptionMgmtLoop. A wakeup carries no data; it means "desired state
	// changed, re-derive". See signalSubscriptionChange for why discarding a
	// wakeup when one is already pending is lossless by construction.
	subWakeCh chan struct{}

	// CONFIRMED subscription state: what this pod has successfully subscribed on
	// the CURRENT dedicated connection. Owned exclusively by subscriptionMgmtLoop
	// (like dedicatedClient) — no mutex; wiped wholesale by reinitDedicatedClient
	// because a new connection has no subscriptions.
	confirmedTenants map[string]struct{}
	confirmedAll     bool

	// confirmedGen is the desired-state generation the last fully successful
	// convergence pass was built from. Written only by subscriptionMgmtLoop;
	// read lock-free by IsHealthy/GetMetrics. Convergence (confirmedGen ==
	// desiredGen) can be transiently false while a pass is pending, but is
	// never true while confirmed differs from desired.
	confirmedGen atomic.Uint64

	// establishedCount mirrors len(confirmedTenants)+confirmedAll for lock-free
	// reads by GetMetrics (confirmed state itself is loop-owned).
	establishedCount atomic.Int64

	// Dedicated connection for SetPubSubHooks. Written only by subscriptionMgmtLoop.
	dedicatedClient valkey.DedicatedClient
	dedicatedCancel func()
	disconnectCh    <-chan error

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Health tracking (atomic for lock-free reads). Publish health and
	// subscription convergence are deliberately separate signals (§XV,
	// ADR-0016): the publish connection can be perfectly healthy while this
	// pod's pub/sub subscriptions are still converging, and vice versa.
	publishHealthy atomic.Bool
	lastPublish    atomic.Int64
	publishErrors  atomic.Uint64
	messagesRecv   atomic.Uint64

	shutdownTimeout           time.Duration
	publishTimeout            time.Duration
	healthCheckInterval       time.Duration
	healthCheckTimeout        time.Duration
	publishStalenessThreshold time.Duration

	metrics *busMetrics
	logger  zerolog.Logger
}

// Compile-time interface check
var _ Bus = (*valkeyBus)(nil)

// --- Constructor ---

// newValkeyBus creates a new Valkey-based broadcast bus with per-tenant channel isolation.
// Config values MUST be validated (e.g., via ServerConfig.Validate()) before calling.
func newValkeyBus(cfg Config, logger zerolog.Logger) (*valkeyBus, error) {
	vcfg := cfg.Valkey

	if len(vcfg.Addrs) == 0 {
		return nil, errors.New("valkey: at least one address is required (VALKEY_ADDRS)")
	}

	busLogger := logger.With().
		Str("component", "broadcast_bus").
		Str("backend", platform.BroadcastTypeValkey).
		Logger()

	// Build TLS config for managed Valkey/Redis services
	var tlsCfg *tls.Config
	if vcfg.TLSEnabled {
		tlsCfg = &tls.Config{
			InsecureSkipVerify: vcfg.TLSInsecure, //nolint:gosec // Controlled by configuration for dev/testing environments
			MinVersion:         tls.VersionTLS12,
		}
		if vcfg.TLSCAPath != "" {
			caCert, err := os.ReadFile(vcfg.TLSCAPath)
			if err != nil {
				return nil, fmt.Errorf("valkey broadcast: read CA cert: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("valkey broadcast: parse CA cert from %s", vcfg.TLSCAPath)
			}
			tlsCfg.RootCAs = pool
		}
		busLogger.Info().
			Bool("insecure", vcfg.TLSInsecure).
			Str("ca_path", vcfg.TLSCAPath).
			Msg("Valkey broadcast TLS enabled")
	}

	opt := valkey.ClientOption{
		InitAddress:      vcfg.Addrs,
		Password:         vcfg.Password,
		SelectDB:         vcfg.DB,
		TLSConfig:        tlsCfg,
		ConnWriteTimeout: vcfg.WriteTimeout,
		// The bus only publishes and (un)subscribes — it issues no cached reads
		// (no DoCache/DoMultiCache anywhere in this package), so client-side
		// caching buys nothing. Leaving it on makes the client negotiate
		// CLIENT TRACKING on every connection, which also bars any server that
		// does not implement it.
		DisableCache: true,
	}

	if platform.UseValkeySentinel(vcfg.Addrs, vcfg.MasterName) {
		opt.Sentinel = valkey.SentinelOption{MasterSet: vcfg.MasterName}
		busLogger.Info().
			Str("mode", "sentinel").
			Strs("sentinel_addrs", vcfg.Addrs).
			Str("master_name", vcfg.MasterName).
			Msg("Connecting to Valkey Sentinel")
	} else {
		busLogger.Info().
			Str("mode", "direct").
			Str("addr", vcfg.Addrs[0]).
			Msg("Connecting to Valkey (direct mode)")
	}

	client, err := valkey.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("valkey: failed to create client: %w", err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), vcfg.StartupPingTimeout)
	defer pingCancel()
	if err := client.Do(pingCtx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		return nil, fmt.Errorf("valkey: failed to connect: %w", err)
	}

	busLogger.Info().
		Str("channel_prefix", vcfg.Channel).
		Int("buffer_size", cfg.BufferSize).
		Msg("Successfully connected to Valkey")

	busCtx, busCancel := context.WithCancel(context.Background())

	b := &valkeyBus{
		client:                    client,
		channelPrefix:             vcfg.Channel,
		bufferSize:                cfg.BufferSize,
		limits:                    cfg.Limits,
		tenantSubscribers:         make(map[string][]subscriberEntry),
		subRefCounts:              make(map[string]int),
		subWakeCh:                 make(chan struct{}, 1),
		confirmedTenants:          make(map[string]struct{}),
		shutdownTimeout:           cfg.ShutdownTimeout,
		publishTimeout:            vcfg.PublishTimeout,
		healthCheckInterval:       vcfg.HealthCheckInterval,
		healthCheckTimeout:        vcfg.HealthCheckTimeout,
		publishStalenessThreshold: vcfg.PublishStalenessThreshold,
		ctx:                       busCtx,
		cancel:                    busCancel,
		logger:                    busLogger,
	}

	// Use provided registerer from Config if present, else default.
	reg := prometheus.DefaultRegisterer
	if cfg.Registerer != nil {
		reg = cfg.Registerer
	}
	b.metrics = newBusMetrics(reg)
	b.publishHealthy.Store(true)
	b.lastPublish.Store(time.Now().Unix())

	return b, nil
}

// --- Publish ---

// Publish sends a message to the per-tenant Valkey pub/sub channel.
// Validates TenantID before publishing (non-empty, no separator character).
//
// Fail-fast (see the Bus interface contract): a failed PUBLISH is logged,
// counted, and returned wrapped in ErrPublishUnavailable — never buffered or
// retried here. Retry policy belongs to the caller: the Kafka consumer retries
// in place to preserve at-least-once delivery, while other callers surface the
// error or deliberately drop.
func (b *valkeyBus) Publish(msg *Message) error {
	if msg.TenantID == "" {
		b.logger.Error().
			Str("subject", msg.Subject).
			Msg("broadcast: publish rejected: empty tenant ID")
		b.metrics.droppedTotal.WithLabelValues(metricTenantLabelEmpty).Inc()
		return ErrEmptyTenantID
	}
	if strings.Contains(msg.TenantID, valkeyChannelSeparator) {
		b.logger.Error().
			Str(logging.LogKeyTenantSlug, msg.TenantID).
			Str("subject", msg.Subject).
			Msg("broadcast: publish rejected: tenant ID contains separator character")
		b.metrics.droppedTotal.WithLabelValues(metricTenantLabelInvalid).Inc()
		return ErrInvalidTenantID
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		b.logger.Error().
			Err(err).
			Str("subject", msg.Subject).
			Msg("broadcast: failed to serialize message")
		b.publishErrors.Add(1)
		b.metrics.publishFailuresTotal.Inc()
		// Permanent reject: retrying an unserializable message cannot succeed,
		// so this is deliberately NOT wrapped in ErrPublishUnavailable.
		return fmt.Errorf("broadcast: serialize message for subject %q: %w", msg.Subject, err)
	}

	ch := tenantChannel(b.channelPrefix, msg.TenantID)

	ctx, cancel := context.WithTimeout(b.ctx, b.publishTimeout)
	defer cancel()

	if err := b.client.Do(ctx, b.client.B().Publish().Channel(ch).Message(string(payload)).Build()).Error(); err != nil {
		b.logger.Error().
			Err(err).
			Str("channel", ch).
			Str("subject", msg.Subject).
			Msg("broadcast: failed to publish message to Valkey")
		b.publishErrors.Add(1)
		b.metrics.publishFailuresTotal.Inc()
		b.publishHealthy.Store(false)
		return fmt.Errorf("%w: publish to %q: %w", ErrPublishUnavailable, ch, err)
	}

	b.lastPublish.Store(time.Now().Unix())
	b.publishHealthy.Store(true)
	return nil
}

// --- Subscribe / Unsubscribe ---

// Subscribe returns a new independent buffered channel for the given tenant.
// Multiple callers with the same tenantID each get their own channel.
// Safe to call at any time after construction — before or after Run().
func (b *valkeyBus) Subscribe(tenantID string) (<-chan *Message, error) {
	if tenantID == "" {
		return nil, ErrEmptyTenantID
	}
	if strings.Contains(tenantID, valkeyChannelSeparator) {
		return nil, ErrInvalidTenantID
	}

	// The channel is bidirectional internally so fanOut can send to it.
	// The caller receives it as <-chan *Message (receive-only) via the interface.
	ch := make(chan *Message, b.bufferSize)
	entry := subscriberEntry{ch: ch}

	b.subMu.Lock()
	b.tenantSubscribers[tenantID] = append(b.tenantSubscribers[tenantID], entry)
	b.subMu.Unlock()

	b.subRefMu.Lock()
	b.subRefCounts[tenantID]++
	isFirst := b.subRefCounts[tenantID] == 1
	if isFirst {
		b.noteDesiredChangedLocked()
	}
	b.subRefMu.Unlock()

	if isFirst {
		b.signalSubscriptionChange()
	}

	return ch, nil
}

// SubscribeAll returns a channel that receives messages for ALL tenants via PSUBSCRIBE.
// Used exclusively by history/writer.go.
func (b *valkeyBus) SubscribeAll() (<-chan *Message, error) {
	// Buffer = min(bufferSize × MaxTenants, BroadcastBufferSizeMax)
	var bufSize int
	if license.IsUnlimited(b.limits.MaxTenants) || b.limits.MaxTenants == 0 {
		bufSize = platform.BroadcastBufferSizeMax
	} else {
		product := b.bufferSize * b.limits.MaxTenants
		bufSize = min(product, platform.BroadcastBufferSizeMax)
	}
	if bufSize < b.bufferSize {
		bufSize = b.bufferSize // minimum = one tenant's worth
	}

	ch := make(chan *Message, bufSize)
	entry := subscriberEntry{ch: ch}

	b.subMu.Lock()
	b.allSubscribers = append(b.allSubscribers, entry)
	b.subMu.Unlock()

	b.subRefMu.Lock()
	b.subscribeAllRefCount++
	isFirst := b.subscribeAllRefCount == 1
	if isFirst {
		b.noteDesiredChangedLocked()
	}
	b.subRefMu.Unlock()

	if isFirst {
		b.signalSubscriptionChange()
	}

	return ch, nil
}

// Unsubscribe removes the subscriber entry for (tenantID, ch).
func (b *valkeyBus) Unsubscribe(tenantID string, ch <-chan *Message) error {
	b.subMu.Lock()
	entries := b.tenantSubscribers[tenantID]
	found := false
	for i, e := range entries {
		if e.ch == ch {
			b.tenantSubscribers[tenantID] = append(entries[:i], entries[i+1:]...)
			if len(b.tenantSubscribers[tenantID]) == 0 {
				delete(b.tenantSubscribers, tenantID)
			}
			found = true
			break
		}
	}
	b.subMu.Unlock()

	if !found {
		return ErrSubscriberNotFound
	}

	b.subRefMu.Lock()
	b.subRefCounts[tenantID]--
	isLast := b.subRefCounts[tenantID] == 0
	if isLast {
		delete(b.subRefCounts, tenantID)
		b.noteDesiredChangedLocked()
	}
	b.subRefMu.Unlock()

	if isLast {
		b.signalSubscriptionChange()
	}

	return nil
}

// UnsubscribeAll removes the SubscribeAll subscriber entry.
func (b *valkeyBus) UnsubscribeAll(ch <-chan *Message) error {
	b.subMu.Lock()
	found := false
	for i, e := range b.allSubscribers {
		if e.ch == ch {
			b.allSubscribers = append(b.allSubscribers[:i], b.allSubscribers[i+1:]...)
			found = true
			break
		}
	}
	b.subMu.Unlock()

	if !found {
		return ErrSubscriberNotFound
	}

	b.subRefMu.Lock()
	b.subscribeAllRefCount--
	isLast := b.subscribeAllRefCount == 0
	if isLast {
		b.noteDesiredChangedLocked()
	}
	b.subRefMu.Unlock()

	if isLast {
		b.signalSubscriptionChange()
	}

	return nil
}

// --- Fan-out ---

// fanOutTenant delivers a message to all per-tenant subscriber channels.
// Called from the OnMessage hook (m.Pattern == "") for SUBSCRIBE events.
func (b *valkeyBus) fanOutTenant(msg *Message) {
	b.subMu.RLock()
	entries := b.tenantSubscribers[msg.TenantID]
	if len(entries) == 0 {
		b.subMu.RUnlock()
		return
	}
	snapshot := make([]subscriberEntry, len(entries))
	copy(snapshot, entries)
	b.subMu.RUnlock()

	b.messagesRecv.Add(1)
	for _, e := range snapshot {
		select {
		case e.ch <- msg:
		default:
			b.metrics.droppedTotal.WithLabelValues(msg.TenantID).Inc()
		}
	}
}

// fanOutAll delivers a message to all SubscribeAll subscribers.
// Called from the OnMessage hook (m.Pattern != "") for PSUBSCRIBE events.
func (b *valkeyBus) fanOutAll(msg *Message) {
	b.subMu.RLock()
	if len(b.allSubscribers) == 0 {
		b.subMu.RUnlock()
		return
	}
	snapshot := make([]subscriberEntry, len(b.allSubscribers))
	copy(snapshot, b.allSubscribers)
	b.subMu.RUnlock()

	b.messagesRecv.Add(1)
	for _, e := range snapshot {
		select {
		case e.ch <- msg:
		default:
			b.metrics.droppedTotal.WithLabelValues(metricTenantLabelAll).Inc()
		}
	}
}

// --- Dedicated client (SetPubSubHooks) ---

// initDedicatedClient obtains a dedicated Valkey connection for pub/sub hooks.
// Only called from subscriptionMgmtLoop — no concurrent access to these fields.
func (b *valkeyBus) initDedicatedClient() {
	dc, cancel := b.client.Dedicate()
	b.dedicatedClient = dc
	b.dedicatedCancel = cancel

	prefix := b.channelPrefix
	disconnectCh := dc.SetPubSubHooks(valkey.PubSubHooks{
		// OnMessage handles both SUBSCRIBE ("message") and PSUBSCRIBE ("pmessage") events.
		// m.Pattern is empty for SUBSCRIBE events; non-empty for PSUBSCRIBE events.
		OnMessage: func(m valkey.PubSubMessage) {
			var msg Message
			if err := json.Unmarshal([]byte(m.Message), &msg); err != nil {
				b.logger.Error().
					Err(err).
					Str("channel", m.Channel).
					Msg("broadcast: failed to deserialize Valkey message")
				return
			}

			// Extract tenantID from channel name by stripping prefix+separator.
			after, found := strings.CutPrefix(m.Channel, prefix+valkeyChannelSeparator)
			if !found || after == "" {
				b.logger.Warn().Str("channel", m.Channel).Msg("broadcast: received message on unexpected channel")
				return
			}
			msg.TenantID = after

			if m.Pattern != "" {
				// PSUBSCRIBE path → deliver to SubscribeAll subscribers only
				b.fanOutAll(&msg)
			} else {
				// SUBSCRIBE path → deliver to per-tenant subscribers only
				b.fanOutTenant(&msg)
			}
		},
	})
	b.disconnectCh = disconnectCh
}

// reinitDedicatedClient releases the current dedicated connection and obtains a new one.
// A fresh connection has no subscriptions, so the confirmed state is wiped wholesale and
// the generation is bumped: convergence reads false until a full pass completes against
// the new connection, even though the desired set itself did not change.
func (b *valkeyBus) reinitDedicatedClient() {
	if b.dedicatedCancel != nil {
		b.dedicatedCancel()
	}
	b.initDedicatedClient()

	clear(b.confirmedTenants)
	b.confirmedAll = false
	b.updateEstablished()
	b.desiredGen.Add(1)
}

// --- Subscription management goroutine ---

// noteDesiredChangedLocked records a desired-state change. Callers MUST hold
// subRefMu: bumping the generation inside the same critical section as the map
// change is what lets converge snapshot (generation, desired set) atomically.
// It also keeps the desired gauge exact under concurrent mutation.
func (b *valkeyBus) noteDesiredChangedLocked() {
	b.desiredGen.Add(1)
	n := len(b.subRefCounts)
	if b.subscribeAllRefCount > 0 {
		n++
	}
	b.metrics.subscriptionsDesired.Set(float64(n))
}

// signalSubscriptionChange wakes the subscription management goroutine.
//
// This is the crux of the declared-state design (ADR-0016): the wakeup channel
// has capacity 1 and carries no data — it means "desired state changed,
// re-derive". When the non-blocking send finds the slot already occupied,
// discarding the signal is CORRECT, not lossy: the pending wakeup causes a
// full re-read of the desired state, which already includes this change.
// Losslessness is structural — there is no bounded queue of commands to
// overflow, so no burst of changes and no reconnect storm can drop
// subscription state.
func (b *valkeyBus) signalSubscriptionChange() {
	select {
	case b.subWakeCh <- struct{}{}:
	default: // a wakeup is already pending; it will observe this change too
	}
}

// subscriptionMgmtLoop is the only goroutine that issues Valkey SUBSCRIBE/UNSUBSCRIBE
// commands. Its sole job is to make the confirmed subscription state match the
// desired state (ADR-0016): every trigger — wakeup, disconnect, retry timer,
// reconcile tick — funnels into the same full convergence pass. A failed pass is
// retried with capped exponential backoff (§IV). It uses a nested-select pattern
// to give ctx.Done() effective priority.
func (b *valkeyBus) subscriptionMgmtLoop() {
	defer logging.RecoverPanic(b.logger, panicComponentSubscriptionMgr, nil)

	reconcileTicker := time.NewTicker(reconcileTickInterval)
	defer reconcileTicker.Stop()

	backoff := retryInitialBackoff
	var retryCh <-chan time.Time // nil (never fires) while no failed pass is pending

	for {
		// Priority shutdown check at the top of each iteration.
		select {
		case <-b.ctx.Done():
			return
		default:
		}

		backstop := false
		select {
		case <-b.ctx.Done():
			return

		case err := <-b.disconnectCh:
			b.logger.Error().Err(err).Msg("broadcast: Valkey pub/sub disconnected, reconnecting")
			b.reinitDedicatedClient()

		case <-b.subWakeCh:
			// Desired state changed — fall through to converge.

		case <-retryCh:
			// Backoff elapsed after a failed pass — try again.

		case <-reconcileTicker.C:
			// Periodic backstop: normally a no-op pass; anything it has to
			// issue is divergence the event-driven path missed and is counted
			// in reconcileCorrectionsTotal.
			backstop = true
		}

		if b.converge(backstop) {
			retryCh = nil // discard any pending retry — the state is converged
			backoff = retryInitialBackoff
		} else {
			retryCh = time.After(backoff)
			backoff = min(backoff*2, retryMaxBackoff)
		}
	}
}

// converge performs one full convergence pass: it snapshots the desired state,
// diffs it against the confirmed state, and issues exactly the SUBSCRIBE /
// UNSUBSCRIBE / PSUBSCRIBE / PUNSUBSCRIBE commands needed to close the gap.
// Only called from subscriptionMgmtLoop. Returns true when confirmed fully
// matches the snapshotted desired state.
//
// Individual SERVER-CLASS command failures (the connection is alive and the
// server answered with an error reply) do not abort the pass — every other
// divergence is still acted on (one tenant's failure can never block another
// tenant's convergence, and no success ever cancels another tenant's pending
// retry). A TRANSPORT-CLASS failure (dead or unresponsive connection) aborts
// the pass after the first failing command: every subsequent command would
// fail identically, so aborting bounds a disconnect storm's per-cycle cost at
// one command and one log line instead of one per tenant. Nothing is lost —
// the caller schedules a backoff retry that re-derives the full diff.
// backstop marks a reconcile-tick-triggered pass: commands issued then are
// counted as corrections.
func (b *valkeyBus) converge(backstop bool) bool {
	// Snapshot generation and desired set in one critical section; no I/O is
	// performed under the lock (§VII).
	b.subRefMu.Lock()
	gen := b.desiredGen.Load()
	desired := make(map[string]struct{}, len(b.subRefCounts))
	for tid := range b.subRefCounts {
		desired[tid] = struct{}{}
	}
	wantAll := b.subscribeAllRefCount > 0
	b.subRefMu.Unlock()

	ok := true
	abort := false // set on a transport-class failure: skip the rest of the pass

	// handleFailure records the failure and classifies it: an error that is
	// not a Valkey server reply means the connection itself is broken.
	// errors.AsType (not valkey.IsValkeyErr, which is a bare type assertion)
	// because issueSubCommand wraps the error for context (§III).
	handleFailure := func(err error, command, tid string) {
		b.recordSubCommandFailure(err, command, tid)
		ok = false
		if _, serverReply := errors.AsType[*valkey.ValkeyError](err); !serverReply {
			abort = true
		}
	}

	// Missing: desired but not confirmed → SUBSCRIBE.
	for tid := range desired {
		if abort || b.ctx.Err() != nil {
			break
		}
		if _, confirmed := b.confirmedTenants[tid]; confirmed {
			continue
		}
		err := b.issueSubCommand(
			b.dedicatedClient.B().Subscribe().Channel(tenantChannel(b.channelPrefix, tid)).Build(),
		)
		if err != nil {
			handleFailure(err, "SUBSCRIBE", tid)
			continue
		}
		b.confirmedTenants[tid] = struct{}{}
		b.recordSubCommandSuccess(backstop)
	}

	// Stale: confirmed but no longer desired → UNSUBSCRIBE (unsubscribes are
	// honored by diffing, never by replaying events).
	for tid := range b.confirmedTenants {
		if abort || b.ctx.Err() != nil {
			break
		}
		if _, want := desired[tid]; want {
			continue
		}
		err := b.issueSubCommand(
			b.dedicatedClient.B().Unsubscribe().Channel(tenantChannel(b.channelPrefix, tid)).Build(),
		)
		if err != nil {
			handleFailure(err, "UNSUBSCRIBE", tid)
			continue
		}
		delete(b.confirmedTenants, tid)
		b.recordSubCommandSuccess(backstop)
	}

	// All-tenant pattern subscription (SubscribeAll).
	if !abort && wantAll && !b.confirmedAll && b.ctx.Err() == nil {
		err := b.issueSubCommand(
			b.dedicatedClient.B().Psubscribe().Pattern(tenantChannelPattern(b.channelPrefix)).Build(),
		)
		if err != nil {
			handleFailure(err, "PSUBSCRIBE", "")
		} else {
			b.confirmedAll = true
			b.recordSubCommandSuccess(backstop)
		}
	}
	if !abort && !wantAll && b.confirmedAll && b.ctx.Err() == nil {
		err := b.issueSubCommand(
			b.dedicatedClient.B().Punsubscribe().Pattern(tenantChannelPattern(b.channelPrefix)).Build(),
		)
		if err != nil {
			handleFailure(err, "PUNSUBSCRIBE", "")
		} else {
			b.confirmedAll = false
			b.recordSubCommandSuccess(backstop)
		}
	}

	if b.ctx.Err() != nil {
		// Shutting down: skip the bookkeeping, the loop exits on its next check.
		return false
	}

	b.updateEstablished()

	if ok {
		// Confirmed now matches the desired state as of gen. If the desired
		// state changed while this pass ran, a wakeup is already pending and
		// the next pass will advance the generation again — convergence may
		// read transiently false, never falsely true.
		b.confirmedGen.Store(gen)
	}
	return ok
}

// issueSubCommand sends one subscription command on the dedicated client with a
// bounded timeout. Only called from subscriptionMgmtLoop.
func (b *valkeyBus) issueSubCommand(cmd valkey.Completed) error {
	ctx, cancel := context.WithTimeout(b.ctx, subCommandTimeout)
	defer cancel()
	if err := b.dedicatedClient.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("broadcast: subscription command: %w", err)
	}
	return nil
}

// recordSubCommandSuccess counts a successfully issued subscription command.
// Commands issued by a reconcile-backstop pass are divergence the event-driven
// path missed, counted separately as corrections.
func (b *valkeyBus) recordSubCommandSuccess(backstop bool) {
	b.metrics.subscribeCommandsTotal.WithLabelValues(metricResultSuccess).Inc()
	if backstop {
		b.metrics.reconcileCorrectionsTotal.Inc()
	}
}

// recordSubCommandFailure logs and counts a failed subscription command. The
// failure leaves the confirmed state untouched, so the next convergence pass
// (scheduled with backoff by the caller) retries it.
func (b *valkeyBus) recordSubCommandFailure(err error, command, tenantID string) {
	b.logger.Error().
		Err(err).
		Str("command", command).
		Str(logging.LogKeyTenantSlug, tenantID).
		Msg("broadcast: subscription command failed, convergence pass will retry")
	b.metrics.subscribeCommandsTotal.WithLabelValues(metricResultRetry).Inc()
}

// updateEstablished refreshes the established gauge and its lock-free mirror
// from the confirmed state. Only called from subscriptionMgmtLoop.
func (b *valkeyBus) updateEstablished() {
	n := int64(len(b.confirmedTenants))
	if b.confirmedAll {
		n++
	}
	b.establishedCount.Store(n)
	b.metrics.subscriptionsEstablished.Set(float64(n))
}

// --- Lifecycle ---

// Run starts the subscription management loop and health monitoring.
func (b *valkeyBus) Run() {
	b.initDedicatedClient()

	b.logger.Info().
		Str("channel_prefix", b.channelPrefix).
		Msg("BroadcastBus started (Valkey Pub/Sub, per-tenant channels)")

	b.wg.Go(b.subscriptionMgmtLoop)
	b.wg.Go(b.healthCheckLoop)
}

// Shutdown gracefully stops the bus with default timeout.
func (b *valkeyBus) Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), b.shutdownTimeout)
	defer cancel()
	b.ShutdownWithContext(ctx)
}

// ShutdownWithContext gracefully stops the bus using the provided context.
// Subscriber channels are NOT closed — the shard (channel owner) handles their lifetime.
func (b *valkeyBus) ShutdownWithContext(ctx context.Context) {
	b.logger.Info().Msg("Shutting down BroadcastBus")

	b.cancel()

	waitDone := make(chan struct{})
	var twg sync.WaitGroup
	// Use twg.Go (Go 1.25+) — manual wg.Add(1)+go is banned per §VII.
	twg.Go(func() {
		defer logging.RecoverPanic(b.logger, "bus_shutdown_wait", nil)
		b.wg.Wait()
		close(waitDone)
	})

	select {
	case <-waitDone:
		b.logger.Info().Msg("All BroadcastBus goroutines stopped")
	case <-ctx.Done():
		b.logger.Warn().Msg("BroadcastBus shutdown timeout, forcing exit")
	}

	// Ensure the timeout wrapper goroutine exits before returning (prevents goleak failures).
	twg.Wait()

	if b.dedicatedCancel != nil {
		b.dedicatedCancel()
	}
	b.client.Close()

	// Subscriber channels are NOT closed here.
	// The shard (write-side owner) manages channel lifetime via context cancellation.
	b.logger.Info().Msg("BroadcastBus shutdown complete")
}

// subscriptionsConverged reports whether the confirmed subscription state
// matches the desired state (ADR-0016). Lock-free: both generations are
// atomics. May read transiently false while a convergence pass is pending or
// in flight; never reads true while confirmed differs from desired.
func (b *valkeyBus) subscriptionsConverged() bool {
	return b.confirmedGen.Load() == b.desiredGen.Load()
}

// IsHealthy returns true if the Valkey connection is operational: the publish
// path is healthy AND this pod's subscriptions have converged. The two signals
// are tracked separately (§XV) — a successful publish or ping never masks a
// missing subscription. Callers using this for liveness/readiness decisions:
// subscription non-convergence is a DEGRADED condition — it must never fail a
// Kubernetes probe (see handleHealth in internal/server).
func (b *valkeyBus) IsHealthy() bool {
	if !b.publishHealthy.Load() || !b.subscriptionsConverged() {
		return false
	}

	lastPub := b.lastPublish.Load()
	if lastPub > 0 && b.publishStalenessThreshold > 0 &&
		time.Since(time.Unix(lastPub, 0)) > b.publishStalenessThreshold {
		if b.logger.GetLevel() <= zerolog.DebugLevel {
			b.logger.Debug().
				Dur("since_last_publish", time.Since(time.Unix(lastPub, 0))).
				Msg("No recent Valkey publish (might be normal)")
		}
	}

	return true
}

// GetMetrics returns current bus metrics.
func (b *valkeyBus) GetMetrics() Metrics {
	lastPubTime := time.Unix(b.lastPublish.Load(), 0)
	var lastPubAgo float64
	if !lastPubTime.IsZero() && lastPubTime.Unix() > 0 {
		lastPubAgo = time.Since(lastPubTime).Seconds()
	} else {
		lastPubAgo = -1
	}

	// Lock ordering (§VII): subMu and subRefMu are taken strictly sequentially,
	// never nested — each is released before the other is acquired.
	b.subMu.RLock()
	subscriberCount := len(b.allSubscribers)
	for _, entries := range b.tenantSubscribers {
		subscriberCount += len(entries)
	}
	b.subMu.RUnlock()

	b.subRefMu.Lock()
	desired := len(b.subRefCounts)
	if b.subscribeAllRefCount > 0 {
		desired++
	}
	b.subRefMu.Unlock()

	return Metrics{
		Type:                     platform.BroadcastTypeValkey,
		Healthy:                  b.IsHealthy(),
		PublishHealthy:           b.publishHealthy.Load(),
		SubscriptionsConverged:   b.subscriptionsConverged(),
		SubscriptionsDesired:     desired,
		SubscriptionsEstablished: int(b.establishedCount.Load()),
		ChannelPrefix:            b.channelPrefix,
		Subscribers:              subscriberCount,
		PublishErrors:            b.publishErrors.Load(),
		MessagesReceived:         b.messagesRecv.Load(),
		LastPublishAgo:           lastPubAgo,
		LastPublishTime:          lastPubTime,
	}
}

// healthCheckLoop periodically pings Valkey to verify connectivity.
func (b *valkeyBus) healthCheckLoop() {
	defer logging.RecoverPanic(b.logger, "valkeyBus.healthCheckLoop", nil)

	ticker := time.NewTicker(b.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(b.ctx, b.healthCheckTimeout)
			err := b.client.Do(ctx, b.client.B().Ping().Build()).Error()
			cancel()

			if err != nil {
				b.logger.Error().Err(err).Msg("Valkey health check failed")
				b.publishHealthy.Store(false)
			} else {
				if b.logger.GetLevel() <= zerolog.DebugLevel {
					b.logger.Debug().Msg("Valkey health check passed")
				}
				b.publishHealthy.Store(true)
			}
		}
	}
}
