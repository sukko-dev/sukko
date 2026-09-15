package broadcast

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

	subCmdChCapacity      = 64
	retryInitialBackoff   = 100 * time.Millisecond
	retryMaxBackoff       = 5 * time.Second
	reconcileTickInterval = 30 * time.Second
	timerNeverFires       = time.Duration(math.MaxInt64)

	panicComponentSubscriptionMgr = "subscription_management_goroutine"
)

// subCmdKind identifies the type of subscription command.
type subCmdKind uint8

const (
	subCmdSubscribe    subCmdKind = iota // issue SUBSCRIBE {prefix}:{tenantID}
	subCmdUnsubscribe                    // issue UNSUBSCRIBE {prefix}:{tenantID}
	subCmdPSubscribe                     // issue PSUBSCRIBE {prefix}:* (SubscribeAll)
	subCmdPUnsubscribe                   // issue PUNSUBSCRIBE {prefix}:* (UnsubscribeAll)
)

// subCmd is a command sent to the subscription management goroutine.
type subCmd struct {
	kind     subCmdKind
	tenantID string // empty for P{UN}SUBSCRIBE commands
}

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

	// Pod-level ref counts for Valkey subscription lifecycle. Protected by subRefMu.
	// SUBSCRIBE/UNSUBSCRIBE enqueued AFTER releasing subRefMu.
	subRefCounts         map[string]int
	subscribeAllRefCount int
	subRefMu             sync.Mutex

	// Subscription command channel — read exclusively by subscriptionMgmtLoop.
	subCmdCh chan subCmd

	// Dedicated connection for SetPubSubHooks. Written only by subscriptionMgmtLoop.
	dedicatedClient valkey.DedicatedClient
	dedicatedCancel func()
	disconnectCh    <-chan error

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Health tracking (atomic for lock-free reads)
	healthy       atomic.Bool
	lastPublish   atomic.Int64
	publishErrors atomic.Uint64
	messagesRecv  atomic.Uint64

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
		subCmdCh:                  make(chan subCmd, subCmdChCapacity),
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
	b.metrics = newBusMetrics(reg)
	b.healthy.Store(true)
	b.lastPublish.Store(time.Now().Unix())

	return b, nil
}

// --- Publish ---

// Publish sends a message to the per-tenant Valkey pub/sub channel.
// Validates TenantID before publishing (non-empty, no separator character).
func (b *valkeyBus) Publish(msg *Message) {
	if msg.TenantID == "" {
		b.logger.Error().
			Str("subject", msg.Subject).
			Msg("broadcast: publish rejected: empty tenant ID")
		b.metrics.droppedTotal.WithLabelValues(metricTenantLabelEmpty).Inc()
		return
	}
	if strings.Contains(msg.TenantID, valkeyChannelSeparator) {
		b.logger.Error().
			Str(logging.LogKeyTenantSlug, msg.TenantID).
			Str("subject", msg.Subject).
			Msg("broadcast: publish rejected: tenant ID contains separator character")
		b.metrics.droppedTotal.WithLabelValues(metricTenantLabelInvalid).Inc()
		return
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		b.logger.Error().
			Err(err).
			Str("subject", msg.Subject).
			Msg("broadcast: failed to serialize message")
		b.publishErrors.Add(1)
		return
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
		b.healthy.Store(false)
		return
	}

	b.lastPublish.Store(time.Now().Unix())
	b.healthy.Store(true)
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
	b.subRefMu.Unlock()

	if isFirst {
		select {
		case b.subCmdCh <- subCmd{kind: subCmdSubscribe, tenantID: tenantID}:
		default:
			b.logger.Warn().
				Str(logging.LogKeyTenantSlug, tenantID).
				Msg("broadcast: subCmdCh full, SUBSCRIBE enqueue dropped (reconciliation will recover)")
			b.metrics.subscribeCommandsTotal.WithLabelValues(metricResultDropped).Inc()
		}
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
	b.subRefMu.Unlock()

	if isFirst {
		select {
		case b.subCmdCh <- subCmd{kind: subCmdPSubscribe}:
		default:
			b.logger.Warn().Msg("broadcast: subCmdCh full, PSUBSCRIBE enqueue dropped")
		}
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
	}
	b.subRefMu.Unlock()

	if isLast {
		select {
		case b.subCmdCh <- subCmd{kind: subCmdUnsubscribe, tenantID: tenantID}:
		default:
			b.logger.Warn().
				Str(logging.LogKeyTenantSlug, tenantID).
				Msg("broadcast: subCmdCh full, UNSUBSCRIBE enqueue dropped (reconciliation will recover)")
			b.metrics.subscribeCommandsTotal.WithLabelValues(metricResultDropped).Inc()
		}
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
	b.subRefMu.Unlock()

	if isLast {
		select {
		case b.subCmdCh <- subCmd{kind: subCmdPUnsubscribe}:
		default:
			b.logger.Warn().Msg("broadcast: subCmdCh full, PUNSUBSCRIBE enqueue dropped")
		}
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
func (b *valkeyBus) reinitDedicatedClient() {
	if b.dedicatedCancel != nil {
		b.dedicatedCancel()
	}
	b.initDedicatedClient()
}

// --- Subscription management goroutine ---

// subscriptionMgmtLoop is the only goroutine that issues Valkey SUBSCRIBE/UNSUBSCRIBE
// commands. It uses a nested-select pattern to give ctx.Done() effective priority.
func (b *valkeyBus) subscriptionMgmtLoop() {
	defer logging.RecoverPanic(b.logger, panicComponentSubscriptionMgr, nil)

	retryTimer := time.NewTimer(timerNeverFires)
	defer retryTimer.Stop()
	reconcileTicker := time.NewTicker(reconcileTickInterval)
	defer reconcileTicker.Stop()

	// pendingKind tracks in-flight retry commands (tenantID → kind).
	// Latest-wins: a new command for the same tenant supersedes any pending retry.
	pendingKind := make(map[string]subCmdKind)
	var retryTenantID string
	var retryKind subCmdKind
	currentBackoff := retryInitialBackoff

	for {
		// Priority shutdown check at the top of each iteration.
		select {
		case <-b.ctx.Done():
			return
		default:
		}

		select {
		case <-b.ctx.Done():
			return

		case err := <-b.disconnectCh:
			b.logger.Error().Err(err).Msg("broadcast: Valkey pub/sub disconnected, reconnecting")
			b.healthy.Store(false)
			b.reinitDedicatedClient()
			b.resubscribeAll()

		case cmd := <-b.subCmdCh:
			// Latest-wins: if there is a pending retry for the same tenant, supersede it.
			if cmd.tenantID != "" {
				if _, hasPending := pendingKind[cmd.tenantID]; hasPending {
					pendingKind[cmd.tenantID] = cmd.kind
					if retryTenantID == cmd.tenantID {
						if !retryTimer.Stop() {
							select {
							case <-retryTimer.C:
							default:
							}
						}
						retryTimer.Reset(timerNeverFires)
						retryTenantID = ""
					}
					continue
				}
			}
			b.issueValkeyCommand(cmd, retryTimer, &retryTenantID, &retryKind, &currentBackoff, pendingKind)

		case <-retryTimer.C:
			if retryTenantID == "" && retryKind != subCmdPSubscribe && retryKind != subCmdPUnsubscribe {
				b.logger.Warn().Msg("broadcast: retry timer fired with no pending retry")
				continue
			}
			b.issueValkeyCommand(
				subCmd{kind: retryKind, tenantID: retryTenantID},
				retryTimer, &retryTenantID, &retryKind, &currentBackoff, pendingKind,
			)

		case <-reconcileTicker.C:
			b.reconcile()
		}
	}
}

// issueValkeyCommand sends a SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE command
// on the dedicated client. On failure: schedules retry with exponential backoff.
func (b *valkeyBus) issueValkeyCommand(
	cmd subCmd,
	retryTimer *time.Timer,
	retryTenantID *string,
	retryKind *subCmdKind,
	currentBackoff *time.Duration,
	pendingKind map[string]subCmdKind,
) {
	ctx, cancel := context.WithTimeout(b.ctx, retryMaxBackoff)
	defer cancel()

	var err error
	switch cmd.kind {
	case subCmdSubscribe:
		err = b.dedicatedClient.Do(ctx,
			b.dedicatedClient.B().Subscribe().Channel(tenantChannel(b.channelPrefix, cmd.tenantID)).Build(),
		).Error()
	case subCmdUnsubscribe:
		err = b.dedicatedClient.Do(ctx,
			b.dedicatedClient.B().Unsubscribe().Channel(tenantChannel(b.channelPrefix, cmd.tenantID)).Build(),
		).Error()
	case subCmdPSubscribe:
		err = b.dedicatedClient.Do(ctx,
			b.dedicatedClient.B().Psubscribe().Pattern(tenantChannelPattern(b.channelPrefix)).Build(),
		).Error()
	case subCmdPUnsubscribe:
		err = b.dedicatedClient.Do(ctx,
			b.dedicatedClient.B().Punsubscribe().Pattern(tenantChannelPattern(b.channelPrefix)).Build(),
		).Error()
	}

	if err == nil {
		b.metrics.subscribeCommandsTotal.WithLabelValues(metricResultSuccess).Inc()
		if cmd.tenantID != "" {
			delete(pendingKind, cmd.tenantID)
		}
		*retryTenantID = ""
		*retryKind = subCmdSubscribe // reset to zero value; prevents stale P* guard bypass on timer fire
		if !retryTimer.Stop() {
			select {
			case <-retryTimer.C:
			default:
			}
		}
		retryTimer.Reset(timerNeverFires)
		*currentBackoff = retryInitialBackoff
		return
	}

	// Schedule retry with exponential backoff.
	b.logger.Error().
		Err(err).
		Str(logging.LogKeyTenantSlug, cmd.tenantID).
		Msg("broadcast: subscription command failed, scheduling retry")
	b.metrics.subscribeCommandsTotal.WithLabelValues(metricResultRetry).Inc()

	*retryTenantID = cmd.tenantID
	*retryKind = cmd.kind
	if cmd.tenantID != "" {
		pendingKind[cmd.tenantID] = cmd.kind
	}

	retryTimer.Reset(*currentBackoff)
	*currentBackoff *= 2
	if *currentBackoff > retryMaxBackoff {
		*currentBackoff = retryMaxBackoff
	}
}

// resubscribeAll re-enqueues SUBSCRIBE for all active tenants and PSUBSCRIBE if needed.
// Called after a Valkey reconnect.
func (b *valkeyBus) resubscribeAll() {
	b.subRefMu.Lock()
	tenants := make([]string, 0, len(b.subRefCounts))
	for tid := range b.subRefCounts {
		tenants = append(tenants, tid)
	}
	hasSubscribeAll := b.subscribeAllRefCount > 0
	b.subRefMu.Unlock()

	for _, tid := range tenants {
		select {
		case b.subCmdCh <- subCmd{kind: subCmdSubscribe, tenantID: tid}:
		default:
			b.logger.Warn().
				Str(logging.LogKeyTenantSlug, tid).
				Str("reason", "reconnect_overflow").
				Msg("broadcast: subCmdCh full during reconnect; tenant will recover via reconciliation tick")
			b.metrics.subscribeCommandsTotal.WithLabelValues(metricResultDropped).Inc()
		}
	}

	if hasSubscribeAll {
		select {
		case b.subCmdCh <- subCmd{kind: subCmdPSubscribe}:
		default:
			b.logger.Warn().Msg("broadcast: subCmdCh full, PSUBSCRIBE reconnect enqueue dropped")
		}
	}
}

// reconcile checks that Valkey subscriptions match expected state and re-issues any missing ones.
// Runs periodically via reconcileTicker. All PUBSUB diagnostic queries use b.client (not dedicatedClient).
func (b *valkeyBus) reconcile() {
	b.subRefMu.Lock()
	tenants := make([]string, 0, len(b.subRefCounts))
	for tid := range b.subRefCounts {
		tenants = append(tenants, tid)
	}
	hasSubscribeAll := b.subscribeAllRefCount > 0
	b.subRefMu.Unlock()

	ctx, cancel := context.WithTimeout(b.ctx, reconcileTickInterval/2)
	defer cancel()

	for _, tid := range tenants {
		ch := tenantChannel(b.channelPrefix, tid)
		result := b.client.Do(ctx, b.client.B().PubsubNumsub().Channel(ch).Build())
		if result.Error() != nil {
			// Do NOT abort — continue to next tenant per
			b.logger.Warn().
				Err(result.Error()).
				Str(logging.LogKeyTenantSlug, tid).
				Msg("broadcast: reconcile PUBSUB NUMSUB error, skipping tenant this tick")
			continue
		}

		m, err := result.AsMap()
		if err != nil {
			b.logger.Warn().Err(err).Str(logging.LogKeyTenantSlug, tid).Msg("broadcast: reconcile PUBSUB NUMSUB parse error")
			continue
		}

		v := m[ch]
		if count, _ := v.AsInt64(); count == 0 {
			b.logger.Info().Str(logging.LogKeyTenantSlug, tid).Msg("broadcast: reconcile detected missing subscription, re-issuing")
			select {
			case b.subCmdCh <- subCmd{kind: subCmdSubscribe, tenantID: tid}:
				b.metrics.reconcileCorrectionsTotal.Inc()
			default:
			}
		}
	}

	if hasSubscribeAll {
		result := b.client.Do(ctx, b.client.B().PubsubNumpat().Build())
		if result.Error() != nil {
			b.logger.Warn().Err(result.Error()).Msg("broadcast: reconcile PUBSUB NUMPAT error, skipping SubscribeAll check this tick")
			return
		}
		if n, _ := result.AsInt64(); n == 0 {
			b.logger.Info().Msg("broadcast: reconcile detected missing PSUBSCRIBE, re-issuing")
			select {
			case b.subCmdCh <- subCmd{kind: subCmdPSubscribe}:
				b.metrics.reconcileCorrectionsTotal.Inc()
			default:
			}
		}
	}
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

// IsHealthy returns true if the Valkey connection is operational.
func (b *valkeyBus) IsHealthy() bool {
	if !b.healthy.Load() {
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

	b.subMu.RLock()
	subscriberCount := len(b.allSubscribers)
	for _, entries := range b.tenantSubscribers {
		subscriberCount += len(entries)
	}
	b.subMu.RUnlock()

	return Metrics{
		Type:             platform.BroadcastTypeValkey,
		Healthy:          b.IsHealthy(),
		ChannelPrefix:    b.channelPrefix,
		Subscribers:      subscriberCount,
		PublishErrors:    b.publishErrors.Load(),
		MessagesReceived: b.messagesRecv.Load(),
		LastPublishAgo:   lastPubAgo,
		LastPublishTime:  lastPubTime,
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
				b.healthy.Store(false)
			} else {
				if b.logger.GetLevel() <= zerolog.DebugLevel {
					b.logger.Debug().Msg("Valkey health check passed")
				}
				b.healthy.Store(true)
			}
		}
	}
}
