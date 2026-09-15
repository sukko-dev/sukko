package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/publisher"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// defaultDeliveryTimeout is the maximum time to wait for message delivery.
const defaultDeliveryTimeout = 5 * time.Second

// denyPollInterval is the cadence for polling the captured-error store while waiting for a deny
// frame. Used by the shared waitForDeny helper (deny_wait.go); fast relative to the re-issue
// interval so deny detection stays sub-100ms.
const denyPollInterval = 100 * time.Millisecond

// misrouteGraceWindow is the pause after all expected receivers have the message, giving a
// misrouted copy time to arrive at a must-NOT-receive user before buildResult snapshots
// MisroutedTo. This is the grace window the tenant-isolation negative assertion relies on
// — its value MUST NOT be reduced.
const misrouteGraceWindow = 100 * time.Millisecond

// Delivery-liveness warmup bounds (see waitForDeliveryLive). Sized for the Kafka cold-start
// window: a freshly-provisioned tenant's ws-server consumer joins its topic at AtEnd only after a
// control-plane propagation delay (WatchTopics delta → topic create → AddConsumeTopics), so early
// publishes are dropped. Direct mode returns within one interval. 45s (was 15s) because the
// consumer's periodic topic-refresh + rebalance alone can take ~30s under CI load and the
// 15s bound flaked ("delivery loop not live after 15s", observed on main's scheduled grid and
// on dispatch runs, always on the SECOND freshly-provisioned tenant); the probe exits on the
// first delivery, so a live loop still completes within one or two intervals.
const (
	deliveryWarmupTimeout  = 45 * time.Second
	deliveryWarmupInterval = 250 * time.Millisecond
)

// waitForDeliveryLive proves the full publish→(produce→consume→)broadcast loop is live for `channel`
// before a caller publishes a measured sequence, then resets the user's received tracker so the
// warmup does not pollute the caller's counts. Backend-agnostic: in direct mode the first probe
// round-trips within one interval; in Kafka mode it absorbs the new-tenant consumer cold-start.
//
// It REPUBLISHES the probe each interval rather than publishing once: in Kafka mode the first
// probe(s) may be produced before the consumer joins the tenant topic (AtEnd) and thus be dropped,
// so a single probe can be lost. Receiving any probe means the loop is live. Returns an error
// (never a silent skip) if the loop is not live within deliveryWarmupTimeout.
func waitForDeliveryLive(ctx context.Context, user *TestUser, channel string, logger zerolog.Logger) error {
	warmupID := "warmup-" + uuid.NewString()
	payload, _ := json.Marshal(map[string]any{"msg_id": warmupID}) // literal map of primitives cannot fail
	publish := func() error {
		if err := user.Client.Publish(channel, payload); err != nil {
			return fmt.Errorf("publish warmup probe on %s: %w", channel, err)
		}
		return nil
	}
	return probeUntilLive(ctx, deliveryWarmupTimeout, deliveryWarmupInterval,
		publish, func() bool { return user.HasReceived(warmupID) }, user.ClearReceived, logger)
}

// probeUntilLive is the transport-agnostic core of waitForDeliveryLive, split out so its retry /
// timeout / reset contract is unit-testable without a live WebSocket (publish/received/reset are
// injected). It publishes once, then on each interval checks for receipt (→ reset + return nil) or
// republishes. Returns an error on timeout or ctx cancel, or if publish fails.
func probeUntilLive(ctx context.Context, timeout, interval time.Duration, publish func() error, received func() bool, reset func(), logger zerolog.Logger) error {
	if err := publish(); err != nil {
		return err
	}

	deadline := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return fmt.Errorf("delivery loop not live after %s (no warmup probe received)", timeout)
		case <-ctx.Done():
			return fmt.Errorf("delivery warmup canceled: %w", ctx.Err())
		case <-ticker.C:
			if received() {
				reset()
				logger.Debug().Msg("delivery loop live (warmup probe received)")
				return nil
			}
			if err := publish(); err != nil {
				return err
			}
		}
	}
}

// PubSubEngine coordinates publishers and subscribers for delivery verification.
// Tracks message IDs (UUIDs) to verify that the right messages reach the right subscribers.
type PubSubEngine struct {
	gatewayURL string
	logger     zerolog.Logger
	timeout    time.Duration
}

// PubSubEngineConfig configures the pub-sub engine.
type PubSubEngineConfig struct {
	GatewayURL string
	Logger     zerolog.Logger
	Timeout    time.Duration // delivery timeout; defaults to 5s
}

// NewPubSubEngine creates a new delivery verification engine.
func NewPubSubEngine(cfg PubSubEngineConfig) *PubSubEngine {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultDeliveryTimeout
	}
	return &PubSubEngine{
		gatewayURL: cfg.GatewayURL,
		logger:     cfg.Logger.With().Str("component", "pubsub_engine").Logger(),
		timeout:    timeout,
	}
}

// TestUser is a WebSocket client with specific JWT claims for scoping tests.
type TestUser struct {
	Subject       string
	Groups        []string
	Token         string
	Client        *testerws.Client
	mu            sync.RWMutex
	received      map[string]receivedMsg // message ID → receipt info
	receivedOrder []string               // message IDs in arrival order
	errFrames     []ErrorFrame           // captured subscribe_error/publish_error frames (deny-check signal)
	subscribed    map[string]struct{}    // channels confirmed by subscription_ack (#242: silent filtering detection)
	controlFrames []testerws.Message     // captured recovery terminators/acks (history_complete, reconnect_ack, replay_complete, ...)
}

type receivedMsg struct {
	channel string
	at      time.Time
	// mid/pos/frameType come from the delivery frame (ADR-0008 identity, the
	// replay cursor, and whether the copy arrived as "message" vs "replay_message")
	// — the recovery suites assert identity equality across live/history/replay
	// copies and anchor replays on pos.
	mid       string
	pos       string
	frameType string
}

// ErrorFrame is a captured inbound error frame (subscribe_error / publish_error) with its
// top-level code — the observable deny signal the authorization deny checks assert on.
type ErrorFrame struct {
	Type string
	Code string
}

// SubscribedAll reports whether every channel has been confirmed by a
// subscription_ack (see onMessage; #242).
func (u *TestUser) SubscribedAll(channels []string) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	for _, ch := range channels {
		if _, ok := u.subscribed[ch]; !ok {
			return false
		}
	}
	return true
}

// waitSubscribeConfirmed re-sends subscribe until confirmed reports true —
// absorbing the channel-rule propagation window for freshly-provisioned
// tenants, where the gateway silently filters the not-yet-authorized channel
// out of the subscribe (#242: the resulting absent subscription made the
// delivery warmup dead forever). Fails loudly at the bound; a subscribe send
// error surfaces immediately (the connection is broken, retrying is noise).
func waitSubscribeConfirmed(ctx context.Context, subscribe func() error, confirmed func() bool, timeout, interval time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := subscribe(); err != nil {
			return fmt.Errorf("subscribe send: %w", err)
		}
		select {
		case <-deadline:
			return fmt.Errorf("subscribe not confirmed within %s (channel likely filtered by not-yet-propagated rules)", timeout)
		case <-ctx.Done():
			return fmt.Errorf("subscribe confirmation canceled: %w", ctx.Err())
		case <-ticker.C:
		}
		if confirmed() {
			return nil
		}
	}
}

// SubscribeConfirmed subscribes to channels and waits until a subscription_ack
// confirms every one of them (see waitSubscribeConfirmed).
func (u *TestUser) SubscribeConfirmed(ctx context.Context, channels []string, timeout, interval time.Duration) error {
	return waitSubscribeConfirmed(ctx,
		func() error { return u.Client.Subscribe(channels) },
		func() bool { return u.SubscribedAll(channels) },
		timeout, interval)
}

// HasReceived returns whether the user received a message with the given ID.
func (u *TestUser) HasReceived(msgID string) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	_, ok := u.received[msgID]
	return ok
}

// ReceivedCount returns the number of messages received.
func (u *TestUser) ReceivedCount() int {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return len(u.received)
}

// ReceivedOrder returns message IDs in the order they arrived.
func (u *TestUser) ReceivedOrder() []string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return slices.Clone(u.receivedOrder)
}

// AsPublisher returns a Publisher that publishes via this user's WebSocket connection.
// Used for authorization + delivery testing — the gateway checks this user's JWT claims.
func (u *TestUser) AsPublisher() publisher.Publisher {
	return publisher.NewClientPublisher(u.Client)
}

// ClearReceived resets the received message tracker. Call between test checks.
func (u *TestUser) ClearReceived() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.received = make(map[string]receivedMsg)
	u.receivedOrder = u.receivedOrder[:0]
}

// HasErrorMatching reports whether a captured error frame matches the exact (type, code) pair.
// Joint match: a frame with a different type OR a different code does NOT satisfy it.
func (u *TestUser) HasErrorMatching(errType, code string) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	for _, f := range u.errFrames {
		if f.Type == errType && f.Code == code {
			return true
		}
	}
	return false
}

// ClearErrors resets the captured error frames. Deny checks call it exactly once before issuing
// their isolated bad request (drain-once-before-wait). MUST NOT touch delivery state.
func (u *TestUser) ClearErrors() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.errFrames = u.errFrames[:0]
}

// acceptsDeliveryEnvelope reports whether a WebSocket message type is a delivered payload
// worth tracking for a delivery check. Delivered broadcasts arrive as {"type":"message",...}
// (the server's BroadcastEnvelope); peer-forwarded publishes as {"type":"publish",...} —
// accept BOTH, otherwise delivered messages are silently ignored and every delivery check
// reports "missing" despite successful delivery. Shared by every suite that asserts WS
// delivery (do not reimplement the predicate per-suite).
func acceptsDeliveryEnvelope(msgType string) bool {
	return msgType == "publish" || msgType == "message" || msgType == "replay_message"
}

// controlFrameTypes are the recovery-flow terminators/acks captured verbatim for
// the history and gap-recovery suites to await (see WaitControlFrame).
var controlFrameTypes = map[string]struct{}{
	"history_complete": {},
	"history_error":    {},
	"reconnect_ack":    {},
	"reconnect_error":  {},
	"replay_complete":  {},
	// Live-replay rejections have no dedicated type — the server sends a generic
	// type:"error" frame with a top-level code (replay_protocol.go, §XVIII note).
	"error": {},
}

// onMessage is the callback for incoming WebSocket messages.
// Called from the client's read loop goroutine — writes are mutex-protected.
func (u *TestUser) onMessage(msg testerws.Message) {
	// Error-capture branch FIRST, returns after capturing: subscribe_error/publish_error
	// are the deny signals the authorization checks assert on. These types are disjoint from the
	// delivery-envelope types (message/publish), so a captured error frame never reaches delivery
	// tracking — even a publish_error whose Data happens to carry a msg_id-shaped field.
	if msg.Type == respTypeSubscribeError || msg.Type == respTypePublishError {
		u.mu.Lock()
		u.errFrames = append(u.errFrames, ErrorFrame{Type: msg.Type, Code: msg.Code})
		u.mu.Unlock()
		return
	}
	// Confirmed-subscription tracking (#242): the ack lists only the channels the
	// gateway ACCEPTED — a channel silently filtered by not-yet-propagated rules
	// is absent, and subscribeConfirmed retries until it appears.
	if msg.Type == "subscription_ack" {
		u.mu.Lock()
		if u.subscribed == nil {
			u.subscribed = make(map[string]struct{}, len(msg.Subscribed))
		}
		for _, ch := range msg.Subscribed {
			u.subscribed[ch] = struct{}{}
		}
		u.mu.Unlock()
		return
	}
	if _, isControl := controlFrameTypes[msg.Type]; isControl {
		u.mu.Lock()
		u.controlFrames = append(u.controlFrames, msg)
		u.mu.Unlock()
		return
	}
	if !acceptsDeliveryEnvelope(msg.Type) {
		return
	}
	var payload struct {
		MsgID string `json:"msg_id"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil || payload.MsgID == "" {
		return // not a tracked message
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if _, dup := u.received[payload.MsgID]; !dup {
		// First receipt wins: a re-delivered copy (e.g. a replay overlapping live)
		// must not overwrite the original copy's identity metadata.
		u.received[payload.MsgID] = receivedMsg{
			channel: msg.Channel, at: time.Now(),
			mid: msg.Mid, pos: msg.Pos, frameType: msg.Type,
		}
		u.receivedOrder = append(u.receivedOrder, payload.MsgID)
	}
}

// ReceivedMeta returns the identity metadata of the FIRST received copy of a
// tracked message: its mid (ADR-0008), pos cursor, and the frame type it rode
// ("message" vs "replay_message").
func (u *TestUser) ReceivedMeta(msgID string) (mid, pos, frameType string, ok bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	r, ok := u.received[msgID]
	return r.mid, r.pos, r.frameType, ok
}

// ReceivedOrderIndex returns the arrival index of a tracked message, or -1.
func (u *TestUser) ReceivedOrderIndex(msgID string) int {
	u.mu.RLock()
	defer u.mu.RUnlock()
	for i, id := range u.receivedOrder {
		if id == msgID {
			return i
		}
	}
	return -1
}

// ControlFrame returns the first captured control frame of the given type
// (history_complete, reconnect_ack, replay_complete, ...).
func (u *TestUser) ControlFrame(frameType string) (testerws.Message, bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	for _, f := range u.controlFrames {
		if f.Type == frameType {
			return f, true
		}
	}
	return testerws.Message{}, false
}

// WaitControlFrame polls until a control frame of the given type arrives or the
// timeout elapses. Terminator-driven (never a fixed sleep): the recovery flows
// define their own end frames, and waiting on anything else is a flake seed.
func (u *TestUser) WaitControlFrame(ctx context.Context, frameType string, timeout, interval time.Duration) (testerws.Message, bool) {
	deadline := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if f, ok := u.ControlFrame(frameType); ok {
			return f, true
		}
		select {
		case <-ctx.Done():
			return testerws.Message{}, false
		case <-deadline:
			return testerws.Message{}, false
		case <-ticker.C:
		}
	}
}

// HasAllMessages reports whether every msgID has been received.
func (u *TestUser) HasAllMessages(msgIDs []string) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	for _, id := range msgIDs {
		if _, ok := u.received[id]; !ok {
			return false
		}
	}
	return true
}

// WaitForMessages polls until every msgID has arrived or the timeout elapses.
func (u *TestUser) WaitForMessages(ctx context.Context, msgIDs []string, timeout, interval time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if u.HasAllMessages(msgIDs) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline:
			return false
		case <-ticker.C:
		}
	}
}

// CreateUser mints a JWT with custom claims, connects to the gateway, and starts tracking messages.
func (e *PubSubEngine) CreateUser(ctx context.Context, minter *auth.Minter, opts auth.MintOptions) (*TestUser, error) {
	token, err := minter.MintWithClaims(opts)
	if err != nil {
		return nil, fmt.Errorf("mint token for %s: %w", opts.Subject, err)
	}

	user := &TestUser{
		Subject:  opts.Subject,
		Groups:   opts.Groups,
		Token:    token,
		received: make(map[string]receivedMsg),
	}

	// Retry the connect: a freshly-minted user's signing key may not have propagated
	// to the gateway's key-registry cache yet (provisioning pushes it over the gRPC
	// stream asynchronously), so the first attempt can 401. connectWithRetry absorbs
	// that key-registry cache race.
	client, err := connectWithRetry(ctx, e.gatewayURL, token, e.logger, user.onMessage)
	if err != nil {
		return nil, fmt.Errorf("connect user %s: %w", opts.Subject, err)
	}
	user.Client = client

	// Start ReadLoop so onMessage callback fires for incoming messages.
	// Without this, PublishAndVerify delivery checks would time out.
	// Goroutine lifecycle: bounded to client connection — exits when client.Close() is called.
	go func() {
		defer logging.RecoverPanic(e.logger, "pubsub-engine-read-loop", nil)
		_, _ = client.ReadLoop(ctx)
	}()

	return user, nil
}

// DeliveryResult captures the outcome of a publish-and-verify check.
type DeliveryResult struct {
	Channel     string
	MessageID   string
	ExpectedBy  []string // subjects that should have received
	ReceivedBy  []string // subjects that actually received
	MisroutedTo []string // subjects that received but shouldn't have
	Missing     []string // subjects that should have received but didn't
	Delivered   bool
	Latency     time.Duration
	// PublishErr is the publish-side failure, when the message never left the
	// publisher — distinct from a delivery timeout (Missing non-empty). Reported
	// so a failed check names its cause instead of a bare "missing: []" (§III).
	PublishErr error
}

// PublishAndVerify publishes a message with a UUID and verifies delivery.
// The pub argument determines the publish path:
//   - TestUser.AsPublisher() → publishes via user's WS connection (tests auth+delivery)
//   - KafkaPublisher → publishes via backend (tests delivery only)
//
// expectedReceivers are users that SHOULD get the message.
// allUsers includes ALL connected users (expected + those that should NOT receive).
func (e *PubSubEngine) PublishAndVerify(
	ctx context.Context,
	pub publisher.Publisher,
	channel string,
	expectedReceivers []*TestUser,
	allUsers []*TestUser,
) DeliveryResult {
	msgID := uuid.NewString()
	payload, _ := json.Marshal(map[string]any{ // json.Marshal on literal map of primitives cannot fail
		"msg_id": msgID,
		"ts":     time.Now().UnixMilli(),
	})

	start := time.Now()

	// Publish via the provided publisher
	if err := pub.Publish(ctx, channel, payload); err != nil {
		return DeliveryResult{
			Channel:    channel,
			MessageID:  msgID,
			Delivered:  false,
			PublishErr: err,
		}
	}

	// Build expected set
	expectedSet := make(map[string]bool, len(expectedReceivers))
	for _, u := range expectedReceivers {
		expectedSet[u.Subject] = true
	}

	// Wait for delivery
	deadline := time.After(e.timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return e.buildResult(channel, msgID, start, expectedSet, allUsers)
		case <-ctx.Done():
			return e.buildResult(channel, msgID, start, expectedSet, allUsers)
		case <-ticker.C:
			// Check if all expected receivers got the message
			allReceived := true
			for _, u := range expectedReceivers {
				if !u.HasReceived(msgID) {
					allReceived = false
					break
				}
			}
			if allReceived {
				// Small grace period for misrouted messages to arrive
				time.Sleep(misrouteGraceWindow)
				return e.buildResult(channel, msgID, start, expectedSet, allUsers)
			}
		}
	}
}

func (e *PubSubEngine) buildResult(
	channel, msgID string,
	start time.Time,
	expectedSet map[string]bool,
	allUsers []*TestUser,
) DeliveryResult {
	result := DeliveryResult{
		Channel:   channel,
		MessageID: msgID,
		Latency:   time.Since(start),
		Delivered: true,
	}

	for _, u := range allUsers {
		got := u.HasReceived(msgID)
		expected := expectedSet[u.Subject]

		if expected {
			result.ExpectedBy = append(result.ExpectedBy, u.Subject)
		}

		if got {
			result.ReceivedBy = append(result.ReceivedBy, u.Subject)
		}

		if got && !expected {
			result.MisroutedTo = append(result.MisroutedTo, u.Subject)
		}

		if !got && expected {
			result.Missing = append(result.Missing, u.Subject)
			result.Delivered = false
		}
	}

	return result
}
