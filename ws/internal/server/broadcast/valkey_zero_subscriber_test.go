package broadcast

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/valkey-io/valkey-go"
)

// newZeroSubBus builds a bus backed by miniredis with a known zero-subscriber
// window. Run() is deliberately NOT called: the subscription management loop
// would own (arm/clear) disruptedSince, and these tests drive that state
// directly to isolate the Publish gate (ADR-0019).
func newZeroSubBus(t *testing.T, window time.Duration) (*valkeyBus, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(mr.Close)
	addr := mr.Addr()

	bus, err := newValkeyBus(Config{
		Type:            "valkey",
		BufferSize:      8,
		ShutdownTimeout: 2 * time.Second,
		Registerer:      prometheus.NewRegistry(),
		Valkey: ValkeyConfig{
			Addrs:                []string{addr},
			WriteTimeout:         500 * time.Millisecond,
			PublishTimeout:       500 * time.Millisecond,
			StartupPingTimeout:   5 * time.Second,
			HealthCheckInterval:  time.Hour,
			HealthCheckTimeout:   time.Second,
			ZeroSubscriberWindow: window,
		},
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("newValkeyBus: %v", err)
	}
	t.Cleanup(bus.Shutdown)
	return bus, mr
}

func zeroSubMetric(t *testing.T, bus *valkeyBus, outcome string) float64 {
	t.Helper()
	return testutil.ToFloat64(bus.metrics.zeroSubscriberRetriesTotal.WithLabelValues(outcome))
}

// A steady-state publish to a channel with no subscribers is a legitimately
// empty channel: accept it (Kafka durability + reconnect replay cover a client
// that connects later), do not gate.
//
//nolint:paralleltest // real TCP listener via miniredis; timing-sensitive (§VIII)
func TestValkeyBus_ZeroSubscriberSteadyStateAccepts(t *testing.T) {
	bus, _ := newZeroSubBus(t, 30*time.Second)
	msg := &Message{TenantID: "acme", Subject: "BTC.trade", Payload: []byte(`{}`)}

	if err := bus.Publish(msg); err != nil {
		t.Fatalf("steady-state empty publish: got %v, want nil", err)
	}
	if got := zeroSubMetric(t, bus, outcomeAcceptedEmpty); got != 1 {
		t.Errorf("accepted_empty = %v, want 1", got)
	}
	if got := zeroSubMetric(t, bus, outcomeHeld); got != 0 {
		t.Errorf("held = %v, want 0 (no episode open)", got)
	}
}

// During an open subscribe-disruption episode, a zero-subscriber publish is the
// publish-recovers-before-subscribe race: hold it (retryable) so the consumer's
// ADR-0015 retry-in-place keeps the record until subscriptions reconverge.
//
//nolint:paralleltest // real TCP listener via miniredis; timing-sensitive (§VIII)
func TestValkeyBus_ZeroSubscriberDuringEpisodeIsRetryable(t *testing.T) {
	bus, _ := newZeroSubBus(t, 30*time.Second)
	bus.disruptedSince.Store(time.Now().UnixNano()) // episode open

	msg := &Message{TenantID: "acme", Subject: "BTC.trade", Payload: []byte(`{}`)}
	err := bus.Publish(msg)
	if !errors.Is(err, ErrPublishUnavailable) {
		t.Fatalf("in-episode empty publish: got %v, want ErrPublishUnavailable", err)
	}
	if got := zeroSubMetric(t, bus, outcomeHeld); got != 1 {
		t.Errorf("held = %v, want 1", got)
	}
	// The pooled connection IS healthy (PUBLISH succeeded); only delivery is
	// pending. publishHealthy must stay true so a long episode does not trip the
	// staleness health check (advisor item 4).
	if !bus.publishHealthy.Load() {
		t.Errorf("publishHealthy = false, want true on the held path (pool is healthy)")
	}
	// A held publish is not a publish failure — it must not inflate the failure counter.
	if got := testutil.ToFloat64(bus.metrics.publishFailuresTotal); got != 0 {
		t.Errorf("publishFailuresTotal = %v, want 0 (held is not a failure)", got)
	}
}

// Once the episode window has elapsed, the gate stops firing even if convergence
// has not completed: flush under plain pub/sub semantics. This bounds the block
// so a genuinely-empty channel cannot livelock the consume loop.
//
//nolint:paralleltest // real TCP listener via miniredis; timing-sensitive (§VIII)
func TestValkeyBus_ZeroSubscriberWindowExpiryFlushes(t *testing.T) {
	bus, _ := newZeroSubBus(t, 50*time.Millisecond)
	bus.disruptedSince.Store(time.Now().UnixNano())
	// The hold window is measured from heldSince (publish recovery); set it far
	// enough back that the window has elapsed.
	bus.heldSince.Store(time.Now().Add(-time.Second).UnixNano())

	msg := &Message{TenantID: "acme", Subject: "BTC.trade", Payload: []byte(`{}`)}
	if err := bus.Publish(msg); err != nil {
		t.Fatalf("expired-episode empty publish: got %v, want nil", err)
	}
	if got := zeroSubMetric(t, bus, outcomeWindowExpired); got != 1 {
		t.Errorf("window_expired = %v, want 1", got)
	}
	if got := zeroSubMetric(t, bus, outcomeHeld); got != 0 {
		t.Errorf("held = %v, want 0 (window expired)", got)
	}
}

// Finding-1 regression: the hold window must be anchored at publish recovery
// (the first held publish), NOT at disruption start. A long outage leaves
// disruptedSince far in the past while no publish has yet been held (publishes
// failed on the error path). The first publish after recovery MUST be held even
// though disruptedSince is much older than the window — otherwise any outage
// longer than the window reproduces the original loss.
//
//nolint:paralleltest // real TCP listener via miniredis; timing-sensitive (§VIII)
func TestValkeyBus_WindowAnchoredAtPublishRecoveryNotDisruptionStart(t *testing.T) {
	bus, _ := newZeroSubBus(t, 100*time.Millisecond)
	bus.disruptedSince.Store(time.Now().Add(-10 * time.Second).UnixNano()) // long outage
	bus.heldSince.Store(0)                                                 // nothing held yet

	msg := &Message{TenantID: "acme", Subject: "BTC.trade", Payload: []byte(`{}`)}
	if err := bus.Publish(msg); !errors.Is(err, ErrPublishUnavailable) {
		t.Fatalf("first held publish after a long outage: got %v, want ErrPublishUnavailable", err)
	}
	if bus.heldSince.Load() == 0 {
		t.Errorf("the first held publish must start the window (heldSince still 0)")
	}
	if got := zeroSubMetric(t, bus, outcomeHeld); got != 1 {
		t.Errorf("held = %v, want 1", got)
	}
}

// armDisruptionEpisode resets the hold-window clock (heldSince) only on a FRESH
// disruption; a reinit that flaps while an episode is already open must not reset
// it (repeated reinits cannot extend the hold window indefinitely). After the
// episode clears, a new disruption resets the clock again.
func TestValkeyBus_ArmDisruptionEpisode(t *testing.T) {
	t.Parallel()
	bus := &valkeyBus{zeroSubscriberWindow: 100 * time.Millisecond}

	// Fresh disruption: arms and clears the hold clock.
	bus.armDisruptionEpisode(1000)
	if got := bus.disruptedSince.Load(); got != 1000 {
		t.Fatalf("fresh arm: disruptedSince = %d, want 1000", got)
	}
	if got := bus.heldSince.Load(); got != 0 {
		t.Fatalf("fresh arm must reset heldSince, got %d", got)
	}

	// A held publish started the window.
	bus.heldSince.Store(5000)

	// Flapping reinit while the episode is still open must NOT reset the clock,
	// but does update the active marker.
	bus.armDisruptionEpisode(6000)
	if got := bus.heldSince.Load(); got != 5000 {
		t.Errorf("flapping reinit reset the hold clock: got %d, want 5000", got)
	}
	if got := bus.disruptedSince.Load(); got != 6000 {
		t.Errorf("disruptedSince should update to the latest arm: got %d, want 6000", got)
	}

	// Clear, then a new disruption resets the clock again.
	bus.clearDisruptionEpisode()
	if bus.disruptedSince.Load() != 0 || bus.heldSince.Load() != 0 {
		t.Fatalf("clear must reset both to 0")
	}
	bus.armDisruptionEpisode(9000)
	if got := bus.heldSince.Load(); got != 0 {
		t.Errorf("fresh arm after clear must reset heldSince, got %d", got)
	}
}

// The load-bearing wiring: reinitDedicatedClient (the only production arm site)
// must open an episode, and clearDisruptionEpisode must end it. Without this,
// deleting the arm call at the reinit site passes every other test.
//
//nolint:paralleltest // real TCP listener via miniredis; timing-sensitive (§VIII)
func TestValkeyBus_ReinitArmsEpisode(t *testing.T) {
	bus, _ := newZeroSubBus(t, 30*time.Second)
	if bus.disruptedSince.Load() != 0 {
		t.Fatalf("no episode should be open before any disruption")
	}
	bus.reinitDedicatedClient() // simulates a dedicated-connection loss
	t.Cleanup(func() {
		if bus.dedicatedCancel != nil {
			bus.dedicatedCancel()
		}
	})
	if bus.disruptedSince.Load() == 0 {
		t.Errorf("reinitDedicatedClient must arm a disruption episode")
	}
	bus.clearDisruptionEpisode()
	if bus.disruptedSince.Load() != 0 {
		t.Errorf("clearDisruptionEpisode must end the episode")
	}
}

// End-to-end of the stale-episode bug: a first episode expires, a second
// disruption arms, and a zero-subscriber publish must gate again.
//
//nolint:paralleltest // real TCP listener via miniredis; timing-sensitive (§VIII)
func TestValkeyBus_GateReArmsForSecondEpisode(t *testing.T) {
	bus, _ := newZeroSubBus(t, 100*time.Millisecond)
	msg := &Message{TenantID: "acme", Subject: "BTC.trade", Payload: []byte(`{}`)}

	// First episode, already expired.
	bus.disruptedSince.Store(time.Now().Add(-time.Second).UnixNano())
	// Second disruption arms fresh.
	bus.armDisruptionEpisode(time.Now().UnixNano())

	if err := bus.Publish(msg); !errors.Is(err, ErrPublishUnavailable) {
		t.Fatalf("second episode must gate again: got %v, want ErrPublishUnavailable", err)
	}
}

// clearDisruptionEpisode ends the episode: a subsequent zero-subscriber publish
// is treated as steady-state (accepted), not held.
//
//nolint:paralleltest // real TCP listener via miniredis; timing-sensitive (§VIII)
func TestValkeyBus_ClearDisruptionEpisode(t *testing.T) {
	bus, _ := newZeroSubBus(t, 30*time.Second)
	bus.disruptedSince.Store(time.Now().UnixNano())
	bus.clearDisruptionEpisode()

	if got := bus.disruptedSince.Load(); got != 0 {
		t.Fatalf("clearDisruptionEpisode: disruptedSince = %d, want 0", got)
	}
	msg := &Message{TenantID: "acme", Subject: "BTC.trade", Payload: []byte(`{}`)}
	if err := bus.Publish(msg); err != nil {
		t.Fatalf("post-clear empty publish: got %v, want nil", err)
	}
	if got := zeroSubMetric(t, bus, outcomeAcceptedEmpty); got != 1 {
		t.Errorf("accepted_empty = %v, want 1", got)
	}
}

// When a subscriber IS present, the gate never fires regardless of episode state:
// count > 0 means the message was delivered.
//
//nolint:paralleltest // real TCP listener via miniredis; timing-sensitive (§VIII)
func TestValkeyBus_NonZeroSubscriberDeliversDuringEpisode(t *testing.T) {
	bus, mr := newZeroSubBus(t, 30*time.Second)
	bus.disruptedSince.Store(time.Now().UnixNano()) // episode open

	// A raw subscriber on the exact tenant channel so PUBLISH reports count >= 1.
	sub, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{mr.Addr()}, DisableCache: true})
	if err != nil {
		t.Fatalf("raw subscriber client: %v", err)
	}
	t.Cleanup(sub.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch := tenantChannel(bus.channelPrefix, "acme")
	go func() {
		_ = sub.Receive(ctx, sub.B().Subscribe().Channel(ch).Build(), func(valkey.PubSubMessage) {})
	}()

	// Wait until miniredis reports the SUBSCRIBE is live before publishing.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mr.PubSubNumSub(ch)[ch] >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := mr.PubSubNumSub(ch)[ch]; got < 1 {
		t.Fatalf("subscriber never registered on %q (num=%d)", ch, got)
	}

	msg := &Message{TenantID: "acme", Subject: "BTC.trade", Payload: []byte(`{}`)}
	if err := bus.Publish(msg); err != nil {
		t.Fatalf("publish with a live subscriber during an episode: got %v, want nil", err)
	}
	if got := zeroSubMetric(t, bus, outcomeHeld); got != 0 {
		t.Errorf("held = %v, want 0 (a subscriber was present)", got)
	}
}
