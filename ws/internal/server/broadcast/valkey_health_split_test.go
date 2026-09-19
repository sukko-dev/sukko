package broadcast

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
)

// TestValkeyBus_IsHealthy_SplitSignals pins the health split (ADR-0016, §XV):
// IsHealthy is publish health AND subscription convergence — one signal can
// never mask the other.
func TestValkeyBus_IsHealthy_SplitSignals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		publishHealthy bool
		desiredGen     uint64
		confirmedGen   uint64
		want           bool
	}{
		{"publish healthy and converged", true, 4, 4, true},
		{"publish unhealthy masks nothing", false, 4, 4, false},
		{"non-convergence despite healthy publish", true, 5, 4, false},
		{"both signals bad", false, 5, 4, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := &valkeyBus{logger: zerolog.Nop()}
			b.publishHealthy.Store(tt.publishHealthy)
			b.desiredGen.Store(tt.desiredGen)
			b.confirmedGen.Store(tt.confirmedGen)

			if got := b.IsHealthy(); got != tt.want {
				t.Errorf("IsHealthy() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestValkeyBus_ConvergenceRetriesWithBackoffAndReportsDivergence exercises the
// command-failure retry path in isolation (no disconnect events): with every
// command failing on a live connection, the convergence loop must keep
// retrying with backoff, and the desired/established pair plus the health
// split must make the divergence visible while publishes would still succeed.
// Once commands succeed again, the loop must converge without any external
// trigger.
//
// Deliberately NOT parallel: binds a real TCP listener and asserts timed
// convergence windows (§VIII).
//
//nolint:paralleltest // real TCP listener + timed convergence windows; see above
func TestValkeyBus_ConvergenceRetriesWithBackoffAndReportsDivergence(t *testing.T) {
	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(mr.Close)

	bus := newDeclaredStateTestBus(t, mr.Addr())

	// Positive control: a healthy subscription first.
	if _, err := bus.Subscribe("gamma"); err != nil {
		t.Fatalf("Subscribe(gamma): %v", err)
	}
	chGamma := tenantChannel(declaredStateChannelPrefix, "gamma")
	awaitSubscriberCounts(t, mr, map[string]int{chGamma: 1}, 5*time.Second)
	if !bus.IsHealthy() {
		t.Fatal("IsHealthy after clean convergence: got false, want true")
	}

	// Fail every command while the connection stays up, then add a tenant.
	mr.SetError("ERR injected outage")
	if _, err := bus.Subscribe("delta"); err != nil {
		t.Fatalf("Subscribe(delta): %v", err)
	}

	// Give the loop several backoff cycles (100ms, 200ms, 400ms...).
	time.Sleep(900 * time.Millisecond)

	// The divergence must be visible while nothing has disconnected:
	// desired > established, convergence false, IsHealthy false — and the
	// failure counted under the retry label.
	if got := testutil.ToFloat64(bus.metrics.subscriptionsDesired); got != 2 {
		t.Errorf("subscriptionsDesired gauge: got %.0f, want 2", got)
	}
	if got := testutil.ToFloat64(bus.metrics.subscriptionsEstablished); got != 1 {
		t.Errorf("subscriptionsEstablished gauge: got %.0f, want 1", got)
	}
	if got := testutil.ToFloat64(bus.metrics.subscribeCommandsTotal.WithLabelValues(metricResultRetry)); got < 1 {
		t.Errorf("subscribeCommandsTotal[retry]: got %.0f, want >= 1", got)
	}
	m := bus.GetMetrics()
	if m.SubscriptionsConverged {
		t.Error("Metrics.SubscriptionsConverged during divergence: got true, want false")
	}
	if m.SubscriptionsDesired != 2 || m.SubscriptionsEstablished != 1 {
		t.Errorf("Metrics desired/established: got %d/%d, want 2/1", m.SubscriptionsDesired, m.SubscriptionsEstablished)
	}
	if m.Healthy {
		t.Error("Metrics.Healthy during subscription divergence: got true, want false")
	}
	if bus.IsHealthy() {
		t.Error("IsHealthy during subscription divergence: got true, want false")
	}

	// Commands succeed again: the backoff retry must converge with no
	// external trigger (no new Subscribe, no disconnect, no reconcile tick —
	// the tick is 30s away and the deadline excludes it).
	mr.SetError("")
	awaitSubscriberCounts(t, mr, map[string]int{
		chGamma: 1,
		tenantChannel(declaredStateChannelPrefix, "delta"): 1,
	}, 8*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for !bus.IsHealthy() {
		if time.Now().After(deadline) {
			t.Fatal("IsHealthy never recovered after convergence")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := testutil.ToFloat64(bus.metrics.subscriptionsEstablished); got != 2 {
		t.Errorf("subscriptionsEstablished after recovery: got %.0f, want 2", got)
	}
}

// TestValkeyBus_UnsubscribeHonoredByDiffing pins that unsubscribes are applied
// by diffing desired against confirmed — including an unsubscribe issued while
// the backend is down, which the confirmed-state wipe on reconnect must not
// resurrect.
//
// Deliberately NOT parallel: binds a real TCP listener and restarts miniredis
// on the same address (§VIII).
//
//nolint:paralleltest // real TCP listener + miniredis restart on same address; see above
func TestValkeyBus_UnsubscribeHonoredByDiffing(t *testing.T) {
	// Registered FIRST so it runs LAST (cleanups are LIFO): commands issued
	// against the dead server leave a valkey-go singleflight retry goroutine in
	// a ~1s non-interruptible sleep; settle past it before the package's goleak
	// TestMain takes its snapshot (same pattern as the publish-failure test).
	t.Cleanup(func() { time.Sleep(1500 * time.Millisecond) })

	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	addr := mr.Addr()

	bus := newDeclaredStateTestBus(t, addr)

	chKeepName := tenantChannel(declaredStateChannelPrefix, "keep")
	chLiveName := tenantChannel(declaredStateChannelPrefix, "livedrop")
	chDeadName := tenantChannel(declaredStateChannelPrefix, "deaddrop")

	chKeep, err := bus.Subscribe("keep")
	if err != nil {
		t.Fatalf("Subscribe(keep): %v", err)
	}
	_ = chKeep
	chLive, err := bus.Subscribe("livedrop")
	if err != nil {
		t.Fatalf("Subscribe(livedrop): %v", err)
	}
	chDead, err := bus.Subscribe("deaddrop")
	if err != nil {
		t.Fatalf("Subscribe(deaddrop): %v", err)
	}
	awaitSubscriberCounts(t, mr, map[string]int{chKeepName: 1, chLiveName: 1, chDeadName: 1}, 5*time.Second)

	// Unsubscribe on a live connection: the diff must issue UNSUBSCRIBE.
	if err := bus.Unsubscribe("livedrop", chLive); err != nil {
		t.Fatalf("Unsubscribe(livedrop): %v", err)
	}
	awaitSubscriberCounts(t, mr, map[string]int{chKeepName: 1, chLiveName: 0}, 5*time.Second)

	// Unsubscribe while the backend is down: after recovery the tenant must
	// NOT be resubscribed, while the kept tenant must be.
	mr.Close()
	time.Sleep(300 * time.Millisecond)
	if err := bus.Unsubscribe("deaddrop", chDead); err != nil {
		t.Fatalf("Unsubscribe(deaddrop) during outage: %v", err)
	}

	mr2 := miniredis.NewMiniRedis()
	if err := mr2.StartAddr(addr); err != nil {
		t.Fatalf("miniredis restart on %s: %v", addr, err)
	}
	t.Cleanup(mr2.Close)

	awaitSubscriberCounts(t, mr2, map[string]int{chKeepName: 1}, 10*time.Second)
	// Convergence with keep re-established implies the full pass ran; deaddrop
	// must have stayed gone.
	if got := subscriberCounts(mr2, []string{chDeadName})[chDeadName]; got != 0 {
		t.Errorf("deaddrop after recovery: got %d subscribers, want 0 (unsubscribe during outage resurrected)", got)
	}
}
