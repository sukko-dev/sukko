package broadcast

import (
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"
)

// declaredStateChannelPrefix is the pub/sub prefix used by every test in this
// file, so channel names can be computed independently of config defaulting.
const declaredStateChannelPrefix = "bcast"

// newDeclaredStateTestBus builds a running valkeyBus pointed at addr with an
// isolated Prometheus registry (several buses coexist in this test binary).
func newDeclaredStateTestBus(t *testing.T, addr string) *valkeyBus {
	t.Helper()
	bus := newUnstartedDeclaredStateTestBus(t, addr)
	bus.Run()
	t.Cleanup(bus.Shutdown)
	return bus
}

// newUnstartedDeclaredStateTestBus builds the bus WITHOUT Run(): no management
// goroutine, so tests can drive converge() synchronously. Callers own cleanup.
func newUnstartedDeclaredStateTestBus(t *testing.T, addr string) *valkeyBus {
	t.Helper()
	bus, err := newValkeyBus(Config{
		Type:            "valkey",
		BufferSize:      8,
		ShutdownTimeout: 2 * time.Second,
		Registerer:      prometheus.NewRegistry(),
		Valkey: ValkeyConfig{
			Addrs:               []string{addr},
			Channel:             declaredStateChannelPrefix,
			WriteTimeout:        2 * time.Second,
			PublishTimeout:      2 * time.Second,
			StartupPingTimeout:  5 * time.Second,
			HealthCheckInterval: time.Hour, // keep the ping loop quiet — these tests target the subscription plane
			HealthCheckTimeout:  time.Second,
		},
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("newValkeyBus: %v", err)
	}
	return bus
}

// subscriberCounts returns the server-side subscriber count for every channel,
// as PUBSUB NUMSUB would report it (channel subscribers only, no patterns).
func subscriberCounts(mr *miniredis.Miniredis, channels []string) map[string]int {
	return mr.PubSubNumSub(channels...)
}

// awaitSubscriberCounts polls until every channel in want has exactly the
// wanted server-side subscriber count, or the deadline passes. On timeout it
// fails the test with the channels that never reached their wanted count.
func awaitSubscriberCounts(t *testing.T, mr *miniredis.Miniredis, want map[string]int, within time.Duration) {
	t.Helper()
	channels := make([]string, 0, len(want))
	for ch := range want {
		channels = append(channels, ch)
	}
	deadline := time.Now().Add(within)
	for {
		got := subscriberCounts(mr, channels)
		mismatched := make(map[string]int)
		for ch, wantN := range want {
			if got[ch] != wantN {
				mismatched[ch] = got[ch]
			}
		}
		if len(mismatched) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("subscriber counts never converged within %v: %d/%d channels wrong (sample: %v)",
				within, len(mismatched), len(want), sampleMismatches(mismatched, want, 5))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func sampleMismatches(mismatched, want map[string]int, n int) []string {
	out := make([]string, 0, n)
	for ch, got := range mismatched {
		out = append(out, fmt.Sprintf("%s: got %d want %d", ch, got, want[ch]))
		if len(out) == n {
			break
		}
	}
	return out
}

// TestValkeyBus_SubscriptionChurnIsLossless is the losslessness property test
// for ADR-0016: a burst of desired-state changes larger than any internal
// buffering must still leave the backend subscription state exactly equal to
// the final desired state — no dropped SUBSCRIBEs (dark tenants) and no
// dropped UNSUBSCRIBEs (stale subscriptions).
//
// Against the replayed-events design this fails structurally: the burst
// overflows the bounded 64-slot command queue, the overflow is dropped, and
// the only recovery is the 30-second reconcile tick — which the 10s deadline
// deliberately excludes, and which never removes stale subscriptions at all.
//
// Deliberately NOT parallel: binds a real TCP listener and asserts a timed
// convergence window (§VIII).
//
//nolint:paralleltest // real TCP listener + timed convergence window; see above
func TestValkeyBus_SubscriptionChurnIsLossless(t *testing.T) {
	const totalTenants = 150
	const removedTenants = 50

	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(mr.Close)

	bus := newDeclaredStateTestBus(t, mr.Addr())

	// Burst-subscribe from one tight loop: enqueueing is orders of magnitude
	// faster than the per-command Valkey round trip, so a design that buffers
	// commands must overflow here.
	chans := make(map[string]<-chan *Message, totalTenants)
	for i := range totalTenants {
		tid := fmt.Sprintf("tenant-%03d", i)
		ch, err := bus.Subscribe(tid)
		if err != nil {
			t.Fatalf("Subscribe(%s): %v", tid, err)
		}
		chans[tid] = ch
	}

	// Immediately churn: drop the first removedTenants subscriptions while the
	// management plane is still working through the burst.
	for i := range removedTenants {
		tid := fmt.Sprintf("tenant-%03d", i)
		if err := bus.Unsubscribe(tid, chans[tid]); err != nil {
			t.Fatalf("Unsubscribe(%s): %v", tid, err)
		}
	}

	want := make(map[string]int, totalTenants)
	for i := range totalTenants {
		tid := fmt.Sprintf("tenant-%03d", i)
		n := 1
		if i < removedTenants {
			n = 0
		}
		want[tenantChannel(declaredStateChannelPrefix, tid)] = n
	}

	// 10s excludes the 30s reconcile tick: convergence must come from the
	// primary (event-driven) path, not the backstop.
	awaitSubscriberCounts(t, mr, want, 10*time.Second)
}

// TestValkeyBus_OutageRecoveryIsNotMaskedBySibling pins the multi-replica
// failure mode ADR-0016 closes: after an outage, this pod must re-establish
// every subscription even though a healthy sibling is already subscribed to
// the same channels. The replayed-events design recovered at most one tenant
// per retry slot plus 64 per reconcile enqueue — and its PUBSUB NUMSUB
// reconciliation saw the sibling's server-global count and concluded nothing
// was missing, so the dark pod stayed dark permanently. The 40s deadline
// deliberately includes a 30s reconcile tick: even with the backstop given a
// chance to run, only local desired-vs-confirmed diffing converges.
//
// Deliberately NOT parallel: binds a real TCP listener, restarts miniredis on
// the same address, and asserts timed recovery windows (§VIII).
//
//nolint:paralleltest // real TCP listener + timed recovery windows; see above
func TestValkeyBus_OutageRecoveryIsNotMaskedBySibling(t *testing.T) {
	// Registered FIRST so it runs LAST (cleanups are LIFO): commands issued
	// against the dead server leave a valkey-go singleflight retry goroutine in
	// a ~1s non-interruptible sleep; settle past it before the package's goleak
	// TestMain takes its snapshot (same pattern as the publish-failure test).
	t.Cleanup(func() { time.Sleep(1500 * time.Millisecond) })

	const totalTenants = 100 // > the old design's 64-slot queue

	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	addr := mr.Addr()

	bus := newDeclaredStateTestBus(t, addr)

	channels := make([]string, totalTenants)
	for i := range totalTenants {
		tid := fmt.Sprintf("tenant-%03d", i)
		if _, err := bus.Subscribe(tid); err != nil {
			t.Fatalf("Subscribe(%s): %v", tid, err)
		}
		channels[i] = tenantChannel(declaredStateChannelPrefix, tid)
	}

	// Positive control: all subscriptions established before the outage.
	preOutage := make(map[string]int, totalTenants)
	for _, ch := range channels {
		preOutage[ch] = 1
	}
	awaitSubscriberCounts(t, mr, preOutage, 10*time.Second)

	// Kill Valkey and let the disconnect storm run against a dead server.
	mr.Close()
	time.Sleep(1500 * time.Millisecond)

	// Valkey returns on the same address.
	mr2 := miniredis.NewMiniRedis()
	if err := mr2.StartAddr(addr); err != nil {
		t.Fatalf("miniredis restart on %s: %v", addr, err)
	}
	t.Cleanup(mr2.Close)

	// The healthy sibling replica: a raw dedicated client subscribed to every
	// channel. Its subscriptions make PUBSUB NUMSUB non-zero server-wide, which
	// masked the dark pod under the old reconciliation.
	sibling, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("sibling client: %v", err)
	}
	t.Cleanup(sibling.Close)
	dc, dcCancel := sibling.Dedicate()
	t.Cleanup(dcCancel)
	dc.SetPubSubHooks(valkey.PubSubHooks{OnMessage: func(valkey.PubSubMessage) {}})
	if err := dc.Do(t.Context(), dc.B().Subscribe().Channel(channels...).Build()).Error(); err != nil {
		t.Fatalf("sibling SUBSCRIBE: %v", err)
	}

	// Recovery: every channel must reach 2 subscribers (sibling + this bus).
	// 40s includes a reconcile tick, so the old design fails even with its
	// backstop given the chance to run.
	postOutage := make(map[string]int, totalTenants)
	for _, ch := range channels {
		postOutage[ch] = 2
	}
	awaitSubscriberCounts(t, mr2, postOutage, 40*time.Second)
}

// TestValkeyBus_OneTenantsSuccessMustNotCancelAnothersRetry is the
// retry-clobber regression test: when SUBSCRIBEs for two tenants fail, the
// later success of one tenant's command must not cancel the other tenant's
// pending convergence. The old design kept a single retry slot that was
// overwritten on every failure and cleared on ANY success, so exactly one of
// the two tenants recovered and the other stayed dark until the 30s tick —
// which the 8s deadline deliberately excludes.
//
// miniredis SetError fails commands on a live connection, so this exercises
// the command-failure path in isolation — no disconnect events, no
// resubscribe-on-reconnect path to hide behind.
//
// Deliberately NOT parallel: binds a real TCP listener and asserts a timed
// convergence window (§VIII).
//
//nolint:paralleltest // real TCP listener + timed convergence window; see above
func TestValkeyBus_OneTenantsSuccessMustNotCancelAnothersRetry(t *testing.T) {
	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(mr.Close)

	bus := newDeclaredStateTestBus(t, mr.Addr())

	chAlpha := tenantChannel(declaredStateChannelPrefix, "alpha")
	chBeta := tenantChannel(declaredStateChannelPrefix, "beta")

	// Fail every command while the connection stays up.
	mr.SetError("ERR injected outage")

	if _, err := bus.Subscribe("alpha"); err != nil {
		t.Fatalf("Subscribe(alpha): %v", err)
	}
	if _, err := bus.Subscribe("beta"); err != nil {
		t.Fatalf("Subscribe(beta): %v", err)
	}

	// Positive control: the injected error must actually be rejecting the
	// SUBSCRIBEs — otherwise this test proves nothing about retries.
	time.Sleep(500 * time.Millisecond)
	if got := subscriberCounts(mr, []string{chAlpha, chBeta}); got[chAlpha] != 0 || got[chBeta] != 0 {
		t.Fatalf("positive control failed: SetError did not reject SUBSCRIBE (counts %v)", got)
	}

	mr.SetError("")

	// Both tenants must converge well before the 30s reconcile tick.
	awaitSubscriberCounts(t, mr, map[string]int{chAlpha: 1, chBeta: 1}, 8*time.Second)
}

// TestValkeyBus_ConvergeAbortsOnConnectionClassErrors pins the disconnect-storm
// cost bound: when a command fails because the CONNECTION is dead (transport
// class), every subsequent command in the pass would fail identically, so the
// pass aborts after the first failure — one command and one log line per
// cycle, not one per tenant. A server ERROR REPLY (the connection is alive and
// answering) does NOT abort: other tenants' commands may still succeed, and
// per-tenant independence within a pass is preserved.
//
// The bus is deliberately not Run(): converge is driven synchronously, so the
// command counts per pass are exact.
//
// Deliberately NOT parallel: binds real TCP listeners (§VIII).
//
//nolint:paralleltest // real TCP listeners; see above
func TestValkeyBus_ConvergeAbortsOnConnectionClassErrors(t *testing.T) {
	// Registered FIRST so it runs LAST: commands against the dead server leave
	// a valkey-go singleflight retry goroutine in a ~1s sleep; settle before
	// goleak's snapshot.
	t.Cleanup(func() { time.Sleep(1500 * time.Millisecond) })

	const tenants = 5

	subscribeTenants := func(t *testing.T, bus *valkeyBus) {
		t.Helper()
		for i := range tenants {
			if _, err := bus.Subscribe(fmt.Sprintf("tenant-%d", i)); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
		}
	}
	retryCount := func(bus *valkeyBus) float64 {
		return testutil.ToFloat64(bus.metrics.subscribeCommandsTotal.WithLabelValues(metricResultRetry))
	}

	t.Run("transport class aborts after first failure", func(t *testing.T) {
		mr := miniredis.NewMiniRedis()
		if err := mr.Start(); err != nil {
			t.Fatalf("miniredis start: %v", err)
		}
		bus := newUnstartedDeclaredStateTestBus(t, mr.Addr())
		t.Cleanup(bus.Shutdown)
		bus.initDedicatedClient()
		subscribeTenants(t, bus)

		mr.Close() // dead connection: every command now fails transport-class

		if ok := bus.converge(false); ok {
			t.Error("converge against a dead server: got ok=true, want false")
		}
		if got := retryCount(bus); got != 1 {
			t.Errorf("failed commands issued against a dead connection: got %.0f, want exactly 1 (pass must abort, not spam one failure per tenant)", got)
		}
	})

	t.Run("server error reply does not abort the pass", func(t *testing.T) {
		mr := miniredis.NewMiniRedis()
		if err := mr.Start(); err != nil {
			t.Fatalf("miniredis start: %v", err)
		}
		t.Cleanup(mr.Close)
		bus := newUnstartedDeclaredStateTestBus(t, mr.Addr())
		t.Cleanup(bus.Shutdown)
		bus.initDedicatedClient()
		subscribeTenants(t, bus)

		mr.SetError("ERR injected") // connection alive, server answers -ERR

		if ok := bus.converge(false); ok {
			t.Error("converge with erroring server: got ok=true, want false")
		}
		if got := retryCount(bus); got < 2 {
			t.Errorf("failed commands with server-class errors: got %.0f, want >= 2 (a server error reply must not abort the pass)", got)
		}
	})
}
