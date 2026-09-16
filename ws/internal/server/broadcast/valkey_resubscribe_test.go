package broadcast

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/rs/zerolog"
)

// probeSeq makes every probe payload unique across the whole test binary.
var probeSeq atomic.Uint64

// newResubscribeTestBus builds a running valkeyBus pointed at addr.
func newResubscribeTestBus(t *testing.T, addr string) *valkeyBus {
	t.Helper()
	bus, err := newValkeyBus(Config{
		Type:            "valkey",
		BufferSize:      64,
		ShutdownTimeout: 2 * time.Second,
		Valkey: ValkeyConfig{
			Addrs:               []string{addr},
			WriteTimeout:        2 * time.Second,
			PublishTimeout:      2 * time.Second,
			StartupPingTimeout:  5 * time.Second,
			HealthCheckInterval: 200 * time.Millisecond,
			HealthCheckTimeout:  1 * time.Second,
		},
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("newValkeyBus: %v", err)
	}
	bus.Run()
	t.Cleanup(bus.Shutdown)
	return bus
}

// drain empties ch without blocking. Called after the outage so that no message
// published BEFORE Valkey died can satisfy the recovery assertion.
func drain(ch <-chan *Message) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// awaitDelivery publishes uniquely-tagged probes until one of them — not merely
// some message — comes back on ch, or the deadline passes.
//
// The nonce matters: the subscriber channel is buffered, so a probe published
// before the outage can still be sitting in it afterwards. Accepting any message
// would let a stale probe satisfy the post-restart assertion and the test would
// pass against the very regression it exists to pin. Only a probe minted after
// the restart proves the subscription was actually restored.
func awaitDelivery(t *testing.T, bus *valkeyBus, ch <-chan *Message, tenantID string, within time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		nonce := fmt.Sprintf("probe-%d", probeSeq.Add(1))
		bus.Publish(&Message{TenantID: tenantID, Subject: "probe", Payload: []byte(nonce)})

		settle := time.After(100 * time.Millisecond)
		for {
			select {
			case msg := <-ch:
				if msg != nil && string(msg.Payload) == nonce {
					return true
				}
				// A stale or older probe — keep reading, it proves nothing.
				continue
			case <-settle:
			}
			break
		}
	}
	return false
}

// TestValkeyBus_ResubscribesAfterOutage pins the recovery contract: when Valkey
// goes away and comes back, the bus must restore its tenant subscriptions and
// resume delivery without a process restart.
//
// Regression test. A Valkey outage drove every SUBSCRIBE to fail, which parked
// the tenant in the management loop's pending-retry map. Every later command for
// that tenant — the reconnect resubscribe, the 30s reconcile tick, and a fresh
// client Subscribe — was then swallowed by the latest-wins branch, which also
// disarmed the single retry timer. Nothing could ever issue the SUBSCRIBE that
// was the only way out of the map, so delivery stayed dead until the pod was
// restarted: silent, permanent, per-tenant message loss after a routine
// dependency restart (§IV).
//
// Found by the sukko-dev/bench fault matrix, not by unit tests: killing Valkey
// mid-run left 480/480 subscriber-channel pairs dark from the kill to the end of
// the run while the bus still reported healthy.
func TestValkeyBus_ResubscribesAfterOutage(t *testing.T) {
	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	addr := mr.Addr()

	bus := newResubscribeTestBus(t, addr)

	const tenant = "acme"
	ch, err := bus.Subscribe(tenant)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Positive control: delivery works before the outage. Without this a broken
	// harness would look identical to the bug.
	if !awaitDelivery(t, bus, ch, tenant, 5*time.Second) {
		t.Fatal("no delivery before the outage — harness is wrong, not the bus")
	}

	// Kill Valkey. This is `docker kill valkey`: the port stops accepting, and
	// every in-flight and subsequent command fails with connection refused.
	mr.Close()

	// Let the reconnect path run against a dead server. The disconnect handler
	// re-enqueues a resubscribe each cycle, so this is where the tenant gets
	// parked and the retry timer gets disarmed.
	time.Sleep(1500 * time.Millisecond)

	// Discard anything buffered from before the outage — see awaitDelivery.
	drain(ch)

	// Valkey comes back on the same address, as a restarted container does.
	mr2 := miniredis.NewMiniRedis()
	if err := mr2.StartAddr(addr); err != nil {
		t.Fatalf("miniredis restart on %s: %v", addr, err)
	}
	t.Cleanup(mr2.Close)

	// The reconcile tick is 30s, so 45s is a generous ceiling: it allows the
	// documented fallback to fire at least once and still fail the test if the
	// only recovery path is a process restart.
	if !awaitDelivery(t, bus, ch, tenant, 45*time.Second) {
		t.Fatal("delivery never resumed after Valkey returned — subscriptions were not restored")
	}
}
