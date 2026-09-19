package broadcast

import (
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
)

// TestValkeyBus_PublishReturnsUnavailableOnBackendFailure pins the fail-fast
// Publish contract for the transient class: when the Valkey PUBLISH command
// fails (backend down), Publish returns an error wrapping ErrPublishUnavailable
// — it does not block, buffer, or retry — and the failure is visible on
// Prometheus via ws_broadcast_publish_failures_total (§VI). Callers that must
// not lose the message (the Kafka consumer) key their retry loop off this
// sentinel, so it returning non-nil IS the at-least-once hook.
//
// Deliberately NOT parallel: binds a real TCP listener via miniredis and stops
// it mid-test; timing-sensitive against concurrent CPU load (§VIII).
//
//nolint:paralleltest // real TCP listener stopped mid-test; see above
func TestValkeyBus_PublishReturnsUnavailableOnBackendFailure(t *testing.T) {
	// Registered FIRST so it runs LAST (cleanups are LIFO): the failed PUBLISH
	// leaves a valkey-go singleflight retry goroutine in a ~1s non-interruptible
	// time.Sleep backoff. It exits on its own right after the sleep, but this
	// package's goleak TestMain would flag it if the binary exits sooner —
	// settle past the backoff before goleak takes its snapshot.
	t.Cleanup(func() { time.Sleep(1500 * time.Millisecond) })

	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(mr.Close)
	addr := mr.Addr() // captured before Close — Addr panics on a closed server

	bus, err := newValkeyBus(Config{
		Type:            "valkey",
		BufferSize:      8,
		ShutdownTimeout: 2 * time.Second,
		Registerer:      prometheus.NewRegistry(), // isolated: a second bus in this binary must not collide on the default registry
		Valkey: ValkeyConfig{
			Addrs:               []string{addr},
			WriteTimeout:        500 * time.Millisecond,
			PublishTimeout:      500 * time.Millisecond,
			StartupPingTimeout:  5 * time.Second,
			HealthCheckInterval: time.Hour, // keep the health loop quiet during the test
			HealthCheckTimeout:  time.Second,
		},
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("newValkeyBus: %v", err)
	}
	t.Cleanup(bus.Shutdown)

	msg := &Message{TenantID: "acme", Subject: "BTC.trade", Payload: []byte(`{}`)}

	// Healthy backend: Publish must succeed and return nil.
	if err := bus.Publish(msg); err != nil {
		t.Fatalf("Publish with healthy backend: got %v, want nil", err)
	}
	failuresBefore := testutil.ToFloat64(bus.metrics.publishFailuresTotal)

	// Kill the backend. The PUBLISH command now fails.
	mr.Close()

	err = bus.Publish(msg)
	if err == nil {
		t.Fatal("Publish with dead backend: got nil, want error (silent drop = permanent data loss upstream)")
	}
	if !errors.Is(err, ErrPublishUnavailable) {
		t.Errorf("Publish error: got %v, want errors.Is(err, ErrPublishUnavailable)", err)
	}
	if got := testutil.ToFloat64(bus.metrics.publishFailuresTotal); got != failuresBefore+1 {
		t.Errorf("publishFailuresTotal: got %.0f, want %.0f (failed PUBLISH must be dashboard-visible, §VI)", got, failuresBefore+1)
	}
	if bus.IsHealthy() {
		t.Error("IsHealthy after failed publish: got true, want false")
	}

	// Backend returns on the same address (as a restarted container does):
	// Publish must recover to nil without any process restart — and the settled
	// client lets the bus shut down leak-free (goleak runs on this package).
	mr2 := miniredis.NewMiniRedis()
	if err := mr2.StartAddr(addr); err != nil {
		t.Fatalf("miniredis restart on %s: %v", addr, err)
	}
	t.Cleanup(mr2.Close)

	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := bus.Publish(msg); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Publish never recovered after the backend returned")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !bus.IsHealthy() {
		t.Error("IsHealthy after recovered publish: got false, want true")
	}
}
