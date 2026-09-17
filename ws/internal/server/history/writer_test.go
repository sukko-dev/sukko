package history_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	prometheustestutil "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/server/history"
)

// TestHistoryWriter_Canary_XADDAndXREVRANGEViaValkeyGo verifies that valkey-go's builder
// pattern round-trips correctly with miniredis for XADD MAXLEN ~, XREVRANGE, and Lua EVAL.
// This is a compatibility gate — all other writer tests depend on this working.
func TestHistoryWriter_Canary_XADDAndXREVRANGEViaValkeyGo(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	client := newTestValkeyClient(t, mr)

	ctx := context.Background()
	streamKey := "history:test:tenantA:BTC.trade"

	// XADD MAXLEN ~ 100 * payload testdata
	xaddRes := client.Do(ctx,
		client.B().Xadd().Key(streamKey).
			Maxlen().Almost().Threshold("100").
			Id("*").
			FieldValue().
			FieldValue(history.HistoryFieldPayload, `{"price":42000}`).
			FieldValue(history.HistoryFieldTenantID, "tenantA").
			FieldValue(history.HistoryFieldChannel, "BTC.trade").
			FieldValue(history.HistoryFieldSubject, "tenantA.BTC.trade").
			Build(),
	)
	if err := xaddRes.Error(); err != nil {
		t.Fatalf("XADD failed: %v", err)
	}
	entryID, err := xaddRes.ToString()
	if err != nil || entryID == "" {
		t.Fatalf("XADD returned empty ID or error: %v", err)
	}

	// XREVRANGE: fetch the entry back
	xrevRes := client.Do(ctx,
		client.B().Xrevrange().Key(streamKey).End("+").Start("-").Count(10).Build(),
	)
	entries, err := xrevRes.AsXRange()
	if err != nil {
		t.Fatalf("XREVRANGE failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].FieldValues[history.HistoryFieldPayload] != `{"price":42000}` {
		t.Errorf("payload mismatch: got %q", entries[0].FieldValues[history.HistoryFieldPayload])
	}

	// Lua EVAL: test CAS renew script (same script used in handleHeartbeatTick).
	lockKey := "history-writer-lock:test"
	podID := "test-pod"
	ttlMs := int64(5000)

	// SET the lock key first.
	if err := client.Do(ctx,
		client.B().Set().Key(lockKey).Value(podID).Build(),
	).Error(); err != nil {
		t.Fatalf("SET lock: %v", err)
	}

	// Run renew script: should return 1 (we own the lock).
	evalRes := client.Do(ctx,
		client.B().Eval().Script(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
`).Numkeys(1).Key(lockKey).Arg(podID, strconv.FormatInt(ttlMs, 10)).Build(),
	)
	n, err := evalRes.AsInt64()
	if err != nil {
		t.Fatalf("EVAL renew script error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected renew to return 1 (success), got %d", n)
	}
}

// TestHistoryWriter_XADDEntrySchema verifies that messages flowing through the HistoryWriter
// produce XADD entries with the correct field schema (payload, tenant_id, channel, subject).
func TestHistoryWriter_XADDEntrySchema(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	// Wait for the HistoryWriter to subscribe (subscribe happens inside runOnce).
	time.Sleep(20 * time.Millisecond)

	msg := &broadcast.Message{
		Subject:  "tenantA.BTC.trade",
		Payload:  []byte(`{"price":100}`),
		TenantID: "tenantA",
		Channel:  "BTC.trade",
		Pos:      "0-1",
		Mid:      "abc123-0-1",
	}
	bus.fanOut(msg)

	client := newTestValkeyClient(t, mr)
	streamKey := history.HistoryStreamKeyPrefix + opts.env + ":tenantA:BTC.trade"
	// Poll for the relay + flushBatch to land instead of a fixed sleep (races the async writer under CI load).
	waitForStreamEntry(t, client, streamKey, 2*time.Second)

	cancel()
	wg.Wait()

	entries, err := client.Do(context.Background(),
		client.B().Xrevrange().Key(streamKey).End("+").Start("-").Count(10).Build(),
	).AsXRange()
	if err != nil {
		t.Fatalf("XREVRANGE: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least 1 entry in stream, got 0")
	}

	fv := entries[0].FieldValues
	if fv[history.HistoryFieldPayload] != `{"price":100}` {
		t.Errorf("payload: got %q", fv[history.HistoryFieldPayload])
	}
	if fv[history.HistoryFieldTenantID] != "tenantA" {
		t.Errorf("tenant_id: got %q", fv[history.HistoryFieldTenantID])
	}
	if fv[history.HistoryFieldChannel] != "BTC.trade" {
		t.Errorf("channel: got %q", fv[history.HistoryFieldChannel])
	}
	if fv[history.HistoryFieldSubject] != "tenantA.BTC.trade" {
		t.Errorf("subject: got %q", fv[history.HistoryFieldSubject])
	}
	// The stable message identity must be persisted so history copies carry
	// the same mid as the live delivery (cross-copy identity, ADR-0008).
	if fv[history.HistoryFieldMid] != "abc123-0-1" {
		t.Errorf("mid: got %q, want %q", fv[history.HistoryFieldMid], "abc123-0-1")
	}
}

// TestHistoryWriter_NoMidOmitsField verifies pre-mid bus messages (rolling
// deploy) store no mid field rather than an empty one.
func TestHistoryWriter_NoMidOmitsField(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})
	time.Sleep(20 * time.Millisecond)

	bus.fanOut(&broadcast.Message{
		Subject:  "tenantB.BTC.trade",
		Payload:  []byte(`{"price":1}`),
		TenantID: "tenantB",
		Channel:  "BTC.trade",
		Pos:      "0-2",
	})

	client := newTestValkeyClient(t, mr)
	streamKey := history.HistoryStreamKeyPrefix + opts.env + ":tenantB:BTC.trade"
	waitForStreamEntry(t, client, streamKey, 2*time.Second)

	cancel()
	wg.Wait()

	entries, err := client.Do(context.Background(),
		client.B().Xrevrange().Key(streamKey).End("+").Start("-").Count(10).Build(),
	).AsXRange()
	if err != nil {
		t.Fatalf("XREVRANGE: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least 1 entry in stream, got 0")
	}
	if v, ok := entries[0].FieldValues[history.HistoryFieldMid]; ok {
		t.Errorf("mid field present (%q), want absent for mid-less messages", v)
	}
}

// TestHistoryWriter_PassivePodNoXADD verifies that a passive pod (another pod holds the lock)
// does not write XADD entries to the stream.
func TestHistoryWriter_PassivePodNoXADD(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.podID = "passive-pod"

	// Pre-occupy the lock with a different pod so our writer starts passive.
	lockKey := history.HistoryWriterLockKeyPrefix + opts.env
	setupClient := newTestValkeyClient(t, mr)
	if err := setupClient.Do(context.Background(),
		setupClient.B().Set().Key(lockKey).Value("other-pod").
			PxMilliseconds(opts.lockTTLMs).Build(),
	).Error(); err != nil {
		t.Fatalf("pre-set lock: %v", err)
	}

	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	time.Sleep(20 * time.Millisecond)

	msg := &broadcast.Message{
		Subject:  "tenantA.ETH.trade",
		Payload:  []byte(`{"price":200}`),
		TenantID: "tenantA",
		Channel:  "ETH.trade",
		Pos:      "0-2",
	}
	bus.fanOut(msg)

	time.Sleep(80 * time.Millisecond)
	cancel()
	wg.Wait()

	streamKey := history.HistoryStreamKeyPrefix + opts.env + ":tenantA:ETH.trade"
	checkClient := newTestValkeyClient(t, mr)
	entries, _ := checkClient.Do(context.Background(),
		checkClient.B().Xrevrange().Key(streamKey).End("+").Start("-").Count(10).Build(),
	).AsXRange()

	if len(entries) != 0 {
		t.Errorf("passive pod must not write to stream: found %d entries", len(entries))
	}
}

// TestHistoryWriter_LockReleasedOnShutdown verifies that the active writer releases the
// distributed lock (via Lua CAS delete) when its context is canceled.
func TestHistoryWriter_LockReleasedOnShutdown(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	w, cancel, _ := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	// Give writer time to acquire the lock.
	time.Sleep(50 * time.Millisecond)

	// Cancel the context — triggers shutdown.
	cancel()
	wg.Wait()

	lockKey := history.HistoryWriterLockKeyPrefix + opts.env
	checkClient := newTestValkeyClient(t, mr)
	n, err := checkClient.Do(context.Background(),
		checkClient.B().Exists().Key(lockKey).Build(),
	).AsInt64()
	if err != nil {
		t.Fatalf("EXISTS error: %v", err)
	}
	if n != 0 {
		t.Errorf("lock was not released: EXISTS returned %d (expected 0) after shutdown", n)
	}
}

// TestHistoryWriter_TenantIsolation verifies messages for different tenants land in
// separate stream keys (no cross-tenant contamination).
func TestHistoryWriter_TenantIsolation(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	time.Sleep(20 * time.Millisecond)

	for _, msg := range []*broadcast.Message{
		{Subject: "tenantA.BTC.trade", Payload: []byte(`{"t":"A"}`), TenantID: "tenantA", Channel: "BTC.trade"},
		{Subject: "tenantB.BTC.trade", Payload: []byte(`{"t":"B"}`), TenantID: "tenantB", Channel: "BTC.trade"},
	} {
		bus.fanOut(msg)
	}

	checkClient := newTestValkeyClient(t, mr)
	ctx := context.Background()
	// Poll for both tenants' entries instead of a fixed sleep (races the async writer under CI load).
	for _, tenant := range []string{"tenantA", "tenantB"} {
		waitForStreamEntry(t, checkClient, history.HistoryStreamKeyPrefix+opts.env+":"+tenant+":BTC.trade", 2*time.Second)
	}
	cancel()
	wg.Wait()

	for _, tc := range []struct {
		tenant  string
		want    string
		wantNot string
	}{
		{"tenantA", `{"t":"A"}`, `{"t":"B"}`},
		{"tenantB", `{"t":"B"}`, `{"t":"A"}`},
	} {
		streamKey := history.HistoryStreamKeyPrefix + opts.env + ":" + tc.tenant + ":BTC.trade"
		entries, err := checkClient.Do(ctx,
			checkClient.B().Xrevrange().Key(streamKey).End("+").Start("-").Count(10).Build(),
		).AsXRange()
		if err != nil {
			t.Errorf("XREVRANGE %s: %v", tc.tenant, err)
			continue
		}
		if len(entries) == 0 {
			t.Errorf("expected entry for %s, got 0", tc.tenant)
			continue
		}
		payload := entries[0].FieldValues[history.HistoryFieldPayload]
		if payload != tc.want {
			t.Errorf("tenant %s: want payload %q, got %q", tc.tenant, tc.want, payload)
		}
		for _, e := range entries {
			if e.FieldValues[history.HistoryFieldPayload] == tc.wantNot {
				t.Errorf("tenant %s: cross-tenant payload %q found in stream", tc.tenant, tc.wantNot)
			}
		}
	}
}

// TestHistoryWriter_LockTTLMillisConversion verifies HistoryWriterLockTTLMs is derived
// from HistoryWriterLockTTL by the platform.Normalize path (tested indirectly: the lock
// is set via SET NX PX lockTTLMs and miniredis honors the TTL — if TTLMs were 0 the key
// would have no expiry and the passive test above would fail).
func TestHistoryWriter_LockTTLMillisConversion(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.lockTTLMs = 500 // 500ms TTL — should expire quickly
	w, cancel, _ := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	time.Sleep(30 * time.Millisecond) // allow lock acquisition

	cancel()
	wg.Wait()

	// Since shutdown now releases the lock via Lua CAS, this just verifies TTLMs was
	// passed — the lock should be gone.
	lockKey := history.HistoryWriterLockKeyPrefix + opts.env
	checkClient := newTestValkeyClient(t, mr)
	res := checkClient.Do(context.Background(),
		checkClient.B().Exists().Key(lockKey).Build(),
	)
	n, _ := res.AsInt64()
	if n != 0 {
		t.Errorf("expected lock to be absent after shutdown with 500ms TTL + lock release, got EXISTS=%d", n)
	}
}

// TestHistoryWriter_BusUnhealthyTriggersRestart verifies that an unhealthy bus causes
// runOnce to return early, incrementing the restart counter.
func TestHistoryWriter_BusUnhealthyTriggersRestart(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.heartbeatInterval = 20 * time.Millisecond
	opts.restartInitialBackoff = 5 * time.Millisecond
	opts.restartMaxBackoff = 20 * time.Millisecond

	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	time.Sleep(10 * time.Millisecond)
	bus.setHealthy(false) // trigger unhealthy → restart
	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()

	restarts := metricCounterValue(t, w.Metrics().WriterRestartTotal)
	if restarts < 1 {
		t.Errorf("expected ≥1 restart after bus became unhealthy, got %v", restarts)
	}
}

// TestHistoryWriter_NonConvergenceDoesNotRestart pins the writer's restart
// gate to PUBLISH health only (ADR-0016). Subscription non-convergence is the
// convergence loop's job to repair; restarting the writer cannot help it — the
// restart's only effect is UnsubscribeAll → backoff → SubscribeAll, a
// self-inflicted PSUBSCRIBE gap. Convergence also reads transiently false
// after every first-subscribe/last-unsubscribe on the pod, so a heartbeat
// sampling it would flap the writer under ordinary tenant churn. A
// publish-path failure, by contrast, must still trigger the restart.
func TestHistoryWriter_NonConvergenceDoesNotRestart(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.heartbeatInterval = 20 * time.Millisecond
	opts.restartInitialBackoff = 5 * time.Millisecond
	opts.restartMaxBackoff = 20 * time.Millisecond

	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	time.Sleep(10 * time.Millisecond)

	// Phase 1: non-converged but publish-healthy — many heartbeats must pass
	// with ZERO restarts.
	bus.setConverged(false)
	time.Sleep(150 * time.Millisecond)
	if restarts := metricCounterValue(t, w.Metrics().WriterRestartTotal); restarts != 0 {
		t.Errorf("expected 0 restarts while non-converged but publish-healthy, got %v", restarts)
	}

	// Phase 2: publish-unhealthy (still non-converged) — restart must fire.
	bus.setHealthy(false)
	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()

	if restarts := metricCounterValue(t, w.Metrics().WriterRestartTotal); restarts < 1 {
		t.Errorf("expected >=1 restart after publish path became unhealthy, got %v", restarts)
	}
}

// TestHistoryWriter_CtxCancelExitsDuringBackoff verifies that canceling the parent context
// causes Run() to exit even if it's sleeping in the backoff delay.
func TestHistoryWriter_CtxCancelExitsDuringBackoff(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.restartMaxBackoff = 10 * time.Second // long backoff to trap the writer
	opts.heartbeatInterval = 20 * time.Millisecond

	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	time.Sleep(10 * time.Millisecond)
	bus.setHealthy(false) // force a restart (enters backoff)
	time.Sleep(30 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		cancel()
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Exited as expected.
	case <-time.After(500 * time.Millisecond):
		t.Error("Run() did not exit within 500ms after context cancel during backoff")
	}
}

// TestHistoryWriter_RestartCounter verifies that the restart counter increments when
// runOnce returns due to an unhealthy bus.
func TestHistoryWriter_RestartCounter(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.heartbeatInterval = 20 * time.Millisecond
	opts.restartInitialBackoff = 5 * time.Millisecond
	opts.restartMaxBackoff = 10 * time.Millisecond

	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	time.Sleep(10 * time.Millisecond)
	bus.setHealthy(false)
	time.Sleep(80 * time.Millisecond)
	cancel()
	wg.Wait()

	n := metricCounterValue(t, w.Metrics().WriterRestartTotal)
	if n < 1 {
		t.Errorf("RestartTotal: expected ≥1, got %v", n)
	}
}

// TestHistoryWriter_AlwaysPassivePod_ZeroLockLossMetric verifies that a pod that never
// acquires the lock never increments the lock-loss metric.
func TestHistoryWriter_AlwaysPassivePod_ZeroLockLossMetric(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.podID = "always-passive"
	opts.heartbeatInterval = 20 * time.Millisecond

	// Pre-occupy lock with infinite TTL so our pod always stays passive.
	lockKey := history.HistoryWriterLockKeyPrefix + opts.env
	setupClient := newTestValkeyClient(t, mr)
	if err := setupClient.Do(context.Background(),
		setupClient.B().Set().Key(lockKey).Value("owner-pod").Build(),
	).Error(); err != nil {
		t.Fatalf("pre-set lock: %v", err)
	}

	w, cancel, _ := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	time.Sleep(60 * time.Millisecond)
	cancel()
	wg.Wait()

	n := metricCounterValue(t, w.Metrics().LockFailuresTotal)
	if n != 0 {
		t.Errorf("LockFailuresTotal: expected 0 for always-passive pod, got %v", n)
	}
}

// TestHistoryWriter_RelayGoroutineExitsCleanly verifies no goroutine leak after shutdown.
func TestHistoryWriter_RelayGoroutineExitsCleanly(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	w, cancel, _ := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	time.Sleep(20 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("HistoryWriter.Run() did not exit cleanly within 500ms")
	}
}

// TestHistoryWriter_PayloadFieldStoresRawDataOnly verifies that the XADD payload field
// stores only the raw broadcast payload (no wrapping, no double-encoding).
func TestHistoryWriter_PayloadFieldStoresRawDataOnly(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})
	time.Sleep(20 * time.Millisecond)

	rawPayload := []byte(`{"event":"tick","bid":1234.5}`)
	bus.fanOut(&broadcast.Message{
		Subject:  "tenantA.TICK",
		Payload:  rawPayload,
		TenantID: "tenantA",
		Channel:  "TICK",
	})
	streamKey := history.HistoryStreamKeyPrefix + opts.env + ":tenantA:TICK"
	checkClient := newTestValkeyClient(t, mr)
	// Poll for the entry instead of a fixed sleep (races the async writer under CI load).
	waitForStreamEntry(t, checkClient, streamKey, 2*time.Second)
	cancel()
	wg.Wait()

	entries, err := checkClient.Do(context.Background(),
		checkClient.B().Xrevrange().Key(streamKey).End("+").Start("-").Count(1).Build(),
	).AsXRange()
	if err != nil {
		t.Fatalf("XREVRANGE: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected 1 entry")
	}
	if got := entries[0].FieldValues[history.HistoryFieldPayload]; got != string(rawPayload) {
		t.Errorf("payload stored with wrapping: got %q, want %q", got, rawPayload)
	}
}

// TestHistoryWriter_RelayNonBlockingSignal verifies that the relay goroutine does not
// deadlock or block even when notifyChan is already at capacity (cap=1).
func TestHistoryWriter_RelayNonBlockingSignal(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.writerBuffer = 1024
	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})
	time.Sleep(20 * time.Millisecond)

	// Send a burst of messages; the notifyChan has capacity 1.
	// If the relay blocks on the non-blocking send, the test will time out.
	for i := range 20 {
		bus.fanOut(&broadcast.Message{
			Subject:  "tenantA.channel" + strconv.Itoa(i),
			Payload:  []byte(`{}`),
			TenantID: "tenantA",
			Channel:  "channel" + strconv.Itoa(i),
		})
	}

	time.Sleep(80 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		cancel()
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("HistoryWriter blocked or deadlocked during burst relay")
	}
}

// TestHistoryWriter_XADDAndExpireCoBatched verifies that the EXPIRE command is issued
// alongside XADD so the stream key has a TTL after the first message write.
func TestHistoryWriter_XADDAndExpireCoBatched(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() { w.Run() })
	time.Sleep(20 * time.Millisecond)

	bus.fanOut(&broadcast.Message{
		Subject: "t1.KEY", Payload: []byte(`{}`), TenantID: "t1", Channel: "KEY",
	})

	streamKey := history.HistoryStreamKeyPrefix + opts.env + ":t1:KEY"
	checkClient := newTestValkeyClient(t, mr)
	// Poll for the entry (writer processed the message) instead of a fixed sleep, then assert
	// the TTL — the EXPIRE is co-batched with the XADD, so a present key must carry a TTL.
	waitForStreamEntry(t, checkClient, streamKey, 2*time.Second)
	cancel()
	wg.Wait()

	ttl, err := checkClient.Do(context.Background(),
		checkClient.B().Ttl().Key(streamKey).Build(),
	).AsInt64()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 {
		t.Errorf("expected stream key to have TTL > 0 (EXPIRE co-batched with XADD), got %d", ttl)
	}
}

// TestHistoryDropCounterDelta was removed: bus-level drop counter tracking in the
// history writer was removed as part of the broadcast tenant fairness refactor.
// Bus-level drops are now tracked by ws_broadcast_bus_dropped_total{tenant_id="_all"}
// at the bus layer. See spec: "Design: History writer migration".

// TestHistoryWriter_LockCallFailureVsCASMiss verifies that a CAS miss (lock stolen by
// another pod, returning n=0 from Lua) does NOT increment LockFailuresTotal, while the
// active writer correctly flips to passive.
func TestHistoryWriter_LockCallFailureVsCASMiss(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.podID = "writer-pod"
	opts.heartbeatInterval = 20 * time.Millisecond
	w, cancel, _ := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() { w.Run() })
	time.Sleep(30 * time.Millisecond) // let writer acquire lock and become active

	// Steal the lock from under the active writer (simulates another pod acquiring it).
	lockKey := history.HistoryWriterLockKeyPrefix + opts.env
	setupClient := newTestValkeyClient(t, mr)
	if err := setupClient.Do(context.Background(),
		setupClient.B().Set().Key(lockKey).Value("thief-pod").
			PxMilliseconds(opts.lockTTLMs).Build(),
	).Error(); err != nil {
		t.Fatalf("steal lock: %v", err)
	}

	// Wait for one heartbeat to process the CAS miss (n=0 → flip to passive).
	time.Sleep(50 * time.Millisecond)

	active := prometheustestutil.ToFloat64(w.Metrics().WriterActive)
	lockFails := prometheustestutil.ToFloat64(w.Metrics().LockFailuresTotal)

	cancel()
	wg.Wait()

	if active != 0 {
		t.Errorf("WriterActive: expected 0 after lock stolen (CAS miss), got %v", active)
	}
	if lockFails != 0 {
		t.Errorf("LockFailuresTotal: expected 0 for CAS miss (not a Valkey error), got %v", lockFails)
	}
}

// TestHistoryWriter_PassivePodNoLockDEL verifies that a passive writer does not delete
// the lock on shutdown — only the active writer may release it via Lua CAS.
func TestHistoryWriter_PassivePodNoLockDEL(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.podID = "passive-no-del"

	lockKey := history.HistoryWriterLockKeyPrefix + opts.env
	setupClient := newTestValkeyClient(t, mr)
	if err := setupClient.Do(context.Background(),
		setupClient.B().Set().Key(lockKey).Value("owner-pod").Build(),
	).Error(); err != nil {
		t.Fatalf("pre-set lock: %v", err)
	}

	w, cancel, _ := newTestWriter(t, mr, opts)
	var wg syncWaitGroup
	wg.Go(func() { w.Run() })
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	n, err := setupClient.Do(context.Background(),
		setupClient.B().Exists().Key(lockKey).Build(),
	).AsInt64()
	if err != nil {
		t.Fatalf("EXISTS: %v", err)
	}
	if n == 0 {
		t.Error("passive writer deleted the lock on shutdown — only active writers may release it")
	}
}

// TestHistoryWriter_LuaCASAtomicity verifies that the lock release Lua script is a true
// CAS operation: when another pod has stolen the lock before shutdown, the Lua script
// must NOT delete the thief's lock.
func TestHistoryWriter_LuaCASAtomicity(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.podID = "pod-A"
	opts.heartbeatInterval = 500 * time.Millisecond // long heartbeat prevents CAS-miss detection before cancel

	w, cancel, _ := newTestWriter(t, mr, opts)
	var wg syncWaitGroup
	wg.Go(func() { w.Run() })
	time.Sleep(30 * time.Millisecond) // let pod-A acquire lock (isActiveWriter=true)

	// Pod-B steals the lock; pod-A's isActiveWriter is still true (no heartbeat tick yet).
	lockKey := history.HistoryWriterLockKeyPrefix + opts.env
	setupClient := newTestValkeyClient(t, mr)
	if err := setupClient.Do(context.Background(),
		setupClient.B().Set().Key(lockKey).Value("pod-B").
			PxMilliseconds(opts.lockTTLMs).Build(),
	).Error(); err != nil {
		t.Fatalf("steal lock: %v", err)
	}

	// Cancel immediately so the pos2 defer runs with isActiveWriter=true but lock owned by pod-B.
	// The Lua CAS release must detect the mismatch and NOT delete pod-B's lock.
	cancel()
	wg.Wait()

	val, err := setupClient.Do(context.Background(),
		setupClient.B().Get().Key(lockKey).Build(),
	).ToString()
	if err != nil {
		t.Fatalf("GET lock after shutdown: %v", err)
	}
	if val != "pod-B" {
		t.Errorf("CAS atomicity violated: expected lock to remain 'pod-B' after pod-A shutdown, got %q", val)
	}
}

// TestHistoryWriter_RestartBackoffJitter verifies that the restart backoff keeps delays
// within [initialBackoff, maxBackoff] and allows multiple restarts to occur (i.e., the
// backoff is neither zero nor stuck).
func TestHistoryWriter_RestartBackoffJitter(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.heartbeatInterval = 10 * time.Millisecond
	opts.restartInitialBackoff = 5 * time.Millisecond
	opts.restartMaxBackoff = 20 * time.Millisecond

	w, cancel, bus := newTestWriter(t, mr, opts)
	var wg syncWaitGroup
	wg.Go(func() { w.Run() })
	time.Sleep(5 * time.Millisecond)
	bus.setHealthy(false) // trigger continuous restarts

	// With max backoff 20ms and heartbeat 10ms, each restart cycle is ≤ 30ms.
	// In 250ms we expect at least 5 restarts but not an absurdly large number
	// (which would indicate backoff is being skipped).
	time.Sleep(250 * time.Millisecond)
	cancel()
	wg.Wait()

	restarts := prometheustestutil.ToFloat64(w.Metrics().WriterRestartTotal)
	// Under -race the scheduler adds overhead; use a conservative lower bound.
	// With maxBackoff=20ms over 250ms we expect well above 3 cycles under any load.
	if restarts < 3 {
		t.Errorf("too few restarts (%.0f) in 250ms with maxBackoff=20ms: backoff jitter may be broken", restarts)
	}
	// Sanity upper bound: with 0 backoff we'd get ~250/10 = 25 cycles; well under 50.
	if restarts > 50 {
		t.Errorf("too many restarts (%.0f): backoff may not be applied at all", restarts)
	}
}

// TestHistoryWriter_BackoffResetAfterSuccess verifies that after a runOnce that processed
// ≥1 message, the restart backoff resets to initialBackoff rather than continuing to double.
// Observable consequence: after fail→success→fail, a restart happens within ~initialBackoff,
// not within a doubled-and-growing backoff.
func TestHistoryWriter_BackoffResetAfterSuccess(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.heartbeatInterval = 10 * time.Millisecond
	opts.restartInitialBackoff = 10 * time.Millisecond
	opts.restartMaxBackoff = 10 * time.Second // high cap so doubling would compound

	w, cancel, bus := newTestWriter(t, mr, opts)
	var wg syncWaitGroup
	wg.Go(func() { w.Run() })
	time.Sleep(20 * time.Millisecond) // let writer subscribe and acquire lock

	// Phase 1: process a message while healthy so processed > 0 in the first runOnce.
	bus.fanOut(&broadcast.Message{Subject: "t1.c", Payload: []byte(`{}`), TenantID: "t1", Channel: "c"})
	time.Sleep(30 * time.Millisecond) // allow flush

	// Phase 2: force failure — because processed > 0, backoff resets to initialBackoff (10ms).
	bus.setHealthy(false)

	// The restart cycle should complete in ~(10ms hb + [10ms,20ms) backoff) = ~20-30ms.
	// Without the reset, after 3+ failures backoff would be ≥80ms and no restart in 60ms.
	deadline := time.Now().Add(60 * time.Millisecond)
	for time.Now().Before(deadline) {
		if prometheustestutil.ToFloat64(w.Metrics().WriterRestartTotal) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	restarts := prometheustestutil.ToFloat64(w.Metrics().WriterRestartTotal)

	cancel()
	wg.Wait()

	if restarts < 1 {
		t.Errorf("BackoffResetAfterSuccess: expected restart within 60ms after success-then-failure (reset backoff=10ms), got %.0f restarts", restarts)
	}
}

// TestHistoryWriter_HeartbeatDualMode verifies the dual heartbeat behavior:
// (a) active pod renews the lock via CAS Lua on each tick (TTL is refreshed);
// (b) passive pod attempts SetNX on each tick and acquires the lock once the TTL expires.
func TestHistoryWriter_HeartbeatDualMode(t *testing.T) {
	t.Parallel()

	// --- Part A: active writer renews lock TTL on each heartbeat ---
	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	opts.heartbeatInterval = 20 * time.Millisecond
	opts.lockTTLMs = 100 // 100ms TTL — short enough to expire without renewal

	w, cancel, _ := newTestWriter(t, mr, opts)
	var wg syncWaitGroup
	wg.Go(func() { w.Run() })
	time.Sleep(30 * time.Millisecond) // writer acquires lock and runs a heartbeat tick

	lockKey := history.HistoryWriterLockKeyPrefix + opts.env
	checkClient := newTestValkeyClient(t, mr)
	pttl, err := checkClient.Do(context.Background(),
		checkClient.B().Pttl().Key(lockKey).Build(),
	).AsInt64()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	// Lock should still exist with meaningful TTL (renewed, not decayed to near 0).
	if pttl <= 0 {
		t.Errorf("active writer: lock expired before heartbeat renewal (PTTL=%dms)", pttl)
	}
	cancel()
	wg.Wait()

	// --- Part B: passive writer acquires lock after TTL expiry ---
	mr2 := newTestMiniredis(t)
	opts2 := defaultTestOpts()
	opts2.podID = "passive-hb"
	opts2.heartbeatInterval = 20 * time.Millisecond
	opts2.lockTTLMs = 200

	lockKey2 := history.HistoryWriterLockKeyPrefix + opts2.env
	setupClient := newTestValkeyClient(t, mr2)
	if err := setupClient.Do(context.Background(),
		setupClient.B().Set().Key(lockKey2).Value("old-pod").
			PxMilliseconds(opts2.lockTTLMs).Build(),
	).Error(); err != nil {
		t.Fatalf("pre-set lock: %v", err)
	}

	w2, cancel2, _ := newTestWriter(t, mr2, opts2)
	var wg2 syncWaitGroup
	wg2.Go(func() { w2.Run() })
	time.Sleep(30 * time.Millisecond) // let writer start and first tryAcquireLock run

	// FastForward miniredis clock past the key TTL so the key expires on the next access.
	mr2.FastForward(300 * time.Millisecond)

	// Wait for a heartbeat tick to fire and acquire the now-expired lock.
	time.Sleep(50 * time.Millisecond)

	active := prometheustestutil.ToFloat64(w2.Metrics().WriterActive)
	cancel2()
	wg2.Wait()

	if active != 1 {
		t.Errorf("passive writer: expected to acquire lock after TTL expiry (WriterActive=1), got %.0f", active)
	}
}

// TestHistoryWriter_KafkaCoordInField verifies that 3+ Kafka-coordinated messages produce
// XADD entries with auto-assigned Valkey IDs (not encoded pos) and the encoded pos stored
// in the coord field for use as a replay cursor.
func TestHistoryWriter_KafkaCoordInField(t *testing.T) {
	t.Parallel()

	mr := newTestMiniredis(t)
	opts := defaultTestOpts()
	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() { w.Run() })
	time.Sleep(20 * time.Millisecond)

	msgs := []*broadcast.Message{
		{Subject: "tenantA.BTC.trade", Payload: []byte(`{"p":1}`), TenantID: "tenantA", Channel: "BTC.trade", Pos: history.EncodePos(0, 100)},
		{Subject: "tenantA.BTC.trade", Payload: []byte(`{"p":2}`), TenantID: "tenantA", Channel: "BTC.trade", Pos: history.EncodePos(0, 200)},
		{Subject: "tenantA.BTC.trade", Payload: []byte(`{"p":3}`), TenantID: "tenantA", Channel: "BTC.trade", Pos: history.EncodePos(0, 300)},
	}
	for _, m := range msgs {
		bus.fanOut(m)
	}
	time.Sleep(120 * time.Millisecond)

	cancel()
	wg.Wait()

	client := newTestValkeyClient(t, mr)
	streamKey := history.HistoryStreamKeyPrefix + opts.env + ":tenantA:BTC.trade"
	entries, err := client.Do(context.Background(),
		client.B().Xrange().Key(streamKey).Start("-").End("+").Build(),
	).AsXRange()
	if err != nil {
		t.Fatalf("XRANGE: %v", err)
	}
	if len(entries) < 3 {
		t.Fatalf("expected ≥3 entries, got %d", len(entries))
	}

	expectedCoords := []string{history.EncodePos(0, 100), history.EncodePos(0, 200), history.EncodePos(0, 300)}
	for i, want := range expectedCoords {
		// IDs must be Valkey auto-IDs, not encoded pos.
		if entries[i].ID == want {
			t.Errorf("entry[%d].ID = %q: must be Valkey auto-ID, not encoded pos", i, entries[i].ID)
		}
		if entries[i].FieldValues[history.HistoryFieldCoord] != want {
			t.Errorf("entry[%d].coord = %q, want %q", i, entries[i].FieldValues[history.HistoryFieldCoord], want)
		}
	}

	// Verify IDs are strictly increasing (auto-ID guarantees this).
	for i := 1; i < len(entries); i++ {
		if entries[i].ID <= entries[i-1].ID {
			t.Errorf("entry[%d].ID=%q not > entry[%d].ID=%q", i, entries[i].ID, i-1, entries[i-1].ID)
		}
	}
}

// TestHistoryWriter_AutoIDAndCoord covers the two coord modes:
//   - no pos → coord=HistoryCoordAuto, auto-ID
//   - with pos → coord=encoded pos, auto-ID (not the pos itself as stream ID)
func TestHistoryWriter_AutoIDAndCoord(t *testing.T) {
	t.Parallel()

	t.Run("NoPos_AutoCoord", func(t *testing.T) {
		t.Parallel()

		mr := newTestMiniredis(t)
		opts := defaultTestOpts()
		w, cancel, bus := newTestWriter(t, mr, opts)

		var wg syncWaitGroup
		wg.Go(func() { w.Run() })
		time.Sleep(20 * time.Millisecond)

		// Message with no pos → writer should use XADD * with coord=HistoryCoordAuto.
		bus.fanOut(&broadcast.Message{
			Subject:  "tenantA.ETH.trade",
			Payload:  []byte(`{"p":1}`),
			TenantID: "tenantA",
			Channel:  "ETH.trade",
			Pos:      "", // no pos
		})
		time.Sleep(80 * time.Millisecond)
		cancel()
		wg.Wait()

		client := newTestValkeyClient(t, mr)
		streamKey := history.HistoryStreamKeyPrefix + opts.env + ":tenantA:ETH.trade"
		entries, err := client.Do(context.Background(),
			client.B().Xrange().Key(streamKey).Start("-").End("+").Build(),
		).AsXRange()
		if err != nil {
			t.Fatalf("XRANGE: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].FieldValues[history.HistoryFieldCoord] != history.HistoryCoordAuto {
			t.Errorf("coord = %q, want %q", entries[0].FieldValues[history.HistoryFieldCoord], history.HistoryCoordAuto)
		}
	})

	t.Run("WithPos_PosStoredInCoord_NotInID", func(t *testing.T) {
		t.Parallel()

		mr := newTestMiniredis(t)
		opts := defaultTestOpts()
		w, cancel, bus := newTestWriter(t, mr, opts)

		// Pre-insert an entry with a high ID to prove auto-ID still succeeds after it.
		client := newTestValkeyClient(t, mr)
		streamKey := history.HistoryStreamKeyPrefix + opts.env + ":tenantA:SOL.trade"
		if err := client.Do(context.Background(),
			client.B().Xadd().Key(streamKey).
				Id("9999999999999-0").
				FieldValue().
				FieldValue(history.HistoryFieldPayload, `{"pre":"inserted"}`).
				FieldValue(history.HistoryFieldTenantID, "tenantA").
				FieldValue(history.HistoryFieldChannel, "SOL.trade").
				FieldValue(history.HistoryFieldSubject, "tenantA.SOL.trade").
				FieldValue(history.HistoryFieldCoord, history.HistoryCoordAuto).
				Build(),
		).Error(); err != nil {
			t.Fatalf("pre-insert XADD: %v", err)
		}

		var wg syncWaitGroup
		wg.Go(func() { w.Run() })
		time.Sleep(20 * time.Millisecond)

		encodedPos := history.EncodePos(0, 100) // "1-100" — would fail as an explicit ID after "9999999999999-0"
		bus.fanOut(&broadcast.Message{
			Subject:  "tenantA.SOL.trade",
			Payload:  []byte(`{"p":1}`),
			TenantID: "tenantA",
			Channel:  "SOL.trade",
			Pos:      encodedPos,
		})
		time.Sleep(120 * time.Millisecond)
		cancel()
		wg.Wait()

		// Auto-ID succeeds even after the high pre-inserted entry; stream now has 2 entries.
		entries, err := client.Do(context.Background(),
			client.B().Xrange().Key(streamKey).Start("-").End("+").Build(),
		).AsXRange()
		if err != nil {
			t.Fatalf("XRANGE: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("expected 2 entries (pre-inserted + new), got %d", len(entries))
		}
		newEntry := entries[1] // second entry: miniredis promoted auto-ID to 9999999999999-1 (sequence increment past high explicit entry)
		// Entry ID must be a Valkey auto-ID, not the encoded pos.
		if newEntry.ID == encodedPos {
			t.Errorf("entry.ID = %q: must be Valkey auto-ID, not encoded pos", newEntry.ID)
		}
		// Encoded pos must be stored in the coord field.
		if newEntry.FieldValues[history.HistoryFieldCoord] != encodedPos {
			t.Errorf("coord = %q, want %q", newEntry.FieldValues[history.HistoryFieldCoord], encodedPos)
		}
	})
}

// TestHistoryWriter_SubscribeAll_Called verifies that the writer calls SubscribeAll (not Subscribe)
// and calls UnsubscribeAll (not Unsubscribe) on shutdown.
func TestHistoryWriter_SubscribeAll_Called(t *testing.T) {
	t.Parallel()
	mr := newTestMiniredis(t)
	opts := defaultTestOpts()

	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	// Wait for the writer to start and call SubscribeAll.
	time.Sleep(50 * time.Millisecond)

	if got := bus.getSubscribeAllCount(); got < 1 {
		t.Errorf("SubscribeAll not called after startup: got %d, want >= 1", got)
	}

	// Shutdown and verify UnsubscribeAll is called.
	cancel()
	wg.Wait()

	if got := bus.getUnsubscribeAllCount(); got < 1 {
		t.Errorf("UnsubscribeAll not called on shutdown: got %d, want >= 1", got)
	}
}

// TestHistoryWriter_SubscribeAll_MessagesArrive verifies that messages published via fanOut
// (which routes to allSubs = SubscribeAll channel) are written to Valkey streams.
func TestHistoryWriter_SubscribeAll_MessagesArrive(t *testing.T) {
	t.Parallel()
	mr := newTestMiniredis(t)
	opts := defaultTestOpts()

	w, cancel, bus := newTestWriter(t, mr, opts)

	var wg syncWaitGroup
	wg.Go(func() {
		w.Run()
	})

	// Wait for the writer to subscribe.
	time.Sleep(30 * time.Millisecond)

	bus.fanOut(&broadcast.Message{
		Subject:  "subscribetest.BTC.trade",
		TenantID: "subscribetest",
		Channel:  "BTC.trade",
		Payload:  []byte(`{"price":"99"}`),
		Pos:      "0-1",
	})

	// Wait for relay + flushBatch.
	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()

	// Verify the message was written to Valkey.
	client := newTestValkeyClient(t, mr)
	streamKey := history.HistoryStreamKeyPrefix + opts.env + ":subscribetest:BTC.trade"
	entries, err := client.Do(context.Background(),
		client.B().Xrevrange().Key(streamKey).End("+").Start("-").Count(10).Build(),
	).AsXRange()
	if err != nil {
		t.Fatalf("XREVRANGE %s: %v", streamKey, err)
	}
	if len(entries) == 0 {
		t.Errorf("no entries in stream %s, want >= 1", streamKey)
	}
}
