package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// Rate-limit enforcement frame type + code. Tester-local constants (§I/§X): the
// tester matches wire frames as bare strings and imports no internal/gateway code,
// mirroring respTypeAuthAck in the upgrade suite. The gateway rejects an over-limit
// WS publish with a publish_error frame carrying a top-level code (proxy.go:582) on
// the still-open connection — NOT a client write error and NOT a connection close.
const (
	respTypePublishError = "publish_error" // server→client: a publish was rejected; the reason is the top-level Code field
	errCodeRateLimited   = "rate_limited"  // publish_error Code emitted when the per-connection publish limiter trips
)

// Rate-limit suite tuning. Every value carries a deliberate margin over the gateway's
// FIXED publish-limiter defaults (GATEWAY_PUBLISH_RATE_LIMIT=10 msg/s, GATEWAY_PUBLISH_BURST=100);
// no stack tuning is required for determinism.
const (
	// rateLimitBurstCount is 5× the default burst allowance (100). A zero-delay burst of
	// 500 leaves ≥300 rejections even if the send somehow took a full 10s (during which only
	// ~100 tokens refill), so at least one rate_limited frame is guaranteed under CI load.
	rateLimitBurstCount = 500
	// rateLimitSignalWait bounds the wait for the first rate_limited frame after the burst.
	// Generous for CI scheduling latency; the signal is in-band and normally arrives during the burst.
	rateLimitSignalWait = 5 * time.Second
	// rateLimitQuiesceWait lets burst-era rate_limited stragglers drain AND refills ~20 tokens
	// (10/s) so the recovery probe is comfortably inside the refilled allowance.
	rateLimitQuiesceWait = 2 * time.Second
	// rateLimitProbeDeliveryWait bounds the self-delivery round-trip of the recovery probe.
	rateLimitProbeDeliveryWait = 5 * time.Second
	// rateLimitSubscribeSettle lets the subscription propagate before the burst starts.
	rateLimitSubscribeSettle = 300 * time.Millisecond
)

// rateLimitFrameClass is the classification the suite cares about for an inbound frame.
type rateLimitFrameClass int

const (
	frameOther       rateLimitFrameClass = iota // acks, pings, publish_error of other codes — ignored
	frameRateLimited                            // publish_error with code rate_limited — the enforcement signal
	frameDelivery                               // a delivered payload envelope — carries a msg_id for self-delivery tracking
)

// classifyRateLimitFrame maps an inbound frame to its class. Pure — table-testable.
// A publish_error with code rate_limited is the enforcement signal; delivery envelopes (via the
// shared acceptsDeliveryEnvelope classifier, read-only) carry self-delivered messages; everything
// else is ignored so acks/pings/other publish_error codes never satisfy the enforcement check.
func classifyRateLimitFrame(msg testerws.Message) rateLimitFrameClass {
	if msg.Type == respTypePublishError && msg.Code == errCodeRateLimited {
		return frameRateLimited
	}
	if acceptsDeliveryEnvelope(msg.Type) {
		return frameDelivery
	}
	return frameOther
}

// deliveredMsgID extracts msg_id from a delivery envelope payload, or "" if absent/malformed. Pure.
func deliveredMsgID(data json.RawMessage) string {
	var payload struct {
		MsgID string `json:"msg_id"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return ""
	}
	return payload.MsgID
}

// rateLimitObservation is the assembled evidence the pure decision function scores.
type rateLimitObservation struct {
	RateLimitedCount         int64 // rate_limited frames observed during + after the burst
	ConnClosed               bool  // ReadLoop returned while the suite ctx was still live (gateway closed us)
	WriteErrors              int   // client Publish write failures during the burst
	Sent                     int   // publishes the client wrote successfully
	ProbeDelivered           bool  // the recovery probe's msg_id round-tripped back via self-delivery
	RateLimitedAfterSnapshot int64 // new rate_limited frames after the post-quiesce snapshot (attributable to the probe window)
}

// decideRateLimitChecks builds the three fail-closed checks from an observation. Pure (no I/O)
// so every pass/fail path is table-testable without a live stack. There is NO skip path:
// the absence of the enforcement signal is a hard fail, never a vacuous skip.
func decideRateLimitChecks(obs rateLimitObservation) []metrics.CheckResult {
	checks := make([]metrics.CheckResult, 0, 3)

	// Check 1 — enforcement signal observed.
	if obs.RateLimitedCount >= 1 {
		checks = append(checks, metrics.CheckResult{
			Name:    "rate limit enforcement",
			Status:  metrics.CheckStatusPass,
			Latency: fmt.Sprintf("sent %d, observed %d rate_limited frames", obs.Sent, obs.RateLimitedCount),
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name:   "rate limit enforcement",
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("no rate_limited frame after bursting %d publishes — limiter disabled or defeated", obs.Sent),
		})
	}

	// Check 2 — connection survives, no client write error (spec AC-2).
	if !obs.ConnClosed && obs.WriteErrors == 0 {
		checks = append(checks, metrics.CheckResult{
			Name:   "connection survives rate limit",
			Status: metrics.CheckStatusPass,
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name:   "connection survives rate limit",
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("connClosed=%v writeErrors=%d — enforcement must reject in-band, not close or error the connection", obs.ConnClosed, obs.WriteErrors),
		})
	}

	// Check 3 — publish recovers after refill: positive self-delivery + no new rate_limited.
	if obs.ProbeDelivered && obs.RateLimitedAfterSnapshot == 0 {
		checks = append(checks, metrics.CheckResult{
			Name:   "publish recovers after refill",
			Status: metrics.CheckStatusPass,
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name:   "publish recovers after refill",
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("probeDelivered=%v newRateLimited=%d — a post-refill probe must be accepted and delivered back", obs.ProbeDelivered, obs.RateLimitedAfterSnapshot),
		})
	}

	return checks
}

// validateRateLimit proves the gateway enforces its per-connection WS publish rate limit by
// reading the in-band publish_error/rate_limited frame (not by inferring from write errors or a
// connection close), and that enforcement discriminates: the connection survives and a post-refill
// probe is accepted and delivered back. No skip path — every check is fail-closed.
func validateRateLimit(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	provClient := run.authResult.ProvClient
	tenantID := run.authResult.TenantID
	channel := tenantChannel(tenantID, "general.test")

	// The suite mints and publishes over a JWT connection; api-key mode has no TokenFunc (defense in depth, §II).
	if run.authResult.TokenFunc == nil {
		return []metrics.CheckResult{{
			Name: "connect", Status: metrics.CheckStatusFail,
			Error: "ratelimit suite requires JWT auth (TokenFunc is nil)",
		}}, nil
	}
	token := run.authResult.TokenFunc(0)

	// Setup: routing rules are best-effort (D6). On the direct backend this suite targets,
	// publishes are broadcast without Kafka topic routing, so routing rules do not affect
	// self-delivery; the Pro-gated routing API's rejection on Community is therefore harmless.
	// (If this suite is ever added to a Kafka cell, switch to setupSuiteRoutingRules so a
	// missing catch-all rule is not silently tolerated.)
	_ = provClient.SetRoutingRules(ctx, tenantID, testRoutingRules)

	// Channel rules are HARD-required: a ruleless tenant is deny-all for subscribe, which
	// would make the recovery probe's self-delivery unobservable. Mirrors validate_pubsub.go:86.
	if err := provClient.SetChannelRules(ctx, tenantID, testChannelRules); err != nil {
		return []metrics.CheckResult{{
			Name: "setup channel rules", Status: metrics.CheckStatusFail, Error: err.Error(),
		}}, nil
	}

	// Shared observation state, written from the single ReadLoop goroutine, read by the suite goroutine.
	var rateLimitedCount atomic.Int64
	var connClosed atomic.Bool
	delivered := newMsgIDTracker() // presence set of self-delivered msg_ids (concurrent-safe)

	onMessage := func(msg testerws.Message) {
		switch classifyRateLimitFrame(msg) {
		case frameRateLimited:
			rateLimitedCount.Add(1)
		case frameDelivery:
			if id := deliveredMsgID(msg.Data); id != "" {
				delivered.Add(id, msg.Mid)
			}
		case frameOther:
			// ignored
		}
	}

	// connectWithRetry absorbs the freshly-minted-JWT key-registry cache race (D2). Single reader only.
	client, err := connectWithRetry(ctx, run.Config.GatewayURL, token, logger.With().Str("suite", "ratelimit").Logger(), onMessage)
	if err != nil {
		return []metrics.CheckResult{{
			Name: "connect", Status: metrics.CheckStatusFail, Error: err.Error(),
		}}, nil
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		defer logging.RecoverPanic(logger, "validate_ratelimit_readloop", nil)
		_, _ = client.ReadLoop(ctx)
		// ReadLoop returned while the suite ctx is still live ⇒ the gateway closed us (not our own
		// deferred Close(), which runs only after the observation is assembled below).
		if ctx.Err() == nil {
			connClosed.Store(true)
		}
	})
	// §VII shutdown order: Close() unblocks the blocking ReadServerText so Wait() returns
	// (the validate_upgrade.go:62-65 client1 order — NOT the reversed Wait();Close()).
	defer func() {
		_ = client.Close()
		wg.Wait()
	}()

	if err := client.Subscribe([]string{channel}); err != nil {
		return []metrics.CheckResult{{
			Name: "subscribe", Status: metrics.CheckStatusFail, Error: err.Error(),
		}}, nil
	}
	time.Sleep(rateLimitSubscribeSettle)

	// Burst: zero-delay publishes far past the burst allowance. Individual write errors do NOT
	// terminate the burst — a write failure is evidence for check 2, not the detection mechanism.
	var sent, writeErrors int
	for i := range rateLimitBurstCount {
		payload, _ := json.Marshal(map[string]any{ // json.Marshal on a literal map of primitives cannot fail
			"msg_id": fmt.Sprintf("rl-%04d", i),
			"ts":     time.Now().UnixMilli(),
		})
		if err := client.Publish(channel, payload); err != nil {
			writeErrors++
		} else {
			sent++
		}
	}

	// Check 1 wait: bounded wait for the first rate_limited frame (usually already counted mid-burst).
	waitForAtLeast(ctx, &rateLimitedCount, 1, rateLimitSignalWait)

	// Quiesce: let burst-era rate_limited stragglers drain and let the token bucket refill.
	select {
	case <-ctx.Done():
	case <-time.After(rateLimitQuiesceWait):
	}

	// Snapshot AFTER quiesce so stragglers are counted in the baseline, then clear delivery
	// tracking so only the probe's own round-trip can satisfy the recovery check.
	snapshot := rateLimitedCount.Load()
	delivered.Clear()

	probeID := "rl-probe-" + uuid.NewString()
	probePayload, _ := json.Marshal(map[string]any{ // json.Marshal on a literal map of primitives cannot fail
		"msg_id": probeID,
		"ts":     time.Now().UnixMilli(),
	})
	probeDelivered := false
	if err := client.Publish(channel, probePayload); err == nil {
		probeDelivered = delivered.WaitFor(ctx, probeID, rateLimitProbeDeliveryWait)
	}

	obs := rateLimitObservation{
		RateLimitedCount:         rateLimitedCount.Load(),
		ConnClosed:               connClosed.Load(),
		WriteErrors:              writeErrors,
		Sent:                     sent,
		ProbeDelivered:           probeDelivered,
		RateLimitedAfterSnapshot: rateLimitedCount.Load() - snapshot,
	}
	return decideRateLimitChecks(obs), nil
}

// waitForAtLeast polls counter until it reaches target, or timeout / ctx cancellation elapses.
func waitForAtLeast(ctx context.Context, counter *atomic.Int64, target int64, timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if counter.Load() >= target {
			return true
		}
		select {
		case <-deadline:
			return counter.Load() >= target
		case <-ctx.Done():
			return counter.Load() >= target
		case <-ticker.C:
		}
	}
}
