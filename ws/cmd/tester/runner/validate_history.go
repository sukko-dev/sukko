package runner

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

// SuiteHistory proves the ADR-0009 message-history data path end to end on the
// kafka backend, license-free: records ingested directly to the tenant topic are
// returned by a history{channel, limit} request with IDENTICAL ADR-0008 mids —
// two of the three legs of the mid identity triple (live == history == replay).
const SuiteHistory = "history"

const historyIngestCount = 4

// validateHistory: subscriber A (connected before ingest) captures the live
// copies; subscriber B connects AFTER ingest, so every tracked frame B receives
// following its history request is a history copy by construction — no source
// tag exists on the wire (history results ride plain "message" frames until
// history_complete), and this two-client split is what makes the discrimination
// deterministic.
func validateHistory(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	if pre := recoverySetupError(run, SuiteHistory); pre != nil {
		return pre, nil
	}
	if fail := setupRecoveryChannel(ctx, run, logger); fail != nil {
		return []metrics.CheckResult{*fail}, nil
	}
	tenantID := run.authResult.TenantID
	channel := tenantID + "." + recoveryChannelSuffix

	engine := NewPubSubEngine(PubSubEngineConfig{
		GatewayURL: run.Config.GatewayURL,
		Logger:     logger,
		Timeout:    kafkaIngestDeliveryTimeout,
	})

	// Subscriber A: live witness (mid reference).
	userA, err := engine.CreateUser(ctx, run.authResult.Minter, auth.MintOptions{Subject: "history-live", ConnIndex: 0})
	if err != nil {
		return []metrics.CheckResult{{Name: "create live subscriber", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}
	defer func() { _ = userA.Client.Close() }() // best-effort test cleanup
	if err := userA.SubscribeConfirmed(ctx, []string{channel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		return []metrics.CheckResult{{Name: "subscribe live", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}

	pub, err := recoveryPublisher(run, tenantID)
	if err != nil {
		return []metrics.CheckResult{{Name: "kafka-connect", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("could not reach Kafka at %s: %v", run.kafkaBrokers, err)}}, nil
	}
	defer func() { _ = pub.Close() }() // best-effort test cleanup

	// Ingest K tracked records; A receives them live.
	msgIDs := make([]string, 0, historyIngestCount)
	for i := range historyIngestCount {
		result := ingestTracked(ctx, engine, pub, channel, []*TestUser{userA}, []*TestUser{userA})
		if !result.Delivered {
			return []metrics.CheckResult{deliveryCheck(fmt.Sprintf("ingest %d", i), result)}, nil
		}
		msgIDs = append(msgIDs, result.MessageID)
	}

	checks := make([]metrics.CheckResult, 0, 4)

	// Precondition: the kafka backend stamps a pos cursor on live frames — a
	// missing pos means a direct-mode misconfiguration and would otherwise
	// surface as a confusing recovery failure downstream.
	if _, pos, _, _ := userA.ReceivedMeta(msgIDs[0]); pos == "" {
		checks = append(checks, metrics.CheckResult{Name: "pos present", Status: metrics.CheckStatusFail,
			Error: "live message carries no pos cursor — is the server on MESSAGE_BACKEND=kafka?"})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "pos present", Status: metrics.CheckStatusPass})

	// Subscriber B: connects AFTER ingest; everything tracked it receives after
	// requesting history is a history copy.
	userB, err := engine.CreateUser(ctx, run.authResult.Minter, auth.MintOptions{Subject: "history-reader", ConnIndex: 1})
	if err != nil {
		checks = append(checks, metrics.CheckResult{Name: "create history reader", Status: metrics.CheckStatusFail, Error: err.Error()})
		return checks, nil
	}
	defer func() { _ = userB.Client.Close() }() // best-effort test cleanup
	if err := userB.SubscribeConfirmed(ctx, []string{channel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		checks = append(checks, metrics.CheckResult{Name: "subscribe history reader", Status: metrics.CheckStatusFail, Error: err.Error()})
		return checks, nil
	}
	if err := userB.Client.History(channel, historyIngestCount*2); err != nil {
		checks = append(checks, metrics.CheckResult{Name: "history request", Status: metrics.CheckStatusFail, Error: err.Error()})
		return checks, nil
	}

	// Terminator-driven: history_complete ends the window; history_error fails loudly.
	if _, ok := userB.WaitControlFrame(ctx, "history_complete", recoveryFrameTimeout, recoveryWaitInterval); !ok {
		if errFrame, isErr := userB.ControlFrame("history_error"); isErr {
			checks = append(checks, metrics.CheckResult{Name: "history complete", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("history_error: code=%s message=%s", errFrame.Code, errFrame.ErrMessage)})
		} else {
			checks = append(checks, metrics.CheckResult{Name: "history complete", Status: metrics.CheckStatusFail,
				Error: "history_complete never arrived"})
		}
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "history complete", Status: metrics.CheckStatusPass})

	// Delivery: every ingested record came back.
	if !userB.HasAllMessages(msgIDs) {
		missing := 0
		for _, id := range msgIDs {
			if _, _, _, ok := userB.ReceivedMeta(id); !ok {
				missing++
			}
		}
		checks = append(checks, metrics.CheckResult{Name: "history delivery", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("%d of %d ingested records absent from history", missing, len(msgIDs))})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "history delivery", Status: metrics.CheckStatusPass})

	// Identity: the history copy of each record carries the SAME non-empty mid
	// as its live copy (ADR-0008 — mid identical across copies).
	for _, id := range msgIDs {
		liveMid, _, _, _ := userA.ReceivedMeta(id)
		histMid, _, _, _ := userB.ReceivedMeta(id)
		if liveMid == "" || liveMid != histMid {
			checks = append(checks, metrics.CheckResult{Name: "history mid equality", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("msg %s: live mid %q vs history mid %q", id, liveMid, histMid)})
			return checks, nil
		}
	}
	checks = append(checks, metrics.CheckResult{Name: "history mid equality", Status: metrics.CheckStatusPass})

	return checks, nil
}
