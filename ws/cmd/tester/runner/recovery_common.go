package runner

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/publisher"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// Shared harness for the recovery suites (history, gap-recovery). Both prove
// ADR-0009 data-path features end to end on the kafka backend by ingesting
// records DIRECTLY to the tenant's default topic (the kafka-ingest pattern —
// license-free by design, ADR-0006: no routing rules involved) and asserting
// the recovery surfaces return them with identical ADR-0008 mids.

const (
	recoveryChannelSuffix = "kafka.recovery.test"
	recoveryChannelPatt   = "kafka.recovery.*"
	// recoveryWaitInterval paces terminator/delivery polls.
	recoveryWaitInterval = 100 * time.Millisecond
	// recoveryFrameTimeout bounds waits for terminator frames (history_complete,
	// reconnect_ack, replay_complete) — generous for a fresh consumer join.
	recoveryFrameTimeout = 30 * time.Second
)

// recoverySetupError returns the skip/fail pre-check shared by the recovery
// suites: no brokers ⇒ the suite SKIPS (not configured); brokers without a
// namespace ⇒ loud fail (publishing to prefixless topics would surface as a
// confusing delivery timeout).
func recoverySetupError(run *TestRun, suite string) []metrics.CheckResult {
	if run.kafkaBrokers == "" {
		return []metrics.CheckResult{{
			Name:   suite,
			Status: metrics.CheckStatusSkip,
			Error:  "KAFKA_BROKERS not set — configure the tester's Kafka connection to run this suite",
		}}
	}
	if run.kafkaNamespace == "" {
		return []metrics.CheckResult{{
			Name:   suite,
			Status: metrics.CheckStatusFail,
			Error:  "KAFKA_TOPIC_NAMESPACE is required when Kafka brokers are set — it must match the server-under-test",
		}}
	}
	return nil
}

// recoveryPublisher builds the direct-to-Kafka publisher targeting the tenant's
// default topic with a local resolver (no provisioned routing rules — ADR-0006).
func recoveryPublisher(run *TestRun, tenantID string) (publisher.Publisher, error) {
	resolver := publisher.NewTopicResolver(run.kafkaNamespace, tenantID, []publisher.RoutingRule{
		{Pattern: "**", Topics: []string{routing.DefaultTopicSuffix}},
	})
	return newKafkaPub(run.kafkaBrokers, resolver, run.kafkaSASL, run.kafkaTLS)
}

// ingestRecovery is ingestTracked's recovery choice after a failed attempt.
type ingestRecovery int

const (
	ingestDone       ingestRecovery = iota // delivered — nothing to recover
	ingestRepublish                        // publish failed — record never reached the log; a fresh record is safe
	ingestExtendWait                       // publish acked, delivery slow — re-wait; republishing would orphan a duplicate
)

// classifyIngestFailure decides how ingestTracked recovers. A publish error
// means the record (almost certainly) never reached the log — republishing
// under a fresh msg_id is safe. A delivery timeout WITHOUT a publish error
// means the record IS in the log and the consumer has not yet delivered it —
// republishing would drop an orphan duplicate into the replay window and inflate
// exact messages_replayed counts, so the only safe recovery is to keep waiting.
// (Before ADR-0010 a delivery timeout could also mean the record was skipped
// outright by the consumer's AtEnd reset racing the produce — extend-wait could
// not recover that. AfterMilli(process-start) makes a fresh topic consume from
// offset 0, so a timeout here is now genuinely just slowness.)
func classifyIngestFailure(res DeliveryResult) ingestRecovery {
	switch {
	case res.Delivered:
		return ingestDone
	case res.PublishErr != nil:
		return ingestRepublish
	default:
		return ingestExtendWait
	}
}

// ingestTracked publishes one tracked message via the direct publisher and
// waits for the expected receivers, recovering once per classifyIngestFailure.
// Returns the delivery result of the record that (finally) delivered.
func ingestTracked(ctx context.Context, engine *PubSubEngine, pub publisher.Publisher, channel string, expected, all []*TestUser) DeliveryResult {
	result := engine.PublishAndVerify(ctx, pub, channel, expected, all)
	switch classifyIngestFailure(result) {
	case ingestDone:
		// delivered on the first attempt — nothing to recover
	case ingestRepublish:
		result = engine.PublishAndVerify(ctx, pub, channel, expected, all)
	case ingestExtendWait:
		delivered := true
		for _, u := range expected {
			if !u.WaitForMessages(ctx, []string{result.MessageID}, recoveryFrameTimeout, recoveryWaitInterval) {
				delivered = false
				break
			}
		}
		if delivered {
			result.Delivered = true
			result.Missing = nil
		}
	}
	return result
}

// setupRecoveryChannel authorizes the recovery test channel for the tenant.
func setupRecoveryChannel(ctx context.Context, run *TestRun, logger zerolog.Logger) *metrics.CheckResult {
	_ = logger
	channelRules := map[string]any{
		"public":  []string{recoveryChannelPatt},
		"default": []string{recoveryChannelPatt},
	}
	if err := run.authResult.ProvClient.SetChannelRules(ctx, run.authResult.TenantID, channelRules); err != nil {
		return &metrics.CheckResult{Name: "setup channel rules", Status: metrics.CheckStatusFail, Error: err.Error()}
	}
	return nil
}
