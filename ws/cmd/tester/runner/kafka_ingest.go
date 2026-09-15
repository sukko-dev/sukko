package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/publisher"
	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

const (
	// SuiteKafkaIngest publishes directly to Kafka/Redpanda (bypassing the gateway)
	// and verifies a gateway-subscribed client receives the message — exercising the
	// ingestion pipeline (Kafka → ws-server consumer → broadcast → client) in
	// isolation. Requires the target server to run MESSAGE_BACKEND=kafka.
	SuiteKafkaIngest = "kafka-ingest"

	kafkaIngestTestChannel = "kafka.ingest.test"
	kafkaIngestChannelPatt = "kafka.ingest.*"

	// kafkaIngestDeliveryTimeout is deliberately generous: a freshly-provisioned
	// tenant's topic is created and subscribed via the provisioning gRPC-stream
	// update, and the consumer may take several seconds to join the group before
	// the record is delivered. The 5s PubSubEngine default is too short here.
	// TODO: confirm/tune this value with the docker-compose round-trip spike.
	kafkaIngestDeliveryTimeout = 30 * time.Second
)

// newKafkaPub constructs the direct-to-Kafka publisher. It is a package var so
// tests can inject a fake publisher.Publisher without a live broker.
var newKafkaPub = func(brokers string, resolver *publisher.TopicResolver, sasl *kafkashared.SASLConfig, tlsCfg *kafkashared.TLSConfig) (publisher.Publisher, error) {
	return publisher.NewKafkaPublisher(brokers, resolver, sasl, tlsCfg)
}

// validateKafkaIngest publishes a record straight to Kafka and verifies a
// gateway-subscribed client receives it, proving the consumer→broadcast→delivery
// half of the pipeline against real infrastructure.
func validateKafkaIngest(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	// Skip when no Kafka target is configured — empty brokers ⇒ skip, not fail.
	if run.kafkaBrokers == "" {
		return []metrics.CheckResult{{
			Name:   "kafka-ingest",
			Status: metrics.CheckStatusSkip,
			Error:  "KAFKA_BROKERS not set — configure the tester's Kafka connection to run this suite",
		}}, nil
	}
	// Brokers are set but no namespace resolved — fail clearly instead of publishing to prefixless
	// topics the server never consumes (which would surface as a confusing delivery timeout).
	if run.kafkaNamespace == "" {
		return []metrics.CheckResult{{
			Name:   "kafka-ingest",
			Status: metrics.CheckStatusFail,
			Error:  "KAFKA_TOPIC_NAMESPACE is required when Kafka brokers are set — it must match the server-under-test",
		}}, nil
	}

	provClient := run.authResult.ProvClient
	tenantID := run.authResult.TenantID

	// Authorize the test channel for the subscriber. Channel rules are Community.
	//
	// Deliberately NO SetRoutingRules here: provisioned routing rules govern the
	// CLIENT/REST publish path (channel→topic mapping, ChannelTopicRouting) and play
	// no role in this suite — the tester publishes with its OWN local TopicResolver
	// below, and the consume side derives the channel from the record's HeaderChannel
	// (kafka/consumer.go), never from routing rules. Keeping the suite free of any
	// provisioning the ingest path does not need is the point: the community-kafka grid
	// cell REQUIRE_PASSes "kafka-ingest delivery" as the ADR-0009 proof that the free
	// tier's ingest→fan-out path delivers, on its own.
	channelRules := map[string]any{
		"public":  []string{kafkaIngestChannelPatt},
		"default": []string{kafkaIngestChannelPatt},
	}
	if err := provClient.SetChannelRules(ctx, tenantID, channelRules); err != nil {
		return []metrics.CheckResult{{Name: "setup channel rules", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}

	// Subscriber via the gateway (normal auth path).
	engine := NewPubSubEngine(PubSubEngineConfig{
		GatewayURL: run.Config.GatewayURL,
		Logger:     logger,
		Timeout:    kafkaIngestDeliveryTimeout,
	})
	user, err := engine.CreateUser(ctx, run.authResult.Minter, auth.MintOptions{Subject: "kafka-ingest-sub", ConnIndex: 0})
	if err != nil {
		return []metrics.CheckResult{{Name: "create subscriber", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}
	defer func() {
		if user.Client != nil {
			_ = user.Client.Close() // best-effort test cleanup
		}
	}()
	// Subscribe to the tenant-qualified channel. The gateway requires every
	// subscribe/publish channel to carry the tenant prefix (interceptSubscribe →
	// ValidateChannelTenant); a bare channel is filtered out, forwarded to the
	// shard as an empty subscription, and silently dropped (subscriptions_count=0).
	// Publish below uses the same qualified channel so the shard's subscription key
	// matches the broadcast subject.
	qualifiedChannel := tenantID + "." + kafkaIngestTestChannel
	// Confirmed subscribe (#242 class, missed by the #246 sweep): a fire-and-forget
	// Subscribe on a freshly-provisioned tenant races channel-rule propagation — the
	// gateway silently filters the channel and nothing retries, so the delivery
	// check below would fail on absence.
	if err := user.SubscribeConfirmed(ctx, []string{qualifiedChannel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		return []metrics.CheckResult{{Name: "subscribe", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}

	// Direct-to-Kafka publisher (bypasses the gateway).
	resolver := publisher.NewTopicResolver(run.kafkaNamespace, tenantID, []publisher.RoutingRule{
		{Pattern: "**", Topics: []string{routing.DefaultTopicSuffix}},
	})
	pub, err := newKafkaPub(run.kafkaBrokers, resolver, run.kafkaSASL, run.kafkaTLS)
	if err != nil {
		// Distinct connection/auth failure. Do NOT format the SASLConfig — it would
		// leak KAFKA_SASL_USERNAME (§IX).
		return []metrics.CheckResult{{
			Name:   "kafka-connect",
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("could not reach Kafka at %s: %v", run.kafkaBrokers, err),
		}}, nil
	}
	defer func() { _ = pub.Close() }() // best-effort test cleanup

	// Publish the same tenant-qualified channel the subscriber registered and verify
	// delivery. Re-publish once on first timeout to defeat the offset race (the
	// consumer may join the topic only after the first publish, and a latest-offset
	// consumer would miss it).
	result := engine.PublishAndVerify(ctx, pub, qualifiedChannel, []*TestUser{user}, []*TestUser{user})
	if !result.Delivered {
		user.ClearReceived()
		result = engine.PublishAndVerify(ctx, pub, qualifiedChannel, []*TestUser{user}, []*TestUser{user})
	}

	check := deliveryCheck("kafka-ingest delivery", result)
	if check.Status == metrics.CheckStatusFail {
		check.Error += " — ensure the target server runs MESSAGE_BACKEND=kafka and resolves the same topic namespace as the tester"
	}
	return []metrics.CheckResult{check}, nil
}
