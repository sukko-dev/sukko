//go:build integration

package kafka

import (
	"context"
	"os/exec"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sukko-dev/sukko/internal/provisioning"
	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
)

// TestPublish_MultiTopicRule_DeliversExactlyOneCopy is the load-bearing regression
// test for the ingress/egress topic split: a routing rule that writes a message to
// more than one Kafka topic MUST result in exactly ONE copy delivered per subscriber.
//
// The consume set is derived from the SAME production function the provisioning
// service uses (provisioning.ConsumeTopicSuffixes) — never a hand-mirrored copy —
// so the test pins the produce path and the consume-set derivation together:
//   - the producer writes the ingress copy (and the egress copy, to a topic the
//     platform never consumes),
//   - the consumer, subscribed to the derived consume set, must broadcast the
//     channel's message exactly once,
//   - the publish ack must carry the ingress record's mid (a multi-topic rule is
//     no excuse for an ack without identity).
//
// Against the pre-split code this test is RED twice over: every rule topic was
// consumed (N broadcast copies of the same channel message) and the fan-out ack
// returned no mid.
func TestPublish_MultiTopicRule_DeliversExactlyOneCopy(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker is not available — skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.1.1")
	if err != nil {
		t.Skipf("failed to start Redpanda container (Docker required): %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("get broker address: %v", err)
	}

	const (
		namespace     = "local"
		tenant        = "acme"
		ingressTopic  = "trades"
		egressTopic   = "audit"
		channel       = "acme.BTC.trade"
		warmupChannel = "acme.warm.up"
		groupID       = "test-single-copy-group"
	)

	// The tenant's rule, in both of its production representations (DB-side for
	// the consume-set derivation, stream-side for the producer's snapshot).
	provRules := []provisioning.TopicRoutingRule{
		{Pattern: "acme.**", IngressTopic: ingressTopic, EgressTopics: []string{egressTopic}, Priority: 1},
	}
	streamRules := syncedProvider(rule("acme.**", ingressTopic, egressTopic))

	// Consume set exactly as provisioning derives it (default ∪ ingress topics).
	var consumeTopics []string
	for _, suffix := range provisioning.ConsumeTopicSuffixes(provRules) {
		consumeTopics = append(consumeTopics, kafkashared.BuildTopicName(namespace, tenant, suffix))
	}

	// Create every topic the tenant owns — egress topics exist on the broker even
	// though they are never consumed.
	allTopics := append([]string{}, consumeTopics...)
	egressFull := kafkashared.BuildTopicName(namespace, tenant, egressTopic)
	if !slices.Contains(allTopics, egressFull) {
		allTopics = append(allTopics, egressFull)
	}
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("create admin kgo client: %v", err)
	}
	adm := kadm.NewClient(adminClient)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, allTopics...); err != nil {
		t.Fatalf("create topics: %v", err)
	}
	adminClient.Close()

	// Count broadcasts per subject — the consumer's broadcast callback is the
	// boundary where "copies delivered per subscriber" is decided.
	var mu sync.Mutex
	broadcasts := make(map[string]int)
	byTopic := make(map[string]int)
	firstSeen := make(chan string, 16)
	broadcastFn := func(subject string, _ []byte, topicName string, _ int32, _ int64) error {
		mu.Lock()
		broadcasts[subject]++
		byTopic[topicName]++
		mu.Unlock()
		select {
		case firstSeen <- subject:
		default:
		}
		return nil
	}

	logger := zerolog.Nop()
	consumer, err := NewConsumer(ConsumerConfig{
		Brokers:               []string{broker},
		ConsumerGroup:         groupID,
		Topics:                consumeTopics,
		Logger:                &logger,
		Broadcast:             broadcastFn,
		ResourceGuard:         &mockResourceGuardFixed{allowKafka: true},
		TenantResolver:        func(string) (string, bool) { return tenant, true },
		ConsumerType:          ConsumerTypeKindShared,
		CommitOnRevokeTimeout: 10 * time.Second,
		AutoCommitInterval:    time.Second,
		Namespace:             namespace,
		Registerer:            prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Stop() })

	// Warmup: the consumer starts AtEnd, so produce warmup records to the default
	// topic until one is broadcast — proof the group has joined and assigned.
	warmupProducer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("create warmup producer: %v", err)
	}
	t.Cleanup(warmupProducer.Close)
	defaultTopic := consumeTopics[0]
	warmupDeadline := time.After(90 * time.Second)
	joined := false
	for !joined {
		rec := &kgo.Record{
			Topic: defaultTopic,
			Key:   []byte(warmupChannel),
			Value: []byte(`{"warmup":true}`),
			Headers: []kgo.RecordHeader{
				{Key: kafkashared.HeaderChannel, Value: []byte(warmupChannel)},
				{Key: kafkashared.HeaderTimestamp, Value: []byte(strconv.FormatInt(time.Now().UnixMilli(), 10))},
			},
		}
		warmupProducer.ProduceSync(ctx, rec) // errors surface as a join timeout below
		select {
		case subj := <-firstSeen:
			if subj == warmupChannel {
				joined = true
			}
		case <-time.After(500 * time.Millisecond):
		case <-warmupDeadline:
			t.Fatal("consumer did not join within 90s")
		}
	}

	// Publish ONE message through the production Producer with the multi-topic rule.
	producer, err := NewProducer(ProducerConfig{
		Brokers:         []string{broker},
		TopicNamespace:  namespace,
		Logger:          &logger,
		RulesProvider:   streamRules,
		FanoutWorkers:   2,
		FanoutQueueSize: 16,
		Registerer:      prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close() })

	mid, err := producer.Publish(ctx, 42, channel, []byte(`{"px":100}`))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if mid == "" {
		t.Error("Publish ack mid is empty — the ingress record's identity must always be returned")
	}

	// Wait until the channel's message is broadcast at least once, then allow a
	// settle window for any (buggy) duplicate copies to arrive before counting.
	deadline := time.After(60 * time.Second)
	for {
		mu.Lock()
		n := broadcasts[channel]
		mu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("published message was never broadcast")
		case <-time.After(200 * time.Millisecond):
		}
	}
	time.Sleep(5 * time.Second) // settle: a duplicate copy from a consumed egress topic would land here

	mu.Lock()
	got := broadcasts[channel]
	t.Logf("broadcasts by subject: %v; by source topic: %v", broadcasts, byTopic)
	mu.Unlock()
	if got != 1 {
		t.Fatalf("subscriber received %d copies of the message, want exactly 1 (duplicate delivery from egress topics)", got)
	}
}
