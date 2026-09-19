//go:build integration

package publisher

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
)

func TestKafkaPublisher_PublishAndConsume(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start Redpanda container
	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.1.1")
	if err != nil {
		t.Skipf("Docker required for integration tests: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	brokers, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("get broker address: %v", err)
	}

	// Create topic resolver: channel "BTC.trade" → topic "test-ns.tenant-1.market-data"
	resolver := NewTopicResolver("test-ns", "tenant-1", []RoutingRule{
		{Pattern: "**", IngressTopic: "market-data"},
	})

	// Create publisher
	pub, err := NewKafkaPublisher(brokers, resolver, nil, nil)
	if err != nil {
		t.Fatalf("create kafka publisher: %v", err)
	}
	defer pub.Close() //nolint:errcheck

	// Publish a test message
	gen := NewGenerator()
	channel := "BTC.trade"
	data, err := gen.Next(channel)
	if err != nil {
		t.Fatalf("generate message: %v", err)
	}

	if err := pub.Publish(ctx, channel, data); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Consume and verify
	topic := "test-ns.tenant-1.market-data"
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("test-consumer-"+t.Name()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	fetches := consumer.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("consume errors: %v", errs)
	}

	records := fetches.Records()
	if len(records) == 0 {
		t.Fatal("expected at least 1 record, got 0")
	}

	record := records[0]

	// Verify key == channel name
	if got := string(record.Key); got != channel {
		t.Errorf("record key = %q, want %q", got, channel)
	}

	// Verify payload deserializes as TestMessage
	var msg TestMessage
	if err := json.Unmarshal(record.Value, &msg); err != nil {
		t.Fatalf("unmarshal record value: %v", err)
	}
	if msg.Sequence != 1 {
		t.Errorf("sequence = %d, want 1", msg.Sequence)
	}
	if msg.Timestamp <= 0 {
		t.Error("timestamp should be positive")
	}

	// Verify headers include source and the x-sukko-channel routing header.
	var foundSource, foundChannel bool
	for _, h := range record.Headers {
		if h.Key == kafkashared.HeaderSource && string(h.Value) == "sukko-tester" {
			foundSource = true
		}
		if h.Key == kafkashared.HeaderChannel && string(h.Value) == channel {
			foundChannel = true
		}
	}
	if !foundSource {
		t.Error("expected header source=sukko-tester")
	}
	if !foundChannel {
		t.Errorf("expected header %s=%q (required for Pro/Enterprise channel routing)", kafkashared.HeaderChannel, channel)
	}
}

func TestKafkaPublisher_ConnectionFailure(t *testing.T) {
	t.Parallel()

	resolver := NewTopicResolver("ns", "t", []RoutingRule{
		{Pattern: "**", IngressTopic: "test"},
	})

	// Use unreachable broker — publish should fail
	pub, err := NewKafkaPublisher("localhost:19999", resolver, nil, nil)
	if err != nil {
		t.Skipf("NewKafkaPublisher failed on connect (acceptable — some drivers fail eagerly): %v", err)
	}
	defer pub.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = pub.Publish(ctx, "test", []byte(`{"test":true}`))
	if err == nil {
		t.Error("expected error publishing to unreachable broker")
	}
}
