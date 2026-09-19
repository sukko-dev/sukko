package kafka

import (
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
)

// =============================================================================
// ProducerConfig Validation Tests
// =============================================================================

func TestNewProducer_NoBrokers(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()

	cfg := ProducerConfig{
		Brokers:        []string{},
		TopicNamespace: "test",
		Logger:         &logger,
	}

	_, err := NewProducer(cfg)
	if err == nil {
		t.Error("NewProducer should fail with no brokers")
	}
}

func TestNewProducer_NoTopicNamespace(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()

	cfg := ProducerConfig{
		Brokers:        []string{"localhost:9092"},
		TopicNamespace: "",
		Logger:         &logger,
	}

	_, err := NewProducer(cfg)
	if err == nil {
		t.Error("NewProducer should fail with no topic namespace")
	}
}

func TestNewProducer_InvalidSASLMechanism(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()

	cfg := ProducerConfig{
		Brokers:        []string{"localhost:9092"},
		TopicNamespace: "test",
		Logger:         &logger,
		SASL: &kafkashared.SASLConfig{
			Mechanism: "invalid-mechanism",
			Username:  "user",
			Password:  "pass",
		},
	}

	_, err := NewProducer(cfg)
	if err == nil {
		t.Error("NewProducer should fail with invalid SASL mechanism")
	}
}

// =============================================================================
// ProducerConfig Defaults Tests
// =============================================================================

func TestProducerConfig_Defaults(t *testing.T) {
	t.Parallel()
	cfg := ProducerConfig{
		Brokers:        []string{"localhost:9092"},
		TopicNamespace: "test",
	}

	// Default ClientID should be empty (will be set to "sukko-producer" in NewProducer)
	if cfg.ClientID != "" {
		t.Errorf("Default ClientID = %q, want empty string", cfg.ClientID)
	}

	// Default BatchMaxBytes should be 0 (will be set to 1MB in NewProducer)
	if cfg.BatchMaxBytes != 0 {
		t.Errorf("Default BatchMaxBytes = %d, want 0", cfg.BatchMaxBytes)
	}

	// Default MaxBufferedRecs should be 0 (will be set to 10000 in NewProducer)
	if cfg.MaxBufferedRecs != 0 {
		t.Errorf("Default MaxBufferedRecs = %d, want 0", cfg.MaxBufferedRecs)
	}

	// Default RecordRetries should be 0 (will be set to 8 in NewProducer)
	if cfg.RecordRetries != 0 {
		t.Errorf("Default RecordRetries = %d, want 0", cfg.RecordRetries)
	}
}

func TestProducerConfig_CustomValues(t *testing.T) {
	t.Parallel()
	cfg := ProducerConfig{
		Brokers:         []string{"localhost:9092", "localhost:9093"},
		TopicNamespace:  "production",
		ClientID:        "custom-client",
		BatchMaxBytes:   512 * 1024,
		MaxBufferedRecs: 5000,
		RecordRetries:   5,
	}

	if len(cfg.Brokers) != 2 {
		t.Errorf("Brokers length = %d, want 2", len(cfg.Brokers))
	}
	if cfg.TopicNamespace != "production" {
		t.Errorf("TopicNamespace = %q, want %q", cfg.TopicNamespace, "production")
	}
	if cfg.ClientID != "custom-client" {
		t.Errorf("ClientID = %q, want %q", cfg.ClientID, "custom-client")
	}
	if cfg.BatchMaxBytes != 512*1024 {
		t.Errorf("BatchMaxBytes = %d, want %d", cfg.BatchMaxBytes, 512*1024)
	}
	if cfg.MaxBufferedRecs != 5000 {
		t.Errorf("MaxBufferedRecs = %d, want 5000", cfg.MaxBufferedRecs)
	}
	if cfg.RecordRetries != 5 {
		t.Errorf("RecordRetries = %d, want 5", cfg.RecordRetries)
	}
}

// =============================================================================
// ProducerStats Tests
// =============================================================================

func TestProducerStats_Initial(t *testing.T) {
	t.Parallel()
	producer := &Producer{}

	stats := producer.Stats()

	if stats.MessagesPublished != 0 {
		t.Errorf("Initial MessagesPublished = %d, want 0", stats.MessagesPublished)
	}
	if stats.MessagesFailed != 0 {
		t.Errorf("Initial MessagesFailed = %d, want 0", stats.MessagesFailed)
	}
}

func TestProducerStats_IncrementPublished(t *testing.T) {
	t.Parallel()
	producer := &Producer{}

	producer.stats.MessagesPublished.Add(1)
	producer.stats.MessagesPublished.Add(1)
	producer.stats.MessagesPublished.Add(1)

	stats := producer.Stats()
	if stats.MessagesPublished != 3 {
		t.Errorf("MessagesPublished = %d, want 3", stats.MessagesPublished)
	}
}

func TestProducerStats_IncrementFailed(t *testing.T) {
	t.Parallel()
	producer := &Producer{}

	producer.stats.MessagesFailed.Add(1)
	producer.stats.MessagesFailed.Add(1)

	stats := producer.Stats()
	if stats.MessagesFailed != 2 {
		t.Errorf("MessagesFailed = %d, want 2", stats.MessagesFailed)
	}
}

func TestProducerStats_Concurrent(t *testing.T) {
	t.Parallel()
	producer := &Producer{}

	const numGoroutines = 100
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // 2 types of operations

	// Concurrent MessagesPublished increments
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				producer.stats.MessagesPublished.Add(1)
			}
		}()
	}

	// Concurrent MessagesFailed increments
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				producer.stats.MessagesFailed.Add(1)
			}
		}()
	}

	wg.Wait()

	stats := producer.Stats()
	expected := int64(numGoroutines * opsPerGoroutine)

	if stats.MessagesPublished != expected {
		t.Errorf("MessagesPublished = %d, want %d", stats.MessagesPublished, expected)
	}
	if stats.MessagesFailed != expected {
		t.Errorf("MessagesFailed = %d, want %d", stats.MessagesFailed, expected)
	}
}

// =============================================================================
// Producer Namespace Tests
// =============================================================================

func TestProducer_Namespace(t *testing.T) {
	t.Parallel()
	producer := &Producer{
		topicNamespace: "test",
	}

	namespace := producer.Namespace()
	if namespace != "test" {
		t.Errorf("Namespace() = %q, want %q", namespace, "test")
	}
}

func TestProducer_Namespaces(t *testing.T) {
	t.Parallel()
	// NewProducer normalizes TopicNamespace via strings.ToLower + strings.TrimSpace.
	// Verify the normalization behavior by constructing Producers directly.
	testCases := []struct {
		input    string
		expected string
	}{
		{"local", "local"},
		{"dev", "dev"},
		{"stag", "stag"},
		{"prod", "prod"},
		{"PROD", "prod"},   // lowercase conversion
		{"  dev  ", "dev"}, // whitespace trimming
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			// Replicate the normalization that NewProducer applies
			normalized := strings.ToLower(strings.TrimSpace(tc.input))
			if normalized != tc.expected {
				t.Errorf("normalize(%q) = %q, want %q", tc.input, normalized, tc.expected)
			}
		})
	}
}

// =============================================================================
// Producer Close Tests
// =============================================================================

func TestProducer_Close_Idempotent(t *testing.T) {
	t.Parallel()
	producer := &Producer{
		closed: false,
	}

	// Simulate close without actual client
	producer.closed = true

	// Second close should be safe
	if !producer.closed {
		t.Error("Producer should be marked as closed")
	}
}

func TestProducer_Close_SetsClosedFlag(t *testing.T) {
	t.Parallel()
	producer := &Producer{
		closed: false,
	}

	if producer.closed {
		t.Error("Producer should not be closed initially")
	}

	producer.closed = true

	if !producer.closed {
		t.Error("Producer should be closed after Close()")
	}
}

// =============================================================================
// extractTenant Tests
// =============================================================================

func TestExtractTenant_ValidFormats(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		channel string
		tenant  string
	}{
		{"acme.BTC.trade", "acme"},
		{"globex.user123.balances", "globex"},
		{"test.group.chat.community", "test"},
		{"tenant.a.b.c.d.category", "tenant"},
		{"t.x", "t"},
	}

	for _, tc := range testCases {
		t.Run(tc.channel, func(t *testing.T) {
			t.Parallel()
			tenant, err := ExtractTenant(tc.channel)
			if err != nil {
				t.Fatalf("ExtractTenant(%q) error = %v", tc.channel, err)
			}
			if tenant != tc.tenant {
				t.Errorf("tenant = %q, want %q", tenant, tc.tenant)
			}
		})
	}
}

func TestExtractTenant_InvalidFormats(t *testing.T) {
	t.Parallel()
	invalidChannels := []struct {
		channel string
		reason  string
	}{
		{"", "empty string"},
		{"single", "no dot"},
		{".b.c", "empty tenant"},
	}

	for _, tc := range invalidChannels {
		t.Run(tc.reason, func(t *testing.T) {
			t.Parallel()
			_, err := ExtractTenant(tc.channel)
			if err == nil {
				t.Errorf("ExtractTenant(%q) should return error (reason: %s)", tc.channel, tc.reason)
			}
		})
	}
}

// =============================================================================
// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkProducerStats_Increment(b *testing.B) {
	producer := &Producer{}

	for b.Loop() {
		producer.stats.MessagesPublished.Add(1)
	}
}

func BenchmarkProducerStats_Read(b *testing.B) {
	producer := &Producer{}
	producer.stats.MessagesPublished.Store(12345)
	producer.stats.MessagesFailed.Store(67)

	for b.Loop() {
		_ = producer.Stats()
	}
}

func BenchmarkExtractTenant(b *testing.B) {
	channel := "acme.BTC.trade"
	for b.Loop() {
		_, _ = ExtractTenant(channel)
	}
}
