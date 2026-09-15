package publisher

import (
	"strings"
	"testing"

	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
)

func TestNewKafkaPublisher_NoBrokers(t *testing.T) {
	t.Parallel()

	_, err := NewKafkaPublisher("", nil, nil, nil)
	if err == nil {
		t.Fatal("expected error for empty brokers, got nil")
	}
	if !strings.Contains(err.Error(), "kafka publisher: no brokers provided") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewKafkaPublisher_InvalidSASL(t *testing.T) {
	t.Parallel()

	_, err := NewKafkaPublisher("localhost:9092", nil, &kafkashared.SASLConfig{
		Mechanism: "oauthbearer", // plain is now supported; use a still-unsupported mechanism
		Username:  "user",
		Password:  "pass",
	}, nil)
	if err == nil {
		t.Fatal("expected error for unsupported SASL mechanism, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported SASL mechanism") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewKafkaPublisher_MissingCA(t *testing.T) {
	t.Parallel()

	_, err := NewKafkaPublisher("localhost:9092", nil, nil, &kafkashared.TLSConfig{
		Enabled: true,
		CAPath:  "/nonexistent/ca.pem",
	})
	if err == nil {
		t.Fatal("expected error for missing CA file, got nil")
	}
	if !strings.Contains(err.Error(), "failed to read CA certificate") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewKafkaPublisher_NilSASLTLS(t *testing.T) {
	t.Parallel()

	// nil SASL/TLS must not produce a config error; kgo.NewClient is lazy so broker
	// unreachability does not surface at construction time.
	pub, err := NewKafkaPublisher("localhost:19092", nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error with nil SASL/TLS: %v", err)
	}
	if pub == nil {
		t.Fatal("expected non-nil publisher, got nil")
	}
	if err := pub.Close(); err != nil {
		t.Fatalf("unexpected error on Close: %v", err)
	}
}
