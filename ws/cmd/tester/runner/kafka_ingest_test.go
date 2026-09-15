package runner

import (
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

// TestValidateKafkaIngest_SkipsWhenNoBrokers verifies the skip contract: with no
// Kafka target configured (empty brokers), the suite records a single skip result
// and never touches the provisioning client or gateway (so the run still passes).
// This is the pure-logic branch. The delivery, connection-error, and verify-timeout
// branches require a live broker + gateway + a Kafka-backend server and are covered
// only by the manual compose E2E — reaching them in a unit test would
// require faking the provisioning client, gateway, and consumer, which the suite is
// not structured to inject. (The publisher package's build-tagged integration test
// separately covers KafkaPublisher's produce/consume wire format, not this suite.)
func TestValidateKafkaIngest_SkipsWhenNoBrokers(t *testing.T) {
	t.Parallel()

	run := &TestRun{kafkaBrokers: ""} // execute() would leave this empty when KAFKA_BROKERS is unset

	checks, err := validateKafkaIngest(context.Background(), run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateKafkaIngest returned error: %v", err)
	}
	if len(checks) != 1 {
		t.Fatalf("got %d checks, want 1", len(checks))
	}
	if checks[0].Status != metrics.CheckStatusSkip {
		t.Errorf("status = %q, want %q", checks[0].Status, metrics.CheckStatusSkip)
	}
}

// TestNewKafkaPub_DefaultConstructsRealPublisher verifies the injection seam points
// at the real constructor by default (tests may override newKafkaPub with a fake).
func TestNewKafkaPub_DefaultConstructsRealPublisher(t *testing.T) {
	t.Parallel()

	// Empty brokers must surface as an error from the real NewKafkaPublisher, proving
	// newKafkaPub is wired to it rather than a stub.
	_, err := newKafkaPub("", nil, nil, nil)
	if err == nil {
		t.Fatal("expected error from real NewKafkaPublisher with empty brokers, got nil")
	}
}
