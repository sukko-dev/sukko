// Serial tests — uses os.Unsetenv which modifies global process state. Do NOT add t.Parallel() to any test in this file.
package main

import (
	"os"
	"reflect"
	"testing"

	"github.com/caarlos0/env/v11"

	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// TestKafkaConnectionConfig_KafkaBrokers_NoDefault asserts KAFKA_BROKERS carries the
// correct env tag and, critically, NO envDefault. A localhost default would defeat
// the kafka-ingest skip contract (empty brokers ⇒ skip) — see spec C1.
func TestKafkaConnectionConfig_KafkaBrokers_NoDefault(t *testing.T) {
	field, ok := reflect.TypeFor[platform.KafkaConnectionConfig]().FieldByName("KafkaBrokers")
	if !ok {
		t.Fatal("platform.KafkaConnectionConfig has no field KafkaBrokers")
	}
	if got := field.Tag.Get("env"); got != "KAFKA_BROKERS" {
		t.Errorf("KafkaBrokers env tag = %q, want %q", got, "KAFKA_BROKERS")
	}
	if got := field.Tag.Get("envDefault"); got != "" {
		t.Errorf("KafkaBrokers envDefault = %q, want empty (no default — see C1)", got)
	}
}

// TestKafkaConnectionConfig_KafkaBrokers_EmptyWhenUnset verifies that with KAFKA_BROKERS
// unset the parsed value is empty, so the kafka-ingest suite skips rather than
// attempting a bogus localhost broker.
func TestKafkaConnectionConfig_KafkaBrokers_EmptyWhenUnset(t *testing.T) {
	os.Unsetenv("KAFKA_BROKERS")
	var conn platform.KafkaConnectionConfig
	if err := env.Parse(&conn); err != nil {
		t.Fatalf("env.Parse failed: %v", err)
	}
	if conn.KafkaBrokers != "" {
		t.Errorf("KafkaBrokers = %q, want empty when unset", conn.KafkaBrokers)
	}
}
