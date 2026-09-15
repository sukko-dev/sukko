package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

// TestGetCapabilities_KafkaIngest verifies the capabilities surface after the
// message_backend selector was replaced by the dedicated kafka-ingest suite:
// the suite is advertised, the per-run broker override is exposed as kafka_brokers,
// and the retired message_backend_urls field is gone.
func TestGetCapabilities_KafkaIngest(t *testing.T) {
	t.Parallel()

	handler, _ := newTestRouter()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/capabilities", http.NoBody)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var caps Capabilities
	if err := json.NewDecoder(w.Body).Decode(&caps); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// (a) kafka-ingest suite is advertised.
	hasSuite := slices.ContainsFunc(caps.Suites, func(s SuiteInfo) bool { return s.Name == "kafka-ingest" })
	if !hasSuite {
		t.Error("Suites missing \"kafka-ingest\"")
	}

	// (b) Context fields: kafka_brokers + kafka_topic_namespace present, message_backend_urls gone.
	var hasKafkaBrokers, hasKafkaNamespace, hasOldField bool
	for _, f := range caps.ContextFields {
		switch f.Name {
		case "kafka_brokers":
			hasKafkaBrokers = true
		case "kafka_topic_namespace":
			hasKafkaNamespace = true
		case "message_backend_urls":
			hasOldField = true
		}
	}
	if !hasKafkaBrokers {
		t.Error("ContextFields missing \"kafka_brokers\"")
	}
	if !hasKafkaNamespace {
		t.Error("ContextFields missing \"kafka_topic_namespace\"")
	}
	if hasOldField {
		t.Error("ContextFields still contains retired \"message_backend_urls\"")
	}
}
