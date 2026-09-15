package api

import (
	"cmp"
	"net/http"
	"slices"

	"github.com/sukko-dev/sukko/cmd/tester/runner"
)

// Capabilities describes the tester's full API surface.
// Consumed by the CLI for auto-discovery — suites and context fields.
type Capabilities struct {
	TestTypes     []TestTypeInfo     `json:"test_types"`
	Suites        []SuiteInfo        `json:"suites"`
	ContextFields []ContextFieldInfo `json:"context_fields"`
}

// TestTypeInfo describes a supported test type.
type TestTypeInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// SuiteInfo describes a validation suite.
type SuiteInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// ContextFieldInfo describes a TestContext field.
type ContextFieldInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Description string `json:"description"`
}

func (h *handlers) getCapabilities(w http.ResponseWriter, _ *http.Request) {
	// Build suites from registry (sorted for stable output)
	suites := make([]SuiteInfo, 0, len(runner.SuiteRegistry))
	for _, s := range runner.SuiteRegistry {
		suites = append(suites, SuiteInfo{Name: s.Name, Description: s.Description})
	}
	slices.SortFunc(suites, func(a, b SuiteInfo) int { return cmp.Compare(a.Name, b.Name) })

	caps := Capabilities{
		TestTypes: []TestTypeInfo{
			{Name: "smoke", Description: "Connectivity and basic message delivery verification"},
			{Name: "load", Description: "Sustained connection load with configurable publish rate"},
			{Name: "stress", Description: "Push beyond capacity to verify backpressure and degradation"},
			{Name: "soak", Description: "Long-running stability test for memory leaks and connection drift"},
			{Name: "validate", Description: "End-to-end correctness validation suites"},
		},
		Suites: suites,
		ContextFields: []ContextFieldInfo{
			{Name: "gateway_url", Type: "string", Required: true, Description: "WebSocket gateway URL (e.g., ws://gateway:3000)"},
			{Name: "provisioning_url", Type: "string", Required: true, Description: "Provisioning API base URL (e.g., http://provisioning:8080)"},
			{Name: "admin_token", Type: "string", Required: true, Description: "Admin authentication token for provisioning API"},
			{Name: "environment", Type: "string", Required: false, Description: "Deployment environment name (e.g., demo, dev, prod)"},
			{Name: "kafka_brokers", Type: "string", Required: false, Description: "Per-run override of the tester's KAFKA_BROKERS for the kafka-ingest suite (comma-separated)"},
			{Name: "kafka_topic_namespace", Type: "string", Required: false, Description: "Per-run override of the tester's KAFKA_TOPIC_NAMESPACE; MUST match the server-under-test"},
		},
	}

	w.Header().Set("Cache-Control", "max-age=3600")
	writeJSON(w, http.StatusOK, caps)
}
