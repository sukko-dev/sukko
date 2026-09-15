package license

import (
	"errors"
	"strings"
	"testing"
)

func TestEditionLimitError_Error(t *testing.T) {
	t.Parallel()
	err := NewLimitError("tenants", 3, 3, Community)
	msg := err.Error()

	for _, want := range []string{"tenants", "3/3", "community", UpgradeURL} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, missing %q", msg, want)
		}
	}
}

func TestEditionLimitError_Code(t *testing.T) {
	t.Parallel()
	tests := []struct {
		dimension string
		wantCode  string
	}{
		{"tenants", "EDITION_LIMIT_TENANTS"},
		{"total_connections", "EDITION_LIMIT_TOTAL_CONNECTIONS"},
		{"shards", "EDITION_LIMIT_SHARDS"},
		{"topics_per_tenant", "EDITION_LIMIT_TOPICS_PER_TENANT"},
		{"routing_rules_per_tenant", "EDITION_LIMIT_ROUTING_RULES_PER_TENANT"},
	}
	for _, tt := range tests {
		err := NewLimitError(tt.dimension, 1, 1, Community)
		if got := err.Code(); got != tt.wantCode {
			t.Errorf("NewLimitError(%q).Code() = %q, want %q", tt.dimension, got, tt.wantCode)
		}
	}
}

func TestEditionFeatureError_Error(t *testing.T) {
	t.Parallel()
	err := NewFeatureError(SSETransport, Community)
	msg := err.Error()

	for _, want := range []string{string(SSETransport), "pro", "community", UpgradeURL} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, missing %q", msg, want)
		}
	}
}

func TestEditionFeatureError_Code(t *testing.T) {
	t.Parallel()
	err := NewFeatureError(SSETransport, Community)
	if got := err.Code(); got != "EDITION_FEATURE_REQUIRED" {
		t.Errorf("Code() = %q, want EDITION_FEATURE_REQUIRED", got)
	}
}

func TestEditionLimitError_ErrorsAs(t *testing.T) {
	t.Parallel()
	err := NewLimitError("tenants", 3, 3, Community)

	limitErr, ok := errors.AsType[*EditionLimitError](err)
	if !ok {
		t.Fatal("errors.As should match *EditionLimitError")
	}
	if limitErr.Dimension != "tenants" {
		t.Errorf("Dimension = %q, want tenants", limitErr.Dimension)
	}
}

func TestEditionFeatureError_ErrorsAs(t *testing.T) {
	t.Parallel()
	err := NewFeatureError(KafkaBackend, Community)

	featureErr, ok := errors.AsType[*EditionFeatureError](err)
	if !ok {
		t.Fatal("errors.As should match *EditionFeatureError")
	}
	if featureErr.Feature != KafkaBackend {
		t.Errorf("Feature = %q, want KafkaBackend", featureErr.Feature)
	}
}
