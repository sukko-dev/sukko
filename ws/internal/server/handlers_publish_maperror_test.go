package server

import (
	"fmt"
	"testing"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// TestPublishErrorToClientCode verifies the backend-error → WS client error-code mapping,
// including the two-sentinel distinction added in #179 C1: ErrNoMatchingRoute (rules present,
// none match) MUST emit no_matching_route, distinct from no_routing_rules (no rules at all),
// even though the former wraps the latter's sentinel.
func TestPublishErrorToClientCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantCode   protocol.ErrorCode
		wantReject bool
	}{
		{
			name:       "no matching route → no_matching_route (checked before not-routable)",
			err:        fmt.Errorf("kafka backend publish: %w", backend.ErrNoMatchingRoute),
			wantCode:   protocol.ErrCodeNoMatchingRoute,
			wantReject: true,
		},
		{
			name:       "no rules provisioned → no_routing_rules",
			err:        fmt.Errorf("kafka backend publish: %w: no rules", backend.ErrPublishNotRoutable),
			wantCode:   protocol.ErrCodeNoRoutingRules,
			wantReject: true,
		},
		{
			name:       "invalid channel → invalid_channel",
			err:        fmt.Errorf("kafka backend publish: %w: bad", protocol.ErrInvalidChannel),
			wantCode:   protocol.ErrCodeInvalidChannel,
			wantReject: true,
		},
		{
			name:       "topic not provisioned → topic_not_provisioned (not reject-class)",
			err:        fmt.Errorf("kafka backend publish: %w", protocol.ErrTopicNotProvisioned),
			wantCode:   protocol.ErrCodeTopicNotProvisioned,
			wantReject: false,
		},
		{
			name:       "service unavailable → service_unavailable",
			err:        fmt.Errorf("kafka backend publish: %w: not synced", protocol.ErrServiceUnavailable),
			wantCode:   protocol.ErrCodeServiceUnavailable,
			wantReject: true,
		},
		{
			name:       "generic failure → publish_failed (not reject-class)",
			err:        fmt.Errorf("kafka produce failed: %w", backend.ErrPublishFailed),
			wantCode:   protocol.ErrCodePublishFailed,
			wantReject: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotCode, gotReject := publishErrorToClientCode(tt.err)
			if gotCode != tt.wantCode {
				t.Errorf("code = %q, want %q", gotCode, tt.wantCode)
			}
			if gotReject != tt.wantReject {
				t.Errorf("reject = %v, want %v", gotReject, tt.wantReject)
			}
		})
	}
}
