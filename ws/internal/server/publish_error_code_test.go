package server

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// TestPublishErrorCode verifies the backend-error → gRPC status mapping (#179 §XII).
// Errors are wrapped the way they arrive at grpc_service (kafkabackend wraps the producer
// error, which wraps the sentinel) to prove errors.Is survives the wrap chain.
func TestPublishErrorCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{
			name: "not routable → FailedPrecondition",
			err:  fmt.Errorf("kafka backend publish: %w: no rules", backend.ErrPublishNotRoutable),
			want: codes.FailedPrecondition,
		},
		{
			// C1 revised (#179): no-match wraps ErrPublishNotRoutable, so it maps to the
			// same FailedPrecondition → gateway 409 without a dedicated switch case.
			name: "no matching route → FailedPrecondition (wraps not-routable)",
			err:  fmt.Errorf("kafka backend publish: %w", backend.ErrNoMatchingRoute),
			want: codes.FailedPrecondition,
		},
		{
			name: "invalid channel → InvalidArgument",
			err:  fmt.Errorf("kafka backend publish: %w: bad", protocol.ErrInvalidChannel),
			want: codes.InvalidArgument,
		},
		{
			name: "service unavailable → Unavailable",
			err:  fmt.Errorf("kafka backend publish: %w: not synced", protocol.ErrServiceUnavailable),
			want: codes.Unavailable,
		},
		{
			name: "generic produce failure → Internal",
			err:  fmt.Errorf("kafka produce failed: %w", backend.ErrPublishFailed),
			want: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := publishErrorCode(tt.err); got != tt.want {
				t.Errorf("publishErrorCode() = %v, want %v", got, tt.want)
			}
		})
	}
}
