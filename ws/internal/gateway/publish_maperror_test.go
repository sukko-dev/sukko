package gateway

import (
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"
)

// TestMapPublishError verifies the gRPC status → REST response mapping (#179 §XII).
func TestMapPublishError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code       codes.Code
		wantStatus int
		wantCode   string
		wantLabel  string
	}{
		{codes.FailedPrecondition, http.StatusConflict, errCodePublishNotRoutable, publishOutcomeNotRoutable},
		{codes.InvalidArgument, http.StatusBadRequest, errCodeInvalidChannel, publishOutcomeInvalidChan},
		{codes.Unavailable, http.StatusServiceUnavailable, errCodeServiceUnavailable, publishOutcomeUnavailable},
		{codes.Internal, http.StatusInternalServerError, errCodeInternal, publishOutcomeError},
		// Any other code falls through to a genuine 500.
		{codes.Unknown, http.StatusInternalServerError, errCodeInternal, publishOutcomeError},
	}

	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			t.Parallel()
			status, code, message, label := mapPublishError(tt.code)
			if status != tt.wantStatus {
				t.Errorf("status = %d, want %d", status, tt.wantStatus)
			}
			if code != tt.wantCode {
				t.Errorf("code = %q, want %q", code, tt.wantCode)
			}
			if label != tt.wantLabel {
				t.Errorf("label = %q, want %q", label, tt.wantLabel)
			}
			if message == "" {
				t.Error("message must not be empty")
			}
		})
	}
}
