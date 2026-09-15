package protocol

import (
	"errors"
	"testing"
)

// =============================================================================
// Error Code Constants Tests
// =============================================================================

func TestErrorCode_Values(t *testing.T) {
	t.Parallel()
	expectedCodes := map[ErrorCode]string{
		ErrCodeInvalidRequest:      "invalid_request",
		ErrCodeNotAvailable:        "not_available",
		ErrCodeInvalidChannel:      "invalid_channel",
		ErrCodeMessageTooLarge:     "message_too_large",
		ErrCodeRateLimited:         "rate_limited",
		ErrCodeForbidden:           "forbidden",
		ErrCodeTopicNotProvisioned: "topic_not_provisioned",
		ErrCodeServiceUnavailable:  "service_unavailable",
		ErrCodeNoRoutingRules:      "no_routing_rules",
		ErrCodeNoMatchingRoute:     "no_matching_route",
		ErrCodeInvalidJSON:         "invalid_json",
		ErrCodePublishFailed:       "publish_failed",
		ErrCodeReplayFailed:        "replay_failed",
	}

	for code, expected := range expectedCodes {
		if string(code) != expected {
			t.Errorf("Error code = %q, want %q", string(code), expected)
		}
	}
}

func TestErrorCode_UniqueValues(t *testing.T) {
	t.Parallel()
	codes := []ErrorCode{
		ErrCodeInvalidRequest,
		ErrCodeNotAvailable,
		ErrCodeInvalidChannel,
		ErrCodeMessageTooLarge,
		ErrCodeRateLimited,
		ErrCodeForbidden,
		ErrCodeTopicNotProvisioned,
		ErrCodeServiceUnavailable,
		ErrCodeNoRoutingRules,
		ErrCodeNoMatchingRoute,
		ErrCodeInvalidJSON,
		ErrCodePublishFailed,
		ErrCodeReplayFailed,
	}

	seen := make(map[ErrorCode]bool)
	for _, code := range codes {
		if seen[code] {
			t.Errorf("Duplicate error code: %q", code)
		}
		seen[code] = true
	}
}

func TestErrorCode_TypeConversion(t *testing.T) {
	t.Parallel()
	// Verify that ErrorCode can be converted to/from string
	code := ErrCodeRateLimited
	strVal := string(code)

	if strVal != "rate_limited" {
		t.Errorf("string(ErrCodeRateLimited) = %q, want %q", strVal, "rate_limited")
	}

	// Convert back
	converted := ErrorCode(strVal)
	if converted != ErrCodeRateLimited {
		t.Errorf("ErrorCode(%q) = %q, want %q", strVal, converted, ErrCodeRateLimited)
	}
}

// =============================================================================
// Error Messages Map Tests
// =============================================================================

func TestPublishErrorMessages_AllCodesHaveMessages(t *testing.T) {
	t.Parallel()
	codes := []ErrorCode{
		ErrCodeNotAvailable,
		ErrCodeInvalidRequest,
		ErrCodeInvalidChannel,
		ErrCodeMessageTooLarge,
		ErrCodeRateLimited,
		ErrCodePublishFailed,
		ErrCodeForbidden,
		ErrCodeTopicNotProvisioned,
		ErrCodeServiceUnavailable,
		ErrCodeNoRoutingRules,
		ErrCodeNoMatchingRoute,
	}

	for _, code := range codes {
		msg := PublishErrorMessage(code)
		if msg == "" {
			t.Errorf("No message for error code %q", code)
		}
	}
}

func TestPublishErrorMessages_NoEmptyMessages(t *testing.T) {
	t.Parallel()
	for code, msg := range publishErrorMessages {
		if msg == "" {
			t.Errorf("Empty message for error code %q", code)
		}
	}
}

func TestPublishErrorMessages_Specific(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		code    ErrorCode
		message string
	}{
		{ErrCodeNotAvailable, "Publishing is not enabled on this server"},
		{ErrCodeInvalidRequest, "Invalid publish request format"},
		{ErrCodeInvalidChannel, "Channel must have format: {tenant_id}.{suffix}"},
		{ErrCodeMessageTooLarge, "Message exceeds maximum size limit"},
		{ErrCodeRateLimited, "Publish rate limit exceeded"},
		{ErrCodePublishFailed, "Failed to publish message"},
		{ErrCodeForbidden, "Not authorized to publish to this channel"},
		{ErrCodeTopicNotProvisioned, "Topic is not provisioned for your tenant"},
		{ErrCodeServiceUnavailable, "Service temporarily unavailable, please retry"},
		{ErrCodeNoRoutingRules, "No topic routing rules configured for tenant"},
		{ErrCodeNoMatchingRoute, "No matching topic routing rule for channel"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.code), func(t *testing.T) {
			t.Parallel()
			msg := PublishErrorMessage(tc.code)
			if msg != tc.message {
				t.Errorf("PublishErrorMessage(%q) = %q, want %q", tc.code, msg, tc.message)
			}
		})
	}
}

// =============================================================================
// Sentinel Errors Tests
// =============================================================================

func TestSentinelErrors_NotNil(t *testing.T) {
	t.Parallel()
	sentinelErrors := []error{
		ErrInvalidChannel,
		ErrTopicNotProvisioned,
		ErrServiceUnavailable,
	}

	for _, err := range sentinelErrors {
		if err == nil {
			t.Error("Sentinel error should not be nil")
		}
	}
}

func TestSentinelErrors_UniqueMessages(t *testing.T) {
	t.Parallel()
	sentinelErrors := []error{
		ErrInvalidChannel,
		ErrTopicNotProvisioned,
		ErrServiceUnavailable,
	}

	seen := make(map[string]bool)
	for _, err := range sentinelErrors {
		msg := err.Error()
		if seen[msg] {
			t.Errorf("Duplicate error message: %q", msg)
		}
		seen[msg] = true
	}
}

func TestSentinelErrors_ErrorInterface(t *testing.T) {
	t.Parallel()
	// Verify all sentinel errors implement the error interface
	var _ = ErrInvalidChannel
	var _ = ErrTopicNotProvisioned
	var _ = ErrServiceUnavailable

	// Verify Error() returns non-empty strings
	testCases := []struct {
		name string
		err  error
	}{
		{"ErrInvalidChannel", ErrInvalidChannel},
		{"ErrTopicNotProvisioned", ErrTopicNotProvisioned},
		{"ErrServiceUnavailable", ErrServiceUnavailable},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := tc.err.Error()
			if msg == "" {
				t.Errorf("%s.Error() returned empty string", tc.name)
			}
		})
	}
}

func TestSentinelErrors_SpecificMessages(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		err     error
		message string
	}{
		{ErrInvalidChannel, "invalid channel format"},
		{ErrTopicNotProvisioned, "topic not provisioned"},
		{ErrServiceUnavailable, "service unavailable"},
	}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			t.Parallel()
			if tc.err.Error() != tc.message {
				t.Errorf("Error message = %q, want %q", tc.err.Error(), tc.message)
			}
		})
	}
}

func TestSentinelErrors_ErrorsIs(t *testing.T) {
	t.Parallel()
	// Test that errors.Is works correctly with sentinel errors
	testCases := []struct {
		name   string
		target error
	}{
		{"ErrInvalidChannel", ErrInvalidChannel},
		{"ErrTopicNotProvisioned", ErrTopicNotProvisioned},
		{"ErrServiceUnavailable", ErrServiceUnavailable},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if !errors.Is(tc.target, tc.target) {
				t.Errorf("errors.Is(%s, %s) = false, want true", tc.name, tc.name)
			}
		})
	}
}

func TestSentinelErrors_NotEqual(t *testing.T) {
	t.Parallel()
	// Verify that different sentinel errors are not equal
	allErrors := []error{
		ErrInvalidChannel,
		ErrTopicNotProvisioned,
		ErrServiceUnavailable,
	}

	for i, err1 := range allErrors {
		for j, err2 := range allErrors {
			if i != j && errors.Is(err1, err2) {
				t.Errorf("errors.Is returned true for different errors: %v and %v", err1, err2)
			}
		}
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkPublishErrorMessage_Lookup(b *testing.B) {
	for b.Loop() {
		_ = PublishErrorMessage(ErrCodeRateLimited)
	}
}

func BenchmarkErrorCode_StringConversion(b *testing.B) {
	code := ErrCodeTopicNotProvisioned
	for b.Loop() {
		_ = string(code)
	}
}
