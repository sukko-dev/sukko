package gateway

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestResolveCloseReason: the pure frame-text → snake_case-label mapping, both sides.
func TestResolveCloseReason(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, stored, fallback, want string }{
		{"token frame text → token_revoked label", CloseReasonTokenRevoked, CloseReasonNormal, CloseReasonTokenRevokedLabel},
		{"user frame text → user_revoked label", CloseReasonUserRevoked, CloseReasonNormal, CloseReasonUserRevokedLabel},
		{"no force-close → fallback (normal)", "", CloseReasonNormal, CloseReasonNormal},
		{"no force-close → fallback (backend_unavailable)", "", CloseReasonBackendUnavailable, CloseReasonBackendUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveCloseReason(tt.stored, tt.fallback); got != tt.want {
				t.Errorf("resolveCloseReason(%q, %q) = %q, want %q", tt.stored, tt.fallback, got, tt.want)
			}
		})
	}
}

// histSampleCount reads the observation count for one close_reason series via dto (NOT testutil.ToFloat64,
// which panics on histograms). Delta-based so it is robust to sibling tests — and token_revoked /
// user_revoked are labels only this test produces, so their deltas are fully isolated.
func histSampleCount(t *testing.T, closeReason string) uint64 {
	t.Helper()
	m := &dto.Metric{}
	h, ok := connectionDuration.WithLabelValues(closeReason).(prometheus.Histogram)
	if !ok {
		t.Fatalf("connectionDuration child is not a prometheus.Histogram")
	}
	if err := h.Write(m); err != nil {
		t.Fatalf("histogram Write: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

// TestRecordWSDisconnect_RevocationLabel: the fix's own end-to-end coverage — a revocation
// force-close is recorded in the disconnect histogram under its snake_case label, NOT "normal", and a
// non-force disconnect produces NO revocation label. Catches a dropped getter→resolve→record override.
func TestRecordWSDisconnect_RevocationLabel(t *testing.T) {
	// Parallel-safe: asserts only via deltas on the token_revoked / user_revoked labels, which no
	// other test produces — so sibling tests observing other close_reason labels cannot perturb it.
	t.Parallel()
	beforeTok := histSampleCount(t, CloseReasonTokenRevokedLabel)
	pt := newFakeConnProxy(&fakeConn{}, &fakeConn{})
	pt.ForceClose(1008, CloseReasonTokenRevoked)
	recordWSDisconnect(pt, CloseReasonNormal, time.Now())
	if got := histSampleCount(t, CloseReasonTokenRevokedLabel); got != beforeTok+1 {
		t.Errorf("token_revoked sample count = %d, want %d (revocation close mislabeled?)", got, beforeTok+1)
	}

	beforeUser := histSampleCount(t, CloseReasonUserRevokedLabel)
	pu := newFakeConnProxy(&fakeConn{}, &fakeConn{})
	pu.ForceClose(1008, CloseReasonUserRevoked)
	recordWSDisconnect(pu, CloseReasonNormal, time.Now())
	if got := histSampleCount(t, CloseReasonUserRevokedLabel); got != beforeUser+1 {
		t.Errorf("user_revoked sample count = %d, want %d", got, beforeUser+1)
	}

	// No force-close → the fallback is recorded and NO revocation label is produced.
	tok, user := histSampleCount(t, CloseReasonTokenRevokedLabel), histSampleCount(t, CloseReasonUserRevokedLabel)
	pn := newFakeConnProxy(&fakeConn{}, &fakeConn{})
	recordWSDisconnect(pn, CloseReasonBackendUnavailable, time.Now())
	if got := histSampleCount(t, CloseReasonTokenRevokedLabel); got != tok {
		t.Errorf("non-force close spuriously produced a token_revoked observation (%d→%d)", tok, got)
	}
	if got := histSampleCount(t, CloseReasonUserRevokedLabel); got != user {
		t.Errorf("non-force close spuriously produced a user_revoked observation (%d→%d)", user, got)
	}
}

// TestRecordWSDisconnect_NilProxy: a connection that ended before the proxy existed must use the
// fallback without panicking (the getter is never dereferenced on a nil Proxy).
func TestRecordWSDisconnect_NilProxy(t *testing.T) {
	t.Parallel()
	recordWSDisconnect(nil, CloseReasonNoCredentials, time.Now()) // must not panic
}
