package gateway

import "time"

// resolveCloseReason maps a stored force-close reason (the WS close-frame text a Proxy.ForceClose was
// called with) to its snake_case `close_reason` metric-label value, or returns `fallback` when no
// force-close occurred (storedForceReason == ""). Pure — unit-tested in close_reason_test.go.
func resolveCloseReason(storedForceReason, fallback string) string {
	switch storedForceReason {
	case CloseReasonTokenRevoked:
		return CloseReasonTokenRevokedLabel
	case CloseReasonUserRevoked:
		return CloseReasonUserRevokedLabel
	case "":
		return fallback
	default:
		// ForceClose is only ever called with the two revocation frame texts; pass through anything
		// else defensively rather than dropping it.
		return storedForceReason
	}
}

// recordWSDisconnect records the WS disconnect duration under the correct `close_reason`: a revocation
// force-close reason (mapped to its snake_case label) when the connection was force-closed, else the
// handler's fallback reason. Extracted from the handler so the getter→resolve→RecordDisconnection
// wiring — the actual fix — is unit-testable end-to-end, not verified by inspection only.
// `p` may be nil (connection ended before the proxy was created); then the fallback is used.
func recordWSDisconnect(p *Proxy, fallbackReason string, start time.Time) {
	reason := fallbackReason
	if p != nil {
		reason = resolveCloseReason(p.ForceCloseReason(), fallbackReason)
	}
	RecordDisconnection(reason, time.Since(start))
}
