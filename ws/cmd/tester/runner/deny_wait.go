package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
)

// Deny-wait tuning. Deliberately named constants, not env vars: an internal test-driver
// robustness bound has no per-deployment tuning need (same reasoning as the hardcoded webhook
// retry schedule, §I). Generalized from the merged #216/#218 api-key `waitForPrivateDeny`; these
// bounds now serve every authorization deny check (subscribe + publish, all suites).
const (
	// defaultDenyWaitDeadline bounds the whole deny wait. Preserves the previous effective 10s
	// bound so the common prompt-deny case is not slower than before.
	defaultDenyWaitDeadline = 10 * time.Second
	// defaultDenyWaitRetryInterval is the fixed re-issue cadence. A re-issue happens only while
	// issue_time + interval ≤ deadline, so every attempt's deny has a full round-trip window
	// before the deadline. Fixed interval (not exponential backoff) is intentional: §IV's backoff
	// mandate covers infrastructure reconnection, not a test-driver poll.
	defaultDenyWaitRetryInterval = 3 * time.Second
)

// Wire error codes as seen on the top-level `code` field of subscribe_error/publish_error frames.
// Local consts (not imported from internal/shared/protocol) both to keep the tester decoupled
// from internal packages (§X, mirroring respTypeSubscribeError/respTypePublishError) AND to avoid
// the pre-existing package collision: runner already defines errCodeInvalidRequest="INVALID_REQUEST"
// (the REST/provisioning namespace, edition_limits.go) — a different, uppercase contract.
const (
	wsErrCodeInvalidRequest = "invalid_request" // subscribe filtered to empty → server "no channels specified"
	wsErrCodeForbidden      = "forbidden"       // publish denied: unauthorized channel / not in publish rules / api-key-only
)

// denyOutcome classifies how a deny wait ended.
type denyOutcome int

const (
	denyOutcomeDenied         denyOutcome = iota // the expected deny frame arrived — the check's success signal
	denyOutcomeTimedOut                          // deadline elapsed with no matching deny — hard fail
	denyOutcomeTransportError                    // an issue (subscribe/publish) write failed with no pending deny and a live parent ctx — hard fail
	denyOutcomeCancelled                         // parent context canceled — clean short-circuit, no result recorded
)

// waitForDeny issues an authorization-denied action (a subscribe to an unauthorized channel, or a
// publish that must be forbidden) and waits for the gateway/server's asynchronous deny frame,
// re-issuing on a bounded fixed interval so a single slow or lost deny round-trip under load cannot
// flake the check (the #216/#218 lesson). Issuing the same denied action again is idempotent.
//
// The error source is abstracted behind `poll` — a closure returning true once the expected
// (type, code) deny has been observed. api-key adapts its bespoke errCh via pollDenyFromErrCh;
// pubsub/tenant-isolation adapt the shared TestUser mutex store via user.HasErrorMatching. The
// caller MUST clear/drain the source exactly ONCE before calling this (drain-once); this
// helper re-issues but NEVER re-clears, so a deny captured on a later attempt still satisfies the
// wait (deny-wins on the accumulated source).
//
// Two cadences: poll at min(interval, denyPollInterval) for fast detection; re-issue every
// `interval` while the windowed-send rule holds. deadline/interval are authoritative (seeded from
// the consts or injected by tests) — deliberately NO <= 0 fallback.
func waitForDeny(ctx context.Context, issue func() error, poll func() bool, deadline, interval time.Duration, logger zerolog.Logger) (denyOutcome, error) {
	start := time.Now()
	denyCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	// transportExit: an issue write failed. Probe poll() once (a deny already observed wins — the
	// check has its success signal), then the parent-ctx guard (a write failing because the battery
	// is tearing down must not record a spurious FAIL), else a genuine transport failure.
	transportExit := func(sendErr error) (denyOutcome, error) {
		if poll() {
			return denyOutcomeDenied, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return denyOutcomeCancelled, fmt.Errorf("deny wait canceled: %w", ctxErr)
		}
		return denyOutcomeTransportError, sendErr
	}

	attempt := 1
	lastIssue := start
	logger.Debug().Int("attempt", attempt).Msg("deny wait: action issued")
	if err := issue(); err != nil {
		return transportExit(err)
	}

	ticker := time.NewTicker(min(interval, denyPollInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if poll() {
				logger.Debug().Int("attempt", attempt).Bool("saved_by_retry", attempt > 1).
					Dur("elapsed", time.Since(start)).Msg("deny observed")
				return denyOutcomeDenied, nil
			}
			// Windowed re-issue: only re-issue once a full interval has elapsed since the last
			// issue AND this attempt's deny still has a full round-trip window before the deadline.
			now := time.Now()
			if now.Sub(lastIssue) >= interval && now.Sub(start)+interval <= deadline {
				attempt++
				lastIssue = now
				logger.Debug().Int("attempt", attempt).Dur("elapsed", now.Sub(start)).Msg("deny wait: action re-issued")
				if err := issue(); err != nil {
					return transportExit(err)
				}
			}
		case <-denyCtx.Done():
			// Boundary deny-wins: a deny observed exactly as the deadline fires must not be lost to
			// select's pseudo-random tie-break (that would reintroduce a narrow #216).
			if poll() {
				return denyOutcomeDenied, nil
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return denyOutcomeCancelled, fmt.Errorf("deny wait canceled: %w", ctxErr)
			}
			logger.Warn().Int("attempts", attempt).Dur("elapsed", time.Since(start)).
				Msg("deny wait: deadline elapsed with no matching deny")
			return denyOutcomeTimedOut, nil
		}
	}
}

// pollDenyFromErrCh adapts a bespoke buffered error channel (the api-key suite pattern) to the
// waitForDeny poll closure: a non-blocking receive that returns true only on the exact (type, code)
// pair. A non-matching frame is consumed and reported false (the caller drained once before the
// wait, so any frame here is a response to this check's issue; a later matching deny is captured by
// a subsequent poll).
func pollDenyFromErrCh(errCh <-chan testerws.Message, errType, code string) func() bool {
	return func() bool {
		select {
		case msg := <-errCh:
			return msg.Type == errType && msg.Code == code
		default:
			return false
		}
	}
}

// drainMessages empties a buffered message channel non-blockingly. Called exactly once before a
// deny wait (drain-once) so a stale frame from a prior check cannot false-pass.
func drainMessages(errCh <-chan testerws.Message) {
	for {
		select {
		case <-errCh:
		default:
			return
		}
	}
}

// denyCheckResult maps a deny-wait outcome to a CheckResult. The canceled outcome is handled by
// the caller (clean short-circuit, no result recorded); this maps the other three.
func denyCheckResult(name string, outcome denyOutcome, err error) metrics.CheckResult {
	switch outcome {
	case denyOutcomeDenied:
		return metrics.CheckResult{Name: name, Status: metrics.CheckStatusPass}
	case denyOutcomeTransportError:
		return metrics.CheckResult{Name: name, Status: metrics.CheckStatusFail, Error: fmt.Sprintf("unexpected transport error awaiting deny: %v", err)}
	default: // denyOutcomeTimedOut
		return metrics.CheckResult{Name: name, Status: metrics.CheckStatusFail, Error: "timed out waiting for deny signal"}
	}
}
