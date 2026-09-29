# ADR-0029: The history writer takes a clock seam for deterministic timing tests

**Status**: Accepted
**Date**: 2026-09-29
**Ticket**: —

## Context

The supervised history writer (`internal/server/history`) has three sources of
nondeterministic timing: the restart-backoff wait (`time.After`), the heartbeat
`time.NewTicker`, and the backoff jitter (`rand.Int64N`). Its test suite
coordinated with the async writer goroutine through fixed `time.Sleep`, which
races CI scheduling latency and flaked — repeatedly breaking release gates.

Tier 1 (test-only, ADR-less) converted the ~12 **positive-wait** tests — the ones
that assert an effect *happened* — to poll the real condition (`waitFor`). That
covered the entire release-breaking class. It cannot fix two remaining classes:
**negative-window** tests, which assert an effect did *not* happen within a
window (a poll would false-pass and they silently weaken), and **timing-value**
tests (`RestartBackoffJitter`, `BackoffResetAfterSuccess`), which assert on the
backoff duration itself and cannot be both robust and discriminating against real
time. `testing/synctest` (Go 1.26) was ruled out: the tests drive miniredis over
real TCP via valkey-go, whose background socket-reader goroutine is not "durably
blocked," so a synctest bubble never quiesces.

## Decision

**The `Writer` takes an injected clock (and jitter randomness) — a real
implementation in production, a controllable fake in tests.**

- A minimal, package-local `Clock` interface — `After` and `NewTicker`
  (returning a `Ticker` with `Chan`/`Stop`) — plus an injectable jitter function.
  The writer uses these instead of the `time`/`rand` package functions directly.
  (`time.Now` is not seamed: the writer never reads it.)
- `NewWriter` defaults to the real clock (thin `time` wrappers) and
  `rand.Int64N`, so production behavior is unchanged.
- Tests inject a **hand-rolled fake clock** whose `Advance` fires timers/tickers
  deterministically and whose `BlockUntil(n)` blocks until `n` goroutines are
  parked on a timer — closing the advance-before-waiter race (advancing before
  the writer is parked would silently drop the tick and create a new flake).
- Negative-window and timing-value tests use the fake clock to assert absence and
  backoff values deterministically. Lock-TTL tests continue to drive *miniredis's*
  clock (`FastForward`); those tests choreograph both clocks explicitly.

## Consequences

- Production dependency footprint is unchanged — no new module; the `Clock` is a
  three-method interface owned by the history package, and the fake lives in
  `_test.go`.
- The fake clock is itself the correctness hazard the advisor flagged; it is
  unit-tested (advance, ticker cadence, `BlockUntil`) before any writer test
  relies on it.
- `BackoffResetAfterSuccess` regains the reset-discrimination that #29 traded away
  for a widened deadline: with a fake clock it can compound the backoff, reset on
  success, and assert the next delay exactly.
- The writer's hot path is untouched — the clock methods are the same operations,
  behind an interface, called on the same cold restart/heartbeat paths (§VII).
- This change lands the seam and converts `BackoffResetAfterSuccess` (the reset
  discrimination). The remaining fake-clock conversions — `RestartBackoffJitter`
  and the negative-window tests (`PassivePodNoXADD`, `NonConvergenceDoesNotRestart`,
  `AlwaysPassivePod`, `PassivePodNoLockDEL`, `LockCallFailureVsCASMiss`,
  `CtxCancelExitsDuringBackoff`) — are tracked as a Tier 3 follow-up on this same
  seam; until they land, those tests keep their fixed sleeps.

## Alternatives rejected

- **`github.com/jonboulle/clockwork`** (the de-facto Go fake-clock library, with a
  proven `BlockUntil`): lowest correctness risk, but its `Clock` type would enter
  the *production* dependency tree for a three-method testing seam. The hand-rolled
  interface keeps production dependency-free (greenfield/§X preference); the
  correctness risk is bounded by unit-testing the fake.
- **A backoff-value gauge only** (`ws_history_writer_restart_backoff_seconds`, real
  §VI observability) to make the timing-value tests deterministic, leaving the
  negative-window class on widened polls: smaller, but it does not deterministically
  fix the negative-window tests (which are the clock's real justification).
- **Blanket clock refactor of every test**: a fake clock still requires the
  post-advance goroutine handoff, so it buys little for the positive-wait tests
  (already fixed by Tier 1) while maximizing churn in §VII-governed code.
