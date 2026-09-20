# ADR-0021: Replay parses records without the live throttle, bounded by an aggregate concurrency cap

**Status**: Accepted
**Date**: 2026-09-20
**Ticket**: —

## Context

Reconnect and live-gap replay recover a client's missed messages from Kafka via
`ReplayFromOffsets`, which routed every scanned record through `prepareMessage` — the
live consume path's per-record processor. That reused three live-path behaviours that
are wrong for a bounded, read-only recovery operation:

- **Rate limiting**: `prepareMessage` reserves a token from the shared
  `ResourceGuard.kafkaLimiter` and, on exhaustion, returns nil — which
  `ReplayFromOffsets` silently drops. Replaying a burst (e.g. an 8× market-data burst
  during a failover window) exhausts the shared budget and drops the burst records, so
  the client is left with permanent holes. Worse, the token is reserved *before* the
  subscription filter, so a reconnect scanning a shared catch-all topic burns one token
  per *scanned* record, and the live consume loop's own drops are marked/committed —
  permanently lost for every client. This is the mechanism behind a prior herd
  regression (recovery holes 480 → 2678).
- **CPU emergency brake**: waits on the consumer-lifetime context, not the replay's own,
  so a replay can block far past its deadline.
- **DLQ routing**: a malformed record is re-routed to the dead-letter queue from the
  read-only temporary replay client, duplicating DLQ entries.

The benchmark fault matrix isolated the recovery gap once the reconnect isolation fix
(ADR-0020) scoped replay to the client's own channel: fault-ws leaves each subscriber's
own channel missing exactly the failover burst window.

Removing the per-record throttle from replay requires bounding replay load elsewhere,
because a replica kill makes all its clients reconnect at once (a herd). Per-reconnect
throttling is insufficient: reconnects arrive on fresh connections, so per-connection
limiter state resets, and each replay still allocates a temporary Kafka client, a large
fetch, and a scan loop. The scarce resources are aggregate (client count, fetch
bandwidth, scan CPU), so the bound must be aggregate.

## Decision

**Replay parses records with a dedicated read-only path that applies no live-path
throttle, and total replay load is bounded by an aggregate concurrency semaphore.**

- **Parse-only path**: a new replay record parser extracts the channel via the pure
  `extractChannel` (retaining its tenant-prefix check as defense in depth beside the
  ADR-0020 subscription filter). It applies **no** rate limiter, **no** CPU brake, **no**
  DLQ routing, and touches **no** live counters; a record it cannot parse is skipped and
  counted on a replay-specific metric. `prepareMessage` stays live-only and unchanged.
- **Aggregate cap**: `ReplayFromOffsets` acquires a non-blocking slot from a
  process-wide semaphore before creating its temporary client and issuing its first
  fetch, releasing on every exit. At capacity it returns a retryable `ErrReplayBusy`.
  The same choke point covers both replay callers (reconnect and live-gap). Size via
  `WS_MAX_CONCURRENT_REPLAYS`. An admission-time CPU-pressure check also returns
  `ErrReplayBusy` — the CPU brake, moved from per-record to per-replay granularity.
- **Handlers**: `ErrReplayBusy` maps to a distinct retryable client code `replay_busy`
  (server capacity — separate from `replay_rate_limited`, which is client frequency).
  `handleReconnect` gains the same per-interval guard `handleReplayRequest` already has,
  as a cheap first layer (not the load bound).
- **Clients retry `replay_busy`** with jittered backoff. Without this, a herd rejection
  becomes an unrecovered hole rather than a silent one — so the SDK retry is part of the
  contract, not an optional follow-up.

## Delivery (phased)

The two halves ship separately because they address different concerns:

- **Parse-only path (this change).** Closes the recovery gap and is the correctness
  fix. Verified: on a clean two-replica stack the fault-ws pod-kill run is green
  (holes=0, harness-sound) across repeated runs. It also removes the herd's *live-drop*
  mechanism (replay no longer drains the live budget or causes marked live-loop drops),
  so it is a net §VII improvement on its own.
- **Aggregate concurrency cap (follow-up).** §VII resource-exhaustion hardening for an
  extreme herd (many simultaneous first-time reconnect-replays each allocating a
  temporary client, a large fetch, and a scan). The benchmark herd (≈240 reconnects)
  does not exhaust resources without it, so it is not required for correctness or for
  the green fault test — but it bounds the worst case and lands with the `replay_busy`
  error code, the reconnect interval guard, and the SDK retry.

## Consequences

- Replay recovers the client's missed messages completely (up to `MaxReplayMessages` per
  request; larger gaps need repeated reconnects with an advanced `pos` — a pre-existing
  bound, far above the observed burst windows). §IX at-least-once holds on the replay
  path.
- Replay no longer drains or is throttled by the live consume budget, and can no longer
  cause live-loop drops — the herd-regression mechanism is removed (§VII).
- Herd recovery is serialized: after a pod kill, replays drain at roughly the cap per
  replay-duration and excess clients retry, so recovery latency spreads over
  seconds-to-tens-of-seconds instead of overwhelming the pod.
- Recovery capacity is a shared, process-wide pool; per-tenant recovery fairness is
  deliberately deferred (§XV — one cap is simpler than per-tenant budgets, revisit if
  noisy-neighbour recovery starvation is observed).
- New surface across repos (§XVI/§XVII): a `replay_busy` error code, a
  `WS_MAX_CONCURRENT_REPLAYS` env var, AsyncAPI + `e2e-testing.md` + config-reference
  updates, and SDK retry logic — all in scope for the change set.

## Alternatives rejected

- **Per-reconnect throttle only.** Nearly vacuous against a herd — fresh connections
  reset per-connection state and each replay still allocates its own client/fetch/scan;
  it bounds client frequency, not aggregate load.
- **Separate per-record replay token budget.** Per-record tokens inside a
  deadline-bounded operation convert load back into holes (drop) or timeout truncation
  (wait), and bound scan rate but not the temporary-client count or fetch bandwidth that
  are the real scarce resources.
- **Wait instead of drop, on a replay-scoped context.** Honest backpressure, but the
  wait re-creates the shared-budget drain and `ReplayTimeout` truncates under load —
  holes via timeout. No per-record throttle of any kind belongs inside replay; bound at
  admission, where rejection is explicit and retryable, never a silent hole.
- **Do nothing.** The shared budget is genuine pod self-protection, but replay
  converting live traffic into permanent holes is indefensible against the measured
  recovery gap.
