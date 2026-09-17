# ADR-0016: Subscription state is declared, not replayed

**Status**: Accepted
**Date**: 2026-09-17
**Ticket**: —

## Context

ADR-0015 stopped the Kafka consumer from committing past records whose broadcast had
failed. Measured against a real Valkey outage, that fix recovered roughly two thirds of
the messages a bus outage had been losing — from 65 missed sequences per channel down to
22. The residual third comes from the other side of the bus, and from a different
mechanism.

Valkey subscriptions are per-tenant: one SUBSCRIBE gates every channel and every
subscriber of that tenant on a pod. Subscription state is currently maintained as a
stream of events pushed through a bounded 64-slot queue with non-blocking sends, so
commands are dropped under load. A real outage dropped between 797 and 961 of them per
run. The drop is not an oversight — `resubscribeAll()` runs inside the only goroutine
that drains the queue, so a blocking send would self-deadlock. Any finite queue drops
under a disconnect storm; the flaw is encoding subscription *state* as replayed *events*.

The consequences compound. After a dropped SUBSCRIBE the pod's ref counts say
"subscribed", Valkey has no subscription, and `Publish` keeps succeeding — the tenant is
silently dark on that pod. The reconciliation intended as a backstop asks Valkey
`PUBSUB NUMSUB`, which reports a **server-global** count, so a healthy sibling replica
masks a dark pod: in any multi-replica deployment the dark pod stays dark permanently.
The management loop's own comment concedes the shape of this: the reconcile tick "is only
a real backstop if the commands it enqueues actually reach Valkey" — and those enqueues
can themselves be dropped, silently. A single-slot retry compounds it further: the slot is
overwritten on every failure and cleared on *any* success without checking that the
succeeding command is the one that was armed, so one tenant's success cancels another's
pending retry. Finally, bus health is a single flag written by both connections, where any
successful publish or ping marks the bus healthy regardless of subscribe state — one value
meaning two things, which §XV forbids.

## Decision

**Subscription state is declared and reconciled, never replayed.**

- `subRefCounts` and `subscribeAllRefCount` are the authoritative **desired** state; how
  they are maintained does not change. A **confirmed** set records what this pod has
  successfully subscribed on its current dedicated connection, cleared wholesale when that
  connection is re-initialised.
- The subscription management loop's only job is to make confirmed match desired,
  retrying with capped exponential backoff (§IV).
- The command channel becomes a **capacity-1 coalescing wakeup** meaning "desired
  changed, re-derive". Discarding a wakeup when one is already pending is correct: the
  pending wakeup causes a full re-read that already includes the change. Losslessness is
  structural, not probabilistic.
- **Reconciliation is local.** It diffs desired against confirmed in-process instead of
  querying `PUBSUB NUMSUB`. A pod always observes its own divergence, reconciliation
  works while Valkey is unreachable, and the corrections metric counts real re-issues.
- **Health is split** into publish health and subscription convergence, with
  `IsHealthy()` derived from both.
- **Subscription non-convergence is degraded, not unready.** It is reported in the health
  body and in metrics but MUST NOT fail the readiness probe. Failing readiness on a bus
  outage would mark every pod unready and remove the whole fleet from the load balancer,
  amplifying a recoverable blip into a total outage; a pod whose subscription is still
  converging continues to serve its existing connections.
- **Recovering the gap is a client-driven replay.** When a subscription is re-established
  after a lapse, the pod emits the existing gap notification for the affected channels
  with the extent marked unknown — a pod that was not subscribed cannot know which
  sequences it missed — and the client replays from the cursor it already maintains. This
  reuses the built gap protocol and requires no position tracking on the delivery hot
  path (§VII). It ships separately from the reconciliation change, with its wire-contract
  and SDK changes.

## Consequences

- A dropped subscription becomes impossible rather than unlikely, and the permanent-dark
  failure mode in multi-replica deployments is closed.
- Four drop sites, the single-slot retry and its cross-tenant clobber, and the
  self-deadlock constraint that forced the drops are all deleted by construction rather
  than patched.
- Convergence moves from a 30-second tick to milliseconds; the tick remains only as a
  backstop, and a 30-second backstop can never have prevented a sub-30-second gap.
- Operators gain `ws_broadcast_subscriptions_desired` and `_established`. The failure this
  ADR addresses presents as publishes succeeding while established sits below desired —
  previously invisible.
- The health endpoint now reports degraded when subscriptions have not converged. This is
  a visible behaviour change for anything scraping the health body, and the truthful
  answer where the old flag was misleading.
- Gap notifications acquire a second trigger, so clients must tolerate a gap whose extent
  is unknown. Duplicate delivery on replay remains acceptable per ADR-0008.
- The `retry` result label on the subscribe-command counter counted "failed and became the
  sole retry candidate" rather than "was retried"; with the retry slot gone, the label is
  re-documented.

## Alternatives rejected

- **Enlarge the command queue** — any finite queue drops under a disconnect storm; this
  buys a larger outage before the same silent divergence.
- **Make the queue send blocking** — self-deadlocks, because the producer during a
  reconnect is the queue's only consumer.
- **Keep `PUBSUB NUMSUB` reconciliation** — structurally cannot see a dark pod whose
  sibling is healthy, which is the deployment that matters.
- **Force resynchronisation by disconnecting affected clients** — reuses a fully
  supported client path with no protocol change, but stampedes reconnections at the
  moment the system is already stressed.
- **Server-driven backfill from Kafka** — works for every client without a protocol
  change, but requires tracking the last delivered position per channel on the fan-out
  hot path, which §VII exists to prevent.
