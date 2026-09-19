# ADR-0017: Recovery is pod-level backfill, not per-client replay

**Status**: Accepted
**Date**: 2026-09-17
**Ticket**: —

## Context

ADR-0016 closed the subscription control plane's loss of state and recovered most
of a broadcast-bus outage's cost. Its final decision bullet chose **client-driven
recovery**: on re-establishing a lapsed subscription the pod notifies every
affected client, and each client replays the missed window for itself. That bullet
is wrong, and it was measured wrong rather than argued wrong.

Against the fault matrix, the change made delivery **worse**. Holes rose from 480
to 2,678 and then 1,815 across two runs, and the damage no longer ended when the
outage did: gaps ran from the fault to the end of the run, where the same stack
without the feature recovered within five or six sequences. Three facts explain it.

First, one replica routed every Kafka partition (195,405 messages) and the other
routed none. A pod learns a channel's topic only by having routed a message for
it, so the non-routing replica could resolve nothing: **all 242 of its replays
failed** with `not_available`, while the routing replica completed 238. Half the
fleet could not recover at all.

Second, the surviving half issued 249 concurrent replays against the one replica
that also carries all ingestion. Recovery competed with delivery on the pod that
every client depends on — including clients attached to the other replica, which
receive their messages from it. Partitioning subscribers by replay outcome showed
holes in **both** groups, so the damage was never confined to the clients doing
the work.

Third, the redundancy is structural rather than incidental. A lapse is a property
of the pod: the message never arrived, so every client of that pod and channel
missed the same window. Asking each client to fetch it separately re-reads one
window once per client, and the bench's channels all share a single topic, so
those reads collide on the same partition.

The pattern behind all three: a recovery mechanism that fans out per client
imposes load proportional to connection count at exactly the moment the system is
least able to absorb it. This is the thundering herd ADR-0016 explicitly rejected
when it declined to force client reconnects — reintroduced through a gentler
trigger, and not costed for the option that was chosen.

One premise of ADR-0016's rejection of server-side backfill has since become
false. It was rejected because tracking a per-channel position would put work on
the fan-out hot path (§VII). The lapse is now **time-anchored**: `lapseStart` is
stamped on the cold path when the dedicated connection is re-initialised, and
Kafka resolves a timestamp to offsets server-side through the same `AfterMilli`
machinery ADR-0010 already put in production. The hot path gains nothing.

## Decision

**Recovery is performed once by the pod, not once by each client.**

- On re-establishing a lapsed subscription, the pod re-reads the lapse window
  **once per topic-partition** using a group-less client, and injects the records
  into the per-tenant bus subscriber channels. Per-channel backfill is explicitly
  rejected as the grain: channels share topics, so it would re-read one partition
  window once per channel.
- Injection at the **bus-subscriber seam** means every local consumer of the
  tenant's stream receives the recovered records — WebSocket shards, the history
  writer's pattern subscription, and the webhook worker. Recovery therefore
  repairs the history and webhook paths for the lapse class in the same mechanism,
  rather than leaving them as separate gaps.
- Recovered records reach WebSocket clients as **`replay_message`**, the type the
  contract already defines, so a client can distinguish recovered data from live
  data. Treating a late record as current is a real hazard for market data, and
  the distinction costs one documentation change rather than a new message type.
- The window start MUST be **padded**. `lapseStart` comes from the pod's clock
  while Kafka record timestamps come from producers and brokers; an earlier start
  yields duplicates, which are safe under the stable message identity of ADR-0008,
  while a later start loses records.
- Backfill is **bounded**. When it cannot cover the lapse — the bound is reached,
  Kafka retention has expired, or the topic cannot be resolved — the pod emits the
  resume gap notification to the affected clients. The notification survives as
  the **shortfall signal**, not as the recovery mechanism: an ordinary lapse is
  silent because the messages simply arrive, and unrecoverable loss is never
  silent.
- **Channel-to-topic resolution MUST come from the topic registry**, not from
  observed traffic. A pod must be able to resolve a channel it has never routed.
  This is a prerequisite: it repairs client-initiated replay on non-routing pods
  independently of anything else here, and pod-level backfill cannot work without
  it.

## Consequences

- Recovery load becomes proportional to partitions and lapses rather than to
  connection count. The herd is removed by construction, not thinned by tuning.
- Recovery works on every pod, including those that route no traffic today.
- The history writer and webhook worker stop losing the lapse window, closing two
  tracked gaps without a separate mechanism.
- Duplicates increase: a padded window and a shared fan-out deliver more than each
  client strictly missed. This is the accepted direction — ADR-0008 identity makes
  duplicates cheap and silent loss is what must not happen.
- Clients receive recovered records without asking, so an ordinary outage needs no
  client participation at all. Client implementations remain necessary only for
  the shortfall path and for overflow gaps.
- **Overflow gaps remain client-driven.** A slow client's dropped message is
  genuinely per-client, has no shared window to replay, and does not correlate
  across clients, so it does not stampede. One mechanism per failure shape.
- Backfill is bounded work on a cold path, but it is not free: a long outage still
  produces a large read. The bound, and surfacing when it is reached, are what keep
  that honest.

## Supersession

This supersedes **only** the final decision bullet of ADR-0016 — client-driven
recovery via gap notification. Everything else in ADR-0016 stands: declared
subscription state and its reconciliation, local reconciliation without
`PUBSUB NUMSUB`, the split of publish health from subscription convergence, and
subscription non-convergence being degraded rather than unready. The monotonic
clock introduced for the replay anchor also stands, because the anchor remains the
client's route to records beyond the backfill bound.

## Alternatives rejected

- **Stagger and coalesce the per-client replays** — thins the herd without
  removing the redundancy, since every client still re-reads a window its peers
  are reading, and it adds recovery latency by design.
- **Per-channel backfill** — the right shape at the wrong grain; channels share
  topics, so it re-reads one partition window once per channel.
- **Shard-level injection** — recovers WebSocket clients only, leaving the history
  and webhook paths holed, including the history that the contract recommends as a
  client's fallback.
- **Backfill as ordinary `message` frames** — removes all client work, but makes a
  recovered record indistinguishable from a live one.
- **Unbounded backfill** — removes the shortfall case at the cost of an unbounded
  read at the worst possible moment.
