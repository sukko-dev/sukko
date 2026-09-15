# ADR-0010: Kafka consumer reset offset is AfterMilli(process start), not AtEnd

**Status**: Accepted
**Date**: 2026-09-05
**Ticket**: #255

## Context

The shared and dedicated Kafka consumers set `ConsumeResetOffset(AtEnd)` — on
first consumption of a topic the group has never committed, start at the current
end. `AtEnd` resolves the start position via a `ListOffsets` round-trip that
completes asynchronously after the partition is assigned. When a record is
produced into a freshly-created tenant topic *before* that round-trip resolves —
a window that widens under CPU contention — the end resolves past the record
(e.g. to offset 1 for a record at offset 0), and the record is **skipped from
the live delivery path** with no error. This was proven with franz-go's debug
trace (#255): the consumer resolved `AtEnd → offset 1` while the record sat at
offset 0, leaving `CURRENT 0 / LOG-END 1 / LAG 1` for the topic's lifetime. It
manifested as an intermittent (~2%/iteration under load) `ingest 0` failure of
the history grid cell, and is a genuine correctness gap: any live subscriber can
miss messages published in the subscribe→resolve window on a fresh topic.

`AtEnd` was chosen to avoid replaying a topic's retained history to live clients
when the group first consumes a topic that already holds messages — the relevant
real scenario being an environment/consumer-group rename or DR against a
pre-populated cluster, where reset applies to every topic at once.

Consumption in this architecture is **provision-driven, not client-driven**: the
shared consumer subscribes to every provisioned tenant topic and consumes
continuously, committing as it goes, regardless of whether a client is
connected. So there is no steady-state accumulation of unconsumed history for a
"late" client to replay — the group never falls behind a live topic.

## Decision

Set the consumer reset offset to `AfterMilli(processStartMillis)` on both the
shared and dedicated consumers, where `processStartMillis` is captured once when
the Kafka backend is constructed.

Per franz-go's own documentation this consumes "at the end of existing
partitions, but at the start of any new partitions that are created later":

- **Fresh tenant topic** (created after this process booted): the first record's
  timestamp ≥ process start, so the reset resolves to **offset 0** — the record
  is always read, the race is gone.
- **Pre-existing filled topic** (env/group rename, DR against an old cluster):
  records predate process start and are skipped, preserving exactly the
  no-history-replay protection `AtEnd` provided.
- **Bonus:** the offset is `OffsetOutOfRange`-resilient — on OOR it resets to the
  first offset after the timestamp rather than skipping to the end, which also
  fixes the stale-cursor case after a tenant topic is deleted and recreated.

## Scope

Applies to the server's shared and dedicated consumers, whose consumption is
provision-driven — they subscribe to every provisioned tenant topic and consume
continuously, so the group never falls behind a live topic. The **push consumer
pool is intentionally excluded** (documented §XVIII deviation in its code): its
consumption is enablement-driven — it consumes only push-enabled tenants' topics,
so a tenant enabling push late has topics hot-added long after boot with a
post-boot backlog already in the log. `AfterMilli(process-start)` would replay
that backlog as a notification storm; the push pool keeps `AtEnd`. The narrow
fresh-topic race that leaves on the push side is tracked separately and needs an
app-level freshness guard, not a reset-policy change.

## Consequences

- The fresh-topic delivery race is eliminated with a one-line reset-policy change
  on each consumer construction; no new offset-commit plumbing, no gating the
  client subscribe ack on broker round-trips (rejected below), no active-group
  offset-commit (which brokers commonly reject).
- **Trust assumption:** the reset relies on producer-stamped record timestamps
  (`CreateTime`). A producer that backdates timestamps below the consumer's boot
  time would have those records skipped on first-consume. First-party
  (`producer.go`) and tester producers stamp ~now, so this does not arise today.
  If a future integration produces backdated timestamps, the hardening is to
  configure the tenant topics with `LogAppendTime` so the broker stamps the
  timestamp used for the offset lookup. Noted, not implemented.
- Only first-consume (no committed offset) is affected. Restart resumes from the
  committed offset unchanged; steady-state consumption is unchanged.
- The tester's `classifyIngestFailure` "extend-wait handles the fresh-consumer
  offset race" comment described a premise that was false under `AtEnd` (the
  record was skipped, not merely late); it is corrected alongside this change.

## Alternatives rejected

- **`AtStart`** — race-free, but on any first-consume of a topic with retained
  history (env/group rename, DR) it replays the entire backlog to live clients
  as if new. The operational hazard that anchors this whole decision; rejected.
- **Pre-commit offset 0 in topic creation** — commits offset 0 for the group
  when ws-server creates a topic. Brokers commonly reject `OffsetCommit` to an
  *active* consumer group (ours is always Stable when this runs), and it commits
  0 unconditionally for created topics without distinguishing empty from
  externally-pre-filled. More plumbing, real broker-rejection risk.
- **Eager end-capture at subscribe** — synchronously read and commit the end
  offset when the client subscribes. Consumer wiring is provision-event-driven
  and asynchronous to the client subscribe ack, so this only narrows the window
  unless the ack itself is gated on kafka round-trips — new latency and failure
  modes on every first subscribe, cross-layer plumbing, for a race that lives at
  topic birth. Rejected.
