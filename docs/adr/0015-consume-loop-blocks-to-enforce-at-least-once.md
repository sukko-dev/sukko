# ADR-0015: The consume loop blocks to enforce at-least-once delivery

**Status**: Accepted
**Date**: 2026-09-17
**Ticket**: —

## Context

ADR-0014 asserts at-least-once delivery as a core property of the platform — the
property that motivates choosing Sukko over a fire-and-forget bus. The Kafka consumer
did not honour it.

The consumer marked every record for commit immediately after calling its broadcast
callback, without checking whether the broadcast had succeeded. The failure was
structurally invisible rather than merely unchecked: the callback, the tenant pool's
`routeMessage`, and `Bus.Publish` all returned nothing. A failed Valkey `PUBLISH` was
caught inside the bus — logged, counted, and dropped.

So while the broadcast bus was unavailable the consumer read records, failed to fan
them out, and committed past them. A bus outage does not break client WebSocket
connections, so no client sends a `reconnect` frame and the Kafka-backed replay path
never fires. The records were lost permanently.

The benchmark fault matrix measured it: killing Valkey mid-run left a permanent
28-sequence hole per (subscriber, channel), while killing Redpanda instead lost
nothing — there the consumer cannot advance past records it has not read, so it
replays. That asymmetry is the whole diagnosis: loss occurred exactly where the
consumer advanced its offset past a record it had not delivered.

## Decision

**The Kafka consume loop blocks until delivery succeeds. That block is where
at-least-once is enforced.**

- `Bus.Publish` returns an `error` and stays **fail-fast** — it never blocks, retries,
  or buffers internally. Retry policy does not belong in the bus, because every other
  caller would inherit it.
- `routeMessage` is the retryability filter: it propagates the transient
  backend-unavailable class and swallows permanent rejects (serialization failures,
  malformed tenant IDs), which no amount of retrying would fix and which would
  otherwise wedge a partition behind an undeliverable record.
- The consumer retries **the same record in place** with capped exponential backoff
  (100 ms → 5 s) until it succeeds or the context is cancelled, and marks the record
  for commit only after a successful broadcast.
- When a record is abandoned because the context was cancelled, **the consumer stops
  consuming.** Commits are cumulative per partition, so any later mark — including a
  deliberate rate-limit or dead-letter mark — would commit past the abandoned record
  and lose it. Refusing to process anything after an abandonment keeps the invariant
  in one place.

The invariant, stated once: *every record that was polled but not successfully
broadcast is broadcast before any later record on that partition is marked for
commit.*

## Consequences

- At-least-once holds during a broadcast-bus outage, and ADR-0014's assertion becomes
  true of the implementation.
- **Duplicates are possible on recovery** and are the accepted cost. Every message
  carries the stable identity established in ADR-0008, so consumers can de-duplicate.
- Outages now cost **consumer lag instead of silent loss**. Kafka retains the backlog;
  nothing is dropped.
- **Head-of-line blocking across tenants**: while the bus is unavailable, every tenant
  sharing a consumer waits behind the undelivered record. This is the deliberate
  trade — a stalled tenant is recoverable, a lost message is not — and it is the
  consequence most likely to be questioned in production.
- The condition is observable: `ws_broadcast_publish_failures_total`,
  `ws_consumer_broadcast_retries_total`, and
  `ws_consumer_broadcast_blocked_seconds_total`. Previously a publish failure was an
  in-process counter behind a health endpoint, invisible to Prometheus.
- Client publishes through the direct backend now return a failure during an outage
  instead of a false acknowledgement.
- §VII is respected. Message Pipeline Protection governs the **delivery** path
  (broadcast → shard fan-out → write pump), not ingestion; pausing ingestion is
  ordinary backpressure. The CPU emergency brake in the same file is the established
  precedent for deliberately blocking the consume loop.

## Cross-repo impact (§XVI)

The three counters and the intentional-backpressure behaviour are operator-facing and
are documented in the observability guide in `sukko-dev/docs`. No configuration
change: the backoff bounds are named constants, not environment variables, because
they bound a recovery loop rather than expose a tuning knob. No API contract changes —
the error surfaced to publishing clients was already specified.

## Alternatives rejected

- **Pause polling while the bus is unhealthy, then resume** — incorrect, and it would
  have reproduced the same loss one layer up. A consumer's fetch position advances
  independently of the committed offset; records already polled but unmarked are not
  redelivered within a live session, only after a rebalance or restart. Pausing and
  resuming abandons exactly the records it intends to save.
- **Withhold the mark without stopping consumption** — useless on its own. Commits are
  cumulative per partition, so marking a later record commits the withheld one.
- **Seek back to the committed offset on recovery** — sound, but it adds partition
  bookkeeping and in-flight batch invalidation for no benefit over retrying a record
  already in hand.
- **Retry inside `Bus.Publish`** — would make client-initiated publishes inherit
  blocking on a path where failing fast is the correct behaviour.
- **A local outbox or replay buffer** — reintroduces the large in-memory buffer this
  codebase deliberately removed, and duplicates durability Kafka already provides.

## Deliberately left open

Two narrow paths in the same family are not closed here, and are tracked rather than
silently accepted: `routeMessage` drops **and marks** a message whose tenant lookup
misses at publish time (a registry race that a retry seconds later would resolve), and
deliberate-drop marks are placed inline rather than ordered within the pending batch,
so a drop-mark can precede unflushed records on the same partition.
