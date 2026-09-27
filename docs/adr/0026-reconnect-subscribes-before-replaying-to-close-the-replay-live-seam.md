# ADR-0026: Reconnect subscribes to live before replaying, to close the replay↔live seam

**Status**: Accepted
**Date**: 2026-09-27
**Ticket**: —

## Context

A ws-server replica dying mid-burst loses a handful of in-flight messages on the
surviving replica's clients — reproduced on both released v1.0.4 and current main
(so it is long-standing, not a regression), on every edition. Systematic debugging
refuted the obvious causes with evidence: it is not fan-out overflow (the bus
`ws_broadcast_bus_dropped_total` delta is 0), not a broadcast-bus lapse (no Valkey
reconnect), not a consumer offset skip (the reset is `AfterMilli(processStartMillis)`
which re-reads rather than skips, and commit-after-broadcast is correct), and not
reconnect-replay truncation or a replay thundering-herd (a reproducing run showed 242
reconnect-replays that all succeeded — 0 truncated, 0 failed — yet 14 holes remained).

The cause is a **seam in the client-driven reconnect protocol**. When a replica dies
its clients re-home to the survivor and each sends two independent messages:
`reconnect{client_id, last_pos}` then `subscribe`. The server answers `reconnect` by
replaying Kafka offsets `last_pos+1 … end-at-replay-time` — a snapshot — and only
begins live delivery when the later `subscribe` registers the client in
`s.subscriptionIndex`. Any message that reaches the bus **after** the replay's
snapshot but **before** the subscribe registers is in neither window and is lost. The
loss is upstream of per-client sequence assignment, so the client's seq stream stays
contiguous and it cannot detect the gap itself.

## Decision

**On reconnect, the server registers live delivery before it replays.** In
`handleReconnect`, before invoking the backend replay, the tenant-authorized channels
named in `last_pos` are registered for live fan-out (`c.subscriptions.AddMultiple` +
`s.subscriptionIndex.AddMultiple(channels, c)`), so live bus messages begin queuing to
the client immediately; the replay of `last_pos → now` then runs on its existing path.

The replayed (older) and live (newer) messages both reach the client, which already
deduplicates by `mid` (ADR-0008) and orders by `pos` — the identical merge it performs
for mid-session gap replay, so no client change is required. The client's subsequent
explicit `subscribe` becomes an idempotent re-add. Because the live subscription is
established first, there is no window in which a bus message is delivered to neither
the replay nor the live stream: the two windows overlap, and the overlap is absorbed by
`mid` dedupe.

This is deliberately a **server-only** change to the reconnect ordering. It complements
ADR-0016/0017 (broadcast-bus-lapse recovery); the seam addressed here is the distinct
client-reconnect path, not a bus lapse.

## Consequences

- Closes the seam for **every** reconnecting client, including already-deployed SDKs —
  no wire-protocol, AsyncAPI, or SDK change.
- More duplicate messages are delivered around the reconnect boundary; safe and
  expected under the stable message identity of ADR-0008 (dedupe by `mid`).
- Registration-before-replay must stay within §VII: subscription registration is a
  map add and live fan-out is already non-blocking, so the hot path is unaffected; the
  implementation must confirm the double-subscribe (server-registered then
  client-sent) is idempotent and ordering/dedupe hold under the re-homing storm.
- **Validation gate**: the bench fault-matrix `ws-server` kill run must drop holes to
  ~0. Only then is `ws-server` returned to gating in the matrix (reverting the interim
  non-gating default), restoring failover coverage to the regression guard.

## Alternatives rejected

- **Client-side reorder** (the client subscribes-live-first, then replays): changes the
  reconnect protocol, so it touches `ws/docs/asyncapi/client-ws.asyncapi.yaml` and all
  three SDKs (§XVI/§XVII), and leaves already-deployed clients buggy until they upgrade.
  The chosen server-side approach closes it for all clients with zero client/contract
  churn.
- **Pod-level reconnect recovery** (an ADR-0017-style once-per-pod backfill for the
  reconnect case): its premise — that per-client replays fail under the re-homing herd —
  is refuted by evidence (the replays succeed); a pod-level mechanism does not inherently
  close a per-client replay↔live seam.
- **Capacity / tuning** (raise replay concurrency, rate limit, timeout, or send-buffer
  size): targets rejection/overload, which the evidence shows is not occurring (zero
  rejections or failures). It cannot recover a message that no client ever requested.
- **Coalesce concurrent replays** (one shared Kafka read for identical requests):
  de-duplicates replay work but does not close the seam.
