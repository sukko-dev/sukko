# ADR-0022: The consume-loop rate limiter paces, it does not drop

**Status**: Accepted
**Date**: 2026-09-21

## Context

The Kafka consume loop protects the server with two throttles applied per record
before broadcast (`internal/server/kafka/consumer.go`): LAYER 1, a token-bucket
rate limiter (`WS_MAX_KAFKA_RATE`), and LAYER 2, a CPU emergency brake. LAYER 2
already backpressures — it *waits* for CPU to recover and never loses a message.
LAYER 1 did the opposite: when the limiter denied a token it **dropped the
record and marked it for commit** (`return nil, false` → caller
`MarkCommitRecords`). Under `kgo.AutoCommitMarks`, marking is exactly what
prevents redelivery, so a dropped record was lost permanently — the inline
comment "let Kafka handle redelivery" was false.

This is normally invisible because steady-state produce sits below the limit.
It becomes a data-loss bug during **recovery catch-up**. When the ws-server that
owns a partition is SIGKILL'd, no `LeaveGroup` is sent, so the group waits the
full `KAFKA_SESSION_TIMEOUT` before reassigning the partition; during that stall
the partition is unowned and the backlog accumulates. When a survivor is finally
assigned the partition it drains that backlog as fast as it can — well above
`WS_MAX_KAFKA_RATE` — and LAYER 1 shed the excess and committed past it.

The `sukko-dev/bench` fault matrix reproduced this deterministically on a
dedicated 8-vCPU VM against the released v1.0.3 images: a deterministic
owner-kill left ~700 permanent per-channel holes across all subscribers, and the
survivor logged thousands of `"Kafka rate limit exceeded - dropping messages"`
at the instant of takeover. Lowering `KAFKA_SESSION_TIMEOUT` (30s→10s) shortened
the stall (p999 30.15s→17.62s) but did **not** close the holes (580→552): the
loss is bounded by the burst that lands during the disruption, not by the stall
length, so a timeout change is not a fix.

## Decision

The consume-loop rate limiter **paces** the loop instead of dropping: when the
limiter denies a token, `paceKafkaRate` bounded-blocks (waiting the limiter's
suggested delay, cancelable via the consumer context) and retries the **same**
record until it is admitted, then the record proceeds to broadcast and is marked
only after a successful broadcast. This mirrors LAYER 2 and the broadcast-retry
backpressure (ADR-0015). A context cancel while pacing abandons the record
**unmarked** and stops the consume loop, exactly as the CPU brake does — the
record redelivers after restart/rebalance, preserving at-least-once.

Drop-and-mark remains only for **permanent rejects** (malformed records and DLQ
routing), which are correct to mark: they can never be delivered, so bus health
must not affect their commit. The rate limiter is no longer a drop site, so the
`ws_kafka_messages_dropped_total` metric and the consumer's `dropped` counter
are removed; recovery is pod-level backfill (ADR-0017), and the analogous
replay-path throttle bypass is ADR-0021.

A new counter, `ws_consumer_rate_limit_paced_seconds_total`, makes the new
backpressure observable — sustained pacing is the signal that the consumer is
falling behind produce and lag is growing.

## Consequences

- **At-least-once is restored across a partition reassignment.** A survivor
  draining a post-stall backlog delivers every record (paced), instead of
  shedding the burst.
- **Sustained overproduce now yields consumer lag, not silent loss.** If produce
  stays above `WS_MAX_KAFKA_RATE` indefinitely, the consumer paces and lag grows;
  loss occurs only if lag exceeds Kafka retention. This is the explicit trade,
  and it matches Kafka's purpose (absorb backlog). Operators MUST monitor
  consumer lag and `ws_consumer_rate_limit_paced_seconds_total`.
- **A third bounded-block on the consume loop.** Rate-limit pacing joins the CPU
  brake and broadcast-retry as intentional backpressure (documented at
  `consumeLoop`). Like the others, it blocks the loop rather than committing past
  an undelivered record; the `KAFKA_SESSION_TIMEOUT` remains an ops-QoL knob, not
  part of correctness.
- **`WS_MAX_KAFKA_RATE` behavior changed** (over-rate paces, not drops) — its
  inline config comment is updated, so `../sukko-docs` `configuration.mdx`
  regenerates. No API contract change; the fix is one repo.

## Alternatives rejected

- **Lower `KAFKA_SESSION_TIMEOUT` to shrink the stall** — measured no material
  effect (580→552 holes); the loss tracks the disruption-window burst, not the
  stall length. A shorter timeout is desirable for recovery latency but is not
  the correctness fix.
- **Client re-replay until gap-free (SDK-side)** — masks an at-least-once
  violation (ADR-0014) instead of fixing it at the source; costs a change to
  three SDKs plus the AsyncAPI contract, needs replay-herd control (the open
  ADR-0021 follow-up), iterates against the `WS_MAX_REPLAY_MESSAGES` cap, and
  leaves non-replaying consumers (SSE) unprotected. Pacing fixes it for every
  consumer in one repo.
- **Keep dropping but stop marking (let redelivery handle it)** — franz-go does
  not redeliver polled-but-unmarked records within a session (the fetch position
  has advanced), so the record is lost until the next rebalance and the dropped
  work simply repeats; retry-in-place is the only sound shape.
- **Seek-to-committed on assignment** — the survivor already resumes from the
  committed offset; the loss was the catch-up drop, not the resume point, so this
  would not have helped.
