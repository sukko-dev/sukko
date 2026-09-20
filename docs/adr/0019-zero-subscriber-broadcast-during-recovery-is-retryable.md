# ADR-0019: A zero-subscriber broadcast during recovery is retryable, not delivered

**Status**: Accepted
**Date**: 2026-09-20
**Ticket**: —

## Context

ADR-0015 made the consume loop block until `Bus.Publish` returns success, so the
Kafka offset never advances past a record the bus refused. That closed consume-side
loss. It did not close delivery-side loss, because a successful `PUBLISH` is not a
successful delivery.

Valkey pub/sub is fire-and-forget: a `PUBLISH` to a channel with zero subscribers is
accepted and dropped. Publish and subscribe recover over **two independent
connections** — `Publish` uses the pooled client (recovery is a single command, fast);
subscriptions use a dedicated pub/sub connection re-established through
`reinitDedicatedClient` + a full `converge` pass (slower). The bus deliberately keeps
these health signals separate (ADR-0016).

The benchmark fault matrix measured the gap: after a Valkey outage, `Publish` recovered
~0.3 s before the dedicated subscriptions reconverged (publish healthy at t≈11.6 s,
`subscriptions_established` 0→1 at t≈11.9 s). The consumer flushed the outage-held
burst — the very records ADR-0015 saved — into a channel with `established=0`, Valkey
dropped them, and the consumer marked them committed. Loss was permanent, intermittent
(~1 run in 3, depending on whether burst traffic fell in the sub-second window), and
uniform: the identical contiguous sequence range was missing for every subscriber on
every channel — the signature of a delivery gap, not a per-client artifact.

`Publish` already had the signal to catch this and discarded it: the `PUBLISH` reply
*is* the number of subscribers that received the message.

## Decision

**A `PUBLISH` that reaches zero subscribers while this pod is recovering its own
subscriptions is a retryable failure, not a delivered message.**

- `Publish` reads the `PUBLISH` reply count. On `count == 0`, it decides between two
  explicit, mutually exclusive cases (§XV) — a genuinely empty channel versus a
  delivery that raced ahead of subscription recovery.
- The discriminator is a **subscribe-disruption episode**, tracked by a single signal
  set the instant a disruption begins and cleared the instant it ends:
  `reinitDedicatedClient` (called *only* on dedicated-connection loss, never on normal
  subscription churn) records the episode start; a fully successful `converge` clears
  it. Normal client connect/disconnect churn does **not** open an episode, so a
  steady-state empty channel is never gated.
- While an episode is open, `count == 0` returns the existing `ErrPublishUnavailable`
  sentinel. It therefore flows through ADR-0015's unchanged retry-in-place path: the
  consumer holds the record and retries until a subscriber appears or the episode ends.
- The episode is **bounded by a window** (`VALKEY_ZERO_SUBSCRIBER_WINDOW`,
  default 30 s). Past the window the gate stops firing even if convergence has not
  completed, and the record is flushed under plain pub/sub semantics. This prevents a
  livelock in which a prolonged, genuinely-empty channel drains one message per backoff
  forever.
- At-least-once is owed to **connected** subscribers. A tenant with no clients anywhere
  relies on Kafka durability and the reconnect/replay path (ADR-0017), not on this
  bus — which is exactly why the window-expiry flush is correct rather than lossy.

## Consequences

- The publish-recovers-before-subscribe window is closed on the publishing pod: the
  reproduced fault-matrix loss (both pods lost their subscriptions on the shared
  outage, so the global subscriber count was 0) no longer commits past undelivered
  records.
- This is a **backstop, not a certification.** The `PUBLISH` count is global across
  pods, so the publishing pod cannot tell "no subscribers anywhere" from "a *remote*
  pod's subscriptions are still reconnecting." The residual — a remote pod lapsed while
  this pod and publish are healthy — is invisible here and is covered by the pod-level
  backfill of ADR-0017, driven by the declared-state resume of ADR-0016. ADR-0019 and
  ADR-0017 together restore the invariant; ADR-0019 alone narrows it.
- Consume-loop latency during a recovery episode is the accepted cost — the same
  blessed backpressure as ADR-0015 and the CPU emergency brake (§VII covers the
  delivery path, not ingestion).
- Duplicates on recovery remain possible and remain acceptable; every copy carries the
  stable message id (ADR-0008) for dedup.
- `Bus.Publish` is also the direct-backend client-publish path (Community mode, no Kafka
  consumer to hold the record). There, a client publish into a currently-empty channel during
  an episode returns `ErrPublishUnavailable` — surfaced to the client as the transient
  `SERVICE_UNAVAILABLE` (retry), not a terminal `PUBLISH_FAILED`. This replaces a false ack of
  a message no subscriber received with an honest retryable error, bounded by the episode
  window; the client, not a consumer, is the retrier on that path.
- Observability: `ws_broadcast_zero_subscriber_retries_total` counts zero-subscriber
  publishes by outcome (`held` during an episode, `accepted_empty` in steady state,
  `window_expired`). A non-trivial `accepted_empty` rate immediately following outages,
  or any `held` outside a real episode, means the episode signal is miscalibrated.

## Alternatives rejected

- **Retry until `count == expected`.** The publisher cannot know the expected
  pod×subscriber count without a shared registry, whose staleness reintroduces the same
  loss class; subscriber acknowledgements are a durable-log reimplementation. (§XV)
- **Gate the consumer's resume on the local `established == desired` signal only.**
  Loss can be on a *different* pod's subscribe recovery, so local health is
  structurally insufficient; it also couples the consumer to the subscribe manager. Its
  kernel survives here as the *publisher-side* disruption signal.
- **Open the episode on `confirmedGen != desiredGen`.** That generation also bumps on
  ordinary subscription churn, so it would gate empty-channel publishes during normal
  operation on a busy multi-tenant pod. The `reinitDedicatedClient` signal is precise
  to real connection loss.
- **Replace pub/sub with Valkey Streams.** Correct by construction but reimplements
  Kafka's durability inside Valkey (retention, trimming, per-pod cursors) and reshapes
  the hot delivery path to `XREAD`; rejected against the prior decision not to duplicate
  what the durable log already provides.
- **Do nothing.** The residual is small, but the public at-least-once claim is then
  false and the benchmark artifact exists to prove the opposite.
