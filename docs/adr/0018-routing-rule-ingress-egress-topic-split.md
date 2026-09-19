# ADR-0018: Routing rules split topics into one ingress topic and egress-only topics

**Status**: Accepted
**Date**: 2026-09-18
**Ticket**: —

## Context

A routing rule mapped a channel glob to a flat list of Kafka topics
(`{pattern, topics[], priority}`), a publish produced one record per listed
topic, and the consume set was derived as *default topic ∪ every suffix in
every rule*. Every topic a rule wrote to was therefore also consumed and
delivered. Measured consequences:

1. **Duplicate delivery.** All N records carry the same channel header, the
   consumer resolves the channel from the header (not the topic), and the bus
   subject *is* the channel — every subscriber received N copies.
2. **The copies could not be deduplicated.** `mid` is
   `hex(fnv1a64(topic))-partition-offset`: the topic is hashed into the
   identity, so the N copies carried N different mids. ADR-0008's cross-copy
   equality holds for copies of one record, never across records.
3. **No mid on the ack.** The fan-out path returned `""`, silently breaking
   the `PublishAccepted.Mid` / `PublishResult.Mid` correlation contract.
4. **Non-deterministic history/replay.** The channel→topic map was learned
   from observed traffic (last-writer-wins): a multi-topic channel resolved to
   an arbitrary one of its topics, and a pod that had routed no traffic could
   resolve nothing at all — on a two-replica deployment where one replica
   owned all partitions, 242 of 480 client replay requests failed
   `not_available`.
5. **Partial fan-out reported success.** 1-of-N topic writes returned 2xx;
   only 0-of-N failed.
6. **No cross-topic ordering exists**, so consuming N topics of copies as one
   stream produced arbitrary interleavings.

Prior art (§XI, surveyed across Ably, Pusher, PubNub, Centrifugo, Phoenix,
Socket.IO, Kafka, SNS, EventBridge, GCP Pub/Sub, Azure Web PubSub): no
platform puts multi-destination fan-out on the client-delivery path.
Multi-destination lives publisher-side, subscription-side (one copy per
subscriber), or on an egress/integration path (Ably Firehose, PubNub Events &
Actions, Centrifugo PRO exports). Platforms with position-derived message
identity all deliver exactly one copy per subscriber by design. Producing one
event to several topics is *not* itself an anti-pattern when the copies serve
different consumer populations — the defect was coupling the consume set to
rule topics.

Follow-up research on egress guarantees: gating egress access behind a paid
tier has direct precedent (PubNub gates retry itself behind paid tiers,
Centrifugo gates its ClickHouse export behind PRO, Ably gates Pulsar behind
Enterprise) — vendors gate *access*, never the *guarantee*. Kafka Connect's
DLQ is deliberately NOT cited as delivery-failure precedent: per KIP-610 it
covers converter/transform errors only (a poison-message queue); the correct
dead-letter comparators are SNS, EventBridge, and GCP Pub/Sub.

## Decision

- **A routing rule names exactly one `ingress_topic` and zero or more
  `egress_topics`** — explicit, mutually exclusive roles (§XV), replacing the
  flat `topics` list on the API, the stream protocol, and storage. Existing
  rows migrate as `ingress_topic = topics[1]`, `egress_topics = topics[2:]`
  (deduplicated, ingress removed; degenerate zero-topic rows are deleted).
- **The consume set is default ∪ ingress topics.** Egress topics travel to
  ws-server as `create_only_topics`: created on the broker (the DLQ precedent
  — written to, never consumed), never subscribed.
- **`ChannelTopic()` resolves from the rules, deterministically**: first
  matching rule's ingress topic; no rules or no match → the tenant default
  topic (where rule-less Community ingest and externally-produced records
  live). The observed-traffic cache is deleted; replay/history now resolve on
  every pod, including pods that have routed nothing.
- **The ack always carries the ingress record's mid.** This supersedes
  ADR-0008's clause "multi-topic fan-out acks (N records → N mids) ack
  without a mid, by design" — egress copies are side-copies that never carry
  delivery identity, so the ingress record is unambiguous.
- **Partial-failure semantics are explicit and non-transactional**: the
  synchronous ingress write alone determines the ack. Egress copies are
  submitted only after ingress success, asynchronously, on the same kgo client
  as the main producer — so every egress write inherits the client's bounded
  retry policy (`KAFKA_PRODUCER_RECORD_RETRIES`, default 8 attempts,
  exponential backoff doubling from 100ms). Only after those retries are
  exhausted, or on a Kafka-classified non-retryable error, is the copy
  dead-lettered to the tenant DLQ — tagged with `x-sukko-failure-kind`
  (`retries_exhausted` / `non_retryable` / `not_attempted`) and
  `x-sukko-failure-cause` (the terminal error) so the failed artifact carries
  its own triage — and counted (`ws_routing_fanout_write_failed_total`,
  `ws_routing_fanout_dropped_total`, `ws_routing_dlq_dropped_total`). franz-go
  exposes no per-record attempt count, so the kind classification is recorded
  rather than an invented number. On graceful shutdown the queued egress
  backlog is drained, bounded by `KAFKA_PRODUCER_SHUTDOWN_TIMEOUT` (§VII —
  shutdown never hangs); anything undrained at the bound is counted and
  logged. Surfaced, never silent, never failing the publish.
- **Role disjointness is enforced at write time**: no suffix may be both an
  ingress topic and an egress topic anywhere in a tenant's rule set (either
  overlap would pull an egress topic into the consume set); `dead-letter` is
  reserved entirely and `default` is reserved as an egress destination.
- **Egress topics are Pro** (`license.EgressTopics`, §XIII), gated at the
  provisioning write paths only when a rule actually carries egress topics.
  Ingress-only rules remain Community (ADR-0014) — the unlicensed benchmark
  stack (`ingress_topic: "default"`, no egress) is unaffected.

## Consequences

- A subscriber receives exactly ONE copy of a published message regardless of
  how many egress topics the matched rule lists; `mid` dedup advice in the
  AsyncAPI becomes universally true.
- **What egress guarantees — and where it can still lose a copy.** Sukko makes
  a best effort to write every egress copy: the client's bounded retry (8
  attempts, exponential backoff from 100ms), then dead-lettering with reason,
  failure-kind, and cause headers, with counters on every loss leg. That is
  the stronger of the two industry shapes — realtime platforms (Ably webhooks,
  Pusher, PubNub, Centrifugo, Azure Web PubSub) do bounded retry then discard
  with no DLQ at all; cloud messaging infrastructure (SNS, EventBridge, GCP
  Pub/Sub) does bounded retry then DLQ — Sukko does retry *and* DLQ. Copies
  can still be lost: a full DLQ queue is terminal loss
  (`ws_routing_dlq_dropped_total`), a shutdown-drain timeout is terminal loss
  (`ws_routing_fanout_dropped_total`), a crash between ack and egress write
  loses the copy with no trace, and a client retry after a lost ack can
  duplicate there. This is within the published norm — no surveyed vendor
  guarantees its dead-letter destination (AWS ships
  `InvocationsFailedToBeSentToDlq`; Google documents Pub/Sub dead-lettering as
  "best effort" and that dead-letter topics without a subscription lose
  messages), and none sells durable or transactional egress at any price. In
  *this* system egress is deliberately weaker than subscriber delivery (the
  consume path re-reads the ingress log; egress is a one-shot side write) —
  claimed for Sukko specifically, not as an industry law. For audit-grade
  needs, derive the copy downstream from the ingress topic (e.g. a Kafka
  Connect sink or Streams job with `read_committed`), where the ingress topic
  is the single source of truth and the publish path pays nothing. An egress
  topic never holds a copy of an un-acked message.
- Kafka transactions were rejected deliberately: they would force
  `read_committed` (Last-Stable-Offset latency) onto the delivery hot path
  (§VII) and onto external egress consumers we do not control, serialize all
  publishes through one in-flight transaction per producer (or a
  transactional-producer pool with coordinator round-trips), defeat the
  fan-out worker pool, amortize wrongly at per-message granularity, and add
  `transactional.id` zombie-fencing lifecycle across replicas.
- Changing a rule's ingress topic re-points history/replay for matching
  channels: records in the old topic keep their mids but are no longer
  reachable through `ChannelTopic()`, and outstanding replay cursors (`pos`)
  for those channels become invalid. Records externally produced into any
  topic other than the resolved ingress/default topic are delivered live but
  are not replayable.
- A pre-split provisioning peer streaming the old `topics` field to a
  post-split ws-server yields rules without an ingress topic; they are
  skipped fail-closed (publish → 409 `PUBLISH_NOT_ROUTABLE`) until both sides
  are upgraded. Released pre-split images interoperate only within their own
  release.
- The DLQ-as-rule-topic hole is closed: a rule could previously name the
  `dead-letter` suffix (it exists, so `TopicExists` passed), putting the DLQ
  into the consume set and delivering dead-lettered records to subscribers.

## Alternatives rejected

- **Two separate resources (routing rules + egress rules)** — cleanest
  conceptual split, but new CRUD/quota/stream surface and a second pattern
  evaluation for a distinction two field names already express (§XV).
- **Positional convention (first topic = delivery)** — zero schema change but
  implicit mode detection, forbidden by §XV.
- **Topic-level purpose via the ADR-0006 Phase 2 topics table** — most
  principled, but blocks this correctness fix on an unbuilt slice.
- **Kafka-transactional fan-out** — see Consequences; hot-path and
  operational costs for a guarantee the egress use case does not need, and
  that a downstream derivation provides better.
