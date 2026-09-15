# ADR-0009: Edition boundaries — the data path is Community; transports and push are edition-gated

**Status**: Accepted
**Date**: 2026-09-05 (records a decision made 2026-08; first published here)
**Ticket**: —

## Context

The platform's headline performance claims (ingest burst delivery, gap recovery
under process kill) must be publicly reproducible: a benchmark that requires a
paid licence is not evidence. At the same time, editions need a real wall —
something that distinguishes the paid tiers beyond a licence string.

## Decision

- **The data path is Community** (available on every edition, license-free):
  the Kafka message backend, message history, live gap recovery, and REST
  publish. A Community stack can ingest from Kafka and prove the full
  ingest→fan-out→recovery pipeline.
- **Edition-gated features** (among others — `features.go` is exhaustive):
  SSE transport (Pro), Web Push notifications (Pro), FCM/APNs mobile push
  (Enterprise), routing rules (Pro), analytics push (Pro).
- **Capacity caps are the tier wall**: per-edition connection/throughput/topic
  limits, not data-path feature removal.
- `internal/shared/license/features.go` is the single source of truth for the
  feature→edition matrix; every implemented gated feature has an
  `EditionHasFeature()` check at its access boundary (§XIII).
- Client publish into the Kafka backend still requires provisioned routing:
  on Community/kafka without routing rules, publish returns
  409 `PUBLISH_NOT_ROUTABLE` — the data path being free does not open the
  Pro publish-routing path.

## Consequences

- The public benchmark story is true in code: the free tier delivers and
  recovers, provably, in CI (the editions×backends grid runs Community/kafka
  cells as REQUIRE_PASS).
- Edition enforcement concentrates in capacity limits and gate checks, both
  covered by the edition-limits e2e suite on every edition.
- Any future feature must declare its edition in `features.go` first; an
  implemented feature without a gate check is a security bug, not an oversight.

## Alternatives rejected

- **Kafka backend requires Pro** — makes the benchmark claims unreproducible
  on the free tier, gutting the distribution strategy the editions serve.
- **Everything free, paid = support only** — leaves no product wall; capacity
  caps alone proved insufficient to differentiate tiers.
- **Per-feature à-la-carte licensing** — combinatorial gate surface and test
  matrix for no user-visible benefit (§XV).
