# ADR-0008: Stable message identity — `mid` is identical across live, history, and replay copies

**Status**: Accepted
**Date**: 2026-09-05 (records a decision made 2026-08; first published here)
**Ticket**: —

## Context

A message can reach a client on three surfaces: live delivery, the message-history
API, and gap-recovery replay. Without a shared identity, clients cannot deduplicate
across surfaces, correlate a publish ack with the delivered copy, or attribute a
webhook delivery to the message that caused it. The replay cursor (`pos`) cannot
serve as identity: it is surface-specific, and cursor semantics (exclusive
`last_pos` vs inclusive `from_pos`) make it unfit for equality checks.

## Decision

Every message carries a stable identity field `mid`, identical on every copy of the
same message:

- **Kafka backend**: `mid` derives deterministically from record coordinates —
  `hex(fnv1a64(topic)) + "-" + partition + "-" + offset` — assigned at ingestion.
  Replay and history recompute the same value from the same record, so cross-copy
  equality is automatic and externally-produced records need nothing from
  producers.
- **Direct backend**: `mid` is minted (UUIDv7) at publish; live and history flow
  from the single broadcast message, so equality is structural.
- Clients treat `mid` as **opaque** (≤64 chars; format may vary by backend).
- `pos` remains the replay cursor; `mid` is identity — never a cursor.
- `mid` is exposed in the WebSocket publish ack, the REST publish response, and
  the webhook delivery header `X-Sukko-Message-Id`. Multi-topic fan-out acks
  (N records → N mids) ack without a mid, by design.

## Consequences

- Cross-surface deduplication and idempotency become client-side trivial:
  the e2e suites assert the identity triple (live == history == replay) as a
  hard invariant.
- Publish-ack ↔ delivery correlation and webhook attribution work without any
  additional protocol surface.
- The deterministic kafka derivation ties identity to record coordinates: a
  re-ingestion of the same payload at a new offset is a *new* message. This is
  intended — identity tracks the record, not the payload.

## Alternatives rejected

- **Producer-supplied IDs** — requires cooperation from every producer and a
  symmetric replay path; nothing is required from producers today. May be
  honored additively later.
- **Random ID per copy** — breaks cross-copy equality, the entire point.
- **`pos` as identity** — cursor semantics differ per surface; cursors are
  ordering, not identity.
