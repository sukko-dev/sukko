# ADR-0006: The consumable topic universe derives from provisioning-owned provisioned topics, not routing rules

**Status**: Accepted
**Date**: 2026-08-30
**Ticket**: — (PR #249's CI red surfaced it)

## Context

The edition-model decision made the Kafka backend (ingest→consume→fan-out) a Community feature — the free
tier must be able to ingest. But the topic registry that drives ws-server's physical
topic creation (`ensureTopicsExist`) and consumption is built by provisioning's
`loadTopicsUpdate` **exclusively from routing rules** (an implementation
choice): a tenant with no rules contributes zero topics. Routing rules are Pro-gated
(`ChannelTopicRouting`), so a Community tenant's topics are never created and never
consumed — Community+kafka ingest is structurally dead, contradicting that decision, the
published editions matrix (`max_topics_per_tenant: 10` for Community), and the shipped
pricing-page/docs claims. The gap was caught by the community-ingest e2e (PR #249):
the tester's publish waited 45s and the tenant's default topic never existed. Two
adjacent defects: provisioning's Kafka admin is a noop whose in-memory topics map is
the only "provisioned topics" record (volatile across restarts — `TOPIC_NOT_PROVISIONED`
validation drifts), and the tenant-create saga creates default/DLQ outside any
API-visible flow (an implicit second layer).

## Decision

**Provisioning is the sole authority on the topic universe; the consume/create set
derives from provisioned topics, with routing rules only ever referencing them.**

1. **Registry (Phase 1)**: per active tenant, the consume/create set =
   `{deterministic default topic}` ∪ `{suffixes referenced by routing rules}`. The DLQ
   is physically created but excluded from consumption and from the quota
   (infrastructure, not a user topic). Community ingest works out of the box via the
   default topic; Pro/Enterprise behavior is unchanged plus restart-safety (the set no
   longer depends on the volatile noop map).
2. **First-class topics (Phase 2)**: a durable topics table becomes the provisioned-
   topics source (fixing `TOPIC_NOT_PROVISIONED` validation across restarts);
   `POST/GET/DELETE /api/v1/tenants/{t}/topics` carries **no edition gate** — the wall
   is the ratified per-tenant quota (`MaxTopics`: Community 10 / Pro 50 / Enterprise
   unlimited), enforced through the existing seeded quota and overridable per plan via
   the license key (`resolveLimits`), consistent with connection banding. The
   tenant-create saga is unified onto the same internal creation path (`default` is
   simply the first topic created through it — no side layer). Admin UI stays Pro
   (already gated); Community drives the API/CLI directly.
3. **The Pro walls are untouched**: client/REST publish into kafka still requires
   routing rules (409 `PUBLISH_NOT_ROUTABLE` on Community); direct-to-Kafka produce
   requires broker credentials (operator-only — that IS the free ingest promise, not a
   leak); capacity caps (500/1/3) unchanged.

## Consequences

- The free-tier benchmark story becomes true in code: free-tier ingest delivers, provable
  by the `community-kafka` cell's `REQUIRE_PASS` (PR #249).
- Ingest tenant-isolation holds structurally: sukko only ever consumes
  provisioning-opened, tenant-scoped, namespace-prefixed topics — records dropped into
  arbitrary topics are never read.
- Phase 2 carries the full §XVI/§XVII ripple: OpenAPI, sukko-cli topic commands, docs,
  tester suite, editions/limits documentation.
- Coupling created: ws-server remains the mechanical executor (creates/consumes only
  what the registry streams say) — one authority, one enforcement arm, mirroring the
  license-key pattern.

## Alternatives rejected

- **Keep rules-derived registry; seed a default catch-all rule at tenant creation** —
  makes Community client-publish routable, silently opening the Pro publish path and
  breaking the tested 409 wall (or demands a second publish gate — §XV).
- **Revert to "kafka needs Pro"** — guts the benchmark/pricing story the
  edition model is built on.
- **Community limited to the default topic only (defer everything)** — silently
  undercuts the published `max_topics_per_tenant: 10` Community limit; a marketed
  limit nothing lets you use is a broken promise in the other direction.
- **Tighten Community's topic cap to 3** — re-litigates a shipped, published number
  without new information; topic count is not a revenue wall (topics consume the
  operator's own broker resources; publish and capacity walls are unaffected).
