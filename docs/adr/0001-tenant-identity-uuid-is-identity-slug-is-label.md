# ADR-0001: Name every tenant reference by what its consumer keys on — UUID is identity, slug is label

**Status**: Accepted
**Date**: 2026-07-09
**Ticket**: #161 / refactor/identity-unification

## Context

Tenant identity is represented two ways: the UUID (`tenants.id`, stable primary key, survives renames) and the slug (`tenants.slug`, client-facing routing label embedded in WebSocket channels `{slug}.{topic}`, broadcast subjects, and Kafka topic names — and mutable via a rename with a grace hold). Across the internal gRPC protos the field `tenant_id` meant UUID in some messages and slug in others, with no naming signal to tell them apart. That ambiguity had already produced three bugs of the same class (cross-tenant JWT acceptance, a JWT-vs-API-key slug/UUID comparison that never matched, and a UUID-keyed map queried with a slug). The system is greenfield — no live deployments to migrate — so proto renames could be a hard, wire-compatible cutover (field numbers unchanged).

## Decision

Every field carrying a tenant reference names its identity type explicitly. The governing rule: **a field carries the identity type its consumer keys by.** `tenant_uuid` is used for stable identity, DB foreign keys, and cross-stream correlation; `tenant_slug` is used for the client-facing routing label (channels, broadcast subjects, Kafka topics). Only the authoritative projection stream (`TenantConfig`) carries both — it *is* the projection. The codebase unifies to `tenant_uuid` where the value is the identity of record (keys, api-keys, webhooks, the tenant-config identity anchor, and DB storage) and deliberately keeps `tenant_slug` on the genuinely slug-native planes (the server data plane, revocation, and the push runtime). The gateway's previously-conflated `authResult.TenantID` is split into explicitly-typed `TenantSlug` and `TenantUUID`, populated consistently regardless of auth method.

## Consequences

Eliminates the ambiguity as a *class* rather than fixing bugs one at a time; a future contributor can read a field name and know its identity type. No external contract changes — the WS channel format `{slug}.{topic}`, broadcast subjects, Kafka topic names, and the client-facing REST/WS `tenant_id` (slug) all stay. The log-field key `tenant_id` retains slug semantics everywhere (~170 sites); UUID, where logged, uses a distinct `tenant_uuid` key, so dashboards/alerts do not break. Slug-keying `push_subscriptions` means a tenant rename orphans its rows — accepted because the `channels` column is already slug-prefixed, so a rename orphans the row regardless. Requires a reverse `slugByUUID` map built from the **current slug only** (never previous-slug aliases) to keep API-key resolution deterministic, with a fail-closed sentinel on a miss.

## Alternatives rejected

- **Convert all four packages to UUID** — adds risk for no benefit on slug-consistent planes; revocation-by-UUID opens a fail-open window (entries live ≤15 min and rename hold already covers them), and the push runtime is slug-native end-to-end so a UUID request would break channel validation and Kafka-topic delivery.
- **Leave `tenant_id` meaning "whatever the author intended"** — the ambiguity had already produced three same-class bugs; status quo was the root cause.
- **Additive/deprecated proto fields for migration** — unnecessary in a greenfield system; a hard rename-in-place cutover is wire-compatible and compiler-enforced.
