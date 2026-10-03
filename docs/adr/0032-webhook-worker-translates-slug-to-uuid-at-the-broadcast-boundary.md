# ADR-0032: The webhook-worker translates tenant slug → UUID at the broadcast boundary

**Status**: Accepted
**Date**: 2026-10-03

## Context

Webhook delivery over the broadcast bus never delivered — for any tenant, any
webhook — a latent bug invisible until the `pro-webhooks` e2e cell first exercised
the path end-to-end (sukko PR #43).

Two tenant identifiers coexist in the platform. The **data path** names a tenant by
its mutable **slug**: Kafka topics are `namespace.slug.suffix`, and ws-server
broadcasts to the Valkey channel `ws.broadcast.<slug>` with
`broadcast.Message.TenantID = slug` (ws-server `resolveTenant`; logged everywhere as
`LogKeyTenantSlug`). The **control plane** names a tenant by its immutable **UUID**:
the webhook-worker's gRPC contract is UUID-based
(`ListWebhooksForTenantRequest.tenant_uuid`, `ListWebhookTenants → tenant_uuids`),
the Valkey cache-invalidation signal carries the UUID (provisioning publishes
`getTenantUUIDFromContext`), and the worker's `WebhookCache` is keyed by UUID.

The worker's `eventConsumer` looked up `Cache.Get(msg.TenantID)` with the broadcast
slug against a UUID-keyed map → miss on every broadcast → zero delivery tasks
enqueued (confirmed: a degraded webhook's status never left `enabled`). The worker
holds no slug↔UUID mapping of its own.

## Decision

Keep the webhook-worker's cache and entire control-plane contract **UUID-keyed**, and
translate **slug → UUID once, at the single point where the data path meets the
control plane**: the worker's broadcast consumer.

- The `ListWebhooksForTenantResponse` gains an additive `tenant_slug` field, filled by
  provisioning (it resolves UUID → slug from the tenant store). This is the one place
  both cache hydration (`Hydrate`, by UUID) and per-tenant refresh (`Refresh`, by UUID)
  pass through, so the worker learns the pair on every fetch — including after a cold
  restart, which an invalidation-payload-only scheme would miss.
- `WebhookCache` maintains a `slugToUUID` index alongside its UUID-keyed records,
  updated under the existing write lock in `fetchAndStore` (deleting any stale old-slug
  entry when a tenant's slug changed — rename-safe). A new `GetBySlug(slug)` resolves
  index → records in a single read lock.
- `eventConsumer` calls `Cache.GetBySlug(msg.TenantID)` instead of `Cache.Get`. The
  delivery task still carries the record's UUID (`rec.TenantID`), so status updates and
  delivery recording stay UUID-correct.

## Consequences

- One component changes (the worker) plus one additive proto field and its
  provisioning filler; every control-plane flow (invalidation, Hydrate, Refresh, status
  updates, `RecordDelivery`, `TestDeliver`/`GetByID`) is unchanged and stays UUID-keyed.
- The identifier boundary is explicit and localized: slug = data-path name, UUID =
  control-plane identity, translation lives where the two meet.
- Proto is a contract artifact (§XVII): `buf generate` + committed codegen + `buf lint`
  land in the same change. The gRPC is internal service-to-service, so there is no
  OpenAPI/AsyncAPI, docs, or CLI impact.
- **Known residual (not fixed here):** a slug rename does not emit a webhook cache
  invalidation, so a renamed tenant's `slugToUUID` entry is stale until the next TTL
  refresh. The window is TTL-bounded and documented in a code comment; closing it is a
  separate change if it ever matters. A stale entry is bounded to at most one cache-TTL
  cycle under normal operation, whereas `SLUG_RENAME_TOPIC_HOLD_PERIOD` (default 168h)
  keeps a released slug unclaimable far longer — so the theoretical cross-tenant
  mis-route (tenant B reclaims A's old slug while the worker still maps it to A) requires
  a continuous refresh outage exceeding the hold period, the same whole-cache-stale class
  of prolonged-outage failure that predates this change.
- A transient `GetSlugByUUID` failure for a live tenant fails the gRPC response rather than
  returning an empty slug: an empty slug would make `fetchAndStore` evict a healthy mapping,
  so the worker instead keeps its last-known-good mapping (its `Refresh` leaves the cache
  untouched on error). Only a genuine `ErrTenantNotFound` yields the empty-slug path (§IV:
  degrade to stale, never to silent loss).

## Alternatives rejected

- **Stamp the UUID on the broadcast `Message.TenantID`.** The broadcast slug is
  load-bearing: it names the Valkey channel `ws.broadcast.<slug>` that every ws-server
  shard and the SSE path subscribe to. Changing it rewires the entire message pipeline —
  enormous blast radius for a webhook-only defect.
- **Key the worker cache (and its gRPC contract) by slug.** Slugs are mutable (the slug
  rename saga, hold periods, `PreviousSlug` grace). Keying a control-plane store by a
  mutable identifier imports the rename problem into every webhook RPC and cache flow.
- **Carry the slug in the webhook record and key the cache by slug (no UUID index).**
  Same mutable-key problem, and tenant-deletion (zero records returned) leaves no slug to
  evict the stale entry — the UUID index avoids both.
