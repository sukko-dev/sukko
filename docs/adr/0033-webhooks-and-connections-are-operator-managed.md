# ADR-0033: Webhooks and connections are operator-managed, like every other tenant resource

**Status**: Accepted
**Date**: 2026-10-03

## Context

The webhooks and connections management APIs behaved differently from every other
tenant-scoped resource, and the difference was an accident, not a design.

The provisioning API is an **operator control plane**. There are only two actor
roles — `admin` and `system` — and no `tenant` role. Every tenant-scoped write
(keys, API keys, routing rules, topics, quotas, channel rules, tenant lifecycle)
is gated by `RequireRole("admin","system")` and resolves its tenant from the URL
slug (`chi.URLParam("tenantSlug")` → `GetTenantBySlug`), so an operator token
manages it on the tenant's behalf. Tenants do not self-manage anything through
this API — there is no tenant dashboard. Investigation confirmed this: `sukko-cli`
has no webhook/connections commands, and `sukko-portal` touches "webhooks" only for
its own Stripe billing and marketing copy — neither calls
`/api/v1/tenants/{slug}/webhooks` or `/connections`.

Two resources diverged. The webhook and connections handlers alone resolve their
tenant from a UUID stashed in the request context by `RequireTenant`
(`getTenantUUIDFromContext`), and `RequireTenant`'s admin/system branch returned
early **without** stashing it ("bypass the tenant ownership check"). The result was
inconsistent in both directions:

- the operator (who manages every other resource) got `401 "missing tenant
  context"` on webhooks/connections — the only resources it could not touch; and
- a plain tenant data-plane token (no admin role) could reach them, the lone
  self-service path in an operator-only API.

Because `sukko-cli` has no webhook commands either, webhooks/connections had **no
working operator management path at all** — the feature shipped unconfigurable
except through a tenant-token path that no product surface drives. (The e2e tester
is the only caller, and it used a tenant token precisely because the admin path
401'd — PR #43.)

Audit log, despite sharing the "UUID-keyed records" comment, is **not** affected:
`GetAuditLog` resolves the tenant by slug and already fails for deleted tenants at
the service layer. The `provisioning:get audit log` skip on the Pro e2e cells is
the Enterprise edition gate, not this issue.

## Decision

Make webhooks and connections operator-managed, uniform with every other tenant
resource.

- **`RequireTenant` admin/system branch resolves and stashes the tenant UUID.** It
  looks the tenant up by the URL slug and stashes its identity before `next`,
  skipping only the *ownership* check (an operator may act on any tenant), not the
  identity *resolution*. A missing/soft-deleted tenant now returns 404 at the
  middleware (a non-regression: every admin handler already resolves via
  `GetBySlug`, which filters `deleted_at IS NULL`, and already fails for such
  tenants at the service layer). The non-admin branch is unchanged.
- **The webhooks and connections route groups require `admin`/`system`.** The whole
  group (reads included) is gated, matching keys/topics; `RequireFeature` stays
  *before* `RequireRole` in the chain so a Community-edition operator probe still
  gets `403 EDITION_LIMIT`, not `INSUFFICIENT_ROLE`.
- **The vestigial tenant-token path is removed.** No product surface used it, and
  keeping it would leave webhooks/connections the only self-service resource in an
  operator-only API.

## Consequences

- Operators can manage webhooks and connections through the provisioning API,
  consistent with every other resource; tenant tokens now get `403
  INSUFFICIENT_ROLE`.
- Contract change (§XVII): the provisioning OpenAPI security for the webhooks and
  connections paths changes (admin role required) and its version is bumped in the
  same PR.
- The e2e webhooks suite reverts its webhook CRUD to admin-token auth (the PR #43
  tenant-token fix matched the server as it *was*; this changes the server to what
  it *should be*). The `pro-webhooks` grid cell is re-run on the branch to prove
  the flip.
- Follow-up (§XVI, not this PR): `sukko-cli` should gain `webhook`/`connections`
  commands so operators have a first-class management path beyond the raw API.

## Alternatives rejected

- **Keep webhooks/connections tenant-self-service (fix only the admin lockout).**
  Rejected: no tenant surface exists to use it, and it would leave them the only
  self-service resource — recreating the inconsistency from the other side.
- **Gate only writes, leave reads tenant-open (the keys/topics shape).** Rejected:
  there is no tenant consumer for the reads either, so a half-gated resource adds
  asymmetry for no benefit.
- **Stamp the tenant on the data path instead.** Out of scope — that was the
  separate webhook-delivery keying fix (ADR-0032); this ADR is about the management
  API's authorization.
