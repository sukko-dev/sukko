# ADR-0002: Bind tenant-scoped JWT auth to the signing key's tenant UUID, default-secure

**Status**: Accepted
**Date**: 2026-07-08
**Ticket**: #158 / fix/jwt-tenant-binding

## Context

A shared JWT core (`jwt_core.go`) validated token signatures but discarded the signing key's owning tenant — the keyfunc returned only the public key, never `key.TenantID`. The JWT `tenant_id` claim is the slug, which is renameable, so binding on it races renames. Worse, the two key registries exposed *different* tenant-id schemes: the gateway's `StreamKeyRegistry` returned a UUID, while provisioning's `DBKeyRegistry` returned a slug (`SELECT t.slug`). Without binding, a token signed by one tenant's key could carry another tenant's slug claim and be accepted — a cross-tenant acceptance hole. This is the first brick of the broader identity-unification effort (see ADR-0001).

## Decision

Tenant-scoped auth binds the resolved claim identity to the signing key's tenant UUID. Provisioning's `DBKeyRegistry` is normalized (`SELECT t.slug` → `t.id`) so both registries expose `KeyInfo.TenantID` as a UUID; the shared core then always compares a resolved-claim-UUID against the key's UUID. `ValidateOpts` gains an injected `TenantResolver` (`ResolveTenantUUID(ctx, slug) (uuid, err)`) and a `DisableTenantBinding bool` whose zero value leaves binding **enabled** (default-secure). After the validity check, when binding is enabled, the core resolves the claim slug to a UUID and rejects unless it equals the captured key UUID. Binding fails closed on a nil resolver, an empty key UUID, or an unresolvable slug. The gateway resolver is backed by a slug→UUID map on the tenant-config stream (readiness-gated); the provisioning resolver is grace-aware (resolves `previous_slug` within the rename hold window). Admin auth builds its own opts and must set `DisableTenantBinding: true`.

## Consequences

A single chokepoint in the shared core covers gateway WS auth (JWT-only, JWT+API-key, auth-refresh) and provisioning tenant middleware — defense in depth, since even callers that bypass the `MultiTenantValidator` wrapper are covered. Default-secure means a forgetful new tenant-scoped caller fails **closed**, not open; admin, conversely, must explicitly disable binding or its auth breaks loudly in tests (never a silent tenant hole). Creates a deploy-sequencing coupling: provisioning must roll out before any binding-enabled gateway/ws-server, and the gateway must treat a tenant-config snapshot whose tenants have empty `tenant_uuid` as "provisioning not yet upgraded" — holding readiness degraded (503) rather than serving-and-rejecting — turning a mis-ordered rollout into a bounded 503 window. Adds an internal-only proto field (`TenantConfig.tenant_uuid`); no token, client, or client-facing error change (reuses `ErrTenantMismatch` → 403).

## Alternatives rejected

- **Scheme-agnostic resolver** (returns UUID for gateway, current-slug for provisioning to match each registry as-is) — the same interface returning different schemes per service is a smell, and provisioning would bind on the mutable slug with a rename race.
- **UUID in the JWT claim** — forces token-minting changes across tester/clients/docs (cross-repo) and requires clients to know their UUID.
- **Bind in the `MultiTenantValidator` wrapper** — misses the shared-core defense-in-depth, and admin bypasses the wrapper.
- **`BindTenantID bool` with true = enabled** — a fail-open default; a forgotten flag would silently disable tenant binding.
