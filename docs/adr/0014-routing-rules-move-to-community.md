# ADR-0014: Routing rules move to Community; the quota is the wall

**Status**: Accepted
**Date**: 2026-09-11
**Ticket**: —

## Context

ADR-0009 moved the data path to Community — the Kafka backend, message history, live
gap recovery, and REST publish — and made capacity caps the tier wall. It
deliberately left routing rules (`ChannelTopicRouting`) on Pro, and scoped
Community+Kafka to the **ingest** path: produce directly to the tenant's topic, then
fan out, with history and gap recovery on top. Client publish into Kafka stayed Pro,
returning 409 `PUBLISH_NOT_ROUTABLE` on Community, because #179 removed the
channel→topic convention fallback and made routing rules the sole mapping.

Two things have changed since.

First, ADR-0013 adopts the agent delivery plane as core scope, and its provisioning
model depends on routing rules as the ordinary way a tenant is set up: wildcard rules
per event class, mapping a family of channels onto a topic. Under the current
boundary, a self-hosted evaluator on the free tier can enable the durable Kafka
backend but cannot configure the mapping that makes publishing into it work. The wall
sits directly in front of the at-least-once property that motivates choosing Sukko in
the first place — and self-hosted, policy-constrained deployments are exactly the
population that evaluates on Community before there is any commercial conversation.

Second, the edition limits already anticipate Community routing rules.
`limits.go` carries `MaxRoutingRulesPerTenant` of **10 for Community**, 100 for Pro,
and unlimited for Enterprise. That Community cap is currently unreachable: the
feature gate rejects the request before any count is evaluated. The capacity wall
ADR-0009 asked for is already defined and simply never runs.

## Decision

- **`ChannelTopicRouting` moves from Pro to Community.** Routing rules are part of
  the data path, which ADR-0009 already established belongs to the free tier.
- **The quota is the wall**, per ADR-0009: Community 10 routing rules per tenant,
  Pro 100, Enterprise unlimited. These values are unchanged — only their
  reachability changes.
- **The #179 decision stands: no convention fallback is re-added.** Routing rules
  remain the sole channel→topic mapping. A channel with no matching rule still fails
  loudly with 409 `PUBLISH_NOT_ROUTABLE`. This ADR changes who may *create* a rule,
  not how routing works.
- **The now-unreachable gate checks are removed**, following the pattern the ADR-0009
  remap established for the features it moved ("the now-unreachable startup gates,
  handler edition denials, and their orphaned metrics/labels/constants are removed").
  A gate that no edition can fail is dead code.
- **Pricing bands do not change.** The commercial tiers are differentiated by
  capacity, transports, push, and support — not by withholding the mapping that makes
  the free data path usable.

## Consequences

- Community+Kafka becomes coherent end to end: a free stack can provision a rule,
  publish through it, and exercise ingest → fan-out → history → gap recovery without
  a licence. The public benchmark can run the client-publish path on the free tier,
  strengthening ADR-0009's reproducibility argument rather than relying on a
  direct-to-Kafka workaround.
- **A previously unreachable error becomes reachable on Community.** The
  service-level over-count denial `403 EDITION_LIMIT_ROUTING_RULES_PER_TENANT` fires
  at 10 rules on Community. On Pro it stays unreachable, because the configured
  `MAX_ROUTING_RULES_PER_TENANT` default (100) is not greater than the Pro edition
  limit, so the config validator returns `400 TOO_MANY_ROUTING_RULES` first. Tests
  asserting the over-count boundary MUST discriminate on the exact `code` per
  edition, and MUST NOT substring-match — `EDITION_LIMIT` is a prefix of
  `EDITION_LIMIT_ROUTING_RULES_PER_TENANT`.
- The edition-limits e2e suite's Community expectation inverts from "feature denied"
  to "succeeds, then caps at 10". That path now needs the same topic provisioning the
  paid path needs, or it fails for a setup reason while appearing to test a gate.
- Pro loses a feature-shaped differentiator. This is the intended direction of
  ADR-0009: tiers are walls of capacity and operational surface, not of data-path
  capability.
- Edition surfaces regenerate from `features.go` — the published editions comparison
  and any prose asserting routing rules are Pro must follow in the same change
  (§XVI, §XVII).
- Released v1.0.0 images gate routing rules at Pro permanently. Any consumer that
  needs this behaviour — including the public benchmark, which pins released
  digests — requires a subsequent tagged release.

## Cross-repo impact (§XVI)

This change moves an entry in `features.go` and makes a new API error code
(`EDITION_LIMIT_ROUTING_RULES_PER_TENANT`) reachable on two endpoints, so companion
changes are required and are **deferred, not waived**:

- **`../sukko-docs`** — the editions comparison page auto-generates from `features.go` and
  needs no hand edit, but hand-authored guide prose stating that routing rules require Pro
  does, as does the API reference for the new 403 on `POST`/`PUT
  /api/v1/tenants/{tenantSlug}/routing-rules`.
- **`../sukko-cli`** — any surface annotating routing-rule commands as Pro (e.g. the
  edition matrix) must be corrected. Per repo convention CLI changes land on their own
  branch under the CLI's own constitution.

Neither is blocking for the platform change, but both MUST land before the next release
that ships this behaviour, and each MUST reference this ADR.

## Alternatives rejected

- **Keep routing rules on Pro; licence the benchmark stack.** Removes the immediate
  blocker but leaves the incoherence: a free tier that can run the durable backend
  but not configure it, with a capacity cap defined for a feature it cannot reach.
- **Re-add a channel→topic convention fallback** so no rule is needed on Community.
  Reverses the ratified #179 decision and reintroduces implicit mapping (§XV: silent
  fallbacks are forbidden), delivering the same commercial outcome through a worse
  mechanism.
- **Split a separate "publish routing" feature**, Community for a single rule and Pro
  beyond. Adds a gate surface and a second source of truth for the same capability
  where a count limit already expresses the wall (§XV).
- **Move routing rules to Community and raise the Community cap.** Conflates two
  decisions; the cap is a pricing lever that should move on pricing evidence, not as
  a side effect of a coherence fix.
