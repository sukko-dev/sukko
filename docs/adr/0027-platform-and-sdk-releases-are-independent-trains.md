# ADR-0027: Platform and SDK releases ship on independent version trains

**Status**: Accepted
**Date**: 2026-09-28
**Ticket**: —

## Context

The platform (ws-server, ws-gateway, provisioning) releases as a single
`vMAJOR.MINOR.PATCH` tag that builds and pins immutable images (currently
v1.0.5). Three official client SDKs exist — Go (`sukko-go`), JS/TS
(`@sukko/*`), and Python (`sukko-py`) — but an audit (2026-09-28) found **none
are actually released**: Go is unmerged on a feature branch with no tag, the JS
packages sit at 1.0.0 but are unpublished (npm E404), and the Python package is
0.1.0 with no PyPI publish pipeline. All three vendor a **stale wire contract**
(1.4.0 / mixed) against platform `main` at AsyncAPI **1.4.3**.

Bundling "make everything production-ready" into one release would couple the
platform's patch cadence to multi-repo, multi-week SDK work (release
engineering, contract re-vendor, per-SDK functional gaps), blocking platform
fixes behind SDK readiness and vice-versa.

## Decision

**Platform releases and SDK releases are independent version trains.**

- **Platform** ships `vX.Y.Z` (images) carrying platform, gateway, provisioning,
  and tester changes.
- **Each SDK** releases on its own per-repo version and registry: `sukko-go`
  module tags, `@sukko/*` on npm, `sukko-py` on PyPI.
- The **only** shared coupling is the wire contract (AsyncAPI/OpenAPI). The
  platform is the single source of truth (currently AsyncAPI 1.4.3, OpenAPI
  1.0.1); each SDK vendors a checksum-pinned copy (per ADR-0003/0023) and
  re-vendors on its own cadence when it adopts a new contract version.

Concretely: **v1.0.6 (platform)** scope = SSE platform hardening (recovery /
`Last-Event-ID` test coverage + SSE in the fault matrix), mobile-push parking
(ADR-0028), WebPush confirmation, release hygiene (history-writer flaky tests),
and the cheap tester wins (webhooks suite, edition-cap topics e2e). **SDK GA** is
a separate, tracked milestone: per-SDK release pipelines, contract re-vendor to
1.4.3, JS WebSocket `?api_key=`/header auth + 1008-retryable, Python first-class
SSE selector + resume plumbing, and Go SSE completion (the two `TODO(S3b)`
blockers) + merge to main.

## Consequences

- The platform can cut patch/minor releases without waiting on SDK work; an SDK
  bug never blocks a platform release.
- SDKs can iterate and publish on their own schedule against a pinned contract.
- Contract changes become the coordination point: a platform contract bump
  (version + content) obliges each SDK to re-vendor before it claims that
  version — tracked per SDK, not gated in the platform release.
- SDK GA requires per-ecosystem release infrastructure (npm publish, PyPI
  publish, Go tag/merge) that does not yet exist — this is the first SDK-GA work
  item, not a platform-release blocker.

## Alternatives rejected

- **One unified release** ("v1.0.6 makes platform + all 3 SDKs production-ready"):
  couples independent cadences, blocks the platform on multi-repo SDK work, and
  makes any single SDK's slip a platform-release slip.
- **Monorepo the SDKs into the platform**: the SDKs are separate repos with
  separate language ecosystems, tooling, and release registries; a monorepo
  would not remove the per-ecosystem publish work and would entangle unrelated
  CI.
