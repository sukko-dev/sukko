# ADR-0040: Prove SDK feature coverage with a shared scenario corpus, a contract-coverage gate, and the live e2e grid

**Status**: Accepted
**Date**: 2026-10-21
**Ticket**: —

## Context

The three client SDKs (`sukko-js`, `sukko-py`, `sukko-go`) are rigorously tested and
remarkably at parity in feature surface — both clients (async/sync), JWT + API-key auth
(header *and* query placement), subscribe/unsubscribe with grant-diff, WS + REST publish,
SSE transport, reconnect/backoff, the full recovery engine (reconnect-replay, gap→replay,
`no_replay`/`replay_truncated`), history, push, channels, close codes, error taxonomy,
backpressure. But every test is hermetic: unit tests over fakes, a static contract-coverage
check, and a checksum-pinned recovery-vector corpus replayed through the pure state machines.

Two gaps follow, and they are exactly the goal this ADR serves — **prove each SDK supports
Sukko's features, that they actually work, and that they keep up with Sukko's changes**:

1. **Nothing is ever run against a real Sukko.** No SDK boots or connects to a live
   gateway+server. `sukko-py` has a default-skipped, operator-booted smoke (its two hardest
   cases stubbed); `sukko-go`'s `integration` job is a stub that boots nothing; `sukko-js`
   has none. So "it works against a real platform" is unproven.
2. **The "keep up" gate is inconsistent and already drifting.** All three have a static
   message-*type* coverage check, but `sukko-go` vendors the contract checksum-pinned while
   `sukko-py` reads a sibling checkout and silently skips when absent, and `sukko-js` vendors
   a copy pinned at **AsyncAPI 1.4.0** while the platform is at **1.4.3** — three minor
   versions stale, and nothing flagged it.

The platform *already* has the comprehensive live battery (`ws/cmd/tester`, 22 validate
suites, an 8-cell compose grid in `taskfiles/e2e.yml`) — but it drives the stack with the
tester's own in-process clients, **not the SDKs**. So the platform proves the platform works;
it does not prove the SDKs work against it.

A language-neutral conformance corpus already exists (`ws/docs/conformance`, ADR-0023),
consumed by each SDK as a checksum-pinned copy — but it holds only the `recovery` machine's
four vectors; the `auth` and `subscriptions` machines are declared and empty. (Note: the
`sukko-js` `packages/sdk/contract/README.md` wrongly claims to be the canonical corpus home;
`ws/docs/conformance` is canonical per ADR-0023 — the sync direction requires a single source.)

### Prior art (§XI)

- **AWS Smithy protocol-compliance tests** — the closest analog: shared declarative test cases
  defined once in the spec, consumed per-language (codegen or runtime load) and run
  idiomatically, each case carrying a stable ID so a language can mark cases it does not yet
  support. This validates the "shared corpus, run per-SDK" spine and the declared-unsupported
  mechanism.
- **gRPC / Connect conformance suite** — a reference client/server with the implementation
  under test driven against the reference; powerful (it surfaced 22 bugs in gRPC itself) but a
  large cross-language driver build.
- **OpenTelemetry** — uses a *compliance matrix* (feature × language) as the visible
  conformance artifact; worth adopting for "prove each SDK supports every feature at a glance."
- **stripe-mock** — a stateless mock server is fine for "did you call the right endpoint" but
  is explicitly *not* for stateful flows ("test real integration against testmode"). Sukko's
  Kafka-backed replay, SSE recovery and delivery semantics are exactly the stateful surface, so
  a mock cannot replace a live stack.

**Where Sukko deviates and why:** unlike Smithy / stripe-mock (stateless serialization checks),
Sukko must prove behavior against a *real stateful* platform (Kafka replay, Valkey broadcast,
provisioning). So the design carries both a hermetic contract/corpus layer *and* a live layer —
the live layer is non-negotiable and is where Sukko goes beyond the prior-art norm.

## Decision

SDK conformance is a **three-layer** architecture. The spine is a **shared, declarative corpus
run per-SDK** (the Smithy model), with a thin per-SDK idiomatic layer for the few things a
language-neutral manifest cannot express.

### Layer L0 — contract-coverage gate ("keep up with changes")

Each SDK vendors Sukko's `client-ws.asyncapi.yaml` + `gateway.openapi.yaml`, **checksum-pinned**
(standardize all three on this; `sukko-py`'s sibling-path-with-skip is replaced). A coverage
test asserts that every **message type, error-code enum value, close code, REST endpoint, and
SSE control frame** in the contract is either handled by the SDK or **explicitly declared
unsupported** by a stable per-item ID (Smithy-style; mirrors the grid's `ALLOWED_SKIPS`). An
auto-generated **coverage matrix** (feature × SDK, OTel-style) is the visible artifact that
answers "does this SDK support every feature."

The forcing function is **PR-free** (an auto-PR bot is rejected — see Alternatives): a scheduled
job in each SDK fetches the contract from a pinned Sukko ref (public raw URL), compares versions,
and **fails when upstream > vendored** (and MAY file an issue — a distinct, usually-permitted
GitHub permission). A **canary in Sukko CI** reads each SDK's vendored contract version and warns
when any lags — so drift surfaces both when touching the SDK and when working in Sukko.

### Layer Tier1 — shared conformance corpus ("they all behave identically")

Language-neutral scenario vectors in `ws/docs/conformance` (the single canonical home;
`sukko-js`'s conflicting claim is corrected), vendored checksum-pinned by each SDK and replayed
through its **pure state machines**. Extend from recovery-only by authoring the declared-but-empty
**`auth`** (refresh/escalation single-flight) and **`subscriptions`** (grant-diff) machines —
which requires adding their **action vocabularies** to the vector schema *and* a harness binding
in each of the three SDKs (not merely "write JSON").

### Layer Tier2 — live scenarios ("they actually work")

A shared **declarative live-scenario set** in `ws/docs/conformance` (provision → act → expect),
each SDK vendoring it checksum-pinned and executing it through a **thin per-SDK runner** against a
booted stack using the SDK's *real* transports. Provisioning is **outside** the scenario format:
a shared setup step (`sukko-cli` or a small helper) provisions tenant + channel rules + routing +
key + JWT and emits a fixture (`ws_url`, tenant, key, token); scenarios are act→expect only, so a
runner is not a provisioning client ×3. A **thin per-SDK idiomatic layer** covers what a neutral
manifest cannot: browser-vs-node WS credential placement (query vs header), framework bindings
(react/vue/svelte), node TCP backpressure.

### Topology — where each layer runs (this is also "where it goes red")

- **Each SDK's own CI:** L0 gate (hermetic) + Tier1 corpus (hermetic) + a cheap
  **Community-direct live smoke** — published GHCR images + the committed prod-signed Community
  key (`taskfiles/e2e.yml`, not a secret), no CI secrets required.
- **Sukko's existing 8-cell e2e grid:** runs the three SDK runners across the full
  edition × backend matrix with dev-signed keys built from source. This is where Pro/Enterprise
  features (SSE, push, token-revocation, webhooks, sse-recovery) are proven, because a
  Community key 403s them and published images trust only the prod signing key (so dev-minted
  Pro/Enterprise tokens do not verify). **The full live matrix lives in Sukko, not the SDK
  repos** — reusing the grid rather than rebuilding stack-boot and edition-key handling ×3.

### Options rejected for the spine

- **(A) reference-driver conformance** (drive each SDK from a Go battery via an RPC protocol) —
  the zero-drift ideal (gRPC Connect model) but a large cross-language driver build; some
  features resist a generic command interface. Recorded as a possible future north star, not
  the MVP.
- **(B) independent per-SDK live suites** — fastest to start but guarantees three-way drift in
  *what* is covered, undercutting "they all behave the same."

### Phasing

- **Phase 1 (implementation gate now): L0 + Tier1.** Hermetic, no live stack, lands "keep up with
  Sukko's changes" within days and fixes the `sukko-js` 1.4.0→1.4.3 drift as instance #1 (the
  first concrete step is to bump that vendored contract and watch the coverage test).
- **Phase 2 (architecture recorded; build re-confirmed before starting): Tier2 live** — the
  Community smoke in each SDK's CI + the full matrix in Sukko's grid.

## Consequences

**Easier:** platform contract changes surface in each SDK's CI within a cadence, PR-free; a single
coverage matrix shows feature support and drift at a glance; cross-SDK parity on stateful logic is
enforced by one shared corpus; the live proof reuses the existing grid, so there is no new
per-SDK stack-boot and no new CI secret for the full edition matrix.

**Harder / cost:** three thin runners + idiomatic layers to build and maintain; Sukko's grid gains
SDK-runner cells (platform CI time); the scenario format must express a provisioning fixture +
act/expect; the `auth`/`subscriptions` machines need new schema vocabularies + three bindings; the
SDK-CI Community smoke covers only Community-direct features — the rest wait for the grid.

**Coupling created:** Sukko's grid now depends on the three SDK runners being buildable (pin SDK
refs); the sync job depends on Sukko's contract being fetchable at a pinned public ref; the
corpus's single canonical home must stay singular.

### Cross-repo impact (§XVI / §XVII)

- **`sukko`:** canonical corpus home gains the live-scenario set + the `auth`/`subscriptions`
  vectors; grid gains SDK-runner cells; add the contract-version canary + coverage-matrix
  generation. The AsyncAPI/OpenAPI contract artifacts themselves are unchanged (this ADR consumes
  them).
- **`sukko-js` / `sukko-py` / `sukko-go`:** standardize vendored+checksum contract; extend L0
  coverage to codes/endpoints/frames + the declared-unsupported mechanism; add the PR-free sync
  job; add Tier1 `auth`/`subscriptions` bindings; (Phase 2) the thin runner + Community smoke +
  idiomatic layer. Each SDK records its own small ADR referencing this one and aligning its
  existing ADR-0003/0004; `sukko-js`'s stale "canonical here" claim is corrected. SDK changes
  follow each repo's own constitution and its per-change prior-art rule.
- **`sukko-docs`:** document the conformance/e2e model (extend `ws/docs/e2e-testing.md` or add a
  conformance page; update `static/llms.txt` if a page is added).

## Alternatives rejected

- **Option A (reference-driver conformance):** zero drift but a large cross-language driver
  protocol; deferred as a north star.
- **Option B (independent per-SDK live suites):** guarantees three-way coverage drift.
- **Mock-server-only (stripe-mock style):** stateless — cannot exercise Kafka replay / SSE
  recovery / delivery, so insufficient for "all features."
- **Auto-PR contract sync:** depends on GitHub Actions creating PRs, which the org blocks
  (demonstrated by the sdk-js changesets Version PR failing with "GitHub Actions is not
  permitted to create or approve pull requests"). Replaced by a scheduled version-compare that
  fails (and MAY file an issue).
- **A per-SDK prod-signed Enterprise CI key (repo secret) instead of grid-reuse:** viable but
  adds a prod-signed secret + rotation to each SDK repo; grid-reuse avoids it.
