# ADR-0023: SDK behavioral-parity vectors are a contract artifact, housed with the AsyncAPI

**Status**: Accepted
**Date**: 2026-09-22

## Context

Sukko ships three client SDKs (`sukko-go`, `sukko-js`, `sukko-py`) that must present the
same behavioral contract while staying idiomatic to each language. All three already vendor
the authoritative AsyncAPI (`ws/docs/asyncapi/client-ws.asyncapi.yaml`) + gateway OpenAPI and
run a contract-coverage test asserting every wire message type is modeled. That proves the
*wire model* matches; it does not prove *behavior* (recovery/auth/reconnect/subscription
FSMs) matches across SDKs — and it does not today, in either direction (sukko-js is ahead of
sukko-py on several recovery/auth behaviors; sukko-py has contract-faithful features sukko-js
lacks).

The intended mechanism for behavioral parity is a **language-neutral scenario-vector corpus**:
JSON scenarios (input frames + client actions with time-advance events → expected observable
effects) that each SDK replays through its pure state machines. This was designed in sukko-js
ADR-0002, which declared sukko-js the **canonical home** and expected sukko-py to vendor the
corpus *from sukko-js*. But no fixtures were ever authored, and that home choice conflicts with
how the SDKs are governed: sukko-go ADR-0003 and sukko-py ADR-0001 both mandate deriving from
the authoritative contract and **never from a sibling SDK** — a rule written after sukko-py's
Direct-degrade clause (copied reasoning) caused silent data loss. Vendoring the corpus from
sukko-js would make it exactly the sibling-source those ADRs forbid.

## Decision

The behavioral-parity vector corpus is a **contract artifact**: it encodes the *contract's*
expected client-observable behavior, so it is housed in the platform repository alongside the
AsyncAPI, at `ws/docs/conformance/vectors/`, and versioned with the AsyncAPI contract version.

- Each SDK **vendors** the corpus the same way it already vendors the AsyncAPI: a checksum-pinned
  copy under the SDK's test data, refreshed by an explicit copy-record-verify procedure. No SDK
  vendors from another SDK; none is the behavioral reference. When a vector and an SDK disagree,
  the vector (contract) wins and the SDK is fixed.
- Vectors specify **observable effects only** — frames the client sends and user-visible events
  it emits — never internal FSM state, and abstract over each SDK's delivery mechanics (event
  emitter vs bounded channel vs async iterator), so one corpus binds to all three idioms.
- The corpus is versioned with the contract. A server change that alters client-visible
  sequences updates the vectors in the same PR that updates the AsyncAPI (§XVII), and bumps the
  contract version so vendoring SDKs re-pin (this is why the reconciliation that added
  `subscribe_limit_exceeded`/close-code/error-status coverage bumped the AsyncAPI to v1.4.1).
- The platform repo additionally runs a **meta-test** that replays each vector's inputs against
  a live compose stack and asserts the expected frames actually occur, so the corpus is verified
  against the running server, not hand-trusted.

## Consequences

- **Easier**: all three SDKs are governed identically (vendor from the contract, never a
  sibling); a new SDK vendors one corpus; behavioral drift becomes a hermetic per-SDK test
  failure; the contract, its coverage, and its behavioral vectors live and version together.
- **Harder**: the corpus must track the contract version by hand and be re-pinned downstream on
  every bump; the meta-test couples the platform CI to a released stack; the schema must be
  expressive enough to bind three different delivery idioms (proven by a spike before the corpus
  is authored, not after).
- **Coupling**: SDKs are bound to a contract-versioned corpus, not to each other. This
  supersedes the *location* clause of sukko-js ADR-0002 (canonical-home-in-sukko-js) while
  preserving its principle (contract-derived parity vectors), and it makes sukko-js's
  "sukko-py is the behavioral reference" wording obsolete — no SDK is the reference.

## Alternatives rejected

- **Keep the corpus canonical in sukko-js (ADR-0002 as written)** — forces sukko-go and sukko-py
  to vendor from a sibling SDK, the exact rule sukko-go ADR-0003 / sukko-py ADR-0001 forbid, and
  the failure mode (copied-from-sibling drift) that already caused silent data loss.
- **A per-SDK, independently authored corpus** — three corpora drift; "parity" becomes
  unfalsifiable. One shared, contract-owned corpus is the point.
- **Live-stack conformance only (no offline vectors)** — proves each SDK works against the
  server but not that the three behave identically, and needs Docker in every SDK's fast CI.
  Kept as the complementary Tier 2, not a replacement.
- **Generate vectors from one SDK's traces** — enshrines that SDK's current behavior (bugs
  included) as the contract; vectors are authored from the contract, corrected against server
  source.
