# ADR-0011: SDKs restate the wire contract — no shared protocol module, no platform-code dependency

**Status**: Accepted
**Date**: 2026-09-05
**Ticket**: —

## Context

With the platform source about to be published, the question arose whether the SDKs
(sdk-go, sdk-js, sdk-py) should drop their local restatements of the wire vocabulary and
reuse the platform's code instead. An audit of the actual overlap found ~600–700 lines of
contract-driven parallels in sdk-go — wire message types and envelope structs, channel
validation (`{tenant}.{suffix}`, ≥2 parts), protocol error codes, close code 4000, and
credential header conventions — none of it copied code: both sides independently derive
from the published AsyncAPI/OpenAPI contracts. Every platform-side counterpart lives under
`ws/internal/`, unimportable outside the module by Go's own rules. The SDKs are MIT; the
platform's eventual public license may be non-permissive (fair-source family), so any code
dependency would flow license obligations onto SDK consumers. sdk-go ADR-0003 already
rejected importing the platform's internal protocol package; source publication is a new
force challenging that decision.

## Decision

Source publication changes nothing. The SDKs continue to restate the wire contract
independently, deriving from the published AsyncAPI/OpenAPI documents as the single source
of truth (vendored and checksum-pinned in each SDK, per sdk-go ADR-0003). No SDK takes a
Go-module (or any code) dependency on the platform repository. The platform will not
extract a shared public protocol module; the contract *documents* are the shared artifact,
not a shared package. Wire-vocabulary code on the platform side stays under `ws/internal/`.
Drift protection is contract-conformance testing in each SDK (e.g. sdk-go's
contract-coverage test), not code sharing.

## Consequences

- **Easier**: SDKs stay dependency-light (sdk-go: one runtime dep) and license-clean under
  MIT regardless of what license the platform publishes under; platform releases stay
  decoupled from any public Go-module compatibility surface; all three SDKs remain
  symmetric — js and py can never share Go code anyway, so go sharing it would be the
  anomaly, not the norm.
- **Harder**: the ~600–700 duplicated lines persist and must track contract changes in each
  SDK by hand; the AsyncAPI/OpenAPI documents carry full weight as the sync mechanism, so
  keeping them current with the server (§XVII) is load-bearing for SDK correctness.
- **Coupling**: SDKs bind to the contract documents, never to platform source or to each
  other.

## Alternatives rejected

- **Import the platform module from the SDKs** — `ws/internal/` is unimportable by design;
  it would drag the server dependency tree (franz-go, Valkey, gRPC, zerolog) into a client
  SDK; and it would encumber MIT-SDK consumers with the platform's license terms.
- **Extract a shared public protocol module (e.g. `sukko-dev/protocol`)** — buys ~200
  deduplicated lines at the cost of a new public compatibility surface to version, couples
  server releases to it, helps only the Go SDK, and has no prior art — established
  real-time vendors (Pusher, Ably, Centrifugo) publish contracts, not shared
  protocol packages.
- **Codegen from AsyncAPI** — already rejected in sdk-go ADR-0003; the 3.0→Go toolchain is
  immature and fights the contract's quirks.
