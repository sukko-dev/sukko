# ADR-0042: SSE is a supported receive-only transport for all client SDKs

**Status**: Accepted
**Date**: 2026-10-23
**Ticket**: #61 (SDK conformance grid)

## Context

The three client SDKs disagree about Server-Sent Events, and the split was never
decided on purpose:

- **sdk-js** ships a full `SseTransport` (browser + Node).
- **sdk-py** ships a full `SseTransport` — exported in `__all__`, documented in the
  README ("WebSocket + SSE transports"), unit-tested (`tests/test_sse.py`), with
  `Last-Event-ID` resume via its `transport_factory`. It is a complete, released
  feature, not legacy cruft.
- **sdk-go** does **not** implement SSE: `WithTransport(TransportSSE)` returns
  `ErrSSENotImplemented`, and the README calls it "planned, not yet implemented."

This surfaced while building the SDK-runner e2e grid (#61): the grid can prove SSE
for js and py but has nothing to run for go, and "planned" is an unreconciled TODO
rather than a decision.

The sharper question raised in review: **why does a *backend* SDK (py) support SSE
at all, when Python does not run in browsers?** SSE's familiar advantage is the
browser (`EventSource`), which is irrelevant to py and go. Two facts bear on the
answer:

1. **The platform treats SSE as first-class**, not as a browser convenience:
   `ws/internal/gateway/sse.go` + `sse_cursor.go`, `Last-Event-ID` reconnect-replay
   recovery (ADR-0030/0031), and a dedicated `sse-recovery` tester suite. The
   platform would not carry that machinery for a throwaway transport.
2. **The backend-relevant justification for SSE is network reachability, not the
   browser**: a *receive-only* consumer in an environment where WebSocket is blocked
   or degraded by HTTP infrastructure — corporate egress proxies that refuse the WS
   `Upgrade`, API gateways/CDNs that mishandle long-lived WS, HTTP/2-only paths —
   can still open a long-lived HTTPS `GET`. That case is language-agnostic (it
   applies to py and go equally) and is **more** likely in the locked-down,
   regulated networks Sukko targets (ADR-0037), where a backend data consumer may
   be allowed plain HTTPS but not WS.

So SSE is not browser-only, and its backend rationale applies to py and go alike —
which means the current py-yes/go-no split is an inconsistency to resolve, not a
principled difference.

## Decision

**SSE is a supported, first-class, receive-only transport for every Sukko client
SDK.** Its purpose for backend SDKs is reachability in WS-hostile HTTP environments
(not the browser); its purpose for the browser SDK is both that and native
`EventSource`. Publishing stays WebSocket/REST only — SSE is receive-only by design,
which is a feature for pure consumers (no accidental publish surface).

Consequently:

- **sdk-js and sdk-py keep their SSE transports** — they are the reference
  implementations of this decision, not accidents.
- **sdk-go's lack of SSE is a tracked gap, not a silent "planned."** It is
  **deferred** (not rejected): building it is real work — a streaming HTTP transport
  with `Last-Event-ID` resume and back-pressure — for a niche whose demand is
  currently unproven, and sdk-go's backend consumers are well served by WebSocket
  today. It will be implemented when a concrete WS-hostile-ingestion need appears
  (or proactively if the compliance-tier pipeline makes it likely). sdk-go's README
  is corrected from "planned" to a cross-reference to this ADR.
- The #61 e2e grid proves SSE for the SDKs that implement it (js, py) and records
  sdk-go SSE as a known, deferred gap — it does not block on parity.

## Consequences

- The SDK transport story becomes statable in one sentence: **every SDK can
  *receive* over WebSocket or SSE; only WebSocket (and REST) can *publish*.**
- A compliance/locked-down operator whose network blocks WS gets a supported
  receive path in js and py today, and a documented roadmap item for go.
- sdk-go carries a tracked gap. The risk is that "deferred" quietly becomes
  "forgotten"; this ADR + the #61 issue are the anchors that keep it visible.
- No shipped code is removed; no SDK user is broken.

## Alternatives rejected

- **SSE is browser-SDK-only; backend SDKs are WebSocket-first; deprecate sdk-py's
  SSE.** Rejected: it would delete a complete, documented, released, tested
  transport (breaking any adopter and discarding real work), and it contradicts the
  platform's own first-class SSE + recovery investment. The WS-hostile-network case
  is real for Sukko's target market, so SSE in a backend SDK is justified, not
  over-scope.
- **Require sdk-go to implement SSE now, for parity.** Rejected: parity for its own
  sake does not justify the build cost against unproven demand; WebSocket covers
  sdk-go's backend consumers today. Deferring-and-tracking beats building on
  speculation — but the gap is recorded, not disguised as "planned."
- **Leave the split undocumented.** Rejected: that is the status quo that produced a
  contradiction between two shipped SDKs; an unreconciled "planned" is a latent TODO,
  not a decision.
