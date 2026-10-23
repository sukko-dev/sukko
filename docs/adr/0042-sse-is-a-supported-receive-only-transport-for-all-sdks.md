# ADR-0042: SSE is a supported receive-only transport for all client SDKs

**Status**: Accepted
**Date**: 2026-10-23
**Ticket**: #61 (SDK conformance grid)

## Context

All three client SDKs implement Server-Sent Events, but that was neither stated as a
decision nor uniformly true in practice, and the inconsistency surfaced while building
the SDK-runner e2e grid (#61):

- **sdk-js** ships a full `SseTransport` (browser + Node), with `Last-Event-ID` resume.
- **sdk-py** ships a full `SseTransport` — exported in `__all__`, documented in the
  README ("WebSocket + SSE transports"), unit-tested (`tests/test_sse.py`), with
  `Last-Event-ID` resume via its `transport_factory`. A complete, released feature.
- **sdk-go** *also* ships an `SseTransport` (`transport_sse.go`, receive-only,
  `Last-Event-ID` recovery) — but it was **defective and its README disowned it.**
  `Open` bound the response stream to the dial context, which the supervisor cancels
  the moment `Open` returns (correct for the WebSocket `Dial`, whose context scopes the
  handshake only; wrong for `net/http`, whose request context governs the response body
  for its whole life). The first `Read` therefore failed with `context canceled`,
  surfaced as a local 1006, and the client reconnect-looped forever on a healthy stream.
  The real gateway stays silent until the first message; the unit fakes wrote a byte on
  connect, so the silent window was never exercised and the defect shipped. The README
  compounded it by claiming SSE was "planned, not yet implemented."

So the apparent "py-yes/go-no" split was not a principled difference — it was a latent
bug plus a stale README on sdk-go. The e2e grid is what forced the reconciliation: a
live SSE cell for go had nothing green to run until the transport actually delivered.

The sharper question raised in review: **why does a *backend* SDK (py, go) support SSE
at all, when neither runs in a browser?** SSE's familiar advantage is the browser
(`EventSource`), which is irrelevant to py and go. Two facts bear on the answer:

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
which means every SDK supporting SSE is the consistent outcome, and the go defect was
a bug to fix, not evidence of a different design.

## Decision

**SSE is a supported, first-class, receive-only transport for every Sukko client
SDK.** Its purpose for backend SDKs is reachability in WS-hostile HTTP environments
(not the browser); its purpose for the browser SDK is both that and native
`EventSource`. Publishing stays WebSocket/REST only — SSE is receive-only by design,
which is a feature for pure consumers (no accidental publish surface).

Consequently:

- **All three SDKs ship a working SSE transport.** sdk-js and sdk-py were already
  correct; **sdk-go's silent-start delivery defect is fixed** (the stream now runs on a
  context detached from the dial ctx, canceled by `Close`; a dial deadline still
  surfaces as `DeadlineExceeded`), with silent-start, stream-outlives-dial,
  dial-deadline, and stalled-error-body regression tests, plus a live e2e cell.
- **The #61 grid proves SSE for every SDK**: `cell:sdk-js` (SSE), `cell:sdk-py-sse`,
  and `cell:sdk-go-sse` each drive the SDK's own SSE client against a source-built
  Pro+kafka stack. SSE is Pro-gated, so each cell asserts a non-vacuous PASS (a
  non-Pro stack would skip the test — the verdict refuses that).
- **sdk-go's README is corrected** from "planned / `ErrSSENotImplemented`" to an
  accurate description (receive-only SSE with `Last-Event-ID` recovery) cross-
  referencing this ADR.

## Consequences

- The SDK transport story becomes statable in one sentence: **every SDK can
  *receive* over WebSocket or SSE; only WebSocket (and REST) can *publish*.**
- A compliance/locked-down operator whose network blocks WS gets a supported,
  e2e-proven receive path in all three SDKs today.
- The grid now gates SSE against regression for every SDK — the class of bug that hid
  in sdk-go (a defect invisible to unit fakes that write on connect, only reproducible
  against a silent-start real gateway) cannot silently ship again.
- No SDK user is broken; sdk-go SSE users move from a non-delivering transport to a
  working one (sdk-go PR #12, released).

## Alternatives rejected

- **SSE is browser-SDK-only; backend SDKs are WebSocket-first; deprecate sdk-py's (and
  sdk-go's) SSE.** Rejected: it would delete complete, documented, released, tested
  transports (breaking adopters and discarding real work), and it contradicts the
  platform's own first-class SSE + recovery investment. The WS-hostile-network case
  is real for Sukko's target market, so SSE in a backend SDK is justified, not
  over-scope.
- **Treat sdk-go's SSE defect as a tracked bug and ship the grid without a go SSE
  cell (parity deferred).** Rejected: a shipped-but-broken transport is worse than an
  unimplemented one — it fails silently in production. The fix is small and the live
  cell is what proves it; deferring would have left a latent production defect
  disguised as a "known gap."
- **Leave the split undocumented.** Rejected: that is the status quo that produced a
  contradiction — a stale "planned" README over a real (if broken) implementation —
  which is a latent TODO masking a bug, not a decision.
