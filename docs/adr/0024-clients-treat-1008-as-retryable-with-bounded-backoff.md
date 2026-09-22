# ADR-0024: Clients treat close 1008 as retryable with bounded backoff, not terminal

**Status**: Accepted
**Date**: 2026-09-22

## Context

The gateway closes a client connection with WebSocket code **1008 (Policy Violation)** in more
than one situation: a slow client shed after N failed deliveries, and — since the spec
reconciliation — token/user **revocation** and graceful shutdown. The three SDKs diverged on how
they react to a remote 1008:

- **sukko-js**: **terminal** — no reconnect (`handleTransportClose` lists 1008 with 1000/4000).
- **sukko-py**: **reconnect** — only 4001 (auth-failed) is terminal.
- **sukko-go**: **reconnect**, counting the attempt toward the backpressure-reconnect cap.

So the identical server close makes one SDK give up while the others retry — a cross-SDK parity
defect. It cannot be resolved by picking a smarter 1008 policy, because **1008's cause is not
recoverable from the close code**: the client cannot tell "slow" from "revoked" from the 1008
alone.

## Decision

A remote **1008 is retryable with bounded backoff** in every SDK. The connection reconnects
through the normal backoff path, and the attempt counts toward the same bounded
reconnect cap the SDK already applies to backpressure disconnects (so a persistently-slow client
gets a bounded number of cycles, never a tight loop).

The two underlying causes are handled at the layer that actually knows them, not at the 1008
branch:

- **Revocation**: a revoked credential is re-rejected at the reconnect **handshake** (401 /
  auth-failure). That is where termination belongs — the auth layer, where the cause is knowable
  — including an SDK's reactive-auth path going terminal when there is no way to obtain a fresh
  credential. The 1008 branch never needs to model revocation.
- **Slow client**: the reconnect either succeeds (the client caught up) or hits 1008 again and
  exhausts the bounded cap, surfacing a "consumer too slow"-class error.

Surfacing the close cause on an SDK's error channel is orthogonal to this decision and is
retained wherever an SDK already does (or planned) it.

## Consequences

- **Uniform**: the same server close produces the same client behavior across all three SDKs.
- **Cost, named**: a genuinely slow-consumer application now sees a bounded series of reconnect
  cycles instead of sukko-js's former immediate stop; the bound prevents a tight loop and the
  "too slow" error is the observability.
- **sukko-js changes** (it was the outlier): 1008 moves from its terminal branch to bounded
  reconnect; this reverses its earlier deliberate terminal-on-1008 choice, whose separate intent
  (a typed error on the error channel) is preserved, not deleted.
- **Enforcement**: per-SDK unit tests pin "remote 1008 → reconnect-class, bounded". A cross-SDK
  guard is a future **Tier 2** live-stack scenario (server closes 1008 → client reconnects), not
  a Tier 1 parity vector — close-code policy lives in the supervisor/connection layer, which the
  vector schema deliberately abstracts over.

## Alternatives rejected

- **Terminal on 1008 (sukko-js's prior behavior)** — a slow client that would recover is killed,
  and it conflates the recoverable slow-client case with revocation, which the code cannot
  distinguish anyway.
- **Branch on 1008 sub-cause** — impossible: the cause is not encoded in the close code.
- **A Tier 1 parity vector for 1008** — would force the vector schema to reach into the
  supervisor/connection layer it was scoped to exclude; per-SDK tests + a Tier 2 scenario are the
  honest guards.
