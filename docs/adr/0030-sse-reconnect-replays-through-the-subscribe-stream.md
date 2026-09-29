# ADR-0030: SSE reconnect replays through the Subscribe stream, live-before-replay

**Status**: Accepted
**Date**: 2026-09-29
**Ticket**: —

## Context

The WebSocket path has full reconnect recovery: a client re-homes with
`reconnect{last_pos}` (a per-channel `{channel: "(partition+1)-offset"}` map) and
ws-server replays the missed Kafka messages, registering live delivery *before*
replaying so nothing falls into a seam (ADR-0026), scoped to tenant-validated
channels (ADR-0020), deduplicated by `mid` (ADR-0008).

The SSE path (`GET /sse`, gateway-terminated) has **none of this**. `HandleSSE`
opens a live-only gRPC `Subscribe` stream, emits `id: {sequence}` per event, and
never reads the incoming `Last-Event-ID` header or replays. Worse, that
`sequence` is the **per-connection** monotonic counter — it restarts every
connection, so it cannot address a position across a reconnect even if the header
were read. A reconnecting SSE client silently loses everything missed while
disconnected. `gateway.openapi.yaml` (1.0.1) already claims "Supports
`Last-Event-ID` header for reconnection" — today that claim is false.

ADR-0027 scoped v1.0.6's SSE work to *test coverage*. This ADR extends that scope:
SSE gains real recovery, and the recovery becomes the thing the tests validate.

## Decision

**SSE reconnect replays through the server's `Subscribe` stream, reusing the WS
replay machinery, with the same live-before-replay ordering.**

1. **Replay location — in the Subscribe stream, not the gateway.** The gRPC
   `SubscribeRequest` gains an optional `last_pos map<string, string>` (the WS
   cursor shape; a buf-compatible field addition). When present, ws-server
   validates each channel against the connection's tenant (ADR-0020), **registers
   the live subscription first, then replays** `last_pos → now` (ADR-0026
   ordering), and interleaves replayed and live messages down the one stream. The
   client contract is identical to WS reconnect: dedupe by `mid`, order by `pos`.
   This inherits tenant validation, channel caps, and replay metrics from existing
   server machinery instead of re-implementing them in the gateway.

2. **The SSE `id:` becomes an opaque, versioned cursor** — a compact encoding of
   the connection's current `{channel: pos}` map (literally the WS `last_pos` map
   serialized), prefixed `v1:`. Because the SSE spec echoes only the *last* id the
   client saw, each id must be self-contained. Clients and the tester already
   treat `id:` as an opaque string (store-and-echo), so this is a
   client-transparent change — no SDK change required for the wire format.

3. **Emission cadence is bounded, not per-event (§VII).** The cursor `id:` is
   emitted at most every `SSE_CURSOR_INTERVAL` (config; default a small number of
   seconds) or every `SSE_CURSOR_EVERY_N` messages — not on every delivered
   message — so encoding the map never sits on the per-message delivery hot path.
   Resume from a slightly-stale cursor replays a few extra messages, absorbed by
   `mid` dedupe (ADR-0008), exactly as ADR-0026 already accepted for the WS
   boundary.

4. **Channel-count is bounded.** `/sse` gains a max-channels-per-connection cap
   (mirroring `WS_MAX_CHANNELS_PER_CLIENT`) so the cursor token cannot exceed
   reconnect-request header limits.

5. **Explicit modes, not fallbacks (§XV).** Recovery requires the Kafka backend
   (like WS replay). On the direct backend, or an absent / unparseable /
   foreign-version / expired-offset token, the server serves **live-only and emits
   an explicit `event: no_replay`** naming the reason — never a silent degrade.

6. **Truncation is signalled, not silent.** If replay hits `MaxReplayMessages`,
   the server emits an explicit `event: replay_truncated` before live resumes, so
   the client knows a gap remains. (This deliberately does *not* clone the WS
   path's known silent-truncation defect — ADR-0026 Fable finding 4 — which stays
   tracked for the WS path.)

7. **The token is untrusted input**, same threat model as WS `last_pos`: every
   channel in the decoded cursor is tenant-validated before it can reach replay,
   and the channel cap bounds fan-out. A forged or cross-tenant token can replay
   nothing it is not authorized for.

## Consequences

- `gateway.openapi.yaml` becomes honest (the existing `Last-Event-ID` claim is
  made real) and gains the `no_replay` / `replay_truncated` events and the channel
  cap — a same-PR contract update with a version bump (§XVII); `ws/docs/e2e-testing.md`
  if the tester surface changes.
- The tester gains an **SSE-recovery suite** (ADR-0027's Part A), wired into the
  pro/enterprise-kafka grid cells; the bench **fault matrix gains SSE subscribers**
  (Part B), gating recovery two-sided exactly as ADR-0026 did (unfixed images lose,
  fixed images hold under owner-kill).
- Cross-repo (§XVI): the JS/Py SDKs likely need **zero** wire changes (opaque
  `id:` echo already); the Go SDK's Phase-11 SSE work (the `TODO(S3b)` items) must
  target this design so it does not build resume against the old per-connection id.
- More duplicate messages around the reconnect boundary — safe under `mid` dedupe.
- Adds `SSE_CURSOR_INTERVAL`, `SSE_CURSOR_EVERY_N`, and an SSE channel-cap env var
  (each with `envDefault` and an inline operator comment, §I).

## Alternatives rejected

- **Gateway orchestrates a replay RPC, then opens a live Subscribe.** This
  recreates the ADR-0026 seam *by construction* — the replay is a snapshot, and
  messages hitting the bus between the snapshot and the live subscribe are lost.
  Rejected by name; replay must be live-before-replay inside the one stream.
- **Per-message durable `id:` = just the last message's `channel:pos`.** A single
  channel's position cannot resume a multi-channel subscription; the next
  connection would replay only one channel. The full-map cursor is required.
- **Single-channel SSE streams** (one stream per channel, so `id:` is trivially
  that channel's offset — the common industry simplification, e.g. some Centrifugo
  setups). It would break Sukko's existing multi-channel `?channels=a,b,c` SSE
  contract and multiply connections; the encoded-cursor keeps one connection.
- **Server-side per-connection cursor store** (opaque token → stored `{channel:pos}`).
  Adds stateful storage keyed by token, does not survive gateway restart, and is
  strictly heavier than a self-contained token. Prior art favors self-contained
  recovery keys (Ably's connection-recovery key; Centrifugo's per-channel
  `offset`+`epoch`). Note Centrifugo's **epoch/generation** guards offset validity
  across topic recreation — the `no_replay` expired-offset mode (decision 5) is
  Sukko's equivalent guard.
