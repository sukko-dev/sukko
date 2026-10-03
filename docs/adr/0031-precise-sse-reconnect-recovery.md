# ADR-0031: Precise SSE reconnect recovery — report the full possible-gap set and a recovery-complete sentinel

**Status**: Accepted
**Date**: 2026-10-03

## Context

ADR-0030 gave SSE reconnect recovery its mechanism (replay through the Subscribe stream) and, in
slice 3b, two outcome frames — `no_replay` (cursor channels the server could not replay) and
`replay_truncated`. But those frames do not let a client compute *precisely* which channels were
recovered, so every SDK had to be imprecise on reconnect:

- **sukko-js** emits a blanket `possible_gap` for *every* desired channel on *every* SSE reopen
  (ADR-0005) — a false positive for every successfully-replayed channel.
- **sukko-py / sukko-go** trust the replay and surface `possible_gap` only for `no_replay` channels
  (ADR-0006 / ADR-0016) — but a channel the client subscribed to that had **no cursor position** (it
  was quiet before the drop) is absent from the cursor, so the server never replays it *and* never
  reports it: a **silent gap**.

Two protocol gaps underlie this: (1) no "recovery complete" marker, so a client cannot conclude that
an unreported channel was fully recovered; and (2) `no_replay` is derived from the cursor only, so
"quiet" requested channels (`requested − cursor`) fall through entirely. The server already holds
both the requested set (`Subscribe.channels`) and the cursor, so it can close both gaps.

## Decision

On the SSE reconnect path — a Subscribe that presented a cursor (`cursor_presented`, or, from a
gateway predating that field, a non-empty `last_pos`) — the server now reports the **complete
possible-gap set** and a sentinel:

1. **`no_replay{channels}` is redefined as `requested − recovered`** — every subscribed channel that
   is *not* confirmed fully recovered. A channel is recovered only if it was in the cursor,
   authorized, and fully replayed (no replay error, no truncation). This subsumes three causes under
   one signal (§XV): cursor channels that couldn't be replayed, quiet channels with no baseline
   (`requested − cursor` — closes gap 2), and — because `MaxReplayMessages` caps a *cross-channel*
   total, so truncation cannot be attributed per channel — **every authorized cursor channel when
   the replay truncated or errored.**
2. **`recovery_complete` sentinel** — emitted after the replayed messages and the `no_replay` /
   `replay_truncated` frames. It marks the end of recovery *emission* (not the stream; live frames
   still interleave with replay per ADR-0026, and the client dedupes by `mid`). After it, the client
   treats exactly the `no_replay` channels as possible gaps and every other subscribed channel as
   recovered.
3. **Capability header `X-Sukko-Recovery: 1`** on the SSE response — a new SDK against an old
   deployment (no sentinel) must not hang waiting or double-fire. The gateway sets the header when it
   supports the sentinel; the SDK keys precise mode on its presence, with no timeout fallback (which
   would be the §XV silent-mode anti-pattern).

The wire `replayed` field moves to an omitempty pointer so it appears only on `replay_truncated`
(it was spuriously present as `replayed:0` on `no_replay`). `recovery_complete` carries only `type`.

**Truncation is detected conservatively.** The replay is reported truncated when it returns a full
batch (≥ `MaxReplayMessages`, a cross-channel total), when the replay timeout fires mid-fetch, or
when a record fails to serialize — in all of which more may remain unrecovered. A false positive at
the exact-cap boundary (one spurious possible-gap report) is the safe direction versus a silent loss.
A degraded consumer pool fails the replay (an error, not an empty success), so those channels are
reported as a possible gap rather than silently asserted recovered.

**The verdict is keyed on a presented cursor, not a surviving one.** The gateway sets a
`cursor_presented` flag on the Subscribe RPC whenever the client reconnected with a `Last-Event-ID`,
even if the decoded cursor ends up empty (a foreign/undecodable token, or every cursor channel
intersected away by the #34 filter). The server emits the full verdict (`no_replay` +
`recovery_complete`) whenever the flag is set, so a precise-recovery client is never left waiting for
a sentinel that never arrives; a fresh subscription (no cursor) emits nothing. "Requested" is the
gateway's permission-filtered channel set the server receives — a channel the gateway dropped at
subscribe time is not in it and is therefore neither delivered nor reported.

**Direct backend degenerates correctly.** No channel is ever pos-bearing on a direct (non-Kafka)
backend, so the cursor is always empty and every reconnect reports all requested channels — precise
mode collapses to "everything may have gapped," which is the truth on a backend with no replay.

**Scope: SSE only.** The WebSocket reconnect path already has both halves — `reconnect_ack` is its
sentinel, and the WS client tracks its own per-channel `pos`, so it knows its own quiet channels.
(The separate `handleReconnect`-discards-`truncated` bug is the WS cousin of this work and stays
filed.)

## Consequences

- SDKs can drop the blanket and surface `possible_gap` for exactly the reported channels; the quiet-
  channel silent-loss window is closed. SDK adoption is per-companion-ADR (supersedes ADR-0005 /
  0006 / 0016 and the blanket clauses of ADR-0014 / 0015), gated on the capability header so old
  SDKs keep working unchanged (they already ignore the new frames — verified when slice 3b shipped).
- `gateway.openapi` 1.0.3 → **1.1.0** (new frame type + a header + extended `no_replay` semantics).
- **Mixed-version deploys are handled.** A new server behind an *old* gateway still emits the verdict
  (keyed on `last_pos` as well as `cursor_presented`), so the old gateway's SSE clients get the new
  frames — safe, since existing SDKs drop unknown frame types. The inverse (a *new* gateway sending
  `X-Sukko-Recovery: 1` in front of an *old* server that emits no sentinel) is moot today — no
  precise-mode SDK has shipped — but the SDK companion PRs MUST be sequenced to roll out only after
  deployments have cleared the mixed-version window, because precise mode waits for the sentinel with
  no timeout fallback (by design).
- This extends ADR-0030's outcome-reporting contract; the replay *mechanism* (live-before-replay,
  opaque cursor, mid-dedup) is unchanged. The gateway cursor-seed fix (PR 1 of this arc) is a
  prerequisite: without it, an intermittently-quiet channel would erode out of the cursor and be
  spuriously re-reported as a possible gap every reconnect.

## Alternatives rejected

- **Keep `no_replay` = cursor-only, add a separate "no_baseline" frame**: two signals for one client
  action (treat as possible gap) — §XV says one. Folded into `no_replay`.
- **A gateway→server "is-reconnect" flag for the all-quiet case** (every channel quiet, so the cursor
  is empty and the reconnect looks like a first connect): unnecessary and unworkable — an all-quiet
  reconnect sends no `Last-Event-ID` at all, so it is indistinguishable server-side, and the case is
  client-detectable (the SDK knows it reconnected with no cursor and self-reports the blanket for
  that one reconnect).
- **Timeout fallback instead of the capability header**: a silent implicit mode (§XV anti-pattern);
  the explicit header is unambiguous and all three SDK SSE transports can read response headers.
