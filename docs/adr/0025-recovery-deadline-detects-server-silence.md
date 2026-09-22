# ADR-0025: The recovery deadline detects server silence, not total replay duration

**Status**: Accepted
**Date**: 2026-09-22

## Context

Every client SDK arms a **recovery deadline** during an in-flight replay: if the replay never
terminates, the client stops waiting and surfaces a `RecoveryInterrupted` so the application can
recover another way. The three SDKs disagreed on what the deadline measures — a cross-SDK parity
defect surfaced by the parity-vector work (ADR-0023):

- **sukko-js / sukko-py** reset the deadline on **every `replay_message`**, so it measures the
  *maximum silence between recovery frames*. A large-but-progressing replay never interrupts.
- **sukko-go** arms the deadline **once at replay start** and only *suspends* it when the delivery
  consumer is parked (back-pressure); it measures the *total replay duration*, park-adjusted. Go
  rejected per-frame reset as "hot-path observational interception on the message pipeline (§VII)."

Observable divergence: a replay that runs longer than the deadline (~10s) while frames keep
arriving and the consumer keeps up (unparked) — exactly the post-outage catch-up replays recovery
exists for — completes on JS/Py but raises `RecoveryInterrupted` on Go. The bench fault matrix
measured a 2.76s *mean* replay under load; tails near a 10s cap are realistic, not theoretical.

Two failure modes matter: the **server** goes silent mid-replay (dead recovery — must interrupt),
and the **consumer** stalls so frames can't be processed (the client's own doing — must not
interrupt). JS/Py's per-frame reset covers the first but not the second; Go's park-suspension
covers the second but not the first. Neither SDK covers both.

## Decision

The canonical rule, stated once: **the recovery deadline detects server *silence* — it fires only
after a full window with no recovery progress under a live connection, where progress is either a
recovery frame arriving or the consumer being unable to accept one.**

- A recovery frame arriving during the window **suspends** (re-arms) the deadline. Server silence
  is the only thing that fires it.
- Consumer park during the window also suspends it (unchanged from Go today; JS/Py owe this as a
  follow-up — their slow-*consumer* hole).
- Epoch churn and disconnection continue to suspend it (unchanged).

sukko-go keeps its park-suspension and adds a **per-channel** delivery-layer recovery-frame
counter (a `sync.Map` of `*atomic.Int64`, one entry per channel, incremented once per
`SourceReplay` record on the decode goroutine — the same lock-free counter-at-tick pattern as
`backpressureBlocks`); `due()` suspends a channel's deadline when *that channel's* counter changed
during the window. This achieves silence-detection with **no per-message owner event and no
pipeline interception** — the objection Go's original comment raised was a false dichotomy: the
choice was never "park-suspension *or* per-frame reset," a lock-free progress counter is a third
way. The counter is **per-channel, not client-global**: progress on one channel must never keep a
different, wedged channel's deadline alive. sukko-js/sukko-py already reset per channel per frame
(their `handleReplayMessage`/`note_replay_message` are REPLAYING- and channel-scoped) and are
conformant for the server-silence mode.

## Consequences

- Large, progressing replays no longer false-interrupt on Go — the property recovery is *for*.
- The client-side deadline no longer depends on an unverifiable cross-config assumption. Go's
  total-duration cap was only safe if the operator never raised the server's `WS_REPLAY_TIMEOUT`
  past the client's stale default; silence-detection needs no knowledge of server config, and the
  server already bounds total replay duration server-side (the client cap was redundant with it).
- A new parity vector (`recovery/replay-slow-steady-server`) pins the semantics: a replay whose
  frames arrive steadily but whose total exceeds the deadline must NOT interrupt. It fails against
  the old Go behavior and passes JS/Py and fixed Go — this is how the parity is *measured*.
- JS/Py's slow-*consumer* hole (no park-suspension) is now the named remaining gap, tracked as a
  separate follow-up; pinning it needs a park/unpark input the vector schema does not yet have.

## Alternatives rejected

- **A client-global recovery-frame counter (not per-channel)** — simpler (one counter), but a
  channel whose replay wedges server-side would have its deadline re-armed indefinitely by *other*
  channels' ongoing replay traffic, so its silence is never detected. JS, Py, and even the old Go
  behavior all scope recovery progress per channel; a client-global counter would be the sole
  regression. Rejected for per-channel counters.
- **Standardize on Go's total-duration cap; change JS/Py** — more work (two SDKs, and JS/Py have
  no park-episode concept to model against) for the weaker semantics: it interrupts live,
  progressing replays and duplicates the server-side duration bound.
- **Per-`replay_message` owner event in Go** (mirroring JS/Py's literal reset) — would route every
  replayed record through the recovery owner's inbox, real pipeline interception (§VII). The
  atomic progress counter read at the owner's existing tick avoids it entirely.
- **Document as an accepted per-SDK divergence, no code change** — leaves a measurable behavioral
  difference on the core recovery path; the parity corpus exists precisely to eliminate these.
