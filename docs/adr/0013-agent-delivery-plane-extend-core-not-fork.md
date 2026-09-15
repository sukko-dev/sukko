# ADR-0013: Serve the agent delivery plane by extending the core, not forking it

**Status**: Accepted
**Date**: 2026-09-11
**Ticket**: —

## Context

Teams building internal agent platforms (chatbots, tool-using agents, self-hosted
LLMs) need a real-time layer to carry run events: coarse lifecycle events
(`step.started`, `tool.called`, `run.finished`), fan-out to multiple observers (a
second device, an ops dashboard, a handoff), and delivery to operators who are away
from the connection. The question was whether Sukko fits that slot, and whether
serving it means extending the existing engine or standing up a separate product.

Sukko is a delivery plane, never a datastore: the replay window (≤100 messages / 5s)
is a blip-smoother, not state sync. The workable pattern is therefore **snapshot from
the application's own database, tail from the plane**. Token-by-token LLM output does
not belong here — at 30–80 tok/s the replay window spans about two seconds, which is
useless at token granularity; tokens stay on a direct stream from the LLM gateway.

Assessed against that slot, Sukko is **under-featured, not over-featured**. Every
capability it lacks is a generic pub/sub primitive that comparable self-hosted
engines already expose and that serves subscribers who have never heard of an agent.
There is no architectural deadweight to shed, which is the usual reason to fork.

## Decision

**Extend the core engine. Do not fork, and do not start a second product.**

Four substrate features are adopted as core scope, in priority order. Each is a
generic primitive; none of them mentions agents:

1. **Run-scoped replay** — subscribe to a channel from the beginning of its life,
   rather than from the tail window.
2. **Dynamic subscribe authorization** — a callback answering "may this subject join
   this channel?" at subscribe time, for grants that cannot be expressed as a static
   JWT pattern.
3. **Channel lifecycle** — a terminal `sealed` event marking a stream complete (which
   a disconnect does not), plus TTL expiry for dead channels.
4. **Presence-lite** — observer count and join/leave notification.

**Agent semantics MUST NOT enter the engine.** Run/step schemas, tool-call event
types, and run-management APIs are application concerns. The engine learns channels,
messages, and subscriptions — never what a "run" is. This is the same boundary already
drawn for chat: harden the substrate (message identity, presence), and leave the chat
*product* to applications above it.
Any agent-shaped conventions belong in a thin optional layer above the engine,
extracted later from a working deployment only if the conventions prove out.

### Hot-path placement rules (normative)

The message pipeline — ingestion → broadcast bus → shard fan-out → per-client write
pump → transport write — is protected by §VII. **None of the four features may add
work, locks, I/O, or observational interception to it.** All four are cold-path or
control-plane by construction:

- **Run-scoped replay**: served by a **bounded** backend read on the subscribe path,
  then spliced to live — the same shape as existing reconnect replay. No per-message
  bookkeeping, buffering, or retention logic in fan-out or the write pump. The live
  pipeline MUST be byte-identical before and after this feature. Stable message
  identity (ADR-0008) makes the backfill→live splice dedupe-safe.
- **Dynamic subscribe authorization**: fires at **subscribe time only**, with the
  result cached on the subscription. Never a per-message check. The callback MUST be
  asynchronous with a timeout and default-deny (§IX), and MUST NOT block the
  message pipeline.
- **Channel lifecycle**: `sealed` is one ordinary control message through the
  existing pipeline — no new pipeline branch. TTL cleanup runs on a background
  goroutine (`wg.Go` + `RecoverPanic`, §VII), never inline in message handling.
  Per-channel Prometheus labels are forbidden (§VI cardinality).
- **Presence-lite**: derived from existing subscribe/unsubscribe control events and
  connection tracking — an atomic snapshot rebuilt on subscription change and read
  lock-free. §VII explicitly forbids subscription tracking that adds forwarding
  latency, so there is no per-message observer accounting and no presence fan-out on
  the data path.

If an implementation sketch for any of these touches `forwardFrame`, `bus.Publish`,
`sendToClient`, or the write pump, it is the wrong design.

### Channel provisioning

Channels stay implicit and are never individually provisioned. Provisioned objects
remain topics (ADR-0006) and routing rules, and rules are wildcard patterns — so a
single rule covers an unbounded family of per-run channels with **zero control-plane
writes when a run starts**. Provision per event class at tenant onboarding, never per
run, and deliberately define no catch-all rule so a mistyped channel fails loudly
rather than silently routing.

Run-scoped replay inherits a constraint from this: channels multiplex onto a shared
topic, so replaying one channel from its beginning MUST be an anchored, bounded read
(from the channel's first recorded position, or time-bounded from its start) — never
an unbounded earliest-offset scan of the whole topic.

## Consequences

- The four features ship on their own merits for the existing market: they close
  real gaps against comparable self-hosted engines regardless of whether any agent
  platform adopts Sukko.
- One engine, one CI matrix, one security surface, one SDK matrix across three
  languages. A fork would have duplicated all of these to carry a roughly
  four-feature divergence that both variants want anyway.
- The SDKs gain surface area once the platform ships these: subscribe-from-start,
  the `sealed` event, and presence. That is a cross-SDK, three-language change with
  documentation impact (§XVI).
- Feature 1 depends on ADR-0008 (`mid`) for safe splice dedupe; it is already shipped.
- Deferring the agent-semantics layer means early adopters invent their own run
  conventions. That is intentional — the conventions are not yet proven enough to
  freeze into an API.

## Open items (not decided here)

- **Push tier placement.** Delivery to an operator who is away from the connection is
  one of the conditions under which this slot is worth serving, but Web Push is Pro
  and FCM/APNs mobile push is Enterprise (ADR-0009). Whether that boundary should
  move is a separate decision; push carries per-message infrastructure cost and
  regulatory support obligations that the routing-rule question does not.
- The optional agent-conventions layer above the engine — deferred until a real
  deployment proves the conventions.

## Alternatives rejected

- **Fork into a separate agent-events product** — loses on every axis: the
  divergence is ~4 features both variants want, and it doubles CI, security review,
  and the three-language SDK matrix.
- **Build agent semantics into the engine** (run/step types, runs API) — the same
  mistake already rejected for chat; couples the engine to one application shape
  and to schemas that are not yet stable.
- **Recommend a different engine for this slot and leave Sukko unchanged** — the
  missing pieces are generic primitives Sukko should have regardless; declining
  them keeps a gap against competing self-hosted engines for all users.
- **Carry token-level LLM output on the plane** — the replay window is ~2s at token
  rates, so the durability guarantee that justifies the plane would not apply where
  it was most visible.
