# ADR-0020: Reconnect replay scopes to tenant-validated requested channels, failing closed

**Status**: Accepted
**Date**: 2026-09-20
**Ticket**: —

## Context

On WebSocket reconnect, a client sends `reconnect{client_id, last_pos}` — a map of
channel → last-received position — and the server replays the messages missed since
each position, so an at-least-once client recovers its own stream (ADR-0015/0016/0017).

The replay was not scoped to the client. `ReplayFromOffsets` filtered delivered records
against the client's current subscription set, but skipped the filter entirely when that
set was empty:

```go
if len(subSet) > 0 {
    if _, subscribed := subSet[msg.subject]; !subscribed {
        return // skip unsubscribed channels
    }
}
```

The documented protocol sends `reconnect` **before** `subscribe`, so the subscription
set is always empty at replay time — the filter never ran. Because a catch-all routing
rule maps many channels onto one Kafka topic, replaying that topic from an offset
returned **every channel on it**. The benchmark fault matrix measured it: one subscriber
of a single channel received the kill-time message of all 120 channels (6713 misroutes);
a clean run leaked nothing, so it is purely reconnect-induced.

The leak is cross-tenant, not merely cross-channel. `handleReconnect` builds the replay
positions from the client-supplied `last_pos` keys with no tenant check, and the
channel→topic map is not tenant-scoped, so a tenant-A connection that sends
`reconnect{last_pos:{"tenantB.md-0":…}}` before subscribing has tenant B's topic
replayed to it. The gateway forwards `reconnect` frames raw — its subscribe/publish
authorization never inspects `last_pos`. This is a §IX tenant-isolation violation
reachable by any authenticated tenant.

## Decision

**Reconnect replay is authorized against the connection's tenant and filtered to the
channels the client explicitly named, and the underlying filter fails closed.**

Three layers, defense in depth (§II):

- **L1 — authorize at the server handler.** `handleReconnect` builds the replay filter
  from the `last_pos` keys, admitting a channel only when
  `auth.ValidateChannelTenant(channel, c.TenantID())` holds (exact `{tenant}.` prefix;
  already fails closed on an empty tenant). Denied channels are skipped, logged, and
  counted. The client's live subscription set is no longer used as the replay filter —
  `last_pos` is the client's own explicit statement of what it wants recovered.
- **L2 — fail closed at the source.** `ReplayFromOffsets` returns nothing when the
  subscription filter is empty, always. With L1 the filter is never empty on a
  legitimate reconnect, so recovery is preserved; the guard removes the fail-open path
  regardless of caller.
- **L3 — authorize at the gateway.** The gateway intercepts `reconnect` frames and runs
  the `last_pos` keys through the same per-tenant channel-rules gate it applies to
  `subscribe` (`filterSubscribeChannels`), so rule-denied same-tenant channels
  (per-user/private channels) cannot be replayed either. L1+L2 close the cross-tenant
  and cross-channel leak server-side and ship first; L3 closes the same-tenant
  rule-denied case.

## Consequences

- The cross-tenant and cross-channel reconnect leak is closed; §IX holds on the replay
  path, and §II is restored (no layer replays what the tenant is not entitled to).
- **Replay semantics narrow to "the channels you named in `last_pos`."** A channel the
  client is subscribed to but supplied no position for is no longer incidentally
  replayed — but that delivery was accidental (no position means no defined start), so
  this tightens an undefined behavior rather than removing a guarantee.
- No wire-protocol or SDK change: the reconnect-before-subscribe order stands, and the
  fix authorizes `last_pos` rather than depending on frame ordering.
- The gateway now parses `reconnect` frames (a small JSON cost on a cold, rare frame).
- A denial metric makes rejected reconnect channels observable (§VI); a persistent
  nonzero rate flags a misbehaving or hostile client.

## Alternatives rejected

- **Fail closed alone** (empty filter → replay nothing, no L1). Closes the leak but
  destroys recovery under the documented reconnect-before-subscribe order — the client's
  own channel would not replay, reintroducing loss exactly during a reconnect storm.
- **Require subscribe before reconnect.** Makes the existing filter populated, but is a
  client+server+SDK contract change and makes authorization depend on message ordering —
  fragile, and still needs the fail-closed guard for a buggy client.
- **Defer replay until the subscribe frame arrives.** Adds per-connection pending-state
  and a timeout path for no advantage over authorizing `last_pos` directly.
- **Filter by the live subscription set** (the prior design). It is empty at replay time
  under the real protocol, which is the root of the fail-open leak.
