# ADR-0039: License reload raises or renews live but never lowers; expiry is boot-only

**Status**: Accepted
**Date**: 2026-10-20
**Ticket**: —
**Home**: `docs/adr/` (committed) — this is an engineering decision about runtime license-enforcement semantics, needed by any contributor working on `internal/shared/license` or the `WatchLicense` stream, and already surfaced to operators in the published license-lifecycle docs (ADR-0007).

## Context

ADR-0038 (private, GTM) inverted the Community default — an empty or expired `SUKKO_LICENSE_KEY` is refused at startup rather than silently resolving to Community — and stated that a running server "ignores license expiry/changes while running — no mid-flight stop, no mid-flight downgrade." That phrasing is too absolute to implement directly: the `WatchLicense` stream and the provisioning admin reload API exist precisely to apply a license change to a running fleet without a redeploy, and an operator who buys an upgrade expects it to take effect live. We need a precise rule for what a *running* server does when a new key arrives, that honours "no mid-flight downgrade" without discarding live upgrades.

Two further facts shaped the rule:

- `internal/shared/license` exposes both an edition **and** per-dimension capacity **limits**, and Pro is sold in capacity bands delivered as key limit-overrides — so a same-edition key can still *reduce* capacity. "Never lower" has to cover limits, not just edition rank.
- ws-server, ws-gateway, and ws-push boot from `SUKKO_LICENSE_KEY` and only then learn the authoritative key (the DB-persisted one) over the `WatchLicense` stream. If the stream never applied a key below the env-booted edition, a persisted downgrade would never reach them, even across their own restarts.

## Decision

A license is validated **only when it is loaded** — at boot (`NewManager`) and on reload (`Manager.Reload*`). There is no background re-evaluation. Within that, a reload applied to a *running* process follows **Model C**:

1. **A live reload raises or renews, but never lowers.** `Manager.ReloadNoDowngrade` applies the new key immediately when it does not lower the edition **and** does not reduce any capacity limit (0 = unlimited is the highest grant). A key that would lower either is **not** applied to the running process — it is accepted, persisted, and broadcast, but takes effect only at the next restart. Both runtime surfaces use this one rule: the `WatchLicense` stream watcher and the provisioning admin reload API (`POST /api/v1/license`), which returns `status: active` when it took effect live and `status: pending_restart` for a deferred downgrade (it no longer returns 409 on an edition change).

2. **Boot resolution is unconditional.** The unconditional `Manager.Reload` is the boot path: provisioning resolves the DB-persisted key (which takes precedence over the env key) at startup, and each stream client applies the **first** key it receives per process lifetime unconditionally — converging to the authoritative edition even when that is *lower* than the env-booted one. Boot is the restart at which a downgrade legitimately takes effect, so this is how "a downgrade takes effect at the next restart" actually reaches the services that boot from the env key.

3. **Expiry never acts on a running server.** A lapsed key triggers no downgrade and no shutdown; the server keeps serving the edition and limits it booted with. An expired key is refused only at the next boot/reload, exactly like a missing one. (This removes the former `scheduleExpiryWatcher` 14-day self-shutdown.)

4. **Replay protection anchors on the last accepted key.** Because a deferred downgrade advances the persisted key without advancing the live manager, the reload API rejects any key whose `iat` is not newer than the most recently *accepted* key — otherwise an older key could overwrite the newer persisted one.

This refines ADR-0038's "ignores changes while running": a running server ignores **downgrades and expiry**, but a live **upgrade or renewal** does apply.

## Consequences

- A license event never causes an outage of live traffic, and a capacity band behaves exactly like an edition for the raise/defer rule.
- "Higher tier keeps serving until restart" holds for both editions and capacity: a downgrade is visible to operators (persisted, broadcast, `pending_restart`) but changes nothing running until a restart, when boot resolution adopts it.
- Because a running server survives expiry and a deferred downgrade, the operator must be warned persistently and must re-key before the next restart (the detect-to-warn channels in ADR-0038: expiry gauge, in-service log ticker, license-service email cadence, portal banner).
- The admin reload API's contract changed: an edition change is accepted (not 409), with `status` distinguishing live from deferred. The provisioning OpenAPI and the published license-lifecycle doc reflect this.

## Alternatives rejected

- **"No mid-flight anything," including upgrades (ADR-0038's literal wording).** Would force a restart to apply a purchased upgrade, gutting the purpose of the hot-reload stream. Rejected — upgrades are safe to apply live.
- **Apply every reload live, including downgrades (the prior `Reload`-on-every-message behaviour).** Reduces capacity mid-flight (connection/tenant/topic caps drop under live traffic) and is the mid-flight downgrade ADR-0038 rejects. Rejected.
- **Compare edition only, ignore limits.** A same-edition capacity cut (a smaller Pro band) would still lower caps live. Rejected — "never lower" must cover capacity.
- **Keep the provisioning reload API stricter than the stream (409 on any edition change).** A deliberate asymmetry documented in code, but it blocks live upgrades through the primary operator surface and leaves two rules to reason about. Rejected in favour of one shared rule.
