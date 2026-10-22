# ADR-0041: Re-pushing the current license key is an idempotent no-op, not a replay

**Status**: Accepted
**Date**: 2026-10-22
**Ticket**: —
**Home**: `docs/adr/` (committed) — an engineering decision about license-reload semantics in `internal/shared/license`, the provisioning admin reload API, and the `WatchLicense` stream; it refines the replay-protection rule ADR-0039 relies on.

## Context

License replay protection (ADR-0039 and the `internal/shared/license` reload path) rejected any pushed key whose `iat` was **not strictly greater** than the current key's — `claims.Iat <= current.Iat → ErrReplayDetected`. The intent is a downgrade guard: a leaked older key must never replace a newer one.

But "`<=`" also rejects re-pushing the **identical current key**, and callers legitimately do that:

- `sukko up`'s push-service reconcile re-pushes the license (to learn the edition and decide whether to start Web Push) *after* provisioning has already auto-loaded that same key from `SUKKO_LICENSE_KEY` at boot. The re-push has the same `iat`, so it was rejected `409 REPLAY_DETECTED`. For Community this was a spurious warning; for Pro/Enterprise it aborted the reconcile and **Web Push never auto-started on `sukko up`**.
- The `WatchLicense` stream re-sends the current key on every reconnect; the stream already had to special-case `ErrReplayDetected` as "snapshot matches booted key — no change", conflating a benign no-op with a genuine replay.

A same-`iat` key is not inherently a replay — only a same-`iat` key with **different claims** (a forged or stale key) is. The guard was too coarse.

## Decision

A reload classifies the incoming key against the current one in a single shared function (`classifyReplay`), used by `Reload`, `ReloadNoDowngrade`, and the handler pre-check so they can never disagree:

- **Strictly newer `iat`, or no current key** → apply.
- **Same `iat` AND byte-equal claims** (edition, org, exp, iat, nodes, limits) → `ErrLicenseUnchanged`: an **idempotent no-op**, not a replay. No state swap, no DB write, no `LicenseChanged` broadcast, no reload-success metric.
- **Strictly older `iat`, OR same `iat` with any differing claim** → `ErrReplayDetected`, unchanged.

`ErrLicenseUnchanged` is a signal carried as an error (like `io.EOF`); callers match it with `errors.Is`. The provisioning admin reload endpoint returns `200` with status `unchanged` (and a distinct `unchanged` metric label) for this case. The replay-vs-edition-change ordering and the downgrade-is-deferred rule (ADR-0039) are unchanged. "The current key" means either the running **live** key (compared structurally, which also covers the first push before any API reload) **or** the most-recently-**accepted** raw key (compared byte-for-byte) — the latter covers a re-push of a still-**deferred downgrade**, whose `iat` equals the persisted anchor but whose claims differ from the live key. Both are no-ops. A genuinely older key, or a forged same-`iat` key matching neither, is still a replay.

## Consequences

- `sukko up` no longer emits a `REPLAY_DETECTED` warning, and Web Push auto-starts for Pro/Enterprise on `sukko up`.
- The downgrade guard is intact: a strictly-older key, or a same-`iat` forged key with different claims, is still rejected. Idempotency never widens into accepting a different key.
- Reload metrics and the `LicenseChanged` event stop firing on no-op re-pushes, so monitoring no longer shows phantom reloads on every `sukko up` / stream reconnect.
- Callers of `Reload`/`ReloadNoDowngrade` must treat `ErrLicenseUnchanged` as success-no-op (the handler, the stream, and provisioning's DB-precedence resolve were updated).

## Alternatives rejected

- **CLI tolerates the 409** (treat `REPLAY_DETECTED` on its own push as "already loaded"): leaves the platform rejecting idempotent re-pushes, so any other caller hits the same wall; a symptom fix, not the contract fix.
- **Relax to `iat <` and let `==` fall through to a swap**: re-applies the same key (redundant event/metric/DB write) and, worse, would let a *different* same-`iat` key overwrite the current one — a silent-downgrade hole (§XV). Rejected in favour of comparing claims and no-oping only a true re-push.
- **Compare the raw key bytes**: the manager stores parsed claims, not the raw key; structural claim equality is equivalent for a validly-signed key and needs no new stored state.
