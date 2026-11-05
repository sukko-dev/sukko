# ADR-0043: Platform stays on the 1.x line with a published SemVer compatibility contract

**Status**: Accepted
**Date**: 2026-11-05
**Ticket**: roadmap #24

## Context

The platform is pre-launch with zero users (no accounts, nothing pins a published
version). It sits on a `v1.0.x` tag line (`v1.0.1`…`v1.0.9`), and there was low
confidence in the `1.0` SemVer commitment — the instinct was to drop the platform to
`0.x` to signal "pre-stable, API may change," and relaunch at `1.0` later.

Investigation found the real gap is not the number but the **absence of any stated
compatibility contract**: nothing in the repo says what a platform `vX.Y.Z` promises.
A lower number does not add that promise; it only relabels the churn.

Three facts make dropping to `0.x` actively harmful rather than merely cheap:

1. **The wire contracts already declare themselves stable on independent trains** —
   gateway OpenAPI, provisioning OpenAPI, client AsyncAPI each carry their own 1.x
   SemVer, and ADR-0027 makes those contracts (not the image tag) the real cross-repo
   coupling. A `0.x` platform image beside `1.x` API contracts is incoherent to an
   enterprise reviewer; making it coherent would mean downgrading the contracts too,
   forcing SDK re-vendors and contradicting ADR-0027.
2. **Positioning (ADR-0037)** targets compliance-mandated operators and enterprises.
   Procurement reads `0.x` as "not approved for production" — a direct hit on the pitch.
3. **The irreversibility window**: cutting the next tag forecloses the `0.x` option.
   A `v0.9.0` tag dated after the `v1.0.9` commits also reads, on due-diligence, as a
   cover-up rather than honesty.

The decision was stress-tested with a fresh-context decision panel, which recommended
against the `0.x` move on the coherence grounds above.

## Decision

The platform **stays on the `1.x` line.** The next release is `v1.0.10` (the two §IX
authorization fixes).

A **published SemVer compatibility contract** is adopted as the confidence mechanism in
place of a smaller number (full text: committed `docs/versioning.md`):

- The **wire contracts own their own SemVer** (gateway/provisioning OpenAPI, client
  AsyncAPI). The platform image version does **not** govern API compatibility (ADR-0027).
- **Platform MAJOR** = a contract-breaking or data-incompatible change (a
  backward-incompatible change to a wire contract, or to stored/on-disk data).
- **Platform MINOR** = may change the operator-facing surface (env vars, Helm values,
  CLI-facing behavior) **with a changelog entry**, until a declared **operator-config
  freeze**. This is the honest acknowledgment that the operator-config surface is still
  settling even though the API is stable.
- **Platform PATCH** = never breaks anything (bug and security fixes, e.g. `v1.0.10`).

The **stability/freeze gate** is contract-anchored and testable — contracts frozen at a
version + contract tests in CI against the live stack (§VIII e2e) + this policy
published — **not** number-driven. "First paying operator" is the *deadline* for that
gate, not its trigger.

**CLI** stays on its own `1.x` train (currently `v1.3.0`): it is a public Go module whose
every version is immutably cached by the Go module proxy, so `go install …@latest`
resolves `v1.3.0` regardless of git tags — the version cannot be pulled back, and per-repo
trains are already policy (ADR-0027). **SDKs** stay on `0.x` and each is promoted to `1.0`
on its own train when its conformance tier (ADR-0040) is complete and its public types
freeze.

Two mechanical consequences land with this decision:
- `Chart.yaml` `appVersion` tracks the release tag from `v1.0.10` onward (it had been
  stuck at `1.0.0`).
- `release-guard` in `build-images.yml` gains a monotonic check (reject a tag that is not
  greater than the highest existing tag) so a mistaken lower tag cannot move `:latest`
  backward.

## Consequences

- Compatibility becomes a **stated, reviewable promise** instead of folklore — the thing
  the "no confidence in 1.0" instinct was actually reaching for.
- **Tagging `v1.0.10` permanently forecloses the `0.x` option** — the irreversibility
  window closes with that tag. Accepted deliberately.
- Ongoing discipline is required: bump `appVersion` each release; add a changelog entry
  for any operator-config change until the freeze.
- The real stability gate is now **contract tests in CI** (tracked as a follow-up), which
  is a stronger guarantee than a version string.
- `docs/versioning.md` is operator-facing, so §XVI puts a `sukko-docs` page and an
  `llms.txt` entry in scope (companion change).

## Alternatives rejected

- **Delete `v1.0.1`–`v1.0.9`, restart the platform at `v0.9.0`.** Honest-looking and cheap
  with zero users, but incoherent beside the `1.x` wire contracts; `0.x` blocks enterprise
  procurement (ADR-0037); a `v0.9.0` dated after `v1.0.9` reads as a cover-up; and it
  relabels churn without reducing it. It also would move `:latest` to a "pre-stable" image
  that fresh `sukko up` / Helm installs pull by default.
- **Restart at `v0.1.0`.** Worse optics than `v0.9.0` and adds no honesty the policy
  doesn't already provide.
- **Cosmetically re-tag the CLI to `~v0.9.0`.** The Go module proxy keeps `@latest` at
  `v1.3.0` permanently, so this only creates a split-brain install surface (Homebrew/binary
  users on `0.9.x`, `go install` users on `1.3.0`) for no gain.
- **Lift the SDKs to `1.x` for uniformity.** Their `0.x` honestly reflects SDK-surface
  churn; promote per conformance, not for cosmetic alignment.
