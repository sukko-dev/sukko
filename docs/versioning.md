# Versioning and compatibility

This document states what a Sukko version promises. It is the single source of truth
for the platform's compatibility contract; the public documentation site mirrors it.

Sukko is distributed as three independent release trains (ADR-0027). Each is versioned
on its own [SemVer](https://semver.org/) line — a platform version and an SDK version
that happen to share digits mean nothing to each other.

| Train | Examples | Line today |
| --- | --- | --- |
| **Platform** (ws-gateway, ws-server, provisioning, push, webhook-worker images + Helm charts) | `v1.0.10` | `1.x` |
| **Wire contracts** (gateway OpenAPI, provisioning OpenAPI, client AsyncAPI) | provisioning OpenAPI `1.3.0`, gateway OpenAPI `1.1.0`, AsyncAPI `1.4.3` | `1.x`, each independent |
| **CLI and SDKs** | CLI `v1.3.0`, `@sukko/sdk` `0.1.x`, sdk-go/sdk-py `v0.1.x` | per package |

## The wire contracts are the compatibility authority

API compatibility is governed by the **wire contracts**, each carrying its own SemVer:

- `ws/docs/openapi/gateway.openapi.yaml`
- `ws/docs/openapi/provisioning.openapi.yaml`
- `ws/docs/asyncapi/client-ws.asyncapi.yaml`

The **platform image version does not govern API compatibility.** A client or SDK that
works against a given contract version keeps working against any platform image that
serves that contract version. This separation is deliberate (ADR-0027): the platform and
the SDKs ship on independent schedules, coupled only through the contracts.

## Platform version semantics

Given a platform version `vMAJOR.MINOR.PATCH`:

- **MAJOR** — a backward-incompatible change to a wire contract, or to stored/on-disk
  data. Upgrading across a MAJOR may require client, SDK, or data changes.
- **MINOR** — may change the **operator-facing surface**: environment variables, Helm
  values, or CLI-facing behavior. Every such change carries a changelog entry. This is
  allowed **until the declared operator-config freeze** (see below) and reflects that the
  operator-config surface is still settling even though the API is stable.
- **PATCH** — never breaks anything. Bug fixes and security fixes only (for example
  `v1.0.10`).

Wire-contract compatibility is never broken by a platform MINOR or PATCH — only by a
platform MAJOR, and only in lockstep with the affected contract's own MAJOR bump.

## Operator-config freeze and the road to a stability guarantee

The operator-config surface (env vars, Helm values) is not yet frozen: a MINOR may still
rename or remove one, with a changelog entry. The surface is frozen when, together:

1. the wire contracts are frozen at a stated version,
2. contract tests run in CI against a live stack (§VIII end-to-end), and
3. this policy is published.

Onboarding the first paying operator is the **deadline** for reaching that gate, not a
substitute for it — a business milestone does not by itself make the surface stable.

## Guidance for operators

- **Pin a version or a digest** — deploy a specific `vX.Y.Z` image tag or an image digest.
  Do not deploy the moving `:latest` tag in production; it follows the newest release and
  will change under you.
- Read the changelog for the target version before a MINOR upgrade; it lists any
  operator-config change.
- Within a MAJOR line, PATCH upgrades are always safe to take.

## For contributors

- Every release commit bumps `appVersion` in the service `Chart.yaml` files to match the
  release tag.
- A change to any env var, Helm value, or CLI-facing behavior requires a changelog entry
  in the same change (MINOR), until the operator-config freeze.
- A change to a wire contract updates the contract file and its own SemVer in the same PR
  (§XVII), and a backward-incompatible contract change is a platform MAJOR.
- Release tags are monotonic — a new tag must be greater than the highest existing tag
  (enforced by `release-guard` in `.github/workflows/build-images.yml`).
