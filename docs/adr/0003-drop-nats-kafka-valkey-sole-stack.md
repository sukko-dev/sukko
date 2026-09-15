# ADR-0003: Drop NATS — Kafka + Valkey are the sole supported infrastructure stack

**Status**: Accepted
**Date**: 2026-05-31
**Ticket**: refactor/drop-nats

## Context

Sukko exposed two pluggable infrastructure choices per server deployment: a message backend (`MESSAGE_BACKEND`: `direct` or `kafka`) and a broadcast bus (`BROADCAST_TYPE`), where NATS was a selectable option alongside Valkey. In practice only the Kafka + Valkey stack was production-tested, documented, and deployed. The NATS paths carried two confirmed blocking defects in `JetStreamBackend` (found during the tester-backend-parity work), had zero integration-test coverage, and required standing up a third infrastructure service alongside the already-mandatory Kafka and Valkey. Maintaining two parallel stacks multiplied CI surface, complicated onboarding, and left the known defects in scope-ambiguity limbo. The system is greenfield (no production NATS deployment to migrate).

## Decision

NATS support is removed entirely. All NATS config struct fields, env-var definitions, implementation files, and feature-gate entries are deleted — the removal policy is explicit: no retained fields, no retained implementation files, no retained feature-gate entries. Valid values become `MESSAGE_BACKEND` ∈ {`direct`, `kafka`} and `BROADCAST_TYPE` ∈ {`valkey`}. `MESSAGE_BACKEND` still defaults to `direct` (the zero-dependency option for quick local testing without Kafka) and `BROADCAST_TYPE` still defaults to `valkey`. A `nats` value is rejected at config validation with an actionable startup error that names the env var and lists the valid alternatives; the tester and CLI additionally reject `nats` client-side (via the advertised capabilities list) and, if bypassed, per-request with HTTP 400.

## Consequences

One supported stack to test, document, and operate. Valkey is always required — the broadcast bus is always external, and even the `direct` backend routes messages through it — while Kafka is needed only when `MESSAGE_BACKEND=kafka`, so a `direct` + Valkey deployment can run with no Kafka/Redpanda. Operators who previously set `MESSAGE_BACKEND=nats` or `BROADCAST_TYPE=nats` fail fast at startup rather than degrading silently. Existing `kafka` + `valkey` deployments are unaffected. Re-adding NATS in a future release is a full re-implementation (config, impl, tests, gate) — there is deliberately no dormant code path to reactivate — which is the intended cost of committing to a single stack.

## Alternatives rejected

- **Maintain the dual (NATS + Kafka/Valkey) stack** — multiplies CI surface, complicates onboarding, requires a third infra service, and keeps the known JetStream defects in permanent scope-ambiguity.
- **Retain NATS config fields/impl behind a disabled feature gate** — violates the removal policy (no retained fields/files/gate entries) and leaves untested, defective code paths latent in the binary.
