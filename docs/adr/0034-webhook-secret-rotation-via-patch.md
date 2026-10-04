# ADR-0034: Webhook HMAC secret rotation is an operator-provided PATCH field

**Status**: Accepted
**Date**: 2026-10-03

## Context

The webhook management API let operators change a webhook's URL, channel pattern,
max-retries, and status via `PATCH`, but **not** its HMAC signing secret — the only
way to change a compromised secret was delete + recreate (which loses the webhook's
ID and history). The platform constitution (§IX) mandates that all credentials be
rotatable via the API without downtime; webhook secrets were the gap. (Surfaced by
the CLI review, which had to remove `webhook update --secret` because the server
silently dropped it — ADR-0033 aftermath.)

The webhook secret is stored encrypted at rest (`crypto.EncryptCredential`,
`CREDENTIALS_ENCRYPTION_KEY`); the webhook-worker caches the ciphertext per webhook
and decrypts it to HMAC-sign each delivery. The update path already publishes a
Valkey cache invalidation, so any persisted change propagates to the worker on its
next refresh (the same propagation window ADR-0032 documents).

## Decision

Add the secret as an optional `PATCH` field.

- **`secret` on the update request**, operator-provided, validated non-empty
  exactly as on create. The service encrypts it (`EncryptCredential`) and the
  repository persists the new `secret_enc` via one more dynamic `SET` clause.
  Encryption happens at the service layer — the plaintext never reaches the repo,
  which reads only the ciphertext field (mirrors create).
- **Distinct audit action.** A rotation logs `rotate_webhook_secret` (with
  `webhook_id`, never the secret); non-secret fields in the same request still log
  `update_webhook`. A `PATCH` carrying both produces two audit entries — the
  rotation must be independently findable in the trail (§IX: credential rotation
  MUST be audit-logged).
- **Propagation via the existing invalidation.** The worker re-reads `secret_enc`
  on each refresh and decrypts per delivery, so the next delivery dispatched after
  the invalidation signs with the new secret. The window where the old secret is
  still in use is the ADR-0032 invalidation/TTL propagation delay **plus the span of
  any already-in-flight retry chain**: a `DeliveryTask` snapshots `secret_enc` at
  task-creation time and its retries reuse that snapshot, so a delivery that began
  before the rotation keeps signing with the old secret across the full
  `webhookRetrySchedule` (1s, 5s, 30s, 2m, 10m ≈ 13 min). Newly-dispatched and
  degraded-poll deliveries read the refreshed secret. The destination MUST accept
  both secrets for at least this combined window.

## Consequences

- Operators rotate a secret in place, keeping the webhook ID and history.
- Contract change (§XVII): `UpdateWebhookRequest` in the provisioning OpenAPI gains
  `secret`; version bumped. This reverses the "PATCH has no secret" note added after
  the CLI review.
- Cross-repo follow-ups (§XVI), deferred and tracked (not in this PR): the CLI
  restores `webhook update --secret` / `--secret-file` (via its existing
  `resolveWebhookSecret`), and sukko-docs drops any "rotation requires delete +
  recreate" wording.
- Rotation is single-secret: Sukko signs with the new secret immediately on
  propagation. The destination is responsible for accepting both the old and new
  secret during its rotation window — the standard receiver-side pattern (Stripe,
  GitHub; §XI). Sukko does not implement dual-secret signing.

## Alternatives rejected

- **Dedicated `POST /webhooks/{id}/rotate-secret` endpoint.** Earns its surface only
  if rotation does something a field-update cannot — server-side generation,
  dual-secret state, or versioning. This rotation does none, so a separate endpoint
  is ceremony (§XV). The distinct audit action gives the §IX findability without it.
- **Server-generated secret returned once.** Inverts the required order: the
  destination must be configured with the secret before Sukko signs with it. An
  operator-provided secret lets the operator set the destination first, then rotate —
  and mirrors create.
- **Dual-secret signing (sign with old and new during an overlap).** Solves the
  receiver's problem in the sender; receivers already handle rotation by accepting
  multiple secrets.
