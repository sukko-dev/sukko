-- Migration 005: Add 'admin' to the provisioning_audit.actor_type CHECK constraint.
--
-- ActorTypeAdmin ("admin") is a code-level actor type (provisioning/types.go) set for
-- admin-authenticated requests (provisioning/api/middleware.go). The initial schema's
-- valid_actor_type constraint (001) only permitted 'user', 'system', 'api_key', so every
-- admin-performed audit insert — set_channel_rules, revoke_key, deprovision_tenant, and the
-- other admin operations — failed with a CHECK violation (SQLSTATE 23514) and the entry was
-- silently dropped (the error is logged, the operation still succeeds). Security-relevant
-- operations were therefore NOT persisted to the audit trail, violating Constitution §IX
-- (Audit Trail). Widen the constraint so admin actions are recorded.

ALTER TABLE provisioning_audit DROP CONSTRAINT valid_actor_type;
ALTER TABLE provisioning_audit ADD CONSTRAINT valid_actor_type
    CHECK (actor_type IN ('user', 'system', 'api_key', 'admin'));
