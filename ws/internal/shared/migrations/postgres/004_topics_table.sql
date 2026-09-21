-- ADR-0006 Phase 2 (slice 1): a durable topics table is the provisioned-topics
-- source of truth, replacing provisioning's volatile in-memory noop admin map.
-- Routing-rule existence checks (TOPIC_NOT_PROVISIONED) resolve against these
-- rows, so validation is restart-safe: a provisioning restart no longer wipes
-- the record of which topics a tenant has provisioned. The DLQ ('dead-letter')
-- is infrastructure, physically created but never a user topic — it is excluded
-- from this table (CHECK) and from the topic quota.

CREATE TABLE topics (
    tenant_id  UUID        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    suffix     TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, suffix),
    CONSTRAINT topic_suffix_not_empty CHECK (char_length(suffix) > 0),
    -- 'default' (deterministic, validated implicitly) and 'dead-letter' (infra)
    -- are never stored here; the composite PK already serves tenant-scoped scans.
    CONSTRAINT topic_suffix_not_default CHECK (suffix <> 'default'),
    CONSTRAINT topic_suffix_not_dead_letter CHECK (suffix <> 'dead-letter')
);

-- Backfill so existing tenants' rules stay valid across the cutover. The
-- per-tenant 'default' topic is deterministic (ADR-0006: the consume/create set
-- is {default} ∪ {rule suffixes}) and is validated implicitly, so it is NOT
-- stored here — only NON-default provisioned topics get rows. Every non-default
-- ingress/egress topic a routing rule already references is recorded (those
-- topics passed the existence check when the rule was created, so they are
-- provisioned). All tenants, not only active ones — a suspended tenant
-- reactivated later must keep its topics.
INSERT INTO topics (tenant_id, suffix)
    SELECT tenant_id, ingress_topic FROM tenant_routing_rules
    WHERE ingress_topic NOT IN ('default', 'dead-letter')
ON CONFLICT DO NOTHING;

INSERT INTO topics (tenant_id, suffix)
    SELECT tenant_id, egress FROM tenant_routing_rules, unnest(egress_topics) AS egress
    WHERE egress NOT IN ('default', 'dead-letter')
ON CONFLICT DO NOTHING;
