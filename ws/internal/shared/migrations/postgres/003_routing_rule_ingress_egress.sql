-- Migration 003: Split routing-rule topics into an explicit ingress topic and
-- egress-only topics (ADR-0018).
--
-- ingress_topic — the single topic suffix the platform consumes; its records
--                  are delivered to subscribers, replayed, and served as history.
-- egress_topics  — additional copies for external consumers; created on the
--                  broker but NEVER consumed.
--
-- Legacy rows must be SANITIZED, not merely reshaped. Pre-split validation had
-- no reserved-suffix and no cross-rule checks, so a stored row may hold shapes
-- the new write path rejects. Carrying them across verbatim would reinstate the
-- two defects ADR-0018 exists to remove:
--
--   * 'dead-letter' as the head topic would become the ingress topic, putting
--     the tenant DLQ into the consume set and DELIVERING DEAD-LETTERED RECORDS
--     TO SUBSCRIBERS.
--   * 'default' (or another rule's ingress topic) among the tail topics would
--     become an egress topic that the platform also consumes, so every publish
--     would be DELIVERED TWICE — with different message identities, so clients
--     could not deduplicate it.
--
-- The produce path carries a defence-in-depth guard for the same two suffixes
-- (§II), so neither hazard can be reintroduced by a row that reaches the table
-- by any other route.

-- 1. Rows with an empty topics array were never valid (writes always required
--    >= 1 topic) and cannot yield a NOT NULL ingress topic.
DELETE FROM tenant_routing_rules WHERE cardinality(topics) = 0;

-- 2. Rows whose topics are ALL reserved have no legitimate ingress topic. The
--    rule cannot be repaired, so it is removed; its channels become unroutable
--    and publishes fail loudly with 409 PUBLISH_NOT_ROUTABLE rather than
--    silently delivering the wrong records.
DELETE FROM tenant_routing_rules
WHERE NOT EXISTS (
    SELECT 1 FROM unnest(topics) AS t
    WHERE t NOT IN ('default', 'dead-letter')
);

ALTER TABLE tenant_routing_rules
    ADD COLUMN ingress_topic TEXT,
    ADD COLUMN egress_topics  TEXT[];

-- 3. Ingress = the first topic that is legal as an ingress topic. 'dead-letter'
--    is never consumable; 'default' IS a valid ingress topic (it is the topic a
--    rule-less tenant ingests on), so it is excluded only from egress.
UPDATE tenant_routing_rules SET
    ingress_topic = (
        SELECT t FROM unnest(topics) WITH ORDINALITY AS u(t, ord)
        WHERE t <> 'dead-letter'
        ORDER BY ord
        LIMIT 1
    );

-- 4. Egress = the remaining topics, deduplicated, excluding the chosen ingress
--    topic and both reserved suffixes. 'default' is always consumed and
--    'dead-letter' is never consumed, so neither may be an egress target.
UPDATE tenant_routing_rules SET
    egress_topics = COALESCE(
        (
            SELECT array_agg(DISTINCT t ORDER BY t)
            FROM unnest(topics) AS t
            WHERE t <> ingress_topic
              AND t NOT IN ('default', 'dead-letter')
        ),
        '{}'
    );

-- 5. Cross-rule disjointness: an egress topic that is any rule's ingress topic
--    for the same tenant would be consumed, so strip it. Enforced at write time
--    tenant-wide by ValidateEgressDisjoint; stored rows predate that check.
UPDATE tenant_routing_rules r SET
    egress_topics = COALESCE(
        (
            SELECT array_agg(t ORDER BY t)
            FROM unnest(r.egress_topics) AS t
            WHERE NOT EXISTS (
                SELECT 1 FROM tenant_routing_rules o
                WHERE o.tenant_id = r.tenant_id
                  AND o.ingress_topic = t
            )
        ),
        '{}'
    )
WHERE cardinality(r.egress_topics) > 0;

ALTER TABLE tenant_routing_rules
    ALTER COLUMN ingress_topic SET NOT NULL,
    ALTER COLUMN egress_topics  SET NOT NULL,
    ALTER COLUMN egress_topics  SET DEFAULT '{}',
    DROP COLUMN topics;

ALTER TABLE tenant_routing_rules
    ADD CONSTRAINT ingress_topic_not_empty CHECK (char_length(ingress_topic) > 0),
    ADD CONSTRAINT ingress_topic_not_reserved CHECK (ingress_topic <> 'dead-letter');
