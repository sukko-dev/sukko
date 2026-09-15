-- Migration 002: Analytics tables as range-partitioned parents with upsert support.
-- Replaces the unpartitioned tables from 001_initial.sql.
-- Greenfield — no existing data to migrate.

-- ============================================================
-- Drop 001 tables and recreate as partitioned parents
-- ============================================================

DROP TABLE IF EXISTS analytics_connections   CASCADE;
DROP TABLE IF EXISTS analytics_messages      CASCADE;
DROP TABLE IF EXISTS analytics_push          CASCADE;
DROP TABLE IF EXISTS analytics_push_patterns CASCADE;
DROP TABLE IF EXISTS analytics_raw_events    CASCADE;

-- ============================================================
-- Minute-level pod tables (daily partitions)
-- ============================================================

CREATE TABLE analytics_connections (
    pod_id           VARCHAR(255)  NOT NULL,
    tenant_id        UUID          NOT NULL,
    bucket_start     TIMESTAMPTZ   NOT NULL,
    bucket_size      VARCHAR(10)   NOT NULL,
    transport        VARCHAR(20)   NOT NULL,
    active_count     INT           NOT NULL DEFAULT 0,
    connect_count    INT           NOT NULL DEFAULT 0,
    disconnect_count INT           NOT NULL DEFAULT 0,
    error_count      INT           NOT NULL DEFAULT 0
) PARTITION BY RANGE (bucket_start);

CREATE UNIQUE INDEX uq_analytics_connections
    ON analytics_connections(pod_id, tenant_id, bucket_start, bucket_size, transport);
CREATE INDEX idx_analytics_connections_tenant_time
    ON analytics_connections(tenant_id, bucket_start, bucket_size);

CREATE TABLE analytics_messages (
    pod_id           VARCHAR(255)  NOT NULL,
    tenant_id        UUID          NOT NULL,
    bucket_start     TIMESTAMPTZ   NOT NULL,
    bucket_size      VARCHAR(10)   NOT NULL,
    channel_prefix   VARCHAR(100)  NOT NULL,
    published_count  INT           NOT NULL DEFAULT 0,
    delivered_count  INT           NOT NULL DEFAULT 0,
    failed_count     INT           NOT NULL DEFAULT 0,
    total_latency_ms BIGINT        NOT NULL DEFAULT 0,
    sample_count     INT           NOT NULL DEFAULT 0
) PARTITION BY RANGE (bucket_start);

CREATE UNIQUE INDEX uq_analytics_messages
    ON analytics_messages(pod_id, tenant_id, bucket_start, bucket_size, channel_prefix);
CREATE INDEX idx_analytics_messages_tenant_time
    ON analytics_messages(tenant_id, bucket_start, bucket_size);

CREATE TABLE analytics_push (
    pod_id             VARCHAR(255)  NOT NULL,
    tenant_id          UUID          NOT NULL,
    bucket_start       TIMESTAMPTZ   NOT NULL,
    bucket_size        VARCHAR(10)   NOT NULL,
    provider           VARCHAR(20)   NOT NULL,
    sent_count         INT           NOT NULL DEFAULT 0,
    success_count      INT           NOT NULL DEFAULT 0,
    failed_count       INT           NOT NULL DEFAULT 0,
    expired_count      INT           NOT NULL DEFAULT 0,
    rate_limited_count INT           NOT NULL DEFAULT 0,
    total_latency_ms   BIGINT        NOT NULL DEFAULT 0,
    sample_count       INT           NOT NULL DEFAULT 0
) PARTITION BY RANGE (bucket_start);

CREATE UNIQUE INDEX uq_analytics_push
    ON analytics_push(pod_id, tenant_id, bucket_start, bucket_size, provider);
CREATE INDEX idx_analytics_push_tenant_time
    ON analytics_push(tenant_id, bucket_start, bucket_size);

-- push_patterns: minute-level only, no hour/day rollup (48h retention sufficient)
CREATE TABLE analytics_push_patterns (
    pod_id        VARCHAR(255) NOT NULL,
    tenant_id     UUID         NOT NULL,
    bucket_start  TIMESTAMPTZ  NOT NULL,
    bucket_size   VARCHAR(10)  NOT NULL,
    pattern       VARCHAR(255) NOT NULL,
    match_count   INT          NOT NULL DEFAULT 0,
    device_count  INT          NOT NULL DEFAULT 0
) PARTITION BY RANGE (bucket_start);

CREATE UNIQUE INDEX uq_analytics_push_patterns
    ON analytics_push_patterns(pod_id, tenant_id, bucket_start, bucket_size, pattern);
CREATE INDEX idx_analytics_push_patterns_tenant_time
    ON analytics_push_patterns(tenant_id, bucket_start, bucket_size);

CREATE TABLE analytics_raw_events (
    pod_id      VARCHAR(255) NOT NULL,
    tenant_id   UUID         NOT NULL,
    event_type  VARCHAR(100) NOT NULL,
    event_data  JSONB,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (created_at);

CREATE INDEX idx_analytics_raw_events_tenant
    ON analytics_raw_events(tenant_id, created_at);

-- ============================================================
-- Hour rollup tables (daily partitions, 30d retention)
-- No pod_id — cluster-wide aggregates across all pods.
-- ============================================================

CREATE TABLE analytics_connections_hour (
    tenant_id        UUID          NOT NULL,
    bucket_start     TIMESTAMPTZ   NOT NULL,
    bucket_size      VARCHAR(10)   NOT NULL DEFAULT 'hour',
    transport        VARCHAR(20)   NOT NULL,
    active_count     BIGINT        NOT NULL DEFAULT 0,
    connect_count    BIGINT        NOT NULL DEFAULT 0,
    disconnect_count BIGINT        NOT NULL DEFAULT 0,
    error_count      BIGINT        NOT NULL DEFAULT 0
) PARTITION BY RANGE (bucket_start);

CREATE UNIQUE INDEX uq_analytics_connections_hour
    ON analytics_connections_hour(tenant_id, bucket_start, bucket_size, transport);
CREATE INDEX idx_analytics_connections_hour_tenant
    ON analytics_connections_hour(tenant_id, bucket_start);

CREATE TABLE analytics_messages_hour (
    tenant_id        UUID          NOT NULL,
    bucket_start     TIMESTAMPTZ   NOT NULL,
    bucket_size      VARCHAR(10)   NOT NULL DEFAULT 'hour',
    channel_prefix   VARCHAR(100)  NOT NULL,
    published_count  BIGINT        NOT NULL DEFAULT 0,
    delivered_count  BIGINT        NOT NULL DEFAULT 0,
    failed_count     BIGINT        NOT NULL DEFAULT 0,
    total_latency_ms BIGINT        NOT NULL DEFAULT 0,
    sample_count     BIGINT        NOT NULL DEFAULT 0
) PARTITION BY RANGE (bucket_start);

CREATE UNIQUE INDEX uq_analytics_messages_hour
    ON analytics_messages_hour(tenant_id, bucket_start, bucket_size, channel_prefix);
CREATE INDEX idx_analytics_messages_hour_tenant
    ON analytics_messages_hour(tenant_id, bucket_start);

CREATE TABLE analytics_push_hour (
    tenant_id          UUID          NOT NULL,
    bucket_start       TIMESTAMPTZ   NOT NULL,
    bucket_size        VARCHAR(10)   NOT NULL DEFAULT 'hour',
    provider           VARCHAR(20)   NOT NULL,
    sent_count         BIGINT        NOT NULL DEFAULT 0,
    success_count      BIGINT        NOT NULL DEFAULT 0,
    failed_count       BIGINT        NOT NULL DEFAULT 0,
    expired_count      BIGINT        NOT NULL DEFAULT 0,
    rate_limited_count BIGINT        NOT NULL DEFAULT 0,
    total_latency_ms   BIGINT        NOT NULL DEFAULT 0,
    sample_count       BIGINT        NOT NULL DEFAULT 0
) PARTITION BY RANGE (bucket_start);

CREATE UNIQUE INDEX uq_analytics_push_hour
    ON analytics_push_hour(tenant_id, bucket_start, bucket_size, provider);
CREATE INDEX idx_analytics_push_hour_tenant
    ON analytics_push_hour(tenant_id, bucket_start);

-- ============================================================
-- Day rollup tables (monthly partitions, 365d retention)
-- ============================================================

CREATE TABLE analytics_connections_day (
    tenant_id        UUID          NOT NULL,
    bucket_start     TIMESTAMPTZ   NOT NULL,
    bucket_size      VARCHAR(10)   NOT NULL DEFAULT 'day',
    transport        VARCHAR(20)   NOT NULL,
    active_count     BIGINT        NOT NULL DEFAULT 0,
    connect_count    BIGINT        NOT NULL DEFAULT 0,
    disconnect_count BIGINT        NOT NULL DEFAULT 0,
    error_count      BIGINT        NOT NULL DEFAULT 0
) PARTITION BY RANGE (bucket_start);

CREATE UNIQUE INDEX uq_analytics_connections_day
    ON analytics_connections_day(tenant_id, bucket_start, bucket_size, transport);
CREATE INDEX idx_analytics_connections_day_tenant
    ON analytics_connections_day(tenant_id, bucket_start);

CREATE TABLE analytics_messages_day (
    tenant_id        UUID          NOT NULL,
    bucket_start     TIMESTAMPTZ   NOT NULL,
    bucket_size      VARCHAR(10)   NOT NULL DEFAULT 'day',
    channel_prefix   VARCHAR(100)  NOT NULL,
    published_count  BIGINT        NOT NULL DEFAULT 0,
    delivered_count  BIGINT        NOT NULL DEFAULT 0,
    failed_count     BIGINT        NOT NULL DEFAULT 0,
    total_latency_ms BIGINT        NOT NULL DEFAULT 0,
    sample_count     BIGINT        NOT NULL DEFAULT 0
) PARTITION BY RANGE (bucket_start);

CREATE UNIQUE INDEX uq_analytics_messages_day
    ON analytics_messages_day(tenant_id, bucket_start, bucket_size, channel_prefix);
CREATE INDEX idx_analytics_messages_day_tenant
    ON analytics_messages_day(tenant_id, bucket_start);

CREATE TABLE analytics_push_day (
    tenant_id          UUID          NOT NULL,
    bucket_start       TIMESTAMPTZ   NOT NULL,
    bucket_size        VARCHAR(10)   NOT NULL DEFAULT 'day',
    provider           VARCHAR(20)   NOT NULL,
    sent_count         BIGINT        NOT NULL DEFAULT 0,
    success_count      BIGINT        NOT NULL DEFAULT 0,
    failed_count       BIGINT        NOT NULL DEFAULT 0,
    expired_count      BIGINT        NOT NULL DEFAULT 0,
    rate_limited_count BIGINT        NOT NULL DEFAULT 0,
    total_latency_ms   BIGINT        NOT NULL DEFAULT 0,
    sample_count       BIGINT        NOT NULL DEFAULT 0
) PARTITION BY RANGE (bucket_start);

CREATE UNIQUE INDEX uq_analytics_push_day
    ON analytics_push_day(tenant_id, bucket_start, bucket_size, provider);
CREATE INDEX idx_analytics_push_day_tenant
    ON analytics_push_day(tenant_id, bucket_start);

-- ============================================================
-- Default partitions (catch-all until PartitionManager runs)
-- The provisioning AnalyticsManager creates named day/month partitions
-- on startup and every ANALYTICS_PARTMAN_INTERVAL thereafter.
-- ============================================================

CREATE TABLE analytics_connections_default      PARTITION OF analytics_connections      DEFAULT;
CREATE TABLE analytics_messages_default         PARTITION OF analytics_messages         DEFAULT;
CREATE TABLE analytics_push_default             PARTITION OF analytics_push             DEFAULT;
CREATE TABLE analytics_push_patterns_default    PARTITION OF analytics_push_patterns    DEFAULT;
CREATE TABLE analytics_raw_events_default       PARTITION OF analytics_raw_events       DEFAULT;
CREATE TABLE analytics_connections_hour_default PARTITION OF analytics_connections_hour DEFAULT;
CREATE TABLE analytics_messages_hour_default    PARTITION OF analytics_messages_hour    DEFAULT;
CREATE TABLE analytics_push_hour_default        PARTITION OF analytics_push_hour        DEFAULT;
CREATE TABLE analytics_connections_day_default  PARTITION OF analytics_connections_day  DEFAULT;
CREATE TABLE analytics_messages_day_default     PARTITION OF analytics_messages_day     DEFAULT;
CREATE TABLE analytics_push_day_default         PARTITION OF analytics_push_day         DEFAULT;
