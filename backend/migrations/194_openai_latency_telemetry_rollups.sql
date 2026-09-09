-- Durable OpenAI latency telemetry aggregates. Raw per-source snapshots remain
-- in Redis; this table stores only finalized absolute 15-minute/hourly payloads.
-- Every dimension is numeric and bounded so credentials, model names, request
-- content, and raw errors cannot become durable labels.

CREATE TABLE IF NOT EXISTS openai_latency_telemetry_rollups (
    bucket_start TIMESTAMPTZ NOT NULL,
    bucket_seconds INTEGER NOT NULL,
    account_id BIGINT NOT NULL,
    cohort INTEGER NOT NULL,
    attempt_role SMALLINT NOT NULL,
    schema_version INTEGER NOT NULL,
    aggregate_payload JSONB NOT NULL,
    source_count INTEGER NOT NULL,
    sample_count NUMERIC(20, 0) NOT NULL,
    coverage_start TIMESTAMPTZ NOT NULL,
    fresh_through TIMESTAMPTZ NOT NULL,
    covered_bucket_count INTEGER NOT NULL,
    expected_bucket_count INTEGER NOT NULL,
    is_complete BOOLEAN NOT NULL,
    finalized_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT pk_openai_latency_telemetry_rollups PRIMARY KEY (
        bucket_start,
        bucket_seconds,
        account_id,
        cohort,
        attempt_role
    ),
    CONSTRAINT chk_openai_latency_telemetry_bucket_seconds
        CHECK (bucket_seconds IN (900, 3600)),
    CONSTRAINT chk_openai_latency_telemetry_account_id
        CHECK (account_id > 0),
    CONSTRAINT chk_openai_latency_telemetry_cohort
        CHECK (cohort BETWEEN 0 AND 65535),
    CONSTRAINT chk_openai_latency_telemetry_attempt_role
        CHECK (attempt_role BETWEEN 0 AND 2),
    CONSTRAINT chk_openai_latency_telemetry_schema_version
        CHECK (schema_version > 0 AND schema_version <= 65535),
    CONSTRAINT chk_openai_latency_telemetry_payload_object
        CHECK (jsonb_typeof(aggregate_payload) = 'object'),
    CONSTRAINT chk_openai_latency_telemetry_source_count
        CHECK (source_count >= 0),
    CONSTRAINT chk_openai_latency_telemetry_sample_count
        CHECK (sample_count >= 0),
    CONSTRAINT chk_openai_latency_telemetry_bucket_coverage
        CHECK (
            covered_bucket_count > 0
            AND expected_bucket_count > 0
            AND covered_bucket_count <= expected_bucket_count
        ),
    CONSTRAINT chk_openai_latency_telemetry_window_coverage
        CHECK (
            coverage_start >= bucket_start
            AND fresh_through > coverage_start
            AND fresh_through <= bucket_start + bucket_seconds * INTERVAL '1 second'
            AND finalized_at >= fresh_through
        )
);

-- Retention deletes always constrain duration before the bucket cutoff.
CREATE INDEX IF NOT EXISTS idx_openai_latency_telemetry_rollups_retention
    ON openai_latency_telemetry_rollups (bucket_seconds, bucket_start);

-- Supports account-scoped trend/recommendation reads without scanning other
-- numeric dimensions or the JSONB aggregate payload.
CREATE INDEX IF NOT EXISTS idx_openai_latency_telemetry_rollups_account_bucket
    ON openai_latency_telemetry_rollups (account_id, bucket_seconds, bucket_start DESC);

-- Supports freshness diagnostics and stale-finalization inspection.
CREATE INDEX IF NOT EXISTS idx_openai_latency_telemetry_rollups_fresh_through
    ON openai_latency_telemetry_rollups (fresh_through);
