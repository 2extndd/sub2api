-- Store streaming monitor time-to-first-token samples separately from full dialog latency.
-- Existing history is intentionally left NULL: TTFT is only meaningful for probes
-- that observed a textual streaming delta after this migration.
ALTER TABLE channel_monitor_histories
    ADD COLUMN IF NOT EXISTS first_token_ms INT;

ALTER TABLE channel_monitor_daily_rollups
    ADD COLUMN IF NOT EXISTS sum_first_token_ms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS count_first_token_ms INT NOT NULL DEFAULT 0;
