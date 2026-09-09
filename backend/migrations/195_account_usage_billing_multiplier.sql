-- Per-routing-account customer usage billing normalization.
-- Existing accounts and historical usage retain behavior through the 1.0 default.
-- Keep this startup migration metadata-only: usage_logs can be large and the
-- migration runner has a 60-second context. NOT VALID constraints still enforce
-- all new writes; existing rows receive the immutable 1.0 fast default.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '55s';

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS usage_billing_multiplier NUMERIC(20,10) NOT NULL DEFAULT 1.0;

ALTER TABLE accounts
    DROP CONSTRAINT IF EXISTS accounts_usage_billing_multiplier_positive;
ALTER TABLE accounts
    ADD CONSTRAINT accounts_usage_billing_multiplier_positive
    CHECK (usage_billing_multiplier > 0 AND usage_billing_multiplier <= 1000000) NOT VALID;

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS usage_billing_multiplier NUMERIC(20,10) NOT NULL DEFAULT 1.0;

ALTER TABLE usage_logs
    DROP CONSTRAINT IF EXISTS usage_logs_usage_billing_multiplier_positive;
ALTER TABLE usage_logs
    ADD CONSTRAINT usage_logs_usage_billing_multiplier_positive
    CHECK (usage_billing_multiplier > 0 AND usage_billing_multiplier <= 1000000) NOT VALID;

ALTER TABLE batch_image_jobs
    ADD COLUMN IF NOT EXISTS usage_billing_multiplier NUMERIC(20,10) NOT NULL DEFAULT 1.0;

ALTER TABLE batch_image_jobs
    DROP CONSTRAINT IF EXISTS batch_image_jobs_usage_billing_multiplier_positive;
ALTER TABLE batch_image_jobs
    ADD CONSTRAINT batch_image_jobs_usage_billing_multiplier_positive
    CHECK (usage_billing_multiplier > 0 AND usage_billing_multiplier <= 1000000) NOT VALID;

COMMENT ON COLUMN accounts.usage_billing_multiplier IS
    'Positive multiplier applied once to final customer usage billing for this routing account; independent of account cost multiplier';
COMMENT ON COLUMN usage_logs.usage_billing_multiplier IS
    'Snapshot of the final successful routing account usage billing multiplier';
COMMENT ON COLUMN batch_image_jobs.usage_billing_multiplier IS
    'Submission-time customer billing multiplier snapshot for the selected routing account';
