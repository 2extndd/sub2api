package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigration194CreatesNumericSecretSafeTelemetryRollups(t *testing.T) {
	content, err := FS.ReadFile("194_openai_latency_telemetry_rollups.sql")
	require.NoError(t, err)
	sql := string(content)

	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS openai_latency_telemetry_rollups")
	require.Contains(t, sql, "bucket_start TIMESTAMPTZ NOT NULL")
	require.Contains(t, sql, "bucket_seconds INTEGER NOT NULL")
	require.Contains(t, sql, "account_id BIGINT NOT NULL")
	require.Contains(t, sql, "cohort INTEGER NOT NULL")
	require.Contains(t, sql, "attempt_role SMALLINT NOT NULL")
	require.Contains(t, sql, "aggregate_payload JSONB NOT NULL")
	require.Contains(t, sql, "source_count INTEGER NOT NULL")
	require.Contains(t, sql, "sample_count NUMERIC(20, 0) NOT NULL")
	require.Contains(t, sql, "coverage_start TIMESTAMPTZ NOT NULL")
	require.Contains(t, sql, "fresh_through TIMESTAMPTZ NOT NULL")
	require.Contains(t, sql, "covered_bucket_count INTEGER NOT NULL")
	require.Contains(t, sql, "expected_bucket_count INTEGER NOT NULL")
	require.Contains(t, sql, "is_complete BOOLEAN NOT NULL")
	require.Contains(t, sql, "finalized_at TIMESTAMPTZ NOT NULL")
	require.Contains(t, sql, "PRIMARY KEY")
	require.Contains(t, sql, "idx_openai_latency_telemetry_rollups_retention")
	require.Contains(t, sql, "idx_openai_latency_telemetry_rollups_account_bucket")
	require.Contains(t, sql, "idx_openai_latency_telemetry_rollups_fresh_through")
	require.Contains(t, sql, "chk_openai_latency_telemetry_bucket_coverage")
	require.Contains(t, sql, "chk_openai_latency_telemetry_window_coverage")

	lower := strings.ToLower(sql)
	for _, forbiddenColumn := range []string{
		"api_key text",
		"credential text",
		"raw_error text",
		"request_body text",
		"model_name text",
	} {
		require.NotContains(t, lower, forbiddenColumn)
	}
}
