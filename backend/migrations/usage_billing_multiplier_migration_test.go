package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigration195AvoidsUnboundedStartupTableScans(t *testing.T) {
	content, err := FS.ReadFile("195_account_usage_billing_multiplier.sql")
	require.NoError(t, err)

	sql := string(content)
	require.Contains(t, sql, "SET LOCAL lock_timeout = '5s'")
	require.Contains(t, sql, "SET LOCAL statement_timeout = '55s'")
	require.NotContains(t, sql, "UPDATE accounts")
	require.NotContains(t, sql, "UPDATE usage_logs")
	require.NotContains(t, sql, "UPDATE batch_image_jobs")
	require.NotContains(t, sql, "VALIDATE CONSTRAINT")
	require.Equal(t, 3, strings.Count(sql, "NUMERIC(20,10) NOT NULL DEFAULT 1.0"))
	require.Equal(t, 3, strings.Count(sql, ") NOT VALID;"))
}
