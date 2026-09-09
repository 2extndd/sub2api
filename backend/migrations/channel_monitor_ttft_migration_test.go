package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigration197AddsNullableTTFTAndRollupCounters(t *testing.T) {
	content, err := FS.ReadFile("197_channel_monitor_ttft.sql")
	require.NoError(t, err)
	sql := string(content)

	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS first_token_ms INT")
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS sum_first_token_ms BIGINT NOT NULL DEFAULT 0")
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS count_first_token_ms INT NOT NULL DEFAULT 0")
	require.Equal(t, 3, strings.Count(sql, "ADD COLUMN IF NOT EXISTS"))
	require.NotContains(t, strings.ToUpper(sql), "UPDATE CHANNEL_MONITOR_HISTORIES")
}
