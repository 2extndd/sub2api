package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigration196DefinesRelationalDenyPolicyWithCascadeCleanup(t *testing.T) {
	content, err := FS.ReadFile("196_user_account_denials.sql")
	require.NoError(t, err)
	sql := string(content)

	require.Contains(t, sql, "SET LOCAL lock_timeout = '5s'")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS user_account_denial_policies")
	require.Contains(t, sql, "revision BIGINT NOT NULL DEFAULT 0")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS user_account_denials")
	require.Contains(t, sql, "PRIMARY KEY (user_id, account_id)")
	require.Equal(t, 3, strings.Count(sql, "ON DELETE CASCADE"))
	require.Contains(t, sql, "user_account_denials_account_id_idx")
}
