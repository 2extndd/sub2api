//go:build unit

package repository

import (
	"database/sql"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpsInsertErrorLogArgsPreservesExplicitZeroUpstreamStatus(t *testing.T) {
	zero := 0
	args := opsInsertErrorLogArgs(&service.OpsInsertErrorLogInput{UpstreamStatusCode: &zero})

	require.Len(t, args, 42)
	encoded, ok := args[27].(sql.NullInt64)
	require.True(t, ok)
	require.True(t, encoded.Valid)
	require.Zero(t, encoded.Int64)
}

func TestOpsInsertErrorLogArgsPreservesFailureDiagnostics(t *testing.T) {
	code := "rate_limit_exceeded"
	typeName := "rate_limit_error"
	network := "timeout"
	retryAfter := 7
	args := opsInsertErrorLogArgs(&service.OpsInsertErrorLogInput{
		ProviderErrorCode: &code,
		ProviderErrorType: &typeName,
		NetworkErrorType:  &network,
		RetryAfterSeconds: &retryAfter,
	})

	codeArg, ok := args[30].(sql.NullString)
	require.True(t, ok)
	require.Equal(t, "rate_limit_exceeded", codeArg.String)
	require.True(t, codeArg.Valid)
	typeArg, ok := args[31].(sql.NullString)
	require.True(t, ok)
	require.Equal(t, "rate_limit_error", typeArg.String)
	require.True(t, typeArg.Valid)
	networkArg, ok := args[32].(sql.NullString)
	require.True(t, ok)
	require.Equal(t, "timeout", networkArg.String)
	require.True(t, networkArg.Valid)
	encoded, ok := args[33].(sql.NullInt64)
	require.True(t, ok)
	require.True(t, encoded.Valid)
	require.EqualValues(t, 7, encoded.Int64)
}

func TestOpsNullableIntPointerDistinguishesNilZeroAndStatus(t *testing.T) {
	missing := opsNullableIntPointer(nil).(sql.NullInt64)
	require.False(t, missing.Valid)

	zeroValue := 0
	zero := opsNullableIntPointer(&zeroValue).(sql.NullInt64)
	require.True(t, zero.Valid)
	require.Zero(t, zero.Int64)

	statusValue := 503
	status := opsNullableIntPointer(&statusValue).(sql.NullInt64)
	require.True(t, status.Valid)
	require.EqualValues(t, 503, status.Int64)
}
