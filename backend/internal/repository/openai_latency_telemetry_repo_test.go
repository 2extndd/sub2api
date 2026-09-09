package repository

import (
	"context"
	"database/sql/driver"
	"errors"
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func openAILatencyTelemetryRollupRepositoryTestRollup() service.OpenAILatencyTelemetryRollup {
	bucketStart := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	key := service.OpenAILatencyTelemetryKey{
		AccountID:   71,
		Cohort:      service.UnknownCohortKey,
		AttemptRole: service.OpenAIAttemptRolePrimary,
	}
	payload := service.OpenAILatencyTelemetrySnapshot{
		SchemaVersion:  service.OpenAILatencyTelemetrySchemaVersion,
		Key:            key,
		SourceID:       0,
		Generation:     1,
		Sequence:       1,
		BucketStart:    bucketStart.UnixNano(),
		BucketDuration: int64(15 * time.Minute),
	}
	payload.Phases[service.OpenAILatencyPhaseTotal].Observations = 17
	return service.OpenAILatencyTelemetryRollup{
		BucketStart:         bucketStart,
		BucketSeconds:       service.OpenAILatencyTelemetry15MinuteRollupSeconds,
		Key:                 key,
		Payload:             payload,
		SourceCount:         2,
		SampleCount:         math.MaxUint64,
		CoverageStart:       bucketStart,
		FreshThrough:        bucketStart.Add(15 * time.Minute),
		CoveredBucketCount:  15,
		ExpectedBucketCount: 15,
		Complete:            true,
		FinalizedAt:         bucketStart.Add(16 * time.Minute),
	}
}

func openAILatencyTelemetrySQLMockValues(values []any) []driver.Value {
	result := make([]driver.Value, len(values))
	for index, value := range values {
		result[index] = value
	}
	return result
}

func TestOpenAILatencyTelemetryRollupRepositoryUpsertIsAbsoluteAndIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := &openAILatencyTelemetryRollupRepository{db: db}
	rollup := openAILatencyTelemetryRollupRepositoryTestRollup()
	prepared, err := prepareOpenAILatencyTelemetryRollups([]service.OpenAILatencyTelemetryRollup{rollup})
	require.NoError(t, err)
	query, args := buildOpenAILatencyTelemetryRollupUpsert(prepared)
	require.Contains(t, query, "ON CONFLICT (bucket_start, bucket_seconds, account_id, cohort, attempt_role)")
	require.Contains(t, query, "aggregate_payload = EXCLUDED.aggregate_payload")
	require.Contains(t, query, "sample_count = EXCLUDED.sample_count")
	require.Contains(t, query, "CASE WHEN current_rollup.is_complete THEN 1 ELSE 0 END")
	require.Contains(t, query, "current_rollup.covered_bucket_count")
	require.Contains(t, query, `WHERE (
    CASE WHEN EXCLUDED.is_complete THEN 1 ELSE 0 END,
    EXCLUDED.covered_bucket_count,
    EXCLUDED.fresh_through,
    EXCLUDED.sample_count,
    EXCLUDED.source_count,
    EXCLUDED.finalized_at
) >= (
    CASE WHEN current_rollup.is_complete THEN 1 ELSE 0 END,
    current_rollup.covered_bucket_count,
    current_rollup.fresh_through,
    current_rollup.sample_count,
    current_rollup.source_count,
    current_rollup.finalized_at
)`)
	require.Equal(t, "18446744073709551615", args[8], "uint64 counts must be passed losslessly as numeric text")

	execPattern := `(?s)INSERT INTO openai_latency_telemetry_rollups .*ON CONFLICT .*DO UPDATE SET.*aggregate_payload = EXCLUDED\.aggregate_payload`
	for range 2 {
		mock.ExpectBegin()
		mock.ExpectExec(execPattern).
			WithArgs(openAILatencyTelemetrySQLMockValues(args)...).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}
	require.NoError(t, repo.UpsertOpenAILatencyTelemetryRollups(context.Background(), []service.OpenAILatencyTelemetryRollup{rollup}))
	require.NoError(t, repo.UpsertOpenAILatencyTelemetryRollups(context.Background(), []service.OpenAILatencyTelemetryRollup{rollup}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOpenAILatencyTelemetryRollupRepositoryRollsBackFailedChunk(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := &openAILatencyTelemetryRollupRepository{db: db}
	mock.ExpectBegin()
	mock.ExpectExec(`(?s)INSERT INTO openai_latency_telemetry_rollups`).
		WillReturnError(errors.New("database unavailable"))
	mock.ExpectRollback()

	err = repo.UpsertOpenAILatencyTelemetryRollups(
		context.Background(),
		[]service.OpenAILatencyTelemetryRollup{openAILatencyTelemetryRollupRepositoryTestRollup()},
	)
	require.ErrorContains(t, err, "database unavailable")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOpenAILatencyTelemetryRollupRepositoryRetentionDeletesByDuration(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := &openAILatencyTelemetryRollupRepository{db: db}
	before15Minute := time.Date(2026, 7, 11, 12, 0, 0, 0, time.FixedZone("test", 3*60*60))
	beforeHourly := time.Date(2026, 2, 11, 12, 0, 0, 0, time.FixedZone("test", -5*60*60))
	deletePattern := regexp.QuoteMeta(`
DELETE FROM openai_latency_telemetry_rollups
WHERE bucket_seconds = $1
  AND bucket_start < $2`)

	mock.ExpectExec(deletePattern).
		WithArgs(service.OpenAILatencyTelemetry15MinuteRollupSeconds, before15Minute.UTC()).
		WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectExec(deletePattern).
		WithArgs(service.OpenAILatencyTelemetryHourlyRollupSeconds, beforeHourly.UTC()).
		WillReturnResult(sqlmock.NewResult(0, 7))

	deleted, err := repo.DeleteOpenAILatencyTelemetryRollupsBefore(
		context.Background(),
		service.OpenAILatencyTelemetry15MinuteRollupSeconds,
		before15Minute,
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), deleted)
	deleted, err = repo.DeleteOpenAILatencyTelemetryRollupsBefore(
		context.Background(),
		service.OpenAILatencyTelemetryHourlyRollupSeconds,
		beforeHourly,
	)
	require.NoError(t, err)
	require.Equal(t, int64(7), deleted)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOpenAILatencyTelemetryRollupRepositoryRejectsInvalidRetentionWithoutSQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := &openAILatencyTelemetryRollupRepository{db: db}

	_, err = repo.DeleteOpenAILatencyTelemetryRollupsBefore(context.Background(), 60, time.Now())
	require.ErrorContains(t, err, "900 or 3600")
	require.NoError(t, mock.ExpectationsWereMet())
}
