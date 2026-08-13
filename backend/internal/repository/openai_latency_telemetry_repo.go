package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

const openAILatencyTelemetryRollupUpsertChunkSize = 500

type openAILatencyTelemetryRollupRepository struct {
	db *sql.DB
}

var _ service.OpenAILatencyTelemetryRollupRepository = (*openAILatencyTelemetryRollupRepository)(nil)

// NewOpenAILatencyTelemetryRollupRepository creates the raw-SQL durable
// absolute-rollup store. No Ent schema is required for the high-volume,
// append-by-bucket telemetry table.
func NewOpenAILatencyTelemetryRollupRepository(db *sql.DB) service.OpenAILatencyTelemetryRollupRepository {
	return &openAILatencyTelemetryRollupRepository{db: db}
}

type preparedOpenAILatencyTelemetryRollup struct {
	rollup  service.OpenAILatencyTelemetryRollup
	payload string
}

func (r *openAILatencyTelemetryRollupRepository) validate() error {
	if r == nil || r.db == nil {
		return errors.New("openai latency telemetry rollup database is nil")
	}
	return nil
}

func prepareOpenAILatencyTelemetryRollups(
	rollups []service.OpenAILatencyTelemetryRollup,
) ([]preparedOpenAILatencyTelemetryRollup, error) {
	prepared := make([]preparedOpenAILatencyTelemetryRollup, 0, len(rollups))
	for index, rollup := range rollups {
		if err := rollup.Validate(); err != nil {
			return nil, fmt.Errorf("validate openai latency telemetry rollup %d: %w", index, err)
		}
		payload, err := json.Marshal(rollup.Payload)
		if err != nil {
			return nil, fmt.Errorf("marshal openai latency telemetry rollup %d: %w", index, err)
		}
		prepared = append(prepared, preparedOpenAILatencyTelemetryRollup{
			rollup:  rollup,
			payload: string(payload),
		})
	}
	return prepared, nil
}

func buildOpenAILatencyTelemetryRollupUpsert(
	rollups []preparedOpenAILatencyTelemetryRollup,
) (string, []any) {
	const columnsPerRow = 15
	var query strings.Builder
	query.WriteString(`
INSERT INTO openai_latency_telemetry_rollups AS current_rollup (
    bucket_start,
    bucket_seconds,
    account_id,
    cohort,
    attempt_role,
    schema_version,
    aggregate_payload,
    source_count,
    sample_count,
    coverage_start,
    fresh_through,
    covered_bucket_count,
    expected_bucket_count,
    is_complete,
    finalized_at
) VALUES `)
	args := make([]any, 0, len(rollups)*columnsPerRow)
	for index, prepared := range rollups {
		if index > 0 {
			query.WriteString(", ")
		}
		query.WriteByte('(')
		for column := range columnsPerRow {
			if column > 0 {
				query.WriteString(", ")
			}
			query.WriteByte('$')
			query.WriteString(strconv.Itoa(index*columnsPerRow + column + 1))
		}
		query.WriteByte(')')
		rollup := prepared.rollup
		args = append(args,
			rollup.BucketStart.UTC(),
			rollup.BucketSeconds,
			rollup.Key.AccountID,
			int64(rollup.Key.Cohort),
			int64(rollup.Key.AttemptRole),
			int64(rollup.Payload.SchemaVersion),
			prepared.payload,
			rollup.SourceCount,
			strconv.FormatUint(rollup.SampleCount, 10),
			rollup.CoverageStart.UTC(),
			rollup.FreshThrough.UTC(),
			rollup.CoveredBucketCount,
			rollup.ExpectedBucketCount,
			rollup.Complete,
			rollup.FinalizedAt.UTC(),
		)
	}
	query.WriteString(`
ON CONFLICT (bucket_start, bucket_seconds, account_id, cohort, attempt_role)
DO UPDATE SET
    schema_version = EXCLUDED.schema_version,
    aggregate_payload = EXCLUDED.aggregate_payload,
    source_count = EXCLUDED.source_count,
    sample_count = EXCLUDED.sample_count,
    coverage_start = EXCLUDED.coverage_start,
    fresh_through = EXCLUDED.fresh_through,
    covered_bucket_count = EXCLUDED.covered_bucket_count,
    expected_bucket_count = EXCLUDED.expected_bucket_count,
    is_complete = EXCLUDED.is_complete,
    finalized_at = EXCLUDED.finalized_at
WHERE (
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
	return query.String(), args
}

func (r *openAILatencyTelemetryRollupRepository) UpsertOpenAILatencyTelemetryRollups(
	ctx context.Context,
	rollups []service.OpenAILatencyTelemetryRollup,
) error {
	if err := r.validate(); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("openai latency telemetry rollup context is nil")
	}
	if len(rollups) == 0 {
		return nil
	}
	prepared, err := prepareOpenAILatencyTelemetryRollups(rollups)
	if err != nil {
		return err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin openai latency telemetry rollup upsert: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for start := 0; start < len(prepared); start += openAILatencyTelemetryRollupUpsertChunkSize {
		end := min(start+openAILatencyTelemetryRollupUpsertChunkSize, len(prepared))
		query, args := buildOpenAILatencyTelemetryRollupUpsert(prepared[start:end])
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("upsert openai latency telemetry rollup chunk: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit openai latency telemetry rollup upsert: %w", err)
	}
	return nil
}

func (r *openAILatencyTelemetryRollupRepository) DeleteOpenAILatencyTelemetryRollupsBefore(
	ctx context.Context,
	bucketSeconds int,
	before time.Time,
) (int64, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	if ctx == nil {
		return 0, errors.New("openai latency telemetry rollup context is nil")
	}
	if bucketSeconds != service.OpenAILatencyTelemetry15MinuteRollupSeconds &&
		bucketSeconds != service.OpenAILatencyTelemetryHourlyRollupSeconds {
		return 0, errors.New("openai latency telemetry rollup bucket seconds must be 900 or 3600")
	}
	if before.IsZero() {
		return 0, errors.New("openai latency telemetry rollup retention cutoff is required")
	}
	result, err := r.db.ExecContext(ctx, `
DELETE FROM openai_latency_telemetry_rollups
WHERE bucket_seconds = $1
  AND bucket_start < $2`, bucketSeconds, before.UTC())
	if err != nil {
		return 0, fmt.Errorf("delete expired openai latency telemetry rollups: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read deleted openai latency telemetry rollup count: %w", err)
	}
	return rows, nil
}
