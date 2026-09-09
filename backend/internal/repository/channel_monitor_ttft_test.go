//go:build unit

package repository

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

func TestFinalizeAvailabilityRowMapsNullableTTFTAverage(t *testing.T) {
	row := &service.ChannelMonitorAvailability{
		TotalChecks:       3,
		OperationalChecks: 2,
	}
	finalizeAvailabilityRow(
		row,
		sql.NullFloat64{Float64: 300, Valid: true},
		sql.NullFloat64{Float64: 150, Valid: true},
	)

	if row.AvgLatencyMs == nil || *row.AvgLatencyMs != 300 {
		t.Fatalf("average latency = %v, want 300", row.AvgLatencyMs)
	}
	if row.AvgFirstTokenMs == nil || *row.AvgFirstTokenMs != 150 {
		t.Fatalf("average TTFT = %v, want 150", row.AvgFirstTokenMs)
	}
	if row.AvailabilityPct < 66.6 || row.AvailabilityPct > 66.7 {
		t.Fatalf("availability = %f, want about 66.67", row.AvailabilityPct)
	}
}

func TestFinalizeAvailabilityRowPreservesMissingTTFT(t *testing.T) {
	row := &service.ChannelMonitorAvailability{TotalChecks: 1, OperationalChecks: 1}
	finalizeAvailabilityRow(row, sql.NullFloat64{}, sql.NullFloat64{})

	if row.AvgLatencyMs != nil {
		t.Fatalf("average latency = %v, want nil", row.AvgLatencyMs)
	}
	if row.AvgFirstTokenMs != nil {
		t.Fatalf("average TTFT = %v, want nil", row.AvgFirstTokenMs)
	}
}

func TestListRecentHistoryForMonitorsScansNullableTTFT(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer func() { _ = db.Close() }()

	checkedAt := time.Unix(0, 0).UTC()
	mock.ExpectQuery(`first_token_ms.*ping_latency_ms`).WillReturnRows(sqlmock.NewRows([]string{
		"monitor_id", "status", "latency_ms", "first_token_ms", "ping_latency_ms", "checked_at",
	}).AddRow(int64(7), "operational", int64(300), int64(123), int64(20), checkedAt))

	repo := &channelMonitorRepository{db: db}
	result, err := repo.ListRecentHistoryForMonitors(
		context.Background(),
		[]int64{7},
		map[int64]string{7: "gpt-test"},
		1,
	)
	if err != nil {
		t.Fatalf("list recent history: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
	entries := result[7]
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if entries[0].FirstTokenMs == nil || *entries[0].FirstTokenMs != 123 {
		t.Fatalf("scanned TTFT = %v, want 123", entries[0].FirstTokenMs)
	}
}
