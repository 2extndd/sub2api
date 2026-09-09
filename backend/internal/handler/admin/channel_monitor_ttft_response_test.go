//go:build unit

package admin

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestChannelMonitorAdminResponsesPreserveNullableTTFT(t *testing.T) {
	token := 812
	checkPayload, err := json.Marshal(checkResultToResponse(&service.CheckResult{
		Model:        "gpt-test",
		Status:       service.MonitorStatusOperational,
		FirstTokenMs: &token,
		CheckedAt:    time.Unix(0, 0).UTC(),
	}))
	require.NoError(t, err)
	require.Contains(t, string(checkPayload), `"first_token_ms":812`)

	historyPayload, err := json.Marshal(historyEntryToResponse(&service.ChannelMonitorHistoryEntry{
		Model:        "gpt-test",
		CheckedAt:    time.Unix(0, 0).UTC(),
		FirstTokenMs: nil,
	}))
	require.NoError(t, err)
	require.Contains(t, string(historyPayload), `"first_token_ms":null`)

	avg := 150
	listPayload, err := json.Marshal(buildListItemResponse(&service.ChannelMonitor{
		ID:   7,
		Name: "Relay",
	}, service.MonitorStatusSummary{AvgFirstToken7dMs: &avg}))
	require.NoError(t, err)
	require.Contains(t, string(listPayload), `"avg_first_token_7d_ms":150`)
}
