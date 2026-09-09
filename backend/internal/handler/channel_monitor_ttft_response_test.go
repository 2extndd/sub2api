package handler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestChannelMonitorUserResponsesPreserveNullableTTFTFields(t *testing.T) {
	token := 812
	view := &service.UserMonitorView{
		ID:                7,
		Name:              "Relay",
		PrimaryModel:      "gpt-test",
		PrimaryStatus:     service.MonitorStatusOperational,
		AvgFirstToken7dMs: &token,
		Timeline: []service.UserMonitorTimelinePoint{{
			Status:       service.MonitorStatusOperational,
			FirstTokenMs: nil,
			CheckedAt:    time.Unix(0, 0).UTC(),
		}},
	}

	payload, err := json.Marshal(userMonitorViewToItem(view, false))
	require.NoError(t, err)
	require.JSONEq(t, `{"id":7,"name":"Relay","provider":"","group_name":"","primary_model":"gpt-test","primary_status":"operational","primary_latency_ms":null,"avg_first_token_7d_ms":812,"primary_ping_latency_ms":null,"availability_7d":0,"extra_models":[],"timeline":[{"status":"operational","latency_ms":null,"first_token_ms":null,"ping_latency_ms":null,"checked_at":"1970-01-01T00:00:00Z"}]}`, string(payload))
}

func TestChannelMonitorUserDetailResponsePreservesNullableTTFT(t *testing.T) {
	token := 150
	payload, err := json.Marshal(userMonitorDetailToResponse(&service.UserMonitorDetail{
		Models: []service.ModelDetail{{
			Model:             "gpt-test",
			AvgFirstToken7dMs: &token,
		}},
	}))
	require.NoError(t, err)
	require.Contains(t, string(payload), `"avg_first_token_7d_ms":150`)
}
