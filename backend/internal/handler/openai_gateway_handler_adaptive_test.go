package handler

import (
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpenAIGatewayHandlerAdaptiveLatencyRuntimeInjection(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.Scheduling.OpenAILatencyShadow.Enabled = false

	runtime, err := service.NewAdaptiveLatencyRuntime(cfg, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, runtime)

	h := NewOpenAIGatewayHandler(nil, nil, nil, nil, nil, nil, nil, nil, cfg)
	require.NotNil(t, h)
	require.Nil(t, h.adaptiveLatencyRuntime)

	h.adaptiveLatencyRuntime = runtime
	require.NotNil(t, h.adaptiveLatencyRuntime)
	require.NotNil(t, h.adaptiveLatencyRuntime.Health())
	require.NotNil(t, h.adaptiveLatencyRuntime.Telemetry())
}

func TestOpenAIGatewayHandlerAdaptiveLatencyRuntimeDisabledIsNoop(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.Scheduling.OpenAILatencyShadow.Enabled = false

	runtime, err := service.NewAdaptiveLatencyRuntime(cfg, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	h := NewOpenAIGatewayHandler(nil, nil, nil, nil, nil, nil, nil, nil, cfg)
	h.adaptiveLatencyRuntime = runtime

	req := service.AdaptiveLatencyObserveRequest{
		Cohort:      service.NewCohortKey(service.OpenAIEndpointChatCompletions, service.OpenAIModelText, true, service.OpenAIInputSizeSmall, service.OpenAIReasoningNone, service.OpenAIToolUseNone),
		AccountID:   100,
		AttemptRole: service.OpenAIAttemptRolePrimary,
		Pool:        service.PoolSnapshot{QuarantinedAccounts: 0, HardValidAccounts: 5},
	}
	outcome := service.AdaptiveLatencyObserveOutcome{
		HTTPStatus:          200,
		LatencyMilliseconds: 1000,
		Phases: map[service.OpenAILatencyPhase]time.Duration{
			service.OpenAILatencyPhaseTotal: 1000 * time.Millisecond,
		},
	}

	require.NotPanics(t, func() {
		h.adaptiveLatencyRuntime.Observe(req, outcome)
	})

	stats := h.adaptiveLatencyRuntime.Stats().Observer
	require.False(t, stats.Enabled)
	require.Zero(t, stats.ObservationsRecorded)
}

func TestOpenAIGatewayHandlerAdaptiveLatencyRuntimeNilIsSafe(t *testing.T) {
	h := NewOpenAIGatewayHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NotNil(t, h)
	require.Nil(t, h.adaptiveLatencyRuntime)

	require.NotPanics(t, func() {
		if h.adaptiveLatencyRuntime != nil {
			h.adaptiveLatencyRuntime.Observe(service.AdaptiveLatencyObserveRequest{}, service.AdaptiveLatencyObserveOutcome{})
		}
	})
}

func TestOpenAIGatewayHandlerDirectConstructorCompatibility(t *testing.T) {
	cfg := &config.Config{}
	h1 := NewOpenAIGatewayHandler(nil, nil, nil, nil, nil, nil, nil, nil, cfg)
	require.NotNil(t, h1)
	require.Nil(t, h1.adaptiveLatencyRuntime)

	runtime, err := service.NewAdaptiveLatencyRuntime(cfg, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	h2 := NewOpenAIGatewayHandler(nil, nil, nil, nil, nil, nil, nil, nil, cfg)
	h2.adaptiveLatencyRuntime = runtime
	require.NotNil(t, h2.adaptiveLatencyRuntime)
}
