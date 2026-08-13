package config

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAILatencyShadowPolicyDefaultsAndValidation(t *testing.T) {
	resetViperWithJWTSecret(t)
	cfg, err := Load()
	require.NoError(t, err)

	shadow := cfg.Gateway.Scheduling.OpenAILatencyShadow
	require.Equal(t, 256, shadow.MaxTrackedAccounts)
	require.Equal(t, 64, shadow.MaxCohortsPerAccount)
	require.Equal(t, 8192, shadow.MaxTracks)
	require.Equal(t, 0.25, shadow.WeightFloor)
	require.Equal(t, 3, shadow.QuarantineCap)
	require.Equal(t, 7, shadow.HardValidFloor)
	require.Equal(t, 90, shadow.IncidentWindowSeconds)
	require.Equal(t, 4, shadow.IncidentMinAffectedAccounts)
	require.Equal(t, 0.4, shadow.IncidentMinAffectedFraction)
	require.Equal(t, 30, shadow.PenalizedCooldownSeconds)
	require.Equal(t, 120, shadow.QuarantinedCooldownSeconds)
	require.Equal(t, 300, shadow.HardInvalidCooldownSeconds)
	require.Equal(t, 1800, shadow.LatencyHalfLifeSeconds)
	require.Equal(t, 900, shadow.ErrorHalfLifeSeconds)
	require.Equal(t, DefaultOpenAILatencyShadowMaxTelemetryContributions, shadow.MaxTelemetryContributions)

	cfg.Gateway.Scheduling.OpenAILatencyShadow.Enabled = true
	valid := cfg.Gateway.Scheduling.OpenAILatencyShadow
	invalidMutations := []struct {
		field  string
		mutate func(*OpenAILatencyShadowConfig)
	}{
		{"max_tracks", func(config *OpenAILatencyShadowConfig) { config.MaxTracks = 0 }},
		{"max_tracks", func(config *OpenAILatencyShadowConfig) { config.MaxTracks = MaximumOpenAILatencyShadowTracks + 1 }},
		{"max_tracks", func(config *OpenAILatencyShadowConfig) { config.MaxTracks = 2*config.MaxTrackedAccounts - 1 }},
		{"max_telemetry_contributions", func(config *OpenAILatencyShadowConfig) { config.MaxTelemetryContributions = 0 }},
		{"max_telemetry_contributions", func(config *OpenAILatencyShadowConfig) {
			config.MaxTelemetryContributions = MaximumOpenAILatencyShadowMaxTelemetryContributions + 1
		}},
		{"weight_floor", func(config *OpenAILatencyShadowConfig) { config.WeightFloor = 0 }},
		{"weight_floor", func(config *OpenAILatencyShadowConfig) { config.WeightFloor = math.NaN() }},
		{"weight_floor", func(config *OpenAILatencyShadowConfig) { config.WeightFloor = 1.01 }},
		{"quarantine_cap", func(config *OpenAILatencyShadowConfig) { config.QuarantineCap = -1 }},
		{"hard_valid_floor", func(config *OpenAILatencyShadowConfig) { config.HardValidFloor = -1 }},
		{"incident_window_seconds", func(config *OpenAILatencyShadowConfig) { config.IncidentWindowSeconds = 0 }},
		{"incident_min_affected_accounts", func(config *OpenAILatencyShadowConfig) { config.IncidentMinAffectedAccounts = 0 }},
		{"incident_min_affected_fraction", func(config *OpenAILatencyShadowConfig) { config.IncidentMinAffectedFraction = 0 }},
		{"incident_min_affected_fraction", func(config *OpenAILatencyShadowConfig) { config.IncidentMinAffectedFraction = math.Inf(1) }},
		{"penalized_cooldown_seconds", func(config *OpenAILatencyShadowConfig) { config.PenalizedCooldownSeconds = 0 }},
		{"quarantined_cooldown_seconds", func(config *OpenAILatencyShadowConfig) { config.QuarantinedCooldownSeconds = 0 }},
		{"hard_invalid_cooldown_seconds", func(config *OpenAILatencyShadowConfig) { config.HardInvalidCooldownSeconds = 0 }},
		{"latency_half_life_seconds", func(config *OpenAILatencyShadowConfig) { config.LatencyHalfLifeSeconds = 0 }},
		{"error_half_life_seconds", func(config *OpenAILatencyShadowConfig) { config.ErrorHalfLifeSeconds = 0 }},
	}
	for _, testCase := range invalidMutations {
		cfg.Gateway.Scheduling.OpenAILatencyShadow = valid
		testCase.mutate(&cfg.Gateway.Scheduling.OpenAILatencyShadow)
		require.ErrorContains(t, cfg.Validate(), "openai_latency_shadow."+testCase.field)
	}
}
