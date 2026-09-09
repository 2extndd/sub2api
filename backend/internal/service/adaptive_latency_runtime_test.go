package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func adaptiveLatencyRuntimeTestConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := &config.Config{}
	cfg.Gateway.Scheduling.OpenAIHedge = config.OpenAIHedgeConfig{
		Enabled:                   false,
		StandardThresholdSeconds:  config.DefaultOpenAIHedgeStandardThresholdSeconds,
		HighThresholdSeconds:      config.DefaultOpenAIHedgeHighThresholdSeconds,
		VeryHeavyThresholdSeconds: config.DefaultOpenAIHedgeVeryHeavyThresholdSeconds,
		EventChannelCapacity:      config.DefaultOpenAIHedgeEventChannelCapacity,
		MaxPrecommitEvents:        config.DefaultOpenAIHedgeMaxPrecommitEvents,
		MaxPrecommitBytes:         config.DefaultOpenAIHedgeMaxPrecommitBytes,
		CancelDrainTimeoutSeconds: config.DefaultOpenAIHedgeCancelDrainTimeoutSeconds,
		MaxAttempts:               2,
	}
	cfg.Gateway.Scheduling.OpenAILatencyShadow = config.OpenAILatencyShadowConfig{
		Enabled:                              false,
		TelemetryFlushIntervalSeconds:        config.DefaultOpenAILatencyShadowTelemetryFlushIntervalSeconds,
		TelemetryBucketSeconds:               config.DefaultOpenAILatencyShadowTelemetryBucketSeconds,
		TelemetryFinalizationIntervalSeconds: config.DefaultOpenAILatencyShadowTelemetryFinalizationIntervalSeconds,
		TelemetryFinalizationGraceSeconds:    config.DefaultOpenAILatencyShadowTelemetryFinalizationGraceSeconds,
		TelemetrySourceTTLSeconds:            config.DefaultOpenAILatencyShadowTelemetrySourceTTLSeconds,
		TelemetryRedisRawRetentionSeconds:    config.DefaultOpenAILatencyShadowTelemetryRedisRawRetentionSeconds,
		MaxTelemetryStreams:                  config.DefaultOpenAILatencyShadowMaxTelemetryStreams,
		MaxTelemetryContributions:            config.DefaultOpenAILatencyShadowMaxTelemetryContributions,
		Telemetry15MinuteRetentionDays:       config.DefaultOpenAILatencyShadowTelemetry15MinuteRetentionDays,
		TelemetryHourlyRetentionDays:         config.DefaultOpenAILatencyShadowTelemetryHourlyRetentionDays,
		MaxTrackedAccounts:                   config.DefaultOpenAILatencyShadowMaxTrackedAccounts,
		MaxCohortsPerAccount:                 config.DefaultOpenAILatencyShadowMaxCohortsPerAccount,
		MaxTracks:                            config.DefaultOpenAILatencyShadowMaxTracks,
		WeightFloor:                          config.DefaultOpenAILatencyShadowWeightFloor,
		QuarantineCap:                        config.DefaultOpenAILatencyShadowQuarantineCap,
		HardValidFloor:                       config.DefaultOpenAILatencyShadowHardValidFloor,
		IncidentWindowSeconds:                config.DefaultOpenAILatencyShadowIncidentWindowSeconds,
		IncidentMinAffectedAccounts:          config.DefaultOpenAILatencyShadowIncidentMinAffectedAccounts,
		IncidentMinAffectedFraction:          config.DefaultOpenAILatencyShadowIncidentMinAffectedFraction,
		PenalizedCooldownSeconds:             config.DefaultOpenAILatencyShadowPenalizedCooldownSeconds,
		QuarantinedCooldownSeconds:           config.DefaultOpenAILatencyShadowQuarantinedCooldownSeconds,
		HardInvalidCooldownSeconds:           config.DefaultOpenAILatencyShadowHardInvalidCooldownSeconds,
		LatencyHalfLifeSeconds:               config.DefaultOpenAILatencyShadowLatencyHalfLifeSeconds,
		ErrorHalfLifeSeconds:                 config.DefaultOpenAILatencyShadowErrorHalfLifeSeconds,
	}
	return cfg
}

func TestProvideAdaptiveLatencyRuntimeConstructsFeatureOffSafely(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	runtime, err := ProvideAdaptiveLatencyRuntime(cfg, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	require.NotPanics(t, runtime.Stop)
	require.NotPanics(t, runtime.Stop)
}

func TestAdaptiveLatencyRuntimeShadowLifecycleStartsAndStops(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	cfg.Gateway.Scheduling.OpenAILatencyShadow.Enabled = true
	guard, err := NewLocalLatencyHealthTransitionGuard(cfg.Gateway.Scheduling.OpenAILatencyShadow.MaxTrackedAccounts)
	require.NoError(t, err)
	runtime, err := ProvideAdaptiveLatencyRuntime(
		cfg,
		&openAILatencyTelemetryCollectorFakeCache{},
		&openAILatencyTelemetryCollectorFakeRollups{},
		nil,
		nil,
		guard,
	)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	runtime.Start()
	require.NotNil(t, runtime.Telemetry().stopCh)
	require.NotNil(t, runtime.Telemetry().doneCh)
	stopped := make(chan struct{})
	go func() {
		runtime.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("shadow runtime did not stop within one second")
	}
	require.NotPanics(t, runtime.Stop)
}

func TestAdaptiveLatencyRuntimeNilIsLifecycleAndStatsSafe(t *testing.T) {
	var runtime *AdaptiveLatencyRuntime
	require.NotPanics(t, runtime.Start)
	require.NotPanics(t, runtime.Stop)
	require.Equal(t, AdaptiveLatencyRuntimeStats{}, runtime.Stats())
	require.Nil(t, runtime.Health())
	require.Nil(t, runtime.Telemetry())
	require.Nil(t, runtime.Hedge())
}

func TestAdaptiveLatencyRuntimeFeatureOffIsInert(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	runtime, err := NewAdaptiveLatencyRuntime(cfg, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	require.NotNil(t, runtime.Health())
	require.NotNil(t, runtime.Telemetry())
	require.NotNil(t, runtime.Hedge())
	require.Nil(t, runtime.Health().accounts)
	require.False(t, runtime.Telemetry().Enabled())
	require.Nil(t, runtime.Telemetry().registry)
	require.Nil(t, runtime.Telemetry().stopCh)
	require.Nil(t, runtime.Telemetry().doneCh)

	runtime.Start()
	runtime.Stop()
	stats := runtime.Stats()
	require.False(t, stats.Health.Enabled)
	require.False(t, stats.Telemetry.Enabled)
	require.Zero(t, stats.Telemetry.Streams)
	require.Zero(t, stats.Telemetry.Observations)
	require.Zero(t, stats.Telemetry.FlushAttempts)
	require.Equal(t, config.DefaultOpenAILatencyShadowMaxTelemetryStreams, stats.Telemetry.MaxStreams)
	require.Equal(t, AdaptiveHedgeRuntimeStats{Configured: false, Constructed: true, AdaptersWired: true, Active: false}, stats.Hedge)

	err = runtime.Health().RecordObservation(OpenAILatencyObservation{})
	require.NoError(t, err)
	require.Nil(t, runtime.Health().accounts)
}

func TestAdaptiveLatencyRuntimeSourceIdentityIsStableAndGenerationChanges(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	cfg.Gateway.Scheduling.OpenAILatencyShadow.Enabled = true
	guard, err := NewLocalLatencyHealthTransitionGuard(cfg.Gateway.Scheduling.OpenAILatencyShadow.MaxTrackedAccounts)
	require.NoError(t, err)
	first, err := NewAdaptiveLatencyRuntime(cfg, &openAILatencyTelemetryCollectorFakeCache{}, &openAILatencyTelemetryCollectorFakeRollups{}, nil, nil, guard)
	require.NoError(t, err)
	second, err := NewAdaptiveLatencyRuntime(cfg, &openAILatencyTelemetryCollectorFakeCache{}, &openAILatencyTelemetryCollectorFakeRollups{}, nil, nil, guard)
	require.NoError(t, err)
	require.NotZero(t, first.Telemetry().sourceID)
	require.Equal(t, first.Telemetry().sourceID, second.Telemetry().sourceID)
	require.NotZero(t, first.Telemetry().generation)
	require.NotZero(t, second.Telemetry().generation)
	require.NotEqual(t, first.Telemetry().generation, second.Telemetry().generation)

	cfg.Server.Port++
	third, err := NewAdaptiveLatencyRuntime(cfg, &openAILatencyTelemetryCollectorFakeCache{}, &openAILatencyTelemetryCollectorFakeRollups{}, nil, nil, guard)
	require.NoError(t, err)
	require.NotEqual(t, first.Telemetry().sourceID, third.Telemetry().sourceID)
}

func TestAdaptiveLatencyRuntimeHedgeEnablementBuildsRequestScopedFactory(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	cfg.Gateway.Scheduling.OpenAIHedge.Enabled = true
	cfg.Gateway.Scheduling.OpenAIHedge.CanaryBasisPoints = config.MaximumOpenAIHedgeCanaryBasisPoints
	runtime, err := NewAdaptiveLatencyRuntime(cfg, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	stats := runtime.Stats().Hedge
	require.True(t, stats.Configured)
	require.True(t, stats.Constructed)
	require.True(t, stats.AdaptersWired)
	require.True(t, stats.Active)
	_, enabled := runtime.HedgeConfig()
	require.True(t, enabled)
}

func TestAdaptiveLatencyRuntimeZeroBasisPointsIsDecisionShadow(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	cfg.Gateway.Scheduling.OpenAIHedge.Enabled = true
	runtime, err := NewAdaptiveLatencyRuntime(cfg, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	stats := runtime.Stats().Hedge
	require.True(t, stats.Configured)
	require.True(t, stats.DecisionShadow)
	require.False(t, stats.Active)
	require.Zero(t, stats.CanaryBasisPoints)
}

func TestAdaptiveLatencyRuntimeCanaryDecisionStatsAreAggregate(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	cfg.Gateway.Scheduling.OpenAIHedge.Enabled = true
	cfg.Gateway.Scheduling.OpenAIHedge.CanaryBasisPoints = 250
	runtime, err := NewAdaptiveLatencyRuntime(cfg, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	runtime.RecordHedgeCanaryDecision(17, UnknownCohortKey, false)
	runtime.RecordHedgeCanaryDecision(17, UnknownCohortKey, true)
	runtime.RecordHedgeCanaryDecision(17, UnknownCohortKey, false)

	stats := runtime.Stats().Hedge
	require.Equal(t, 250, stats.CanaryBasisPoints)
	require.Equal(t, uint64(1), stats.CanarySelected)
	require.Equal(t, uint64(2), stats.CanaryRejected)
}

func TestAdaptiveLatencyRuntimeCanaryDecisionStatsAreRaceSafe(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	cfg.Gateway.Scheduling.OpenAIHedge.Enabled = true
	runtime, err := NewAdaptiveLatencyRuntime(cfg, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	const decisions = 500
	var workers sync.WaitGroup
	workers.Add(decisions * 2)
	for range decisions {
		go func() {
			defer workers.Done()
			runtime.RecordHedgeCanaryDecision(17, UnknownCohortKey, true)
		}()
		go func() {
			defer workers.Done()
			runtime.RecordHedgeCanaryDecision(17, UnknownCohortKey, false)
		}()
	}
	workers.Wait()

	stats := runtime.Stats().Hedge
	require.Equal(t, uint64(decisions), stats.CanarySelected)
	require.Equal(t, uint64(decisions), stats.CanaryRejected)
}

func TestAdaptiveLatencyRuntimeCanaryDecisionsPersistBoundedTelemetry(t *testing.T) {
	clock := &openAILatencyTelemetryCollectorFakeClock{now: time.Date(2026, 8, 14, 16, 0, 0, 0, time.UTC)}
	collector := newOpenAILatencyTelemetryCollectorForTest(
		t,
		enabledOpenAILatencyTelemetryCollectorConfig(8),
		clock,
		nil,
		nil,
		nil,
		91,
	)
	runtime := &AdaptiveLatencyRuntime{telemetry: collector}
	cohort := NewCohortKey(
		OpenAIEndpointResponses,
		OpenAIModelText,
		true,
		OpenAIInputSizeSmall,
		OpenAIReasoningLow,
		OpenAIToolUseNone,
	)

	runtime.RecordHedgeCanaryDecision(17, cohort, true)
	runtime.RecordHedgeCanaryDecision(17, cohort, false)

	key := OpenAILatencyTelemetryKey{AccountID: 17, Cohort: cohort, AttemptRole: OpenAIAttemptRolePrimary}
	collector.mu.Lock()
	track := collector.registry[key]
	collector.mu.Unlock()
	require.NotNil(t, track)
	snapshot := track.current.Snapshot()
	require.Equal(t, OpenAILatencyTelemetrySchemaVersion, snapshot.SchemaVersion)
	require.Equal(t, uint64(1), snapshot.Hedges[OpenAIHedgeOutcomeCanarySelected])
	require.Equal(t, uint64(1), snapshot.Hedges[OpenAIHedgeOutcomeCanaryRejected])
}

func TestAdaptiveLatencyRuntimeShadowWiresHealthAndTelemetryWithoutRoutingMutation(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	cfg.Gateway.Scheduling.OpenAILatencyShadow.Enabled = true
	clock := &openAILatencyTelemetryCollectorFakeClock{now: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)}
	cache := &openAILatencyTelemetryCollectorFakeCache{}
	rollups := &openAILatencyTelemetryCollectorFakeRollups{}
	guard, err := NewLocalLatencyHealthTransitionGuard(cfg.Gateway.Scheduling.OpenAILatencyShadow.MaxTrackedAccounts)
	require.NoError(t, err)

	runtime, err := NewAdaptiveLatencyRuntime(cfg, cache, rollups, nil, nil, guard)
	require.NoError(t, err)
	runtime.Telemetry().clock = clock
	require.True(t, runtime.Stats().Health.Enabled)
	require.True(t, runtime.Stats().Telemetry.Enabled)
	require.NotNil(t, runtime.Health().accounts)
	require.NotNil(t, runtime.Telemetry().registry)

	key := openAILatencyTelemetryCollectorTestKey(77)
	require.NoError(t, runtime.Telemetry().ObservePhase(key, OpenAILatencyPhaseTotal, time.Second))
	require.NoError(t, runtime.Telemetry().Flush(context.Background()))
	stats := runtime.Stats()
	require.Equal(t, uint64(1), stats.Telemetry.Observations)
	require.Equal(t, uint64(1), stats.Telemetry.FlushSuccesses)
	require.True(t, stats.Hedge.AdaptersWired)
	require.False(t, stats.Hedge.Active)
	require.True(t, stats.Hedge.Constructed)
	require.True(t, stats.Recommendation.Fresh)
	require.Equal(t, uint64(1), stats.Recommendation.SampleCount)
}

func TestAdaptiveLatencyRuntimeShadowFailureStatsStaySecretFreeAndFailOpen(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	cfg.Gateway.Scheduling.OpenAILatencyShadow.Enabled = true
	clock := &openAILatencyTelemetryCollectorFakeClock{now: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)}
	cache := &openAILatencyTelemetryCollectorFakeCache{storeErr: errors.New("redis secret detail")}
	guard, err := NewLocalLatencyHealthTransitionGuard(cfg.Gateway.Scheduling.OpenAILatencyShadow.MaxTrackedAccounts)
	require.NoError(t, err)
	runtime, err := NewAdaptiveLatencyRuntime(cfg, cache, &openAILatencyTelemetryCollectorFakeRollups{}, nil, nil, guard)
	require.NoError(t, err)
	runtime.Telemetry().clock = clock

	require.NoError(t, runtime.Telemetry().ObservePhase(openAILatencyTelemetryCollectorTestKey(78), OpenAILatencyPhaseTotal, time.Second))
	require.Error(t, runtime.Telemetry().Flush(context.Background()))
	stats := runtime.Stats().Telemetry
	require.Equal(t, uint64(1), stats.FlushFailures)
	require.Equal(t, "*errors.errorString", stats.LastFlushError)
	require.NotContains(t, stats.LastFlushError, "secret")
	require.True(t, runtime.Stats().Recommendation.Fresh)
	clock.Advance(time.Duration(cfg.Gateway.Scheduling.OpenAILatencyShadow.TelemetrySourceTTLSeconds+1) * time.Second)
	require.False(t, runtime.Stats().Recommendation.Fresh)
	require.Greater(t, runtime.Stats().Recommendation.Freshness, time.Duration(cfg.Gateway.Scheduling.OpenAILatencyShadow.TelemetrySourceTTLSeconds)*time.Second)
}

func TestAdaptiveLatencyRuntimeObserveDisabledIsImmediateNoop(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	runtime, err := NewAdaptiveLatencyRuntime(cfg, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	req := AdaptiveLatencyObserveRequest{
		Cohort:      NewCohortKey(OpenAIEndpointChatCompletions, OpenAIModelText, true, OpenAIInputSizeSmall, OpenAIReasoningNone, OpenAIToolUseNone),
		AccountID:   123,
		AttemptRole: OpenAIAttemptRolePrimary,
		Pool:        PoolSnapshot{QuarantinedAccounts: 0, HardValidAccounts: 10},
	}
	outcome := AdaptiveLatencyObserveOutcome{
		HTTPStatus:          200,
		LatencyMilliseconds: 1500,
		Phases: map[OpenAILatencyPhase]time.Duration{
			OpenAILatencyPhaseTotal: 1500 * time.Millisecond,
		},
	}

	runtime.Observe(req, outcome)

	stats := runtime.Stats().Observer
	require.False(t, stats.Enabled)
	require.Zero(t, stats.ObservationsRecorded)
	require.Zero(t, stats.HealthFailures)
	require.Zero(t, stats.TelemetryFailures)
}

func TestAdaptiveLatencyRuntimeObserveEnabledRecordsHealthAndTelemetry(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	cfg.Gateway.Scheduling.OpenAILatencyShadow.Enabled = true
	clock := &openAILatencyTelemetryCollectorFakeClock{now: time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)}
	cache := &openAILatencyTelemetryCollectorFakeCache{}
	rollups := &openAILatencyTelemetryCollectorFakeRollups{}
	guard, err := NewLocalLatencyHealthTransitionGuard(cfg.Gateway.Scheduling.OpenAILatencyShadow.MaxTrackedAccounts)
	require.NoError(t, err)

	runtime, err := NewAdaptiveLatencyRuntime(cfg, cache, rollups, nil, nil, guard)
	require.NoError(t, err)
	runtime.Telemetry().clock = clock

	req := AdaptiveLatencyObserveRequest{
		Cohort:      NewCohortKey(OpenAIEndpointChatCompletions, OpenAIModelText, true, OpenAIInputSizeSmall, OpenAIReasoningNone, OpenAIToolUseNone),
		AccountID:   456,
		AttemptRole: OpenAIAttemptRolePrimary,
		Pool:        PoolSnapshot{QuarantinedAccounts: 1, HardValidAccounts: 15},
	}
	outcome := AdaptiveLatencyObserveOutcome{
		HTTPStatus:          200,
		LatencyMilliseconds: 2500,
		Phases: map[OpenAILatencyPhase]time.Duration{
			OpenAILatencyPhaseTotal:           2500 * time.Millisecond,
			OpenAILatencyPhaseFirstByte:       100 * time.Millisecond,
			OpenAILatencyPhaseFirstSSE:        150 * time.Millisecond,
			OpenAILatencyPhaseFirstMeaningful: 200 * time.Millisecond,
		},
	}

	runtime.Observe(req, outcome)

	stats := runtime.Stats().Observer
	require.True(t, stats.Enabled)
	require.Equal(t, uint64(1), stats.ObservationsRecorded)
	require.Zero(t, stats.HealthFailures)
	require.Zero(t, stats.TelemetryFailures)
	require.Empty(t, stats.LastHealthErrorType)
	require.Empty(t, stats.LastTelemetryErrorType)

	require.NoError(t, runtime.Telemetry().Flush(context.Background()))
	require.Equal(t, uint64(1), runtime.Stats().Telemetry.FlushSuccesses)
}

func TestAdaptiveLatencyRuntimeObserveFailureCountersIncrementSecretFree(t *testing.T) {
	cfg := adaptiveLatencyRuntimeTestConfig(t)
	cfg.Gateway.Scheduling.OpenAILatencyShadow.Enabled = true
	cfg.Gateway.Scheduling.OpenAILatencyShadow.MaxTrackedAccounts = 1
	clock := &openAILatencyTelemetryCollectorFakeClock{now: time.Date(2026, 8, 12, 11, 0, 0, 0, time.UTC)}
	cache := &openAILatencyTelemetryCollectorFakeCache{}
	rollups := &openAILatencyTelemetryCollectorFakeRollups{}
	guard, err := NewLocalLatencyHealthTransitionGuard(cfg.Gateway.Scheduling.OpenAILatencyShadow.MaxTrackedAccounts)
	require.NoError(t, err)

	runtime, err := NewAdaptiveLatencyRuntime(cfg, cache, rollups, nil, nil, guard)
	require.NoError(t, err)
	runtime.Telemetry().clock = clock

	// Fill the health tracking capacity first
	req1 := AdaptiveLatencyObserveRequest{
		Cohort:      NewCohortKey(OpenAIEndpointChatCompletions, OpenAIModelText, true, OpenAIInputSizeSmall, OpenAIReasoningNone, OpenAIToolUseNone),
		AccountID:   100,
		AttemptRole: OpenAIAttemptRolePrimary,
		Pool:        PoolSnapshot{QuarantinedAccounts: 0, HardValidAccounts: 20},
	}
	outcome1 := AdaptiveLatencyObserveOutcome{
		HTTPStatus:          200,
		LatencyMilliseconds: 1000,
		Phases: map[OpenAILatencyPhase]time.Duration{
			OpenAILatencyPhaseTotal: 1000 * time.Millisecond,
		},
	}
	runtime.Observe(req1, outcome1)

	// Now observe a different account which should fail due to capacity
	req2 := AdaptiveLatencyObserveRequest{
		Cohort:      NewCohortKey(OpenAIEndpointChatCompletions, OpenAIModelReasoning, true, OpenAIInputSizeMedium, OpenAIReasoningMedium, OpenAIToolUseNone),
		AccountID:   789,
		AttemptRole: OpenAIAttemptRolePrimary,
		Pool:        PoolSnapshot{QuarantinedAccounts: 0, HardValidAccounts: 20},
	}
	outcome2 := AdaptiveLatencyObserveOutcome{
		HTTPStatus:          500,
		LatencyMilliseconds: 3000,
		Phases: map[OpenAILatencyPhase]time.Duration{
			OpenAILatencyPhaseTotal: 3000 * time.Millisecond,
		},
	}

	runtime.Observe(req2, outcome2)

	stats := runtime.Stats().Observer
	require.True(t, stats.Enabled)
	require.Equal(t, uint64(2), stats.ObservationsRecorded)
	require.Equal(t, uint64(1), stats.HealthFailures)
	require.Zero(t, stats.TelemetryFailures)
	require.Contains(t, stats.LastHealthErrorType, "error")
	require.Empty(t, stats.LastTelemetryErrorType)
}

func TestAdaptiveLatencyRuntimeObserveNilIsNoop(t *testing.T) {
	var runtime *AdaptiveLatencyRuntime
	require.NotPanics(t, func() {
		runtime.Observe(AdaptiveLatencyObserveRequest{}, AdaptiveLatencyObserveOutcome{})
	})
}
