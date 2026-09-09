package service

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenAILatencyCohortPreservesReasoningEffortAndToolCountClasses(t *testing.T) {
	reasoningCases := []struct {
		raw   string
		class OpenAIReasoningClass
		label string
	}{
		{"", OpenAIReasoningUnknown, "reasoning-unknown"},
		{"none", OpenAIReasoningNone, "reasoning-none"},
		{"minimal", OpenAIReasoningMinimal, "reasoning-minimal"},
		{"low", OpenAIReasoningLow, "reasoning-low"},
		{"medium", OpenAIReasoningMedium, "reasoning-medium"},
		{"high", OpenAIReasoningHigh, "reasoning-high"},
		{"xhigh", OpenAIReasoningXHigh, "reasoning-xhigh"},
		{"unbounded-caller-value", OpenAIReasoningUnknown, "reasoning-unknown"},
	}
	reasoningKeys := make(map[CohortKey]struct{})
	for _, testCase := range reasoningCases[:7] {
		require.Equal(t, testCase.class, classifyOpenAIReasoning(testCase.raw))
		key := CanonicalCohortKey("responses", "o3", false, 1, testCase.raw, 0)
		require.Contains(t, key.String(), testCase.label)
		reasoningKeys[key] = struct{}{}
	}
	require.Len(t, reasoningKeys, 7)
	require.Equal(t, OpenAIReasoningUnknown, classifyOpenAIReasoning(reasoningCases[7].raw))

	toolCases := []struct {
		count int
		class OpenAIToolUseClass
		label string
	}{
		{-1, OpenAIToolUseUnknown, "tools-unknown"},
		{0, OpenAIToolUseNone, "tools-none"},
		{1, OpenAIToolUseOne, "tools-one"},
		{2, OpenAIToolUseTwoToFour, "tools-2-4"},
		{4, OpenAIToolUseTwoToFour, "tools-2-4"},
		{5, OpenAIToolUseFivePlus, "tools-5+"},
		{1000, OpenAIToolUseFivePlus, "tools-5+"},
	}
	toolKeys := make(map[CohortKey]struct{})
	for _, testCase := range toolCases {
		require.Equal(t, testCase.class, classifyOpenAIToolUse(testCase.count))
		key := CanonicalCohortKey("responses", "o3", false, 1, "none", testCase.count)
		require.Contains(t, key.String(), testCase.label)
		toolKeys[key] = struct{}{}
	}
	require.Len(t, toolKeys, 5)

	maxKey := NewCohortKey(
		OpenAIEndpointBatches,
		OpenAIModelAudio,
		true,
		OpenAIInputSizeLarge,
		OpenAIReasoningXHigh,
		OpenAIToolUseFivePlus,
	)
	require.Equal(t, maxKey, maxKey.Canonical())
	require.Equal(t, UnknownCohortKey, CohortKey(1<<15).Canonical())
	require.Equal(t, OverflowCohortKey, OverflowCohortKey.Canonical())
}

func TestOpenAILatencyHistogramUsesOperationalRequestBoundaries(t *testing.T) {
	require.Equal(t, [openAILatencyHistogramFiniteBoundaryCount]time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		15 * time.Second,
		25 * time.Second,
		40 * time.Second,
		60 * time.Second,
		90 * time.Second,
		120 * time.Second,
		180 * time.Second,
	}, OpenAILatencyHistogramUpperBounds())

	for index, boundary := range OpenAILatencyHistogramUpperBounds() {
		require.Equal(t, index, latencyHistogramBucket(boundary))
	}
	require.Equal(t, OpenAILatencyHistogramBucketCount-1, latencyHistogramBucket(181*time.Second))
}

func TestOpenAILatencyHealthPolicyDefaultsAreValidatedAndTunable(t *testing.T) {
	config := DefaultLatencyHealthConfig()
	require.Equal(t, 256, config.MaxTrackedAccounts)
	require.Equal(t, 64, config.MaxCohortsPerAccount)
	require.Equal(t, 8192, config.MaxTracks)
	require.Equal(t, 0.25, config.WeightFloor)
	require.Equal(t, 3, config.QuarantineCap)
	require.Equal(t, 7, config.HardValidFloor)
	require.Equal(t, 90*time.Second, config.IncidentWindow)
	require.Equal(t, 4, config.IncidentMinAffectedAccounts)
	require.Equal(t, 0.4, config.IncidentMinAffectedFraction)
	require.Equal(t, 30*time.Second, config.PenalizedCooldown)
	require.Equal(t, 120*time.Second, config.QuarantinedCooldown)
	require.Equal(t, 300*time.Second, config.HardInvalidCooldown)
	require.Equal(t, 1800*time.Second, config.LatencyHalfLife)
	require.Equal(t, 900*time.Second, config.ErrorHalfLife)
	require.NoError(t, config.Validate())

	config.Enabled = true
	invalidMutations := []func(*LatencyHealthConfig){
		func(config *LatencyHealthConfig) { config.WeightFloor = 0 },
		func(config *LatencyHealthConfig) { config.WeightFloor = math.NaN() },
		func(config *LatencyHealthConfig) { config.WeightFloor = 1.01 },
		func(config *LatencyHealthConfig) { config.QuarantineCap = -1 },
		func(config *LatencyHealthConfig) { config.HardValidFloor = -1 },
		func(config *LatencyHealthConfig) { config.IncidentWindow = 0 },
		func(config *LatencyHealthConfig) { config.IncidentMinAffectedAccounts = 0 },
		func(config *LatencyHealthConfig) { config.IncidentMinAffectedFraction = 0 },
		func(config *LatencyHealthConfig) { config.IncidentMinAffectedFraction = math.Inf(1) },
		func(config *LatencyHealthConfig) { config.PenalizedCooldown = 0 },
		func(config *LatencyHealthConfig) { config.QuarantinedCooldown = 0 },
		func(config *LatencyHealthConfig) { config.HardInvalidCooldown = 0 },
		func(config *LatencyHealthConfig) { config.LatencyHalfLife = 0 },
		func(config *LatencyHealthConfig) { config.ErrorHalfLife = 0 },
	}
	for _, mutate := range invalidMutations {
		invalid := config
		mutate(&invalid)
		require.Error(t, invalid.Validate())
	}

	config.PenalizedCooldown = 7 * time.Second
	config.QuarantinedCooldown = 11 * time.Second
	config.HardInvalidCooldown = 13 * time.Second
	config.QuarantineCap = 1
	config.HardValidFloor = 2
	clock := newLatencyHealthFakeClock(time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC))
	service, err := NewOpenAILatencyHealthService(config, clock)
	require.NoError(t, err)
	cohort := testCohort(0)

	pool := PoolSnapshot{QuarantinedAccounts: 1, HardValidAccounts: 3}
	require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 503, pool)))
	snapshot, ok := service.GetCohortHealth(1, cohort)
	require.True(t, ok)
	require.Equal(t, 7*time.Second, snapshot.CooldownRemaining)

	decision, err := service.Observe(testHealthObservation(1, cohort, 503, pool))
	require.NoError(t, err)
	require.True(t, decision.GuardrailBlocked)

	require.NoError(t, service.RecordObservation(testHealthObservation(2, cohort, 401, PoolSnapshot{HardValidAccounts: 2})))
	snapshot, ok = service.GetCohortHealth(2, cohort)
	require.True(t, ok)
	require.NotEqual(t, HealthStateHardInvalid, snapshot.State)

	require.NoError(t, service.RecordObservation(testHealthObservation(3, cohort, 401, PoolSnapshot{HardValidAccounts: 3})))
	snapshot, ok = service.GetCohortHealth(3, cohort)
	require.True(t, ok)
	require.Equal(t, HealthStateHardInvalid, snapshot.State)
	require.Equal(t, 13*time.Second, snapshot.CooldownRemaining)
}

func TestOpenAILatencyHealthMaintainsStableTimeDecayedAggregates(t *testing.T) {
	config := DefaultLatencyHealthConfig()
	config.Enabled = true
	config.MaxTrackedAccounts = 2
	config.MaxCohortsPerAccount = 2
	clock := newLatencyHealthFakeClock(time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC))
	service, err := NewOpenAILatencyHealthService(config, clock)
	require.NoError(t, err)
	cohort := testCohort(0)
	pool := PoolSnapshot{HardValidAccounts: 20}

	require.NoError(t, service.RecordObservation(OpenAILatencyObservation{
		AccountID: 1, Cohort: cohort, Latency: 2 * time.Second, HTTPStatus: 200, PoolSnapshot: pool,
	}))
	clock.Advance(config.LatencyHalfLife)
	require.NoError(t, service.RecordObservation(OpenAILatencyObservation{
		AccountID: 1, Cohort: cohort, Latency: 4 * time.Second, HTTPStatus: 500, PoolSnapshot: pool,
	}))

	cohortSnapshot, ok := service.GetCohortHealth(1, cohort)
	require.True(t, ok)
	require.InDelta(t, 1.5, cohortSnapshot.DecayedLatencySamples, 1e-12)
	require.InDelta(t, 5000, cohortSnapshot.DecayedLatencySumMilliseconds, 1e-9)
	require.InDelta(t, 1.25, cohortSnapshot.DecayedErrorSamples, 1e-12)
	require.InDelta(t, 1, cohortSnapshot.DecayedErrors, 1e-12)

	accountSnapshot, ok := service.GetAccountHealth(1)
	require.True(t, ok)
	require.InDelta(t, cohortSnapshot.DecayedLatencySamples, accountSnapshot.DecayedLatencySamples, 1e-12)
	require.InDelta(t, cohortSnapshot.DecayedErrors, accountSnapshot.DecayedErrors, 1e-12)

	clock.Advance(config.LatencyHalfLife)
	cohortSnapshot, ok = service.GetCohortHealth(1, cohort)
	require.True(t, ok)
	require.InDelta(t, 0.75, cohortSnapshot.DecayedLatencySamples, 1e-12)
	require.InDelta(t, 2500, cohortSnapshot.DecayedLatencySumMilliseconds, 1e-9)
	require.InDelta(t, 0.3125, cohortSnapshot.DecayedErrorSamples, 1e-12)
	require.InDelta(t, 0.25, cohortSnapshot.DecayedErrors, 1e-12)

	clock.Advance(100 * 365 * 24 * time.Hour)
	cohortSnapshot, ok = service.GetCohortHealth(1, cohort)
	require.True(t, ok)
	require.False(t, math.IsNaN(cohortSnapshot.DecayedLatencySamples))
	require.False(t, math.IsInf(cohortSnapshot.DecayedLatencySamples, 0))
	require.False(t, math.IsNaN(cohortSnapshot.DecayedLatencySumMilliseconds))
	require.False(t, math.IsInf(cohortSnapshot.DecayedLatencySumMilliseconds, 0))
	require.GreaterOrEqual(t, cohortSnapshot.DecayedLatencySamples, float64(0))
	require.GreaterOrEqual(t, cohortSnapshot.DecayedLatencySumMilliseconds, float64(0))

	// Raw latency is telemetry only: even an extreme successful duration cannot
	// hard-disable or otherwise penalize an account.
	require.NoError(t, service.RecordObservation(OpenAILatencyObservation{
		AccountID: 2, Cohort: cohort, Latency: 24 * time.Hour, HTTPStatus: 200, PoolSnapshot: pool,
	}))
	extremeSnapshot, ok := service.GetCohortHealth(2, cohort)
	require.True(t, ok)
	require.Equal(t, HealthStateHealthy, extremeSnapshot.State)
}

func TestOpenAILatencyTelemetryCapturesHedgeAndLoserLifecycle(t *testing.T) {
	key := OpenAILatencyTelemetryKey{
		AccountID:   1,
		Cohort:      testCohort(0),
		AttemptRole: OpenAIAttemptRolePrimary,
	}
	first, err := NewOpenAILatencyTelemetryAggregate(key)
	require.NoError(t, err)
	phases := []OpenAILatencyPhase{
		OpenAILatencyPhaseHedgeEligible,
		OpenAILatencyPhaseSecondaryDispatch,
		OpenAILatencyPhaseWinnerCommit,
		OpenAILatencyPhaseTerminal,
	}
	for _, phase := range phases {
		require.NoError(t, first.ObservePhase(phase, time.Second))
	}
	events := []OpenAILoserLifecycleEvent{
		OpenAILoserLifecycleCancelRequested,
		OpenAILoserLifecycleTransportClosed,
		OpenAILoserLifecycleTerminalAfterCancel,
		OpenAILoserLifecycleUsageAfterCancel,
		OpenAILoserLifecycleBillingAfterCancel,
	}
	for _, event := range events {
		require.NoError(t, first.ObserveLoserLifecycle(event))
	}

	second, err := NewOpenAILatencyTelemetryAggregate(key)
	require.NoError(t, err)
	require.NoError(t, second.ObservePhase(OpenAILatencyPhaseTerminal, 2*time.Second))
	require.NoError(t, second.ObserveLoserLifecycle(OpenAILoserLifecycleTerminalAfterCancel))
	require.NoError(t, first.Merge(second.Snapshot()))
	merged := first.Snapshot()
	for _, phase := range phases[:3] {
		require.Equal(t, uint64(1), merged.Phases[phase].Observations)
	}
	require.Equal(t, uint64(2), merged.Phases[OpenAILatencyPhaseTerminal].Observations)
	for _, event := range events {
		expected := uint64(1)
		if event == OpenAILoserLifecycleTerminalAfterCancel {
			expected = 2
		}
		require.Equal(t, expected, merged.LoserLifecycle[event])
	}

	var cumulative OpenAILatencyTelemetrySnapshot
	require.NoError(t, cumulative.MergeCumulative(merged))
	once := cumulative
	require.NoError(t, cumulative.MergeCumulative(merged))
	require.Equal(t, once, cumulative)

	newer := merged
	newer.Sequence++
	newer.LoserLifecycle[OpenAILoserLifecycleBillingAfterCancel]++
	newer.Phases[OpenAILatencyPhaseTerminal].Observations++
	require.NoError(t, cumulative.MergeCumulative(newer))
	require.Equal(t, newer.LoserLifecycle, cumulative.LoserLifecycle)
	require.Equal(t, newer.Phases[OpenAILatencyPhaseTerminal], cumulative.Phases[OpenAILatencyPhaseTerminal])
}

func TestOpenAILatencyTransitionGuardReservesConcurrentAccountTransitions(t *testing.T) {
	const accountCount = 32
	config := DefaultLatencyHealthConfig()
	config.Enabled = true
	config.MaxTrackedAccounts = accountCount
	config.MaxCohortsPerAccount = 2
	config.IncidentMinAffectedAccounts = accountCount + 1
	config.QuarantineCap = 3
	clock := newLatencyHealthFakeClock(time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC))
	guard, err := NewLocalLatencyHealthTransitionGuard(accountCount)
	require.NoError(t, err)
	service, err := NewOpenAILatencyHealthServiceWithTransitionGuard(config, clock, guard)
	require.NoError(t, err)
	pool := PoolSnapshot{HardValidAccounts: 100}

	for accountID := int64(1); accountID <= accountCount; accountID++ {
		require.NoError(t, service.RecordObservation(testHealthObservation(accountID, testCohort(0), 500, pool)))
	}

	var wait sync.WaitGroup
	errorsCh := make(chan error, accountCount)
	for accountID := int64(1); accountID <= accountCount; accountID++ {
		accountID := accountID
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsCh <- service.RecordObservation(testHealthObservation(accountID, testCohort(0), 500, pool))
		}()
	}
	wait.Wait()
	close(errorsCh)
	for observeErr := range errorsCh {
		require.NoError(t, observeErr)
	}

	quarantined := 0
	for accountID := int64(1); accountID <= accountCount; accountID++ {
		snapshot, ok := service.GetAccountHealth(accountID)
		require.True(t, ok)
		if snapshot.State == HealthStateQuarantined {
			quarantined++
		}
	}
	require.Equal(t, config.QuarantineCap, quarantined)
	require.Equal(t, config.QuarantineCap, guard.Stats().SoftQuarantined)

	hardConfig := config
	hardConfig.HardValidFloor = 7
	hardGuard, err := NewLocalLatencyHealthTransitionGuard(accountCount)
	require.NoError(t, err)
	hardService, err := NewOpenAILatencyHealthServiceWithTransitionGuard(hardConfig, clock, hardGuard)
	require.NoError(t, err)
	hardPool := PoolSnapshot{HardValidAccounts: 10}
	errorsCh = make(chan error, accountCount)
	for accountID := int64(1); accountID <= accountCount; accountID++ {
		accountID := accountID
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsCh <- hardService.RecordObservation(testHealthObservation(accountID, testCohort(0), 401, hardPool))
		}()
	}
	wait.Wait()
	close(errorsCh)
	for observeErr := range errorsCh {
		require.NoError(t, observeErr)
	}

	hardInvalid := 0
	for accountID := int64(1); accountID <= accountCount; accountID++ {
		snapshot, ok := hardService.GetAccountHealth(accountID)
		require.True(t, ok)
		if snapshot.State == HealthStateHardInvalid {
			hardInvalid++
		}
	}
	require.Equal(t, hardPool.HardValidAccounts-hardConfig.HardValidFloor, hardInvalid)
	require.Equal(t, hardInvalid, hardGuard.Stats().HardInvalid)
}

func TestOpenAILatencyHealthRechecksIncidentAfterAccountLock(t *testing.T) {
	config := DefaultLatencyHealthConfig()
	config.Enabled = true
	config.MaxTrackedAccounts = 4
	config.MaxCohortsPerAccount = 2
	config.IncidentMinAffectedAccounts = 2
	config.IncidentMinAffectedFraction = 1
	clock := newLatencyHealthFakeClock(time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC))
	service, err := NewOpenAILatencyHealthService(config, clock)
	require.NoError(t, err)
	pool := PoolSnapshot{HardValidAccounts: 20}
	cohort := testCohort(0)
	for accountID := int64(1); accountID <= 2; accountID++ {
		require.NoError(t, service.RecordObservation(testHealthObservation(accountID, cohort, 200, pool)))
	}

	targetAccount, ok := service.lookupAccount(1)
	require.True(t, ok)
	targetAccount.mu.Lock()
	locked := true
	defer func() {
		if locked {
			targetAccount.mu.Unlock()
		}
	}()

	type observationResult struct {
		decision LatencyHealthDecision
		err      error
	}
	targetResult := make(chan observationResult, 1)
	go func() {
		decision, observeErr := service.Observe(testHealthObservation(1, cohort, 500, pool))
		targetResult <- observationResult{decision: decision, err: observeErr}
	}()
	require.Eventually(t, func() bool {
		return service.IncidentCorrelation(FailureFingerprint(1)).AffectedAccounts == 1
	}, time.Second, time.Millisecond)

	activatorResult := make(chan observationResult, 1)
	go func() {
		decision, observeErr := service.Observe(testHealthObservation(2, cohort, 500, pool))
		activatorResult <- observationResult{decision: decision, err: observeErr}
	}()
	require.Eventually(t, func() bool {
		return service.IncidentCorrelation(FailureFingerprint(1)).Active
	}, time.Second, time.Millisecond)

	targetAccount.mu.Unlock()
	locked = false
	target := <-targetResult
	activator := <-activatorResult
	require.NoError(t, target.err)
	require.NoError(t, activator.err)
	require.True(t, target.decision.ProviderIncident)
	require.True(t, target.decision.PenaltySuppressed)
	targetSnapshot, ok := service.GetAccountHealth(1)
	require.True(t, ok)
	require.Equal(t, HealthStateHealthy, targetSnapshot.State)
}

func TestOpenAILatencyIncidentGenerationLiftsOncePerConcurrentActivation(t *testing.T) {
	config := DefaultLatencyHealthConfig()
	config.Enabled = true
	config.MaxTrackedAccounts = 3
	config.MaxCohortsPerAccount = 2
	config.IncidentMinAffectedAccounts = 2
	config.IncidentMinAffectedFraction = 0.5
	clock := newLatencyHealthFakeClock(time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC))
	service, err := NewOpenAILatencyHealthService(config, clock)
	require.NoError(t, err)
	pool := PoolSnapshot{HardValidAccounts: 20}
	cohort := testCohort(0)

	require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 500, pool)))
	firstActivation, err := service.Observe(testHealthObservation(2, cohort, 500, pool))
	require.NoError(t, err)
	require.True(t, firstActivation.ProviderIncident)
	require.Equal(t, 2, firstActivation.LiftedSoftPenalties)
	firstCorrelation := service.IncidentCorrelation(FailureFingerprint(1))
	require.True(t, firstCorrelation.Active)
	require.Equal(t, uint64(1), firstCorrelation.Generation)

	retry, err := service.Observe(testHealthObservation(2, cohort, 500, pool))
	require.NoError(t, err)
	require.Zero(t, retry.LiftedSoftPenalties)

	clock.Advance(config.IncidentWindow + time.Second)
	require.False(t, service.IncidentCorrelation(FailureFingerprint(1)).Active)
	require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 500, pool)))

	results := make(chan LatencyHealthDecision, 2)
	errorsCh := make(chan error, 2)
	var wait sync.WaitGroup
	for accountID := int64(2); accountID <= 3; accountID++ {
		accountID := accountID
		wait.Add(1)
		go func() {
			defer wait.Done()
			decision, observeErr := service.Observe(testHealthObservation(accountID, cohort, 500, pool))
			results <- decision
			errorsCh <- observeErr
		}()
	}
	wait.Wait()
	close(results)
	close(errorsCh)
	for observeErr := range errorsCh {
		require.NoError(t, observeErr)
	}
	lifted := 0
	for decision := range results {
		lifted += decision.LiftedSoftPenalties
	}
	require.Equal(t, 2, lifted)
	secondCorrelation := service.IncidentCorrelation(FailureFingerprint(1))
	require.True(t, secondCorrelation.Active)
	require.Equal(t, uint64(2), secondCorrelation.Generation)

	accountSnapshot, ok := service.GetAccountHealth(1)
	require.True(t, ok)
	cohortSnapshot, ok := service.GetCohortHealth(1, cohort)
	require.True(t, ok)
	require.Equal(t, uint64(2), accountSnapshot.IncidentLiftedPenalties)
	require.Equal(t, uint64(2), cohortSnapshot.IncidentLiftedPenalties)
}

func TestOpenAILatencyTelemetryMergerDeduplicatesConcurrentCumulativeSources(t *testing.T) {
	key := OpenAILatencyTelemetryKey{
		AccountID:   42,
		Cohort:      testCohort(0),
		AttemptRole: OpenAIAttemptRolePrimary,
	}
	identity := func(sourceID, generation uint64) OpenAILatencyTelemetryIdentity {
		return OpenAILatencyTelemetryIdentity{
			SourceID:       sourceID,
			Generation:     generation,
			BucketStart:    1_723_204_800_000_000_000,
			BucketDuration: int64(time.Minute),
		}
	}

	first, err := NewOpenAILatencyTelemetryAggregateWithIdentity(key, identity(1, 1))
	require.NoError(t, err)
	require.NoError(t, first.ObservePhase(OpenAILatencyPhaseTotal, 10*time.Millisecond))
	older := first.Snapshot()
	require.NoError(t, first.ObservePhase(OpenAILatencyPhaseTotal, 20*time.Millisecond))
	newer := first.Snapshot()

	second, err := NewOpenAILatencyTelemetryAggregateWithIdentity(key, identity(2, 1))
	require.NoError(t, err)
	require.NoError(t, second.ObservePhase(OpenAILatencyPhaseTotal, 30*time.Millisecond))

	restarted, err := NewOpenAILatencyTelemetryAggregateWithIdentity(key, identity(1, 2))
	require.NoError(t, err)
	require.NoError(t, restarted.ObservePhase(OpenAILatencyPhaseTotal, 40*time.Millisecond))

	merger, err := NewOpenAILatencyTelemetryMerger(4)
	require.NoError(t, err)
	inputs := []OpenAILatencyTelemetrySnapshot{older, newer, second.Snapshot(), restarted.Snapshot()}
	errorsCh := make(chan error, 64)
	var wait sync.WaitGroup
	for index := 0; index < 64; index++ {
		input := inputs[index%len(inputs)]
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, mergeErr := merger.MergeCumulative(input)
			errorsCh <- mergeErr
		}()
	}
	wait.Wait()
	close(errorsCh)
	for mergeErr := range errorsCh {
		require.NoError(t, mergeErr)
	}

	merged := merger.Snapshot()
	require.Equal(t, uint64(4), merged.Phases[OpenAILatencyPhaseTotal].Observations)
	require.Equal(t, uint64(100), merged.Phases[OpenAILatencyPhaseTotal].LatencyMilliseconds)
	require.Zero(t, merged.SourceID)
	require.Equal(t, int64(time.Minute), merged.BucketDuration)
	require.Equal(t, 3, merger.Stats().Streams)

	var singleSource OpenAILatencyTelemetrySnapshot
	require.NoError(t, singleSource.MergeCumulative(older))
	require.NoError(t, singleSource.MergeCumulative(newer))
	require.Equal(t, newer, singleSource)
	require.NoError(t, singleSource.MergeCumulative(older))
	require.Equal(t, newer, singleSource)

	replacement := newer
	replacement.Sequence++
	replacement.Phases[OpenAILatencyPhaseTotal] = OpenAIPhaseTelemetryAggregate{
		Observations:        1,
		LatencyMilliseconds: 5,
	}
	require.NoError(t, singleSource.MergeCumulative(replacement))
	require.Equal(t, replacement, singleSource)
}

func TestOpenAILatencyHealthGlobalTrackBudgetIsConcurrentAndObservable(t *testing.T) {
	const (
		accountCount   = 16
		cohortsPerTest = 16
		maxTracks      = 2*accountCount + 8
	)
	config := DefaultLatencyHealthConfig()
	config.Enabled = true
	config.MaxTrackedAccounts = accountCount
	config.MaxCohortsPerAccount = MaximumLatencyHealthCohortsPerAccount
	config.MaxTracks = maxTracks
	config.IncidentMinAffectedAccounts = accountCount + 1
	clock := newLatencyHealthFakeClock(time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC))
	service, err := NewOpenAILatencyHealthService(config, clock)
	require.NoError(t, err)
	pool := PoolSnapshot{HardValidAccounts: 100}

	var wait sync.WaitGroup
	var trackLimitDecisions atomic.Uint64
	errorsCh := make(chan error, accountCount*cohortsPerTest)
	for accountID := int64(1); accountID <= accountCount; accountID++ {
		for cohortIndex := 0; cohortIndex < cohortsPerTest; cohortIndex++ {
			accountID := accountID
			cohort := testCohort(int(accountID)*cohortsPerTest + cohortIndex)
			wait.Add(1)
			go func() {
				defer wait.Done()
				decision, observeErr := service.Observe(testHealthObservation(accountID, cohort, 200, pool))
				if decision.TrackLimitOverflow {
					trackLimitDecisions.Add(1)
				}
				errorsCh <- observeErr
			}()
		}
	}
	wait.Wait()
	close(errorsCh)
	for observeErr := range errorsCh {
		require.NoError(t, observeErr)
	}

	stats := service.Stats()
	require.Equal(t, accountCount, stats.Accounts)
	require.Equal(t, maxTracks, stats.Tracks)
	require.LessOrEqual(t, stats.Tracks, stats.MaxTracks)
	require.Equal(t, 8, stats.TrackedCohorts)
	require.Positive(t, stats.OverflowAccounts)
	require.Equal(t, trackLimitDecisions.Load(), stats.TrackLimitOverflows)
	require.Zero(t, stats.DroppedObservations)

	before := stats.Tracks
	decision, err := service.Observe(testHealthObservation(1, testCohort(10_000), 200, pool))
	require.NoError(t, err)
	require.True(t, decision.UsedOverflow)
	require.True(t, decision.TrackLimitOverflow)
	require.Equal(t, before, service.Stats().Tracks)

	dropped, err := service.Observe(testHealthObservation(accountCount+1, testCohort(0), 200, pool))
	require.ErrorIs(t, err, ErrLatencyHealthAccountLimit)
	require.True(t, dropped.ObservationDropped)
	require.Equal(t, uint64(1), service.Stats().DroppedObservations)
	require.Equal(t, before, service.Stats().Tracks)
}
