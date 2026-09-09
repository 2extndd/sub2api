package service

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

type latencyHealthFakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newLatencyHealthFakeClock(now time.Time) *latencyHealthFakeClock {
	return &latencyHealthFakeClock{now: now}
}

func (c *latencyHealthFakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *latencyHealthFakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func newEnabledLatencyHealthService(t *testing.T, maxAccounts, maxCohorts int) (*OpenAILatencyHealthService, *latencyHealthFakeClock) {
	t.Helper()
	config := DefaultLatencyHealthConfig()
	config.Enabled = true
	config.MaxTrackedAccounts = maxAccounts
	config.MaxCohortsPerAccount = maxCohorts
	clock := newLatencyHealthFakeClock(time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC))
	service, err := NewOpenAILatencyHealthService(config, clock)
	require.NoError(t, err)
	return service, clock
}

func testCohort(index int) CohortKey {
	endpointCount := int(openAIEndpointClassCount - 1)
	modelCount := int(openAIModelClassCount - 1)
	reasoningCount := int(openAIReasoningClassCount - 1)
	toolUseCount := int(openAIToolUseClassCount - 1)
	endpoint := OpenAIEndpointClass(1 + index%endpointCount)
	model := OpenAIModelClass(1 + (index/endpointCount)%modelCount)
	streaming := (index/(endpointCount*modelCount))%2 == 1
	size := OpenAIInputSizeClass(1 + (index/(2*endpointCount*modelCount))%3)
	reasoning := OpenAIReasoningClass(1 + (index/(6*endpointCount*modelCount))%reasoningCount)
	toolUse := OpenAIToolUseClass(1 + (index/(6*endpointCount*modelCount*reasoningCount))%toolUseCount)
	return NewCohortKey(endpoint, model, streaming, size, reasoning, toolUse)
}

func testHealthObservation(accountID int64, cohort CohortKey, status int, pool PoolSnapshot) OpenAILatencyObservation {
	return OpenAILatencyObservation{
		AccountID:    accountID,
		Cohort:       cohort,
		Latency:      250 * time.Millisecond,
		HTTPStatus:   status,
		PoolSnapshot: pool,
	}
}

func TestOpenAILatencyHealthConfigValidationReturnsErrors(t *testing.T) {
	clock := newLatencyHealthFakeClock(time.Now())

	zeroValueService, err := NewOpenAILatencyHealthService(LatencyHealthConfig{}, clock)
	require.NoError(t, err)
	require.False(t, zeroValueService.Stats().Enabled)
	require.Nil(t, zeroValueService.accounts)

	valid := DefaultLatencyHealthConfig()
	valid.Enabled = true
	service, err := NewOpenAILatencyHealthService(valid, clock)
	require.NoError(t, err)
	require.NotNil(t, service)

	require.Equal(t, 256, valid.MaxTrackedAccounts)
	require.Equal(t, 64, valid.MaxCohortsPerAccount)
	require.Equal(t, 8192, valid.MaxTracks)
	require.Equal(t, 0.25, valid.WeightFloor)
	require.Equal(t, 3, valid.QuarantineCap)
	require.Equal(t, 7, valid.HardValidFloor)
	require.Equal(t, 90*time.Second, valid.IncidentWindow)
	require.Equal(t, 4, valid.IncidentMinAffectedAccounts)
	require.Equal(t, 0.4, valid.IncidentMinAffectedFraction)
	require.Equal(t, 30*time.Second, valid.PenalizedCooldown)
	require.Equal(t, 120*time.Second, valid.QuarantinedCooldown)
	require.Equal(t, 300*time.Second, valid.HardInvalidCooldown)
	require.Equal(t, 1800*time.Second, valid.LatencyHalfLife)
	require.Equal(t, 900*time.Second, valid.ErrorHalfLife)

	tests := map[string]func(*LatencyHealthConfig){
		"zero accounts":             func(c *LatencyHealthConfig) { c.MaxTrackedAccounts = 0 },
		"zero cohorts":              func(c *LatencyHealthConfig) { c.MaxCohortsPerAccount = 0 },
		"zero tracks":               func(c *LatencyHealthConfig) { c.MaxTracks = 0 },
		"insufficient tracks":       func(c *LatencyHealthConfig) { c.MaxTracks = 2*c.MaxTrackedAccounts - 1 },
		"too many accounts":         func(c *LatencyHealthConfig) { c.MaxTrackedAccounts = MaximumLatencyHealthTrackedAccounts + 1 },
		"too many cohorts":          func(c *LatencyHealthConfig) { c.MaxCohortsPerAccount = MaximumLatencyHealthCohortsPerAccount + 1 },
		"too many tracks":           func(c *LatencyHealthConfig) { c.MaxTracks = MaximumLatencyHealthTracks + 1 },
		"zero weight floor":         func(c *LatencyHealthConfig) { c.WeightFloor = 0 },
		"non-finite weight floor":   func(c *LatencyHealthConfig) { c.WeightFloor = math.NaN() },
		"negative quarantine cap":   func(c *LatencyHealthConfig) { c.QuarantineCap = -1 },
		"negative hard-valid floor": func(c *LatencyHealthConfig) { c.HardValidFloor = -1 },
		"zero incident window":      func(c *LatencyHealthConfig) { c.IncidentWindow = 0 },
		"zero incident accounts":    func(c *LatencyHealthConfig) { c.IncidentMinAffectedAccounts = 0 },
		"zero incident fraction":    func(c *LatencyHealthConfig) { c.IncidentMinAffectedFraction = 0 },
		"zero penalized cooldown":   func(c *LatencyHealthConfig) { c.PenalizedCooldown = 0 },
		"zero quarantine cooldown":  func(c *LatencyHealthConfig) { c.QuarantinedCooldown = 0 },
		"zero hard cooldown":        func(c *LatencyHealthConfig) { c.HardInvalidCooldown = 0 },
		"zero latency half-life":    func(c *LatencyHealthConfig) { c.LatencyHalfLife = 0 },
		"zero error half-life":      func(c *LatencyHealthConfig) { c.ErrorHalfLife = 0 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config := valid
			mutate(&config)
			require.Error(t, ValidateLatencyHealthConfig(config))
			_, constructionErr := NewOpenAILatencyHealthService(config, clock)
			require.Error(t, constructionErr)
		})
	}

	disabledInvalid := LatencyHealthConfig{MaxTrackedAccounts: -1}
	require.Error(t, ValidateLatencyHealthConfig(disabledInvalid))

	_, err = NewOpenAILatencyHealthService(DefaultLatencyHealthConfig(), nil)
	require.ErrorContains(t, err, "clock")
}

func TestOpenAILatencyHealthDisabledModeAllocatesNoMapAndIsNoop(t *testing.T) {
	service, err := NewOpenAILatencyHealthService(DefaultLatencyHealthConfig(), newLatencyHealthFakeClock(time.Now()))
	require.NoError(t, err)
	require.Nil(t, service.accounts)
	require.Nil(t, service.incidentSampled.entries)

	observation := OpenAILatencyObservation{
		AccountID:    -1,
		Cohort:       CohortKey(math.MaxUint16 - 1),
		Latency:      -time.Second,
		HTTPStatus:   999,
		Failure:      FailureFingerprint(255),
		PoolSnapshot: PoolSnapshot{QuarantinedAccounts: -1, HardValidAccounts: -1},
	}
	decision, err := service.Observe(observation)
	require.NoError(t, err)
	require.True(t, decision.Disabled)
	require.Nil(t, service.accounts)
	require.Nil(t, service.incidentSampled.entries)
	_, ok := service.GetAccountHealth(1)
	require.False(t, ok)

	allocations := testing.AllocsPerRun(1_000, func() {
		if err := service.RecordObservation(observation); err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocations)
}

func TestOpenAILatencyHealthCohortKeyIsCanonicalAndBounded(t *testing.T) {
	first := CanonicalCohortKey(
		" HTTPS://api.openai.com/v1/responses?trace=secret ",
		" O3-mini ",
		true,
		4_096,
		" HIGH ",
		2,
	)
	second := CanonicalCohortKey("/v1/responses", "o3-mini", true, 4_096, "high", 4)
	require.Equal(t, second, first)
	require.Equal(t, uintptr(2), unsafe.Sizeof(CohortKey(0)))
	require.NotContains(t, first.String(), "o3-mini")
	require.NotContains(t, first.String(), "trace")
	require.Contains(t, first.String(), "reasoning-high")
	require.Contains(t, first.String(), "tools-2-4")

	withoutReasoning := CanonicalCohortKey("/v1/responses", "o3-mini", true, 4_096, "none", 1)
	withoutTools := CanonicalCohortKey("/v1/responses", "o3-mini", true, 4_096, "high", 0)
	require.NotEqual(t, first, withoutReasoning)
	require.NotEqual(t, first, withoutTools)

	longModel := strings.Repeat("caller-controlled-model-", 10_000)
	longReasoning := strings.Repeat("caller-controlled-reasoning-", 10_000)
	longKey := CanonicalCohortKey("/unknown/"+longModel, longModel, false, math.MaxInt64, longReasoning, -1)
	require.Equal(t, NewCohortKey(
		OpenAIEndpointUnknown,
		OpenAIModelText,
		false,
		OpenAIInputSizeLarge,
		OpenAIReasoningUnknown,
		OpenAIToolUseUnknown,
	), longKey)
	require.NotContains(t, longKey.String(), "caller-controlled")

	reasoningClasses := map[string]OpenAIReasoningClass{
		"":        OpenAIReasoningUnknown,
		"invalid": OpenAIReasoningUnknown,
		"none":    OpenAIReasoningNone,
		"minimal": OpenAIReasoningMinimal,
		"low":     OpenAIReasoningLow,
		"medium":  OpenAIReasoningMedium,
		" HIGH ":  OpenAIReasoningHigh,
		"xhigh":   OpenAIReasoningXHigh,
	}
	for effort, expected := range reasoningClasses {
		require.Equal(t, expected, classifyOpenAIReasoning(effort), effort)
	}
	toolUseClasses := map[int]OpenAIToolUseClass{
		-1: OpenAIToolUseUnknown,
		0:  OpenAIToolUseNone,
		1:  OpenAIToolUseOne,
		2:  OpenAIToolUseTwoToFour,
		4:  OpenAIToolUseTwoToFour,
		5:  OpenAIToolUseFivePlus,
		99: OpenAIToolUseFivePlus,
	}
	for count, expected := range toolUseClasses {
		require.Equal(t, expected, classifyOpenAIToolUse(count), count)
	}

	require.Equal(t, UnknownCohortKey, CohortKey(65_000).Canonical())
	require.Equal(t, OverflowCohortKey, OverflowCohortKey.Canonical())

	keys := make(map[CohortKey]struct{})
	for endpoint := OpenAIEndpointClass(0); endpoint < openAIEndpointClassCount; endpoint++ {
		for model := OpenAIModelClass(0); model < openAIModelClassCount; model++ {
			for size := OpenAIInputSizeClass(0); size < openAIInputSizeClassCount; size++ {
				for reasoning := OpenAIReasoningClass(0); reasoning < openAIReasoningClassCount; reasoning++ {
					for toolUse := OpenAIToolUseClass(0); toolUse < openAIToolUseClassCount; toolUse++ {
						keys[NewCohortKey(endpoint, model, false, size, reasoning, toolUse)] = struct{}{}
						keys[NewCohortKey(endpoint, model, true, size, reasoning, toolUse)] = struct{}{}
					}
				}
			}
		}
	}
	maximumKeys := int(openAIEndpointClassCount) * int(openAIModelClassCount) *
		int(openAIInputSizeClassCount) * int(openAIReasoningClassCount) *
		int(openAIToolUseClassCount) * 2
	require.Equal(t, maximumKeys, len(keys))
	require.Equal(t, 64, DefaultLatencyHealthConfig().MaxCohortsPerAccount)
}

func TestOpenAILatencyHealthBoundsAccountsCohortsHistogramsAndTimeBuckets(t *testing.T) {
	service, _ := newEnabledLatencyHealthService(t, 2, 3)
	pool := PoolSnapshot{HardValidAccounts: 20}

	for accountID := int64(1); accountID <= 2; accountID++ {
		for cohortIndex := 0; cohortIndex < 100; cohortIndex++ {
			err := service.RecordObservation(testHealthObservation(accountID, testCohort(cohortIndex), 200, pool))
			require.NoError(t, err)
		}
	}

	stats := service.Stats()
	require.Equal(t, 2, stats.Accounts)
	require.Equal(t, 6, stats.TrackedCohorts)
	require.Equal(t, 2, stats.OverflowAccounts)
	require.Equal(t, 10, stats.Tracks)

	for accountID := int64(1); accountID <= 2; accountID++ {
		snapshot, ok := service.SnapshotAccount(accountID)
		require.True(t, ok)
		require.Equal(t, 3, snapshot.TrackedCohorts)
		require.True(t, snapshot.OverflowUsed)
		require.Len(t, snapshot.Cohorts, 4)
		require.Len(t, snapshot.Aggregate.LatencyHistogram, OpenAILatencyHistogramBucketCount)
		require.Len(t, snapshot.Aggregate.Window.Buckets, OpenAILatencyTimeBucketCount)
		require.Equal(t, uint64(100), snapshot.Aggregate.TotalRequests)
	}

	err := service.RecordObservation(testHealthObservation(3, testCohort(0), 200, pool))
	require.ErrorIs(t, err, ErrLatencyHealthAccountLimit)
	require.Equal(t, 2, service.Stats().Accounts)
}

func TestOpenAILatencyHealthMaintainsAccountAggregateAndCohortIsolation(t *testing.T) {
	service, _ := newEnabledLatencyHealthService(t, 8, 4)
	pool := PoolSnapshot{HardValidAccounts: 12}
	cohortA := testCohort(0)
	cohortB := testCohort(1)

	require.NoError(t, service.RecordObservation(testHealthObservation(1, cohortA, 500, pool)))
	require.NoError(t, service.RecordObservation(testHealthObservation(1, cohortA, 500, pool)))
	require.NoError(t, service.RecordObservation(testHealthObservation(1, cohortB, 200, pool)))
	require.NoError(t, service.RecordObservation(testHealthObservation(2, cohortA, 200, pool)))

	accountOne, ok := service.GetAccountHealth(1)
	require.True(t, ok)
	require.Equal(t, HealthStateQuarantined, accountOne.State)
	require.Equal(t, uint64(3), accountOne.TotalRequests)
	require.Equal(t, uint64(2), accountOne.TotalHTTP5xx)

	accountOneA, ok := service.GetCohortHealth(1, cohortA)
	require.True(t, ok)
	require.Equal(t, HealthStateQuarantined, accountOneA.State)
	require.Equal(t, uint64(2), accountOneA.TotalFailures)

	accountOneB, ok := service.GetCohortHealth(1, cohortB)
	require.True(t, ok)
	require.Equal(t, HealthStateHealthy, accountOneB.State)
	require.Equal(t, uint64(1), accountOneB.TotalSuccesses)

	accountTwo, ok := service.GetAccountHealth(2)
	require.True(t, ok)
	require.Equal(t, HealthStateHealthy, accountTwo.State)
	require.Equal(t, uint64(1), accountTwo.TotalRequests)
}

func TestOpenAILatencyHealthSoftFailuresUseDefaultCooldownsAndNeverHardInvalidate(t *testing.T) {
	runtimeFailures := []FailureFingerprint{
		FailureFingerprintTimeout,
		FailureFingerprintConnection,
		FailureFingerprintStream,
		FailureFingerprintProtocol,
		FailureFingerprintCapacity,
		FailureFingerprintOther,
	}

	t.Run("http 5xx remains soft", func(t *testing.T) {
		service, _ := newEnabledLatencyHealthService(t, 4, 2)
		cohort := testCohort(0)
		pool := PoolSnapshot{HardValidAccounts: 100}

		require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 503, pool)))
		snapshot, ok := service.GetCohortHealth(1, cohort)
		require.True(t, ok)
		require.Equal(t, HealthStatePenalized, snapshot.State)
		require.Equal(t, OpenAILatencyPenalizedCooldown, snapshot.CooldownRemaining)

		require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 503, pool)))
		snapshot, _ = service.GetCohortHealth(1, cohort)
		require.Equal(t, HealthStateQuarantined, snapshot.State)
		require.Equal(t, OpenAILatencyQuarantinedCooldown, snapshot.CooldownRemaining)

		for range 10 {
			require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 599, pool)))
		}
		snapshot, _ = service.GetCohortHealth(1, cohort)
		require.Equal(t, HealthStateQuarantined, snapshot.State)
		require.Equal(t, OpenAILatencyQuarantinedCooldown, snapshot.CooldownRemaining)
		account, _ := service.GetAccountHealth(1)
		require.Equal(t, HealthStateQuarantined, account.State)
	})

	for _, failure := range runtimeFailures {
		failure := failure
		t.Run(failure.String(), func(t *testing.T) {
			service, _ := newEnabledLatencyHealthService(t, 2, 2)
			observation := testHealthObservation(1, testCohort(0), 0, PoolSnapshot{HardValidAccounts: 100})
			observation.Failure = failure
			for range 10 {
				require.NoError(t, service.RecordObservation(observation))
			}
			snapshot, _ := service.GetAccountHealth(1)
			require.Equal(t, HealthStateQuarantined, snapshot.State)
			require.NotEqual(t, HealthStateHardInvalid, snapshot.State)
		})
	}
}

func TestOpenAILatencyHealthEnforcesQuarantineCapAndHardValidFloorFromPoolSnapshot(t *testing.T) {
	t.Run("quarantine cap", func(t *testing.T) {
		service, _ := newEnabledLatencyHealthService(t, 2, 2)
		cohort := testCohort(0)
		pool := PoolSnapshot{QuarantinedAccounts: OpenAILatencyQuarantineCap, HardValidAccounts: 20}
		require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 500, pool)))
		decision, err := service.Observe(testHealthObservation(1, cohort, 500, pool))
		require.NoError(t, err)
		require.True(t, decision.GuardrailBlocked)
		snapshot, _ := service.GetCohortHealth(1, cohort)
		require.Equal(t, HealthStatePenalized, snapshot.State)
		require.Equal(t, uint64(1), snapshot.GuardrailBlockedTransitions)
	})

	t.Run("hard-valid floor blocks authentication invalidation", func(t *testing.T) {
		service, _ := newEnabledLatencyHealthService(t, 2, 2)
		cohort := testCohort(0)
		pool := PoolSnapshot{HardValidAccounts: OpenAILatencyHardValidFloor}
		decision, err := service.Observe(testHealthObservation(1, cohort, 401, pool))
		require.NoError(t, err)
		require.True(t, decision.GuardrailBlocked)
		require.Equal(t, FailureFingerprintUnauthorized, decision.Failure)
		snapshot, _ := service.GetCohortHealth(1, cohort)
		require.Equal(t, HealthStateHealthy, snapshot.State)
		require.Equal(t, uint64(1), snapshot.GuardrailBlockedTransitions)
	})

	t.Run("401 and 403 invalidate only when seven remain", func(t *testing.T) {
		for _, status := range []int{401, 403} {
			service, _ := newEnabledLatencyHealthService(t, 2, 2)
			cohort := testCohort(0)
			pool := PoolSnapshot{HardValidAccounts: OpenAILatencyHardValidFloor + 1}
			decision, err := service.Observe(testHealthObservation(1, cohort, status, pool))
			require.NoError(t, err)
			require.False(t, decision.ProviderIncident)
			require.Equal(t, HealthStateHardInvalid, decision.AccountState)
			require.Equal(t, HealthStateHardInvalid, decision.CohortState)
			snapshot, _ := service.GetCohortHealth(1, cohort)
			require.Equal(t, HealthStateHardInvalid, snapshot.State)
			require.Equal(t, OpenAILatencyHardInvalidCooldown, snapshot.CooldownRemaining)
		}
	})
}

func TestOpenAILatencyHealthProbationAndSuccessfulHardRevalidation(t *testing.T) {
	t.Run("soft quarantine expires into probation", func(t *testing.T) {
		service, clock := newEnabledLatencyHealthService(t, 2, 2)
		cohort := testCohort(0)
		pool := PoolSnapshot{HardValidAccounts: 12}
		require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 504, pool)))
		require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 504, pool)))
		clock.Advance(OpenAILatencyQuarantinedCooldown)

		snapshot, _ := service.GetCohortHealth(1, cohort)
		require.Equal(t, HealthStateProbation, snapshot.State)
		require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 200, pool)))
		snapshot, _ = service.GetCohortHealth(1, cohort)
		require.Equal(t, HealthStateHealthy, snapshot.State)
	})

	t.Run("hard revalidation succeeds into probation", func(t *testing.T) {
		service, clock := newEnabledLatencyHealthService(t, 2, 2)
		cohort := testCohort(0)
		pool := PoolSnapshot{HardValidAccounts: 8}
		require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 401, pool)))
		require.NoError(t, service.RecordHardRevalidation(1, cohort, false))
		clock.Advance(10 * time.Second)
		snapshot, _ := service.GetCohortHealth(1, cohort)
		require.Equal(t, HealthStateHardInvalid, snapshot.State)
		require.Equal(t, OpenAILatencyHardInvalidCooldown-10*time.Second, snapshot.CooldownRemaining)
		require.Equal(t, uint64(1), snapshot.HardRevalidationFailures)

		require.NoError(t, service.RecordSuccessfulHardRevalidation(1, cohort))
		snapshot, _ = service.GetCohortHealth(1, cohort)
		require.Equal(t, HealthStateProbation, snapshot.State)
		require.Equal(t, uint64(1), snapshot.HardRevalidationSuccesses)
		account, _ := service.GetAccountHealth(1)
		require.Equal(t, HealthStateProbation, account.State)

		require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 200, pool)))
		snapshot, _ = service.GetCohortHealth(1, cohort)
		require.Equal(t, HealthStateHealthy, snapshot.State)
	})
}

func TestOpenAILatencyHealthClassifiesEveryHTTP5xx(t *testing.T) {
	service, _ := newEnabledLatencyHealthService(t, 128, 2)
	pool := PoolSnapshot{HardValidAccounts: 128}
	seen := make(map[FailureFingerprint]struct{}, 100)

	for status := 500; status <= 599; status++ {
		fingerprint, ok := ClassifyHTTPFailure(status)
		require.True(t, ok, "status %d", status)
		roundTrip, ok := fingerprint.HTTPStatus()
		require.True(t, ok)
		require.Equal(t, status, roundTrip)
		seen[fingerprint] = struct{}{}

		require.NoError(t, service.RecordObservation(testHealthObservation(int64(status-499), testCohort(0), status, pool)))
		snapshot, ok := service.GetAccountHealth(int64(status - 499))
		require.True(t, ok)
		require.Equal(t, uint64(1), snapshot.TotalFailures)
		require.Equal(t, uint64(1), snapshot.TotalHTTP5xx)
	}
	require.Len(t, seen, 100)
	_, ok := ClassifyHTTPFailure(499)
	require.False(t, ok)
	_, ok = ClassifyHTTPFailure(600)
	require.False(t, ok)
}

func TestOpenAILatencyHealthCorrelatesIncidentsByDistinctAccountsAndFingerprint(t *testing.T) {
	t.Run("exact forty percent suppresses and lifts matching soft penalties", func(t *testing.T) {
		service, clock := newEnabledLatencyHealthService(t, 16, 2)
		cohort := testCohort(0)
		pool := PoolSnapshot{HardValidAccounts: 16}
		for accountID := int64(1); accountID <= 10; accountID++ {
			require.NoError(t, service.RecordObservation(testHealthObservation(accountID, cohort, 200, pool)))
		}

		for accountID := int64(1); accountID <= 3; accountID++ {
			decision, err := service.Observe(testHealthObservation(accountID, cohort, 503, pool))
			require.NoError(t, err)
			require.False(t, decision.ProviderIncident)
		}
		decision, err := service.Observe(testHealthObservation(4, cohort, 503, pool))
		require.NoError(t, err)
		require.True(t, decision.ProviderIncident)
		require.True(t, decision.PenaltySuppressed)
		require.Equal(t, 6, decision.LiftedSoftPenalties)

		fingerprint, _ := ClassifyHTTPFailure(503)
		incident := service.IncidentCorrelation(fingerprint)
		require.True(t, incident.Active)
		require.Equal(t, 10, incident.SampledAccounts)
		require.Equal(t, 4, incident.AffectedAccounts)
		require.InDelta(t, 0.4, incident.AffectedFraction, 0.000001)
		for accountID := int64(1); accountID <= 4; accountID++ {
			snapshot, _ := service.GetCohortHealth(accountID, cohort)
			require.Equal(t, HealthStateHealthy, snapshot.State)
		}

		decision, err = service.Observe(testHealthObservation(5, cohort, 503, pool))
		require.NoError(t, err)
		require.True(t, decision.PenaltySuppressed)
		snapshot, _ := service.GetCohortHealth(5, cohort)
		require.Equal(t, HealthStateHealthy, snapshot.State)

		decision, err = service.Observe(testHealthObservation(6, cohort, 502, pool))
		require.NoError(t, err)
		require.False(t, decision.ProviderIncident)
		snapshot, _ = service.GetCohortHealth(6, cohort)
		require.Equal(t, HealthStatePenalized, snapshot.State)

		clock.Advance(OpenAILatencyIncidentWindow + time.Second)
		decision, err = service.Observe(testHealthObservation(1, cohort, 503, pool))
		require.NoError(t, err)
		require.False(t, decision.ProviderIncident)
		require.False(t, decision.PenaltySuppressed)
		snapshot, _ = service.GetCohortHealth(1, cohort)
		require.Equal(t, HealthStatePenalized, snapshot.State)
	})

	t.Run("ratio below forty percent does not suppress", func(t *testing.T) {
		service, _ := newEnabledLatencyHealthService(t, 16, 2)
		cohort := testCohort(0)
		pool := PoolSnapshot{HardValidAccounts: 16}
		for accountID := int64(1); accountID <= 11; accountID++ {
			require.NoError(t, service.RecordObservation(testHealthObservation(accountID, cohort, 200, pool)))
		}
		for accountID := int64(1); accountID <= 4; accountID++ {
			decision, err := service.Observe(testHealthObservation(accountID, cohort, 507, pool))
			require.NoError(t, err)
			require.False(t, decision.ProviderIncident)
		}
		fingerprint, _ := ClassifyHTTPFailure(507)
		incident := service.IncidentCorrelation(fingerprint)
		require.False(t, incident.Active)
		require.Equal(t, 4, incident.AffectedAccounts)
		require.InDelta(t, 4.0/11.0, incident.AffectedFraction, 0.000001)
	})

	t.Run("repeated failures from one account are not distinct", func(t *testing.T) {
		service, _ := newEnabledLatencyHealthService(t, 4, 2)
		cohort := testCohort(0)
		pool := PoolSnapshot{HardValidAccounts: 8}
		for range 8 {
			require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 508, pool)))
		}
		fingerprint, _ := ClassifyHTTPFailure(508)
		incident := service.IncidentCorrelation(fingerprint)
		require.False(t, incident.Active)
		require.Equal(t, 1, incident.AffectedAccounts)
	})
}

func TestOpenAILatencyHealthUsesConfiguredPolicy(t *testing.T) {
	baseConfig := DefaultLatencyHealthConfig()
	baseConfig.Enabled = true
	baseConfig.MaxTrackedAccounts = 16
	baseConfig.MaxCohortsPerAccount = 2
	baseConfig.QuarantineCap = 1
	baseConfig.HardValidFloor = 2
	baseConfig.IncidentWindow = 4 * time.Second
	baseConfig.IncidentMinAffectedAccounts = 16
	baseConfig.IncidentMinAffectedFraction = 1
	baseConfig.PenalizedCooldown = 3 * time.Second
	baseConfig.QuarantinedCooldown = 7 * time.Second
	baseConfig.HardInvalidCooldown = 11 * time.Second

	t.Run("cooldowns and quarantine cap", func(t *testing.T) {
		clock := newLatencyHealthFakeClock(time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC))
		service, err := NewOpenAILatencyHealthService(baseConfig, clock)
		require.NoError(t, err)
		cohort := testCohort(0)
		blockedPool := PoolSnapshot{QuarantinedAccounts: 1, HardValidAccounts: 10}
		require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 500, blockedPool)))
		blocked, err := service.Observe(testHealthObservation(1, cohort, 500, blockedPool))
		require.NoError(t, err)
		require.True(t, blocked.GuardrailBlocked)
		snapshot, _ := service.GetCohortHealth(1, cohort)
		require.Equal(t, HealthStatePenalized, snapshot.State)
		require.Equal(t, 3*time.Second, snapshot.CooldownRemaining)

		openPool := PoolSnapshot{HardValidAccounts: 10}
		require.NoError(t, service.RecordObservation(testHealthObservation(2, cohort, 500, openPool)))
		require.NoError(t, service.RecordObservation(testHealthObservation(2, cohort, 500, openPool)))
		snapshot, _ = service.GetCohortHealth(2, cohort)
		require.Equal(t, HealthStateQuarantined, snapshot.State)
		require.Equal(t, 7*time.Second, snapshot.CooldownRemaining)
	})

	t.Run("hard-valid floor and cooldown", func(t *testing.T) {
		clock := newLatencyHealthFakeClock(time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC))
		service, err := NewOpenAILatencyHealthService(baseConfig, clock)
		require.NoError(t, err)
		cohort := testCohort(0)

		blocked, err := service.Observe(testHealthObservation(1, cohort, 401, PoolSnapshot{HardValidAccounts: 2}))
		require.NoError(t, err)
		require.True(t, blocked.GuardrailBlocked)
		require.Equal(t, HealthStateHealthy, blocked.CohortState)

		allowed, err := service.Observe(testHealthObservation(2, cohort, 403, PoolSnapshot{HardValidAccounts: 3}))
		require.NoError(t, err)
		require.False(t, allowed.GuardrailBlocked)
		snapshot, _ := service.GetCohortHealth(2, cohort)
		require.Equal(t, HealthStateHardInvalid, snapshot.State)
		require.Equal(t, 11*time.Second, snapshot.CooldownRemaining)
	})

	t.Run("incident thresholds and window", func(t *testing.T) {
		config := baseConfig
		config.IncidentMinAffectedAccounts = 2
		config.IncidentMinAffectedFraction = 0.5
		clock := newLatencyHealthFakeClock(time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC))
		service, err := NewOpenAILatencyHealthService(config, clock)
		require.NoError(t, err)
		cohort := testCohort(0)
		pool := PoolSnapshot{HardValidAccounts: 10}

		first, err := service.Observe(testHealthObservation(1, cohort, 503, pool))
		require.NoError(t, err)
		require.False(t, first.ProviderIncident)
		second, err := service.Observe(testHealthObservation(2, cohort, 503, pool))
		require.NoError(t, err)
		require.True(t, second.ProviderIncident)

		clock.Advance(4*time.Second + time.Nanosecond)
		fingerprint, _ := ClassifyHTTPFailure(503)
		require.False(t, service.IncidentCorrelation(fingerprint).Active)
	})
}

func TestOpenAILatencyHealthMaintainsDecayedObservationSums(t *testing.T) {
	config := DefaultLatencyHealthConfig()
	config.Enabled = true
	config.MaxTrackedAccounts = 4
	config.MaxCohortsPerAccount = 1
	config.IncidentMinAffectedAccounts = 4
	config.LatencyHalfLife = 10 * time.Second
	config.ErrorHalfLife = 5 * time.Second
	clock := newLatencyHealthFakeClock(time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC))
	service, err := NewOpenAILatencyHealthService(config, clock)
	require.NoError(t, err)
	pool := PoolSnapshot{HardValidAccounts: 10}
	cohort := testCohort(0)
	overflowCohort := testCohort(1)

	success := testHealthObservation(1, cohort, 200, pool)
	success.Latency = 100 * time.Millisecond
	require.NoError(t, service.RecordObservation(success))
	failure := testHealthObservation(1, overflowCohort, 500, pool)
	failure.Latency = 300 * time.Millisecond
	require.NoError(t, service.RecordObservation(failure))

	aggregate, _ := service.GetAccountHealth(1)
	require.InDelta(t, 400, aggregate.DecayedLatencySumMilliseconds, 0.000001)
	require.InDelta(t, 2, aggregate.DecayedLatencySamples, 0.000001)
	require.InDelta(t, 1, aggregate.DecayedErrors, 0.000001)
	require.InDelta(t, 2, aggregate.DecayedErrorSamples, 0.000001)
	tracked, _ := service.GetCohortHealth(1, cohort)
	require.InDelta(t, 100, tracked.DecayedLatencySumMilliseconds, 0.000001)
	overflow, _ := service.GetCohortHealth(1, OverflowCohortKey)
	require.True(t, overflow.Overflow)
	require.InDelta(t, 300, overflow.DecayedLatencySumMilliseconds, 0.000001)
	require.InDelta(t, 1, overflow.DecayedErrors, 0.000001)

	clock.Advance(5 * time.Second)
	aggregate, _ = service.GetAccountHealth(1)
	latencyDecay := math.Exp2(-0.5)
	require.InDelta(t, 400*latencyDecay, aggregate.DecayedLatencySumMilliseconds, 0.000001)
	require.InDelta(t, 2*latencyDecay, aggregate.DecayedLatencySamples, 0.000001)
	require.InDelta(t, 0.5, aggregate.DecayedErrors, 0.000001)
	require.InDelta(t, 1, aggregate.DecayedErrorSamples, 0.000001)

	verySlowSuccess := testHealthObservation(2, cohort, 200, pool)
	verySlowSuccess.Latency = 24 * time.Hour
	require.NoError(t, service.RecordObservation(verySlowSuccess))
	slowSnapshot, _ := service.GetAccountHealth(2)
	require.Equal(t, HealthStateHealthy, slowSnapshot.State)
	require.Zero(t, slowSnapshot.TotalFailures)
}

func TestOpenAILatencyHealthUsesFixedHistogramAndRollingBuckets(t *testing.T) {
	bounds := OpenAILatencyHistogramUpperBounds()
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
	}, bounds)
	for index := 1; index < len(bounds); index++ {
		require.Greater(t, bounds[index], bounds[index-1])
	}

	service, clock := newEnabledLatencyHealthService(t, 2, 2)
	cohort := testCohort(0)
	pool := PoolSnapshot{HardValidAccounts: 12}
	observations := []OpenAILatencyObservation{
		{AccountID: 1, Cohort: cohort, Latency: 50 * time.Millisecond, HTTPStatus: 200, PoolSnapshot: pool},
		{AccountID: 1, Cohort: cohort, Latency: 100 * time.Millisecond, HTTPStatus: 200, PoolSnapshot: pool},
		{AccountID: 1, Cohort: cohort, Latency: 250 * time.Millisecond, HTTPStatus: 200, PoolSnapshot: pool},
		{AccountID: 1, Cohort: cohort, Latency: 180 * time.Second, HTTPStatus: 200, PoolSnapshot: pool},
		{AccountID: 1, Cohort: cohort, Latency: 181 * time.Second, HTTPStatus: 404, PoolSnapshot: pool},
	}
	for _, observation := range observations {
		require.NoError(t, service.RecordObservation(observation))
	}

	snapshot, _ := service.GetCohortHealth(1, cohort)
	require.Equal(t, uint64(5), snapshot.Window.Requests)
	require.Equal(t, uint64(4), snapshot.Window.Successes)
	require.Equal(t, uint64(1), snapshot.Window.Neutral)
	require.Equal(t, uint64(1), snapshot.LatencyHistogram[0])
	require.Equal(t, uint64(1), snapshot.LatencyHistogram[1])
	require.Equal(t, uint64(1), snapshot.LatencyHistogram[2])
	require.Equal(t, uint64(1), snapshot.LatencyHistogram[14])
	require.Equal(t, uint64(1), snapshot.LatencyHistogram[OpenAILatencyHistogramBucketCount-1])

	clock.Advance(OpenAILatencyTimeBucketDuration)
	require.NoError(t, service.RecordObservation(testHealthObservation(1, cohort, 200, pool)))
	snapshot, _ = service.GetCohortHealth(1, cohort)
	require.Equal(t, uint64(6), snapshot.Window.Requests)

	clock.Advance(OpenAILatencyIncidentWindow + OpenAILatencyTimeBucketDuration)
	snapshot, _ = service.GetCohortHealth(1, cohort)
	require.Zero(t, snapshot.Window.Requests)
	require.Equal(t, uint64(6), snapshot.TotalRequests)
	require.Len(t, snapshot.Window.Buckets, OpenAILatencyTimeBucketCount)
}

type openAILatencyTelemetrySinkStub struct {
	snapshot OpenAILatencyTelemetrySnapshot
}

func (s *openAILatencyTelemetrySinkStub) StoreOpenAILatencyTelemetrySnapshot(_ context.Context, snapshot OpenAILatencyTelemetrySnapshot) error {
	s.snapshot = snapshot
	return nil
}

var _ OpenAILatencyTelemetrySink = (*openAILatencyTelemetrySinkStub)(nil)
var _ OpenAILatencyTelemetrySnapshotter = (*OpenAILatencyTelemetryAggregate)(nil)

func TestOpenAILatencyTelemetryIsFixedCardinalitySecretSafeAndMergeable(t *testing.T) {
	assertTelemetrySnapshotFixedShape(t, reflect.TypeOf(OpenAILatencyTelemetrySnapshot{}), "snapshot")

	key := OpenAILatencyTelemetryKey{
		AccountID:   42,
		Cohort:      testCohort(7),
		AttemptRole: OpenAIAttemptRolePrimary,
	}
	first, err := NewOpenAILatencyTelemetryAggregate(key)
	require.NoError(t, err)
	requiredPhases := []OpenAILatencyPhase{
		OpenAILatencyPhaseFirstSSE,
		OpenAILatencyPhaseFirstMeaningful,
		OpenAILatencyPhaseHedgeEligible,
		OpenAILatencyPhaseSecondaryDispatch,
		OpenAILatencyPhaseWinnerCommit,
		OpenAILatencyPhaseLoserCancel,
		OpenAILatencyPhaseUsage,
		OpenAILatencyPhaseTerminal,
	}
	for index, phase := range requiredPhases {
		require.NoError(t, first.ObservePhase(phase, time.Duration(index+1)*25*time.Millisecond))
	}
	require.NoError(t, first.ObservePhase(OpenAILatencyPhaseTotal, 100*time.Millisecond))
	require.NoError(t, first.ObserveHedge(OpenAIHedgeOutcomePrimaryWon))
	require.NoError(t, first.ObserveWinner(OpenAIWinnerOutcomePrimary))
	require.NoError(t, first.ObserveLoserCancellation(OpenAILoserCancelOutcomeSucceeded))
	for event := OpenAILoserLifecycleCancelRequested; event < OpenAILoserLifecycleEventCardinality; event++ {
		require.NoError(t, first.ObserveLoserLifecycle(event))
	}
	require.NoError(t, first.ObserveUsage(OpenAIUsageOutcomeCaptured))
	require.NoError(t, first.ObserveCancellation(OpenAICancelReasonClient))
	require.NoError(t, first.ObserveBilling(OpenAIBillingOutcomeCharged))
	require.NoError(t, first.ObserveCapacity(OpenAICapacityOutcomeAvailable))

	second, err := NewOpenAILatencyTelemetryAggregate(key)
	require.NoError(t, err)
	require.NoError(t, second.ObservePhase(OpenAILatencyPhaseTotal, 2*time.Second))
	require.NoError(t, second.ObserveHedge(OpenAIHedgeOutcomeHedgeWon))
	require.NoError(t, second.ObserveWinner(OpenAIWinnerOutcomeHedge))
	require.NoError(t, second.ObserveLoserCancellation(OpenAILoserCancelOutcomeAlreadyComplete))
	require.NoError(t, second.ObserveLoserLifecycleEvent(OpenAILoserLifecycleCancelRequested))
	require.Error(t, second.ObserveLoserLifecycle(OpenAILoserLifecycleEvent(255)))
	require.NoError(t, second.ObserveUsage(OpenAIUsageOutcomeReconciled))
	require.NoError(t, second.ObserveCancellation(OpenAICancelReasonDeadline))
	require.NoError(t, second.ObserveBilling(OpenAIBillingOutcomeRefunded))
	require.NoError(t, second.ObserveCapacity(OpenAICapacityOutcomeConstrained))

	require.NoError(t, first.Merge(second.Snapshot()))
	merged := first.Snapshot()
	require.Equal(t, key, merged.Key)
	for _, phase := range requiredPhases {
		require.Equal(t, uint64(1), merged.Phases[phase].Observations)
	}
	require.Equal(t, uint64(2), merged.Phases[OpenAILatencyPhaseTotal].Observations)
	require.Equal(t, uint64(2_100), merged.Phases[OpenAILatencyPhaseTotal].LatencyMilliseconds)
	require.Equal(t, uint64(1), merged.Hedges[OpenAIHedgeOutcomePrimaryWon])
	require.Equal(t, uint64(1), merged.Hedges[OpenAIHedgeOutcomeHedgeWon])
	require.Equal(t, uint64(1), merged.Winners[OpenAIWinnerOutcomePrimary])
	require.Equal(t, uint64(1), merged.Winners[OpenAIWinnerOutcomeHedge])
	require.Equal(t, uint64(1), merged.LoserCancellations[OpenAILoserCancelOutcomeSucceeded])
	require.Equal(t, uint64(1), merged.LoserCancellations[OpenAILoserCancelOutcomeAlreadyComplete])
	require.Equal(t, uint64(2), merged.LoserLifecycle[OpenAILoserLifecycleCancelRequested])
	require.Equal(t, uint64(1), merged.LoserLifecycle[OpenAILoserLifecycleTransportClosed])
	require.Equal(t, uint64(1), merged.LoserLifecycle[OpenAILoserLifecycleTerminalAfterCancel])
	require.Equal(t, uint64(1), merged.LoserLifecycle[OpenAILoserLifecycleUsageAfterCancel])
	require.Equal(t, uint64(1), merged.LoserLifecycle[OpenAILoserLifecycleBillingAfterCancel])
	require.Equal(t, uint64(1), merged.Usage[OpenAIUsageOutcomeCaptured])
	require.Equal(t, uint64(1), merged.Usage[OpenAIUsageOutcomeReconciled])
	require.Equal(t, uint64(1), merged.Cancellations[OpenAICancelReasonClient])
	require.Equal(t, uint64(1), merged.Cancellations[OpenAICancelReasonDeadline])
	require.Equal(t, uint64(1), merged.Billing[OpenAIBillingOutcomeCharged])
	require.Equal(t, uint64(1), merged.Billing[OpenAIBillingOutcomeRefunded])
	require.Equal(t, uint64(1), merged.Capacity[OpenAICapacityOutcomeAvailable])
	require.Equal(t, uint64(1), merged.Capacity[OpenAICapacityOutcomeConstrained])

	// Snapshot is non-draining and returns a value copy.
	require.Equal(t, merged, first.Snapshot())
	mutated := first.Snapshot()
	mutated.Phases[OpenAILatencyPhaseTotal].Observations = 999
	require.Equal(t, merged, first.Snapshot())

	// Cumulative reconciliation is idempotent under retry.
	var cumulative OpenAILatencyTelemetrySnapshot
	require.NoError(t, cumulative.MergeCumulative(merged))
	once := cumulative
	require.NoError(t, cumulative.MergeCumulative(merged))
	require.Equal(t, once, cumulative)

	// Additive merge is transactional on overflow.
	overflow := merged
	overflow.Phases[OpenAILatencyPhaseTotal].Observations = math.MaxUint64
	before := overflow
	require.Error(t, overflow.Merge(merged))
	require.Equal(t, before, overflow)

	_, err = NewOpenAILatencyTelemetryAggregate(OpenAILatencyTelemetryKey{})
	require.Error(t, err)
	canonicalized, err := NewOpenAILatencyTelemetryAggregate(OpenAILatencyTelemetryKey{
		AccountID:   42,
		Cohort:      CohortKey(65_000),
		AttemptRole: OpenAIAttemptRole(255),
	})
	require.NoError(t, err)
	require.Equal(t, UnknownCohortKey, canonicalized.Snapshot().Key.Cohort)
	require.Equal(t, OpenAIAttemptRoleUnknown, canonicalized.Snapshot().Key.AttemptRole)

	otherKeyAggregate, err := NewOpenAILatencyTelemetryAggregate(OpenAILatencyTelemetryKey{
		AccountID:   43,
		Cohort:      key.Cohort,
		AttemptRole: OpenAIAttemptRoleHedge,
	})
	require.NoError(t, err)
	mismatched := merged
	require.Error(t, mismatched.Merge(otherKeyAggregate.Snapshot()))
	require.Equal(t, merged, mismatched)

	sink := &openAILatencyTelemetrySinkStub{}
	require.NoError(t, sink.StoreOpenAILatencyTelemetrySnapshot(context.Background(), merged))
	require.Equal(t, merged, sink.snapshot)
}

func assertTelemetrySnapshotFixedShape(t *testing.T, valueType reflect.Type, path string) {
	t.Helper()
	switch valueType.Kind() {
	case reflect.Struct:
		for index := 0; index < valueType.NumField(); index++ {
			field := valueType.Field(index)
			assertTelemetrySnapshotFixedShape(t, field.Type, path+"."+field.Name)
		}
	case reflect.Array:
		assertTelemetrySnapshotFixedShape(t, valueType.Elem(), path+"[]")
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return
	default:
		t.Fatalf("%s has non-fixed or secret-bearing kind %s", path, valueType.Kind())
	}
}

func TestOpenAILatencyHealthAndTelemetryAreConcurrentSafe(t *testing.T) {
	service, _ := newEnabledLatencyHealthService(t, 8, 4)
	telemetry, err := NewOpenAILatencyTelemetryAggregate(OpenAILatencyTelemetryKey{
		AccountID:   1,
		Cohort:      testCohort(0),
		AttemptRole: OpenAIAttemptRolePrimary,
	})
	require.NoError(t, err)
	pool := PoolSnapshot{HardValidAccounts: 20}

	const goroutines = 24
	const observationsPerGoroutine = 300
	errorsCh := make(chan error, goroutines*observationsPerGoroutine*2)
	var waitGroup sync.WaitGroup
	for goroutineIndex := 0; goroutineIndex < goroutines; goroutineIndex++ {
		goroutineIndex := goroutineIndex
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			accountID := int64(goroutineIndex%8 + 1)
			for observationIndex := 0; observationIndex < observationsPerGoroutine; observationIndex++ {
				status := 200
				if observationIndex%5 == 0 {
					status = 500 + goroutineIndex%10
				}
				observation := testHealthObservation(accountID, testCohort(observationIndex%12), status, pool)
				if err := service.RecordObservation(observation); err != nil {
					errorsCh <- err
				}
				if err := telemetry.ObservePhase(OpenAILatencyPhase(goroutineIndex%int(OpenAILatencyPhaseCardinality)), observation.Latency); err != nil {
					errorsCh <- err
				}
				if err := telemetry.ObserveHedge(OpenAIHedgeOutcome(goroutineIndex % int(OpenAIHedgeOutcomeCardinality))); err != nil {
					errorsCh <- err
				}
			}
		}()
	}
	waitGroup.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}

	var totalHealthObservations uint64
	for accountID := int64(1); accountID <= 8; accountID++ {
		snapshot, ok := service.GetAccountHealth(accountID)
		require.True(t, ok)
		totalHealthObservations += snapshot.TotalRequests
	}
	require.Equal(t, uint64(goroutines*observationsPerGoroutine), totalHealthObservations)

	telemetrySnapshot := telemetry.Snapshot()
	var totalPhaseObservations uint64
	for _, phase := range telemetrySnapshot.Phases {
		totalPhaseObservations += phase.Observations
	}
	require.Equal(t, uint64(goroutines*observationsPerGoroutine), totalPhaseObservations)
	require.LessOrEqual(t, service.Stats().TrackedCohorts, 8*4)
	require.Equal(t, 8, service.Stats().OverflowAccounts)
}

func TestOpenAILatencyHealthObservationValidationDoesNotAllocateState(t *testing.T) {
	service, _ := newEnabledLatencyHealthService(t, 2, 2)
	invalid := []OpenAILatencyObservation{
		{AccountID: 0, HTTPStatus: 200},
		{AccountID: 1, Latency: -1, HTTPStatus: 200},
		{AccountID: 1, HTTPStatus: 99},
		{AccountID: 1, HTTPStatus: 600},
		{AccountID: 1, HTTPStatus: 200, PoolSnapshot: PoolSnapshot{QuarantinedAccounts: -1}},
	}
	for _, observation := range invalid {
		require.Error(t, service.RecordObservation(observation))
	}
	require.Zero(t, service.Stats().Accounts)

	require.ErrorIs(t, service.RecordSuccessfulHardRevalidation(1, testCohort(0)), ErrLatencyHealthNotFound)
	require.True(t, errors.Is(ErrLatencyHealthAccountLimit, ErrLatencyHealthAccountLimit))
}
