package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// AdaptiveLatencyRuntime is the feature-off composition boundary for latency health,
// durable shadow telemetry, and hedge policy. It is intentionally not injected
// into the scheduler or gateway in S04, so constructing it cannot alter active
// account selection, sticky sessions, streams, or billing.
type AdaptiveLatencyRuntime struct {
	health               *OpenAILatencyHealthService
	telemetry            *OpenAILatencyTelemetryCollector
	hedge                *HedgeCoordinator
	hedgeConfig          config.OpenAIHedgeConfig
	hedgeConfigured      bool
	observationsRecorded atomic.Uint64
	healthFailures       atomic.Uint64
	telemetryFailures    atomic.Uint64
	lastHealthError      atomic.Value // stores string (type name)
	lastTelemetryError   atomic.Value // stores string (type name)
}

// AdaptiveLatencyRuntimeStats is a bounded, secret-free operational status view.
type AdaptiveLatencyRuntimeStats struct {
	Health         LatencyHealthServiceStats
	Telemetry      OpenAILatencyTelemetryCollectorStats
	Recommendation OpenAILatencyTelemetryRecommendationSnapshot
	Hedge          AdaptiveHedgeRuntimeStats
	Observer       AdaptiveLatencyObserverStats
}

// AdaptiveLatencyObserverStats tracks observer success and failure counts.
type AdaptiveLatencyObserverStats struct {
	Enabled                bool
	ObservationsRecorded   uint64
	HealthFailures         uint64
	TelemetryFailures      uint64
	LastHealthErrorType    string
	LastTelemetryErrorType string
}

type AdaptiveHedgeRuntimeStats struct {
	Configured    bool
	Constructed   bool
	AdaptersWired bool
	Active        bool
}

// NewAdaptiveLatencyRuntime wires real shadow persistence dependencies but fails closed if
// active hedging is requested before provider-native attempt/candidate/sink and
// billing adapters exist. Feature-off construction allocates no health maps or
// telemetry channels and starts no goroutine.
func NewAdaptiveLatencyRuntime(
	cfg *config.Config,
	cache OpenAILatencyTelemetryCache,
	rollups OpenAILatencyTelemetryRollupRepository,
	leaderLock LeaderLockCache,
	db *sql.DB,
	transitionGuard LatencyHealthTransitionGuard,
) (*AdaptiveLatencyRuntime, error) {
	if cfg == nil {
		return nil, errors.New("adaptive latency runtime config is required")
	}
	configuredHedge := cfg.Gateway.Scheduling.OpenAIHedge
	if _, err := newOpenAIHedgeCoordinatorConfig(configuredHedge); err != nil {
		return nil, err
	}
	var hedge *HedgeCoordinator
	if !configuredHedge.Enabled {
		var err error
		hedge, err = NewHedgeCoordinator(hedgeConfigFromOpenAI(configuredHedge), HedgeCoordinatorDependencies{})
		if err != nil {
			return nil, err
		}
	}

	shadow := cfg.Gateway.Scheduling.OpenAILatencyShadow
	healthConfig := latencyHealthConfigFromShadow(shadow)
	health, err := NewOpenAILatencyHealthServiceWithTransitionGuard(healthConfig, SystemClock{}, transitionGuard)
	if err != nil {
		return nil, err
	}
	telemetry, err := NewOpenAILatencyTelemetryCollector(shadow, OpenAILatencyTelemetryCollectorDependencies{
		Cache:            cache,
		RollupRepository: rollups,
		LeaderLock:       leaderLock,
		DB:               db,
		Clock:            SystemClock{},
		SourceID:         adaptiveLatencySourceID(cfg),
		Generation:       mintOpenAILatencyBootToken(),
	})
	if err != nil {
		return nil, err
	}
	return &AdaptiveLatencyRuntime{
		health:          health,
		telemetry:       telemetry,
		hedge:           hedge,
		hedgeConfig:     configuredHedge,
		hedgeConfigured: configuredHedge.Enabled,
	}, nil
}

func adaptiveLatencySourceID(cfg *config.Config) uint64 {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(hostname))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write([]byte(cfg.Server.Address()))
	sourceID := hasher.Sum64()
	if sourceID == 0 {
		return 1
	}
	return sourceID
}

func hedgeConfigFromOpenAI(source config.OpenAIHedgeConfig) HedgeConfig {
	return HedgeConfig{
		Enabled:                   source.Enabled,
		StandardThresholdSeconds:  source.StandardThresholdSeconds,
		HighThresholdSeconds:      source.HighThresholdSeconds,
		VeryHeavyThresholdSeconds: source.VeryHeavyThresholdSeconds,
		VeryHeavyEnabled:          source.VeryHeavyEnabled,
		NoProgressEnabled:         source.NoProgressEnabled,
		EventChannelCapacity:      source.EventChannelCapacity,
		MaxPrecommitEvents:        source.MaxPrecommitEvents,
		MaxPrecommitBytes:         source.MaxPrecommitBytes,
		CancelDrainTimeoutSeconds: source.CancelDrainTimeoutSeconds,
		MaxAttempts:               source.MaxAttempts,
		MaxDuplicateCostMicros:    source.MaxDuplicateCostMicros,
	}
}

func latencyHealthConfigFromShadow(shadow config.OpenAILatencyShadowConfig) LatencyHealthConfig {
	return LatencyHealthConfig{
		Enabled:                     shadow.Enabled,
		MaxTrackedAccounts:          shadow.MaxTrackedAccounts,
		MaxCohortsPerAccount:        shadow.MaxCohortsPerAccount,
		MaxTracks:                   shadow.MaxTracks,
		WeightFloor:                 shadow.WeightFloor,
		QuarantineCap:               shadow.QuarantineCap,
		HardValidFloor:              shadow.HardValidFloor,
		IncidentWindow:              time.Duration(shadow.IncidentWindowSeconds) * time.Second,
		IncidentMinAffectedAccounts: shadow.IncidentMinAffectedAccounts,
		IncidentMinAffectedFraction: shadow.IncidentMinAffectedFraction,
		PenalizedCooldown:           time.Duration(shadow.PenalizedCooldownSeconds) * time.Second,
		QuarantinedCooldown:         time.Duration(shadow.QuarantinedCooldownSeconds) * time.Second,
		HardInvalidCooldown:         time.Duration(shadow.HardInvalidCooldownSeconds) * time.Second,
		LatencyHalfLife:             time.Duration(shadow.LatencyHalfLifeSeconds) * time.Second,
		ErrorHalfLife:               time.Duration(shadow.ErrorHalfLifeSeconds) * time.Second,
	}
}

// Start is idempotent. Disabled telemetry makes this a no-op.
func (runtime *AdaptiveLatencyRuntime) Start() {
	if runtime == nil || runtime.telemetry == nil {
		return
	}
	runtime.telemetry.Start()
}

// Stop is idempotent and must run before Redis/Postgres are closed.
func (runtime *AdaptiveLatencyRuntime) Stop() {
	if runtime == nil || runtime.telemetry == nil {
		return
	}
	runtime.telemetry.Stop()
}

func (runtime *AdaptiveLatencyRuntime) Stats() AdaptiveLatencyRuntimeStats {
	stats := AdaptiveLatencyRuntimeStats{}
	if runtime == nil {
		return stats
	}
	if runtime.health != nil {
		stats.Health = runtime.health.Stats()
	}
	if runtime.telemetry != nil {
		stats.Telemetry = runtime.telemetry.Stats()
		stats.Recommendation = runtime.telemetry.RecommendationSnapshot()
	}
	stats.Hedge = AdaptiveHedgeRuntimeStats{
		Configured:    runtime.hedgeConfigured,
		Constructed:   runtime.hedge != nil || runtime.hedgeConfigured,
		AdaptersWired: true,
		Active:        runtime.hedgeConfigured,
	}
	stats.Observer = AdaptiveLatencyObserverStats{
		Enabled:              runtime.health != nil && runtime.telemetry != nil && runtime.telemetry.Enabled(),
		ObservationsRecorded: runtime.observationsRecorded.Load(),
		HealthFailures:       runtime.healthFailures.Load(),
		TelemetryFailures:    runtime.telemetryFailures.Load(),
	}
	if lastHealthErr := runtime.lastHealthError.Load(); lastHealthErr != nil {
		stats.Observer.LastHealthErrorType = lastHealthErr.(string)
	}
	if lastTelemetryErr := runtime.lastTelemetryError.Load(); lastTelemetryErr != nil {
		stats.Observer.LastTelemetryErrorType = lastTelemetryErr.(string)
	}
	return stats
}

// Health and Telemetry expose observation/status-only components to future
// adapters without coupling this runtime to scheduler or gateway constructors.
func (runtime *AdaptiveLatencyRuntime) Health() *OpenAILatencyHealthService {
	if runtime == nil {
		return nil
	}
	return runtime.health
}

func (runtime *AdaptiveLatencyRuntime) Telemetry() *OpenAILatencyTelemetryCollector {
	if runtime == nil {
		return nil
	}
	return runtime.telemetry
}

func (runtime *AdaptiveLatencyRuntime) Hedge() *HedgeCoordinator {
	if runtime == nil {
		return nil
	}
	return runtime.hedge
}

func (runtime *AdaptiveLatencyRuntime) HedgeConfig() (config.OpenAIHedgeConfig, bool) {
	if runtime == nil {
		return config.OpenAIHedgeConfig{}, false
	}
	return runtime.hedgeConfig, runtime.hedgeConfigured
}

func (runtime *AdaptiveLatencyRuntime) NewRequestHedgeCoordinator(
	dependencies OpenAIHedgeCoordinatorDependencies,
) (*OpenAIHedgeCoordinator, error) {
	if runtime == nil || !runtime.hedgeConfigured {
		return nil, ErrOpenAIHedgeDisabled
	}
	return NewOpenAIHedgeCoordinator(runtime.hedgeConfig, dependencies)
}

// RecordHedgeLoserExposure persists provider-cost exposure only. It never calls
// user billing or usage-quota services.
func (runtime *AdaptiveLatencyRuntime) RecordHedgeLoserExposure(
	ctx context.Context,
	key OpenAILatencyTelemetryKey,
	billing OpenAIHedgeLoserBilling,
) error {
	if runtime == nil || runtime.telemetry == nil || !runtime.telemetry.Enabled() {
		return nil
	}
	if err := runtime.telemetry.ObserveBilling(key, OpenAIBillingOutcomeNotBillable); err != nil {
		return err
	}
	return runtime.telemetry.ObserveLoserCostMicros(key, billing.AmountMicros)
}

// AdaptiveLatencyObserveRequest contains bounded request traits for shadow observation.
type AdaptiveLatencyObserveRequest struct {
	Cohort      CohortKey
	AccountID   int64
	AttemptRole OpenAIAttemptRole
	Pool        PoolSnapshot
}

// AdaptiveLatencyObserveOutcome contains bounded numeric outcome data for shadow observation.
type AdaptiveLatencyObserveOutcome struct {
	HTTPStatus          int
	LatencyMilliseconds int64
	Failure             FailureFingerprint
	Phases              map[OpenAILatencyPhase]time.Duration
	Winner              OpenAIWinnerOutcome
	Hedge               OpenAIHedgeOutcome
	Capacity            OpenAICapacityOutcome
	Cancellation        OpenAICancelReason
}

// Observe records request and outcome to health and telemetry in fail-open mode.
// When disabled, this is an immediate no-op with zero allocation.
// When enabled, it records health observation and telemetry phases, incrementing
// secret-free failure counters on error without leaking raw errors or request bodies.
func (runtime *AdaptiveLatencyRuntime) Observe(req AdaptiveLatencyObserveRequest, outcome AdaptiveLatencyObserveOutcome) {
	if runtime == nil {
		return
	}
	if runtime.telemetry == nil || !runtime.telemetry.Enabled() {
		return
	}

	runtime.observationsRecorded.Add(1)

	// Record health observation
	if runtime.health != nil {
		healthObs := OpenAILatencyObservation{
			AccountID:    req.AccountID,
			Cohort:       req.Cohort,
			Latency:      time.Duration(outcome.LatencyMilliseconds) * time.Millisecond,
			HTTPStatus:   outcome.HTTPStatus,
			Failure:      outcome.Failure,
			PoolSnapshot: req.Pool,
		}
		if err := runtime.health.RecordObservation(healthObs); err != nil {
			runtime.healthFailures.Add(1)
			runtime.lastHealthError.Store(fmt.Sprintf("%T", err))
		}
	}

	// Record telemetry phases and allowlisted outcomes. Every call is fail-open;
	// the first error is counted without retaining its payload.
	key := openAILatencyTelemetryKey(req)
	observe := func(err error) bool {
		if err == nil {
			return true
		}
		runtime.telemetryFailures.Add(1)
		runtime.lastTelemetryError.Store(fmt.Sprintf("%T", err))
		return false
	}
	for phase, latency := range outcome.Phases {
		if !observe(runtime.telemetry.ObservePhase(key, phase, latency)) {
			return
		}
	}
	if outcome.Winner != OpenAIWinnerOutcomeUnknown && !observe(runtime.telemetry.ObserveWinner(key, outcome.Winner)) {
		return
	}
	if outcome.Hedge != OpenAIHedgeOutcomeNone && !observe(runtime.telemetry.ObserveHedge(key, outcome.Hedge)) {
		return
	}
	if outcome.Capacity != OpenAICapacityOutcomeUnknown && !observe(runtime.telemetry.ObserveCapacity(key, outcome.Capacity)) {
		return
	}
	if outcome.Cancellation != OpenAICancelReasonNone {
		observe(runtime.telemetry.ObserveCancellation(key, outcome.Cancellation))
	}
}

func (runtime *AdaptiveLatencyRuntime) Enabled() bool {
	return runtime != nil && runtime.telemetry != nil && runtime.telemetry.Enabled()
}

func openAILatencyTelemetryKey(req AdaptiveLatencyObserveRequest) OpenAILatencyTelemetryKey {
	return OpenAILatencyTelemetryKey{
		AccountID:   req.AccountID,
		Cohort:      req.Cohort,
		AttemptRole: req.AttemptRole,
	}
}
