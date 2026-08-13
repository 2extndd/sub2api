package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

const (
	openAILatencyTelemetryFinalizerLeaderKey  = "openai:latency:telemetry:finalizer"
	openAILatencyTelemetryFinalizerLockTTL    = 90 * time.Second
	openAILatencyTelemetryBucketListLimit     = 4096
	openAILatencyTelemetryBucketReadBatchSize = 256

	OpenAILatencyTelemetry15MinuteRollupSeconds = 15 * 60
	OpenAILatencyTelemetryHourlyRollupSeconds   = 60 * 60

	OpenAILatencySuggestedPenalizedThreshold   = 25 * time.Second
	OpenAILatencySuggestedQuarantinedThreshold = 40 * time.Second
	OpenAILatencySuggestedHardInvalidThreshold = 60 * time.Second

	openAILatencyRecommendationFullConfidenceSamples = 100
)

var (
	ErrOpenAILatencyTelemetryCacheUnavailable  = errors.New("openai latency telemetry cache is unavailable")
	ErrOpenAILatencyTelemetryRollupUnavailable = errors.New("openai latency telemetry rollup repository is unavailable")
)

// OpenAILatencyTelemetryBucket identifies one raw fixed-duration bucket. Values
// use the same Unix-nanosecond representation as the T01 snapshot identity.
type OpenAILatencyTelemetryBucket struct {
	BucketStart    int64
	BucketDuration int64
}

func (b OpenAILatencyTelemetryBucket) StartTime() time.Time {
	return time.Unix(0, b.BucketStart).UTC()
}

func (b OpenAILatencyTelemetryBucket) Duration() time.Duration {
	return time.Duration(b.BucketDuration)
}

func (b OpenAILatencyTelemetryBucket) EndTime() time.Time {
	return b.StartTime().Add(b.Duration())
}

func (b OpenAILatencyTelemetryBucket) Validate() error {
	if b.BucketDuration <= 0 {
		return errors.New("telemetry bucket duration must be positive")
	}
	if b.BucketStart%b.BucketDuration != 0 {
		return errors.New("telemetry bucket start must align to its duration")
	}
	return nil
}

// OpenAILatencyTelemetrySourceHeartbeat contains only numeric source identity
// and timing metadata. It is safe to persist and expose in diagnostics.
type OpenAILatencyTelemetrySourceHeartbeat struct {
	SourceID   uint64
	Generation uint64
	ObservedAt time.Time
}

// OpenAILatencyTelemetryCache is the Redis-facing persistence port. Stores are
// absolute cumulative values, so retries must be sequence-idempotent.
type OpenAILatencyTelemetryCache interface {
	StoreOpenAILatencyTelemetrySnapshots(
		context.Context,
		[]OpenAILatencyTelemetrySnapshot,
		OpenAILatencyTelemetrySourceHeartbeat,
		time.Duration,
		time.Duration,
	) error
	ListOpenAILatencyTelemetryBuckets(
		context.Context,
		time.Time,
		int,
	) ([]OpenAILatencyTelemetryBucket, error)
	LoadOpenAILatencyTelemetryBucket(
		context.Context,
		OpenAILatencyTelemetryBucket,
		int,
	) ([]OpenAILatencyTelemetrySnapshot, error)
	LoadOpenAILatencyTelemetryBucketBatch(
		context.Context,
		OpenAILatencyTelemetryBucket,
		uint64,
		int,
	) ([]OpenAILatencyTelemetrySnapshot, uint64, error)
	ListOpenAILatencyTelemetrySourceHeartbeats(
		context.Context,
		time.Time,
		int,
	) ([]OpenAILatencyTelemetrySourceHeartbeat, error)
	DeleteOpenAILatencyTelemetryBucket(context.Context, OpenAILatencyTelemetryBucket) error
}

// OpenAILatencyTelemetryRollup is an absolute, numeric, secret-safe durable
// aggregate. Payload contains only the fixed-shape T01 telemetry snapshot.
type OpenAILatencyTelemetryRollup struct {
	BucketStart         time.Time
	BucketSeconds       int
	Key                 OpenAILatencyTelemetryKey
	Payload             OpenAILatencyTelemetrySnapshot
	SourceCount         int
	SampleCount         uint64
	CoverageStart       time.Time
	FreshThrough        time.Time
	CoveredBucketCount  int
	ExpectedBucketCount int
	Complete            bool
	FinalizedAt         time.Time
}

func (r OpenAILatencyTelemetryRollup) Validate() error {
	if r.BucketStart.IsZero() {
		return errors.New("telemetry rollup bucket start is required")
	}
	if r.BucketSeconds != OpenAILatencyTelemetry15MinuteRollupSeconds &&
		r.BucketSeconds != OpenAILatencyTelemetryHourlyRollupSeconds {
		return errors.New("telemetry rollup bucket seconds must be 900 or 3600")
	}
	if err := validateOpenAILatencyTelemetryKey(r.Key); err != nil {
		return fmt.Errorf("validate telemetry rollup key: %w", err)
	}
	if r.Payload.Key != r.Key {
		return errors.New("telemetry rollup payload key does not match row key")
	}
	if r.SourceCount < 0 {
		return errors.New("telemetry rollup source count cannot be negative")
	}
	if r.CoverageStart.IsZero() || r.FreshThrough.IsZero() || r.FinalizedAt.IsZero() {
		return errors.New("telemetry rollup coverage and freshness timestamps are required")
	}
	windowEnd := r.BucketStart.Add(time.Duration(r.BucketSeconds) * time.Second)
	if r.CoverageStart.Before(r.BucketStart) || !r.CoverageStart.Before(r.FreshThrough) || r.FreshThrough.After(windowEnd) {
		return errors.New("telemetry rollup coverage must stay within its window")
	}
	if r.FinalizedAt.Before(r.FreshThrough) {
		return errors.New("telemetry rollup finalization cannot precede freshness")
	}
	if r.ExpectedBucketCount <= 0 || r.CoveredBucketCount <= 0 || r.CoveredBucketCount > r.ExpectedBucketCount {
		return errors.New("telemetry rollup bucket coverage is invalid")
	}
	return nil
}

// OpenAILatencyTelemetryRollupRepository is deliberately small and raw-SQL
// friendly. Upserts advance absolute payload quality monotonically; cleanup is
// duration-specific.
type OpenAILatencyTelemetryRollupRepository interface {
	UpsertOpenAILatencyTelemetryRollups(context.Context, []OpenAILatencyTelemetryRollup) error
	DeleteOpenAILatencyTelemetryRollupsBefore(context.Context, int, time.Time) (int64, error)
}

// OpenAILatencyTelemetryCollectorDependencies are injected explicitly so the
// collector can be exercised without production wiring.
type OpenAILatencyTelemetryCollectorDependencies struct {
	Cache            OpenAILatencyTelemetryCache
	RollupRepository OpenAILatencyTelemetryRollupRepository
	LeaderLock       LeaderLockCache
	DB               *sql.DB
	Clock            Clock
	SourceID         uint64
	Generation       uint64
}

type openAILatencyTelemetryCollectorConfig struct {
	enabled                    bool
	flushInterval              time.Duration
	bucketDuration             time.Duration
	finalizationInterval       time.Duration
	finalizationGrace          time.Duration
	sourceTTL                  time.Duration
	rawRetention               time.Duration
	maxStreams                 int
	maxContributions           int
	fifteenMinuteRetentionDays int
	hourlyRetentionDays        int
}

func newOpenAILatencyTelemetryCollectorConfig(
	shadow config.OpenAILatencyShadowConfig,
) (openAILatencyTelemetryCollectorConfig, error) {
	collectorConfig := openAILatencyTelemetryCollectorConfig{
		enabled:                    shadow.Enabled,
		flushInterval:              time.Duration(shadow.TelemetryFlushIntervalSeconds) * time.Second,
		bucketDuration:             time.Duration(shadow.TelemetryBucketSeconds) * time.Second,
		finalizationInterval:       time.Duration(shadow.TelemetryFinalizationIntervalSeconds) * time.Second,
		finalizationGrace:          time.Duration(shadow.TelemetryFinalizationGraceSeconds) * time.Second,
		sourceTTL:                  time.Duration(shadow.TelemetrySourceTTLSeconds) * time.Second,
		rawRetention:               time.Duration(shadow.TelemetryRedisRawRetentionSeconds) * time.Second,
		maxStreams:                 shadow.MaxTelemetryStreams,
		maxContributions:           shadow.MaxTelemetryContributions,
		fifteenMinuteRetentionDays: shadow.Telemetry15MinuteRetentionDays,
		hourlyRetentionDays:        shadow.TelemetryHourlyRetentionDays,
	}
	if !collectorConfig.enabled {
		return collectorConfig, nil
	}
	if collectorConfig.flushInterval <= 0 {
		return collectorConfig, errors.New("telemetry flush interval must be positive")
	}
	if collectorConfig.bucketDuration <= 0 {
		return collectorConfig, errors.New("telemetry bucket duration must be positive")
	}
	if collectorConfig.flushInterval > collectorConfig.bucketDuration {
		return collectorConfig, errors.New("telemetry flush interval cannot exceed bucket duration")
	}
	if collectorConfig.finalizationInterval <= 0 {
		return collectorConfig, errors.New("telemetry finalization interval must be positive")
	}
	if collectorConfig.finalizationInterval < collectorConfig.bucketDuration {
		return collectorConfig, errors.New("telemetry finalization interval cannot be shorter than bucket duration")
	}
	if collectorConfig.finalizationInterval > time.Duration(config.MaximumOpenAILatencyShadowTelemetryFinalizationIntervalSeconds)*time.Second {
		return collectorConfig, errors.New("telemetry finalization interval cannot exceed 15 minutes")
	}
	if collectorConfig.finalizationGrace <= 0 {
		return collectorConfig, errors.New("telemetry finalization grace must be positive")
	}
	if collectorConfig.sourceTTL < collectorConfig.flushInterval {
		return collectorConfig, errors.New("telemetry source TTL must be at least the flush interval")
	}
	if collectorConfig.rawRetention < time.Hour+collectorConfig.finalizationGrace+collectorConfig.sourceTTL {
		return collectorConfig, errors.New("telemetry raw retention is shorter than one hour, grace, and source TTL")
	}
	if collectorConfig.maxStreams <= 0 ||
		collectorConfig.maxStreams > config.MaximumOpenAILatencyShadowMaxTelemetryStreams {
		return collectorConfig, fmt.Errorf(
			"telemetry max streams must be between 1 and %d",
			config.MaximumOpenAILatencyShadowMaxTelemetryStreams,
		)
	}
	if collectorConfig.maxContributions <= 0 ||
		collectorConfig.maxContributions > config.MaximumOpenAILatencyShadowMaxTelemetryContributions {
		return collectorConfig, fmt.Errorf(
			"telemetry max contributions must be between 1 and %d",
			config.MaximumOpenAILatencyShadowMaxTelemetryContributions,
		)
	}
	if collectorConfig.fifteenMinuteRetentionDays <= 0 || collectorConfig.hourlyRetentionDays <= 0 {
		return collectorConfig, errors.New("telemetry durable retention days must be positive")
	}
	if collectorConfig.hourlyRetentionDays < collectorConfig.fifteenMinuteRetentionDays {
		return collectorConfig, errors.New("telemetry hourly retention cannot be shorter than 15-minute retention")
	}
	if (15*time.Minute)%collectorConfig.bucketDuration != 0 || time.Hour%collectorConfig.bucketDuration != 0 {
		return collectorConfig, errors.New("telemetry bucket duration must evenly divide 15 minutes and one hour")
	}
	return collectorConfig, nil
}

type openAILatencyTelemetryTrack struct {
	current        *OpenAILatencyTelemetryAggregate
	currentStart   int64
	previous       *OpenAILatencyTelemetryAggregate
	previousStart  int64
	lastObservedAt time.Time
}

type openAILatencyTelemetryCollectorDiagnostics struct {
	mu                      sync.Mutex
	observations            uint64
	droppedObservations     uint64
	overflowedStreams       uint64
	staleObservations       uint64
	staleBuckets            uint64
	flushAttempts           uint64
	flushSuccesses          uint64
	flushFailures           uint64
	flushedSnapshots        uint64
	finalizationAttempts    uint64
	finalizationSuccesses   uint64
	finalizationFailures    uint64
	finalizationLeaderSkips uint64
	lastFlushAt             time.Time
	lastFlushError          string
	lastFinalizationAt      time.Time
	lastFinalizationError   string
}

// OpenAILatencyTelemetryCollectorStats is a bounded, secret-free status view.
type OpenAILatencyTelemetryCollectorStats struct {
	Enabled                 bool
	Streams                 int
	MaxStreams              int
	CurrentBuckets          int
	PreviousBuckets         int
	Observations            uint64
	DroppedObservations     uint64
	OverflowedStreams       uint64
	StaleObservations       uint64
	StaleBuckets            uint64
	FlushAttempts           uint64
	FlushSuccesses          uint64
	FlushFailures           uint64
	FlushedSnapshots        uint64
	FinalizationAttempts    uint64
	FinalizationSuccesses   uint64
	FinalizationFailures    uint64
	FinalizationLeaderSkips uint64
	LastFlushAt             time.Time
	LastFlushError          string
	LastFinalizationAt      time.Time
	LastFinalizationError   string
}

// OpenAILatencyTelemetryRecommendationSnapshot is advisory only. No method on
// the collector accepts a scheduler/config mutator, making accidental policy
// activation impossible in this slice.
type OpenAILatencyTelemetryRecommendationSnapshot struct {
	GeneratedAt                   time.Time
	Fresh                         bool
	Freshness                     time.Duration
	Confidence                    float64
	SampleCount                   uint64
	SuggestedPenalizedThreshold   time.Duration
	SuggestedQuarantinedThreshold time.Duration
	SuggestedHardInvalidThreshold time.Duration
}

// OpenAILatencyTelemetryFinalizationResult describes one leader-gated pass.
type OpenAILatencyTelemetryFinalizationResult struct {
	Leader                   bool
	BucketsListed            int
	BucketsLoaded            int
	SnapshotsLoaded          int
	ContributionsLoaded      int
	ContributionLimitReached bool
	RollupsPersisted         int
	StaleSources             int
	RetentionRowsDeleted     int64
}

// OpenAILatencyTelemetryCollector owns only local aggregation and persistence
// coordination. It has no routing/config mutation dependency.
type OpenAILatencyTelemetryCollector struct {
	config      openAILatencyTelemetryCollectorConfig
	cache       OpenAILatencyTelemetryCache
	rollups     OpenAILatencyTelemetryRollupRepository
	leaderLock  LeaderLockCache
	db          *sql.DB
	clock       Clock
	sourceID    uint64
	generation  uint64
	leaderOwner string

	mu       sync.Mutex
	registry map[OpenAILatencyTelemetryKey]*openAILatencyTelemetryTrack

	diagnostics openAILatencyTelemetryCollectorDiagnostics

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}
}

// NewOpenAILatencyTelemetryCollector does not allocate maps/channels or launch
// a goroutine when shadow telemetry is disabled.
func NewOpenAILatencyTelemetryCollector(
	shadow config.OpenAILatencyShadowConfig,
	dependencies OpenAILatencyTelemetryCollectorDependencies,
) (*OpenAILatencyTelemetryCollector, error) {
	collectorConfig, err := newOpenAILatencyTelemetryCollectorConfig(shadow)
	if err != nil {
		return nil, err
	}
	collector := &OpenAILatencyTelemetryCollector{config: collectorConfig}
	if !collectorConfig.enabled {
		return collector, nil
	}
	if dependencies.SourceID == 0 {
		return nil, errors.New("telemetry collector source id must be positive")
	}
	generation := dependencies.Generation
	if generation == 0 {
		generation = mintOpenAILatencyBootToken()
	}
	clock := dependencies.Clock
	if clock == nil {
		clock = SystemClock{}
	}
	collector.cache = dependencies.Cache
	collector.rollups = dependencies.RollupRepository
	collector.leaderLock = dependencies.LeaderLock
	collector.db = dependencies.DB
	collector.clock = clock
	collector.sourceID = dependencies.SourceID
	collector.generation = generation
	collector.leaderOwner = strconv.FormatUint(dependencies.SourceID, 10) + ":" + strconv.FormatUint(generation, 10)
	collector.registry = make(map[OpenAILatencyTelemetryKey]*openAILatencyTelemetryTrack)
	collector.stopCh = make(chan struct{})
	collector.doneCh = make(chan struct{})
	return collector, nil
}

func (c *OpenAILatencyTelemetryCollector) Enabled() bool {
	return c != nil && c.config.enabled
}

// Start launches independent flush and finalization loops only when enabled.
func (c *OpenAILatencyTelemetryCollector) Start() {
	if c == nil || !c.config.enabled {
		return
	}
	c.startOnce.Do(func() {
		go c.run()
	})
}

func (c *OpenAILatencyTelemetryCollector) run() {
	defer close(c.doneCh)
	var loops sync.WaitGroup
	loops.Add(2)
	go func() {
		defer loops.Done()
		c.runFlushLoop()
	}()
	go func() {
		defer loops.Done()
		c.runFinalizationLoop()
	}()
	loops.Wait()
}

func (c *OpenAILatencyTelemetryCollector) runFlushLoop() {
	ticker := time.NewTicker(c.config.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), c.config.flushInterval)
			if err := c.Flush(ctx); err != nil {
				slog.Warn("openai latency telemetry flush failed",
					"source_id", c.sourceID,
					"generation", c.generation,
					"error_type", fmt.Sprintf("%T", err),
				)
			}
			cancel()
		case <-c.stopCh:
			return
		}
	}
}

func (c *OpenAILatencyTelemetryCollector) runFinalizationLoop() {
	ticker := time.NewTicker(c.config.finalizationInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), c.config.finalizationInterval)
			if _, err := c.Finalize(ctx); err != nil {
				slog.Warn("openai latency telemetry finalization failed",
					"source_id", c.sourceID,
					"generation", c.generation,
					"error_type", fmt.Sprintf("%T", err),
				)
			}
			cancel()
		case <-c.stopCh:
			return
		}
	}
}

func (c *OpenAILatencyTelemetryCollector) Stop() {
	if c == nil || !c.config.enabled {
		return
	}
	c.stopOnce.Do(func() {
		close(c.stopCh)
		// Claim startOnce when Stop wins the race, making Start-after-Stop a
		// documented no-op and ensuring doneCh is always closed.
		c.startOnce.Do(func() { close(c.doneCh) })
	})
	<-c.doneCh
}

func (c *OpenAILatencyTelemetryCollector) aggregateForObservation(
	key OpenAILatencyTelemetryKey,
) (*OpenAILatencyTelemetryAggregate, error) {
	canonicalKey, err := canonicalOpenAILatencyTelemetryKey(key)
	if err != nil {
		return nil, err
	}
	now := c.clock.Now().UTC()
	bucketStart := now.Truncate(c.config.bucketDuration).UnixNano()
	bucketDuration := int64(c.config.bucketDuration)

	c.mu.Lock()
	track := c.registry[canonicalKey]
	if track == nil {
		if len(c.registry) >= c.config.maxStreams {
			c.mu.Unlock()
			c.recordDropped(true, false)
			return nil, nil
		}
		aggregate, aggregateErr := NewOpenAILatencyTelemetryAggregateWithIdentity(canonicalKey, OpenAILatencyTelemetryIdentity{
			SourceID:       c.sourceID,
			Generation:     c.generation,
			BucketStart:    bucketStart,
			BucketDuration: bucketDuration,
		})
		if aggregateErr != nil {
			c.mu.Unlock()
			return nil, aggregateErr
		}
		track = &openAILatencyTelemetryTrack{
			current:        aggregate,
			currentStart:   bucketStart,
			lastObservedAt: now,
		}
		c.registry[canonicalKey] = track
		c.mu.Unlock()
		return aggregate, nil
	}

	var aggregate *OpenAILatencyTelemetryAggregate
	switch {
	case bucketStart == track.currentStart:
		aggregate = track.current
	case bucketStart == track.previousStart && track.previous != nil:
		aggregate = track.previous
	case bucketStart > track.currentStart:
		if track.previous != nil {
			c.recordStaleBucketLocked()
		}
		if bucketStart-track.currentStart > bucketDuration {
			c.recordStaleBucketLocked()
		}
		track.previous = track.current
		track.previousStart = track.currentStart
		track.current, err = NewOpenAILatencyTelemetryAggregateWithIdentity(canonicalKey, OpenAILatencyTelemetryIdentity{
			SourceID:       c.sourceID,
			Generation:     c.generation,
			BucketStart:    bucketStart,
			BucketDuration: bucketDuration,
		})
		if err == nil {
			track.currentStart = bucketStart
			aggregate = track.current
		}
	default:
		c.mu.Unlock()
		c.recordDropped(false, true)
		return nil, nil
	}
	track.lastObservedAt = now
	c.mu.Unlock()
	return aggregate, err
}

func (c *OpenAILatencyTelemetryCollector) observeWithAggregate(
	key OpenAILatencyTelemetryKey,
	observe func(*OpenAILatencyTelemetryAggregate) error,
) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	aggregate, err := c.aggregateForObservation(key)
	if err != nil || aggregate == nil {
		return err
	}
	if err := observe(aggregate); err != nil {
		return err
	}
	c.diagnostics.mu.Lock()
	c.diagnostics.observations++
	c.diagnostics.mu.Unlock()
	return nil
}

func (c *OpenAILatencyTelemetryCollector) ObservePhase(key OpenAILatencyTelemetryKey, phase OpenAILatencyPhase, latency time.Duration) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	return c.observeWithAggregate(key, func(aggregate *OpenAILatencyTelemetryAggregate) error {
		return aggregate.ObservePhase(phase, latency)
	})
}

func (c *OpenAILatencyTelemetryCollector) ObserveHedge(key OpenAILatencyTelemetryKey, outcome OpenAIHedgeOutcome) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	return c.observeWithAggregate(key, func(aggregate *OpenAILatencyTelemetryAggregate) error {
		return aggregate.ObserveHedge(outcome)
	})
}

func (c *OpenAILatencyTelemetryCollector) ObserveWinner(key OpenAILatencyTelemetryKey, outcome OpenAIWinnerOutcome) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	return c.observeWithAggregate(key, func(aggregate *OpenAILatencyTelemetryAggregate) error {
		return aggregate.ObserveWinner(outcome)
	})
}

func (c *OpenAILatencyTelemetryCollector) ObserveLoserCancellation(key OpenAILatencyTelemetryKey, outcome OpenAILoserCancelOutcome) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	return c.observeWithAggregate(key, func(aggregate *OpenAILatencyTelemetryAggregate) error {
		return aggregate.ObserveLoserCancellation(outcome)
	})
}

func (c *OpenAILatencyTelemetryCollector) ObserveLoserLifecycle(key OpenAILatencyTelemetryKey, event OpenAILoserLifecycleEvent) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	return c.observeWithAggregate(key, func(aggregate *OpenAILatencyTelemetryAggregate) error {
		return aggregate.ObserveLoserLifecycle(event)
	})
}

func (c *OpenAILatencyTelemetryCollector) ObserveUsage(key OpenAILatencyTelemetryKey, outcome OpenAIUsageOutcome) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	return c.observeWithAggregate(key, func(aggregate *OpenAILatencyTelemetryAggregate) error {
		return aggregate.ObserveUsage(outcome)
	})
}

func (c *OpenAILatencyTelemetryCollector) ObserveCancellation(key OpenAILatencyTelemetryKey, reason OpenAICancelReason) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	return c.observeWithAggregate(key, func(aggregate *OpenAILatencyTelemetryAggregate) error {
		return aggregate.ObserveCancellation(reason)
	})
}

func (c *OpenAILatencyTelemetryCollector) ObserveBilling(key OpenAILatencyTelemetryKey, outcome OpenAIBillingOutcome) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	return c.observeWithAggregate(key, func(aggregate *OpenAILatencyTelemetryAggregate) error {
		return aggregate.ObserveBilling(outcome)
	})
}

func (c *OpenAILatencyTelemetryCollector) ObserveLoserCostMicros(key OpenAILatencyTelemetryKey, amount uint64) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	return c.observeWithAggregate(key, func(aggregate *OpenAILatencyTelemetryAggregate) error {
		return aggregate.ObserveLoserCostMicros(amount)
	})
}

func (c *OpenAILatencyTelemetryCollector) ObserveCapacity(key OpenAILatencyTelemetryKey, outcome OpenAICapacityOutcome) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	return c.observeWithAggregate(key, func(aggregate *OpenAILatencyTelemetryAggregate) error {
		return aggregate.ObserveCapacity(outcome)
	})
}

func (c *OpenAILatencyTelemetryCollector) recordDropped(overflow, stale bool) {
	c.diagnostics.mu.Lock()
	c.diagnostics.droppedObservations++
	if overflow {
		c.diagnostics.overflowedStreams++
	}
	if stale {
		c.diagnostics.staleObservations++
	}
	c.diagnostics.mu.Unlock()
}

func (c *OpenAILatencyTelemetryCollector) recordStaleBucketLocked() {
	c.diagnostics.mu.Lock()
	c.diagnostics.staleBuckets++
	c.diagnostics.mu.Unlock()
}

// CurrentOpenAILatencyTelemetrySnapshots returns deterministic cumulative
// copies of each stream's current bucket. Callers cannot mutate live state.
func (c *OpenAILatencyTelemetryCollector) CurrentOpenAILatencyTelemetrySnapshots() []OpenAILatencyTelemetrySnapshot {
	if c == nil || !c.config.enabled {
		return nil
	}
	c.mu.Lock()
	snapshots := make([]OpenAILatencyTelemetrySnapshot, 0, len(c.registry))
	for _, track := range c.registry {
		if track.current == nil {
			continue
		}
		snapshot := track.current.Snapshot()
		if snapshot.Sequence > 0 {
			snapshots = append(snapshots, snapshot)
		}
	}
	c.mu.Unlock()
	sortOpenAILatencyTelemetrySnapshots(snapshots)
	return snapshots
}

type openAILatencyTelemetryFlushPlan struct {
	snapshots []OpenAILatencyTelemetrySnapshot
	sequences map[*OpenAILatencyTelemetryAggregate]uint64
}

func (c *OpenAILatencyTelemetryCollector) snapshotExpired(
	snapshot OpenAILatencyTelemetrySnapshot,
	now time.Time,
) bool {
	bucketEnd := time.Unix(0, snapshot.BucketStart).UTC().Add(time.Duration(snapshot.BucketDuration))
	return !bucketEnd.Add(c.config.rawRetention).After(now)
}

func (c *OpenAILatencyTelemetryCollector) snapshotsForFlush(now time.Time) openAILatencyTelemetryFlushPlan {
	c.mu.Lock()
	plan := openAILatencyTelemetryFlushPlan{
		snapshots: make([]OpenAILatencyTelemetrySnapshot, 0, len(c.registry)*2),
		sequences: make(map[*OpenAILatencyTelemetryAggregate]uint64, len(c.registry)*2),
	}
	for _, track := range c.registry {
		for _, aggregate := range []*OpenAILatencyTelemetryAggregate{track.current, track.previous} {
			if aggregate == nil {
				continue
			}
			snapshot := aggregate.Snapshot()
			plan.sequences[aggregate] = snapshot.Sequence
			if snapshot.Sequence > 0 && !c.snapshotExpired(snapshot, now) {
				plan.snapshots = append(plan.snapshots, snapshot)
			}
		}
	}
	c.mu.Unlock()
	sortOpenAILatencyTelemetrySnapshots(plan.snapshots)
	return plan
}

func (c *OpenAILatencyTelemetryCollector) pruneExpiredTracksAfterFlush(
	now time.Time,
	plan openAILatencyTelemetryFlushPlan,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, track := range c.registry {
		if track.previous != nil {
			snapshot := track.previous.Snapshot()
			if c.snapshotExpired(snapshot, now) && plan.sequences[track.previous] == snapshot.Sequence {
				track.previous = nil
				track.previousStart = 0
			}
		}
		if track.current == nil {
			delete(c.registry, key)
			continue
		}
		current := track.current.Snapshot()
		if !c.snapshotExpired(current, now) || plan.sequences[track.current] != current.Sequence {
			continue
		}
		if track.previous == nil {
			delete(c.registry, key)
		}
	}
}

func sortOpenAILatencyTelemetrySnapshots(snapshots []OpenAILatencyTelemetrySnapshot) {
	sort.Slice(snapshots, func(i, j int) bool {
		left, right := snapshots[i], snapshots[j]
		if left.BucketStart != right.BucketStart {
			return left.BucketStart < right.BucketStart
		}
		if left.Key.AccountID != right.Key.AccountID {
			return left.Key.AccountID < right.Key.AccountID
		}
		if left.Key.Cohort != right.Key.Cohort {
			return left.Key.Cohort < right.Key.Cohort
		}
		return left.Key.AttemptRole < right.Key.AttemptRole
	})
}

// Flush is fail-open with respect to observations: an error is returned for
// diagnostics, but no local aggregate is drained or reset.
func (c *OpenAILatencyTelemetryCollector) Flush(ctx context.Context) error {
	if c == nil || !c.config.enabled {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := c.clock.Now().UTC()
	plan := c.snapshotsForFlush(now)
	snapshots := plan.snapshots
	c.diagnostics.mu.Lock()
	c.diagnostics.flushAttempts++
	c.diagnostics.mu.Unlock()

	if c.cache == nil {
		c.recordFlushFailure(ErrOpenAILatencyTelemetryCacheUnavailable)
		return ErrOpenAILatencyTelemetryCacheUnavailable
	}
	err := c.cache.StoreOpenAILatencyTelemetrySnapshots(
		ctx,
		snapshots,
		OpenAILatencyTelemetrySourceHeartbeat{
			SourceID:   c.sourceID,
			Generation: c.generation,
			ObservedAt: now,
		},
		c.config.rawRetention,
		c.config.sourceTTL,
	)
	if err != nil {
		c.recordFlushFailure(err)
		return fmt.Errorf("store openai latency telemetry snapshots: %w", err)
	}
	c.pruneExpiredTracksAfterFlush(now, plan)
	c.diagnostics.mu.Lock()
	c.diagnostics.flushSuccesses++
	c.diagnostics.flushedSnapshots += uint64(len(snapshots))
	c.diagnostics.lastFlushAt = now
	c.diagnostics.lastFlushError = ""
	c.diagnostics.mu.Unlock()
	return nil
}

func (c *OpenAILatencyTelemetryCollector) recordFlushFailure(err error) {
	c.diagnostics.mu.Lock()
	c.diagnostics.flushFailures++
	c.diagnostics.lastFlushError = fmt.Sprintf("%T", err)
	c.diagnostics.mu.Unlock()
}

func (c *OpenAILatencyTelemetryCollector) Stats() OpenAILatencyTelemetryCollectorStats {
	if c == nil {
		return OpenAILatencyTelemetryCollectorStats{}
	}
	currentBuckets, previousBuckets, streams := 0, 0, 0
	if c.config.enabled {
		c.mu.Lock()
		streams = len(c.registry)
		for _, track := range c.registry {
			if track.current != nil {
				currentBuckets++
			}
			if track.previous != nil {
				previousBuckets++
			}
		}
		c.mu.Unlock()
	}
	c.diagnostics.mu.Lock()
	stats := OpenAILatencyTelemetryCollectorStats{
		Enabled:                 c.config.enabled,
		Streams:                 streams,
		MaxStreams:              c.config.maxStreams,
		CurrentBuckets:          currentBuckets,
		PreviousBuckets:         previousBuckets,
		Observations:            c.diagnostics.observations,
		DroppedObservations:     c.diagnostics.droppedObservations,
		OverflowedStreams:       c.diagnostics.overflowedStreams,
		StaleObservations:       c.diagnostics.staleObservations,
		StaleBuckets:            c.diagnostics.staleBuckets,
		FlushAttempts:           c.diagnostics.flushAttempts,
		FlushSuccesses:          c.diagnostics.flushSuccesses,
		FlushFailures:           c.diagnostics.flushFailures,
		FlushedSnapshots:        c.diagnostics.flushedSnapshots,
		FinalizationAttempts:    c.diagnostics.finalizationAttempts,
		FinalizationSuccesses:   c.diagnostics.finalizationSuccesses,
		FinalizationFailures:    c.diagnostics.finalizationFailures,
		FinalizationLeaderSkips: c.diagnostics.finalizationLeaderSkips,
		LastFlushAt:             c.diagnostics.lastFlushAt,
		LastFlushError:          c.diagnostics.lastFlushError,
		LastFinalizationAt:      c.diagnostics.lastFinalizationAt,
		LastFinalizationError:   c.diagnostics.lastFinalizationError,
	}
	c.diagnostics.mu.Unlock()
	return stats
}

// RecommendationSnapshot computes read-only guidance from current local data.
// It never writes config, scheduler state, health state, cache, or routing.
func (c *OpenAILatencyTelemetryCollector) RecommendationSnapshot() OpenAILatencyTelemetryRecommendationSnapshot {
	recommendation := OpenAILatencyTelemetryRecommendationSnapshot{
		SuggestedPenalizedThreshold:   OpenAILatencySuggestedPenalizedThreshold,
		SuggestedQuarantinedThreshold: OpenAILatencySuggestedQuarantinedThreshold,
		SuggestedHardInvalidThreshold: OpenAILatencySuggestedHardInvalidThreshold,
	}
	if c == nil || !c.config.enabled {
		return recommendation
	}
	now := c.clock.Now().UTC()
	recommendation.GeneratedAt = now
	var lastObservedAt time.Time
	c.mu.Lock()
	for _, track := range c.registry {
		if track.current != nil {
			snapshot := track.current.Snapshot()
			recommendation.SampleCount = saturatingAddUint64(
				recommendation.SampleCount,
				snapshot.Phases[OpenAILatencyPhaseTotal].Observations,
			)
		}
		if track.lastObservedAt.After(lastObservedAt) {
			lastObservedAt = track.lastObservedAt
		}
	}
	c.mu.Unlock()
	if !lastObservedAt.IsZero() {
		recommendation.Freshness = now.Sub(lastObservedAt)
		if recommendation.Freshness < 0 {
			recommendation.Freshness = 0
		}
		recommendation.Fresh = recommendation.Freshness <= c.config.sourceTTL
	}
	recommendation.Confidence = math.Min(
		1,
		float64(recommendation.SampleCount)/openAILatencyRecommendationFullConfidenceSamples,
	)
	return recommendation
}

func saturatingAddUint64(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

type openAILatencyMinuteAggregate struct {
	payload OpenAILatencyTelemetrySnapshot
	sources map[openAILatencyTelemetryStreamKey]struct{}
}

type openAILatencyRollupKey struct {
	bucketStart   int64
	bucketSeconds int
	telemetryKey  OpenAILatencyTelemetryKey
}

type openAILatencyRollupAccumulator struct {
	payload        OpenAILatencyTelemetrySnapshot
	sources        map[openAILatencyTelemetryStreamKey]struct{}
	coveredBuckets map[int64]struct{}
	coverageStart  time.Time
	freshThrough   time.Time
}

func (c *OpenAILatencyTelemetryCollector) acquireFinalizationFence(
	ctx context.Context,
) (func(), bool, error) {
	releaseRedis := func() {}
	redisAcquired := false
	if c.leaderLock != nil {
		acquired, err := c.leaderLock.TryAcquireLeaderLock(
			ctx,
			openAILatencyTelemetryFinalizerLeaderKey,
			c.leaderOwner,
			openAILatencyTelemetryFinalizerLockTTL,
		)
		if err == nil {
			if !acquired {
				return nil, false, nil
			}
			redisAcquired = true
			releaseRedis = func() {
				releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = c.leaderLock.ReleaseLeaderLock(
					releaseCtx,
					openAILatencyTelemetryFinalizerLeaderKey,
					c.leaderOwner,
				)
			}
		} else if c.db == nil {
			return nil, false, fmt.Errorf(
				"acquire telemetry finalization fence: Redis leadership unavailable and no database fence configured: %w",
				err,
			)
		}
	}

	if c.db != nil {
		releaseDB, acquired, err := tryAcquireDBAdvisoryLockWithError(
			ctx,
			c.db,
			hashAdvisoryLockID(openAILatencyTelemetryFinalizerLeaderKey),
		)
		if err != nil {
			if redisAcquired {
				releaseRedis()
			}
			return nil, false, fmt.Errorf("acquire telemetry finalization advisory fence: %w", err)
		}
		if !acquired {
			if redisAcquired {
				releaseRedis()
			}
			return nil, false, nil
		}
		return func() {
			releaseDB()
			if redisAcquired {
				releaseRedis()
			}
		}, true, nil
	}
	if redisAcquired {
		return releaseRedis, true, nil
	}
	return nil, false, errors.New("openai latency telemetry finalization requires a Redis or database coordination fence")
}

// Finalize persists 15-minute and hourly absolute rollups under a Redis lease
// and, when configured, a connection-scoped Postgres advisory fence.
func (c *OpenAILatencyTelemetryCollector) Finalize(ctx context.Context) (result OpenAILatencyTelemetryFinalizationResult, err error) {
	if c == nil || !c.config.enabled {
		return result, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := c.clock.Now().UTC()
	c.diagnostics.mu.Lock()
	c.diagnostics.finalizationAttempts++
	c.diagnostics.mu.Unlock()
	defer func() {
		c.diagnostics.mu.Lock()
		if err != nil {
			c.diagnostics.finalizationFailures++
			c.diagnostics.lastFinalizationError = fmt.Sprintf("%T", err)
		} else if result.Leader {
			c.diagnostics.finalizationSuccesses++
			c.diagnostics.lastFinalizationAt = now
			c.diagnostics.lastFinalizationError = ""
		}
		c.diagnostics.mu.Unlock()
	}()

	if c.cache == nil {
		return result, ErrOpenAILatencyTelemetryCacheUnavailable
	}
	if c.rollups == nil {
		return result, ErrOpenAILatencyTelemetryRollupUnavailable
	}
	release, leader, fenceErr := c.acquireFinalizationFence(ctx)
	if fenceErr != nil {
		return result, fenceErr
	}
	if !leader {
		c.diagnostics.mu.Lock()
		c.diagnostics.finalizationLeaderSkips++
		c.diagnostics.mu.Unlock()
		return result, nil
	}
	result.Leader = true
	defer release()

	finalizeBefore := now.Add(-c.config.finalizationGrace)
	buckets, err := c.cache.ListOpenAILatencyTelemetryBuckets(
		ctx,
		finalizeBefore,
		openAILatencyTelemetryBucketListLimit,
	)
	if err != nil {
		return result, fmt.Errorf("list telemetry buckets: %w", err)
	}
	result.BucketsListed = len(buckets)
	heartbeats, err := c.cache.ListOpenAILatencyTelemetrySourceHeartbeats(
		ctx,
		now.Add(-c.config.sourceTTL),
		c.config.maxContributions,
	)
	if err != nil {
		return result, fmt.Errorf("list telemetry source heartbeats: %w", err)
	}

	rollupAccumulators := make(map[openAILatencyRollupKey]*openAILatencyRollupAccumulator)
	incompleteRollupWindows := make(map[[2]int64]struct{})
	markBucketWindowsIncomplete := func(bucket OpenAILatencyTelemetryBucket) {
		for _, seconds := range [...]int{
			OpenAILatencyTelemetry15MinuteRollupSeconds,
			OpenAILatencyTelemetryHourlyRollupSeconds,
		} {
			windowStart := bucket.StartTime().Truncate(time.Duration(seconds) * time.Second).Unix()
			incompleteRollupWindows[[2]int64{windowStart, int64(seconds)}] = struct{}{}
		}
	}
	for _, bucket := range buckets {
		if err := bucket.Validate(); err != nil {
			return result, fmt.Errorf("validate listed telemetry bucket: %w", err)
		}
		bucketEnd := bucket.EndTime()
		if bucketEnd.Add(c.config.finalizationGrace).After(now) {
			continue
		}
		ready := true
		for _, heartbeat := range heartbeats {
			if heartbeat.ObservedAt.Before(bucketEnd) {
				ready = false
				break
			}
		}
		if !ready {
			markBucketWindowsIncomplete(bucket)
			continue
		}

		remaining := c.config.maxContributions - result.ContributionsLoaded
		if remaining <= 0 {
			result.ContributionLimitReached = true
			markBucketWindowsIncomplete(bucket)
			break
		}
		snapshots := make([]OpenAILatencyTelemetrySnapshot, 0, min(remaining, openAILatencyTelemetryBucketReadBatchSize))
		var cursor uint64
		bucketComplete := true
		for {
			remaining = c.config.maxContributions - result.ContributionsLoaded
			if remaining <= 0 {
				bucketComplete = false
				result.ContributionLimitReached = true
				break
			}
			batchLimit := min(remaining, openAILatencyTelemetryBucketReadBatchSize)
			batch, nextCursor, loadErr := c.cache.LoadOpenAILatencyTelemetryBucketBatch(
				ctx,
				bucket,
				cursor,
				batchLimit,
			)
			if loadErr != nil {
				return result, fmt.Errorf("load telemetry bucket batch: %w", loadErr)
			}
			snapshots = append(snapshots, batch...)
			result.SnapshotsLoaded += len(batch)
			result.ContributionsLoaded += len(batch)
			if nextCursor == 0 {
				break
			}
			if result.ContributionsLoaded >= c.config.maxContributions {
				bucketComplete = false
				result.ContributionLimitReached = true
				break
			}
			cursor = nextCursor
		}
		result.BucketsLoaded++
		if !bucketComplete {
			markBucketWindowsIncomplete(bucket)
		}
		minuteAggregates, aggregateErr := aggregateOpenAILatencyMinuteSnapshots(snapshots, c.config.maxContributions)
		if aggregateErr != nil {
			return result, fmt.Errorf("aggregate telemetry bucket: %w", aggregateErr)
		}
		for key, minute := range minuteAggregates {
			for _, seconds := range [...]int{
				OpenAILatencyTelemetry15MinuteRollupSeconds,
				OpenAILatencyTelemetryHourlyRollupSeconds,
			} {
				rollupDuration := time.Duration(seconds) * time.Second
				rollupStart := bucket.StartTime().Truncate(rollupDuration)
				if rollupStart.Add(rollupDuration).Add(c.config.finalizationGrace).After(now) {
					continue
				}
				rollupKey := openAILatencyRollupKey{
					bucketStart:   rollupStart.UnixNano(),
					bucketSeconds: seconds,
					telemetryKey:  key,
				}
				accumulator := rollupAccumulators[rollupKey]
				if accumulator == nil {
					accumulator = &openAILatencyRollupAccumulator{
						sources:        make(map[openAILatencyTelemetryStreamKey]struct{}),
						coveredBuckets: make(map[int64]struct{}),
					}
					rollupAccumulators[rollupKey] = accumulator
				}
				if mergeErr := mergeOpenAILatencyTelemetryPayload(&accumulator.payload, minute.payload); mergeErr != nil {
					return result, fmt.Errorf("merge telemetry rollup payload: %w", mergeErr)
				}
				for source := range minute.sources {
					accumulator.sources[source] = struct{}{}
				}
				accumulator.coveredBuckets[bucket.BucketStart] = struct{}{}
				if accumulator.coverageStart.IsZero() || bucket.StartTime().Before(accumulator.coverageStart) {
					accumulator.coverageStart = bucket.StartTime()
				}
				if bucketEnd.After(accumulator.freshThrough) {
					accumulator.freshThrough = bucketEnd
				}
			}
		}
		if result.ContributionLimitReached {
			break
		}
	}

	rollups := make([]OpenAILatencyTelemetryRollup, 0, len(rollupAccumulators))
	for key, accumulator := range rollupAccumulators {
		windowStartUnix := time.Unix(0, key.bucketStart).Unix()
		_, incomplete := incompleteRollupWindows[[2]int64{windowStartUnix, int64(key.bucketSeconds)}]
		coveredBucketCount := len(accumulator.coveredBuckets)
		expectedBucketCount := key.bucketSeconds / int(c.config.bucketDuration/time.Second)
		accumulator.payload.SchemaVersion = OpenAILatencyTelemetrySchemaVersion
		accumulator.payload.Key = key.telemetryKey
		accumulator.payload.setIdentity(OpenAILatencyTelemetryIdentity{
			SourceID:       0,
			Generation:     1,
			BucketStart:    key.bucketStart,
			BucketDuration: int64(time.Duration(key.bucketSeconds) * time.Second),
		}, 1)
		rollup := OpenAILatencyTelemetryRollup{
			BucketStart:         time.Unix(0, key.bucketStart).UTC(),
			BucketSeconds:       key.bucketSeconds,
			Key:                 key.telemetryKey,
			Payload:             accumulator.payload,
			SourceCount:         len(accumulator.sources),
			SampleCount:         accumulator.payload.Phases[OpenAILatencyPhaseTotal].Observations,
			CoverageStart:       accumulator.coverageStart,
			FreshThrough:        accumulator.freshThrough,
			CoveredBucketCount:  coveredBucketCount,
			ExpectedBucketCount: expectedBucketCount,
			Complete:            !incomplete && coveredBucketCount == expectedBucketCount,
			FinalizedAt:         now,
		}
		if validateErr := rollup.Validate(); validateErr != nil {
			return result, validateErr
		}
		rollups = append(rollups, rollup)
	}
	sort.Slice(rollups, func(i, j int) bool {
		left, right := rollups[i], rollups[j]
		if !left.BucketStart.Equal(right.BucketStart) {
			return left.BucketStart.Before(right.BucketStart)
		}
		if left.BucketSeconds != right.BucketSeconds {
			return left.BucketSeconds < right.BucketSeconds
		}
		if left.Key.AccountID != right.Key.AccountID {
			return left.Key.AccountID < right.Key.AccountID
		}
		if left.Key.Cohort != right.Key.Cohort {
			return left.Key.Cohort < right.Key.Cohort
		}
		return left.Key.AttemptRole < right.Key.AttemptRole
	})
	if err := c.rollups.UpsertOpenAILatencyTelemetryRollups(ctx, rollups); err != nil {
		return result, fmt.Errorf("upsert telemetry rollups: %w", err)
	}
	result.RollupsPersisted = len(rollups)

	fifteenDeleted, err := c.rollups.DeleteOpenAILatencyTelemetryRollupsBefore(
		ctx,
		OpenAILatencyTelemetry15MinuteRollupSeconds,
		now.AddDate(0, 0, -c.config.fifteenMinuteRetentionDays),
	)
	if err != nil {
		return result, fmt.Errorf("delete expired 15-minute telemetry rollups: %w", err)
	}
	hourlyDeleted, err := c.rollups.DeleteOpenAILatencyTelemetryRollupsBefore(
		ctx,
		OpenAILatencyTelemetryHourlyRollupSeconds,
		now.AddDate(0, 0, -c.config.hourlyRetentionDays),
	)
	if err != nil {
		return result, fmt.Errorf("delete expired hourly telemetry rollups: %w", err)
	}
	result.RetentionRowsDeleted = fifteenDeleted + hourlyDeleted
	return result, nil
}

func aggregateOpenAILatencyMinuteSnapshots(
	snapshots []OpenAILatencyTelemetrySnapshot,
	maxStreams int,
) (map[OpenAILatencyTelemetryKey]openAILatencyMinuteAggregate, error) {
	mergers := make(map[OpenAILatencyTelemetryKey]*OpenAILatencyTelemetryMerger)
	sources := make(map[OpenAILatencyTelemetryKey]map[openAILatencyTelemetryStreamKey]struct{})
	for _, snapshot := range snapshots {
		canonicalKey, err := canonicalOpenAILatencyTelemetryKey(snapshot.Key)
		if err != nil {
			return nil, err
		}
		merger := mergers[canonicalKey]
		if merger == nil {
			merger, err = NewOpenAILatencyTelemetryMerger(maxStreams)
			if err != nil {
				return nil, err
			}
			mergers[canonicalKey] = merger
			sources[canonicalKey] = make(map[openAILatencyTelemetryStreamKey]struct{})
		}
		if _, err := merger.MergeCumulative(snapshot); err != nil {
			return nil, err
		}
		sources[canonicalKey][openAILatencyTelemetryStreamKey{
			sourceID:   snapshot.SourceID,
			generation: snapshot.Generation,
		}] = struct{}{}
	}
	result := make(map[OpenAILatencyTelemetryKey]openAILatencyMinuteAggregate, len(mergers))
	for key, merger := range mergers {
		result[key] = openAILatencyMinuteAggregate{
			payload: merger.Snapshot(),
			sources: sources[key],
		}
	}
	return result, nil
}
