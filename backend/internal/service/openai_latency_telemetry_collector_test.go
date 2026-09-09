package service

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type openAILatencyTelemetryCollectorFakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *openAILatencyTelemetryCollectorFakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *openAILatencyTelemetryCollectorFakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type openAILatencyTelemetryCollectorFakeCache struct {
	mu sync.Mutex

	storeErr      error
	storeCalls    int
	stored        [][]OpenAILatencyTelemetrySnapshot
	storedBeats   []OpenAILatencyTelemetrySourceHeartbeat
	buckets       []OpenAILatencyTelemetryBucket
	loads         map[OpenAILatencyTelemetryBucket][]OpenAILatencyTelemetrySnapshot
	heartbeats    []OpenAILatencyTelemetrySourceHeartbeat
	deleteBuckets []OpenAILatencyTelemetryBucket

	listEntered  chan struct{}
	listRelease  chan struct{}
	listOnce     sync.Once
	storeEntered chan struct{}
	storeRelease chan struct{}
	storeOnce    sync.Once
}

func (c *openAILatencyTelemetryCollectorFakeCache) StoreOpenAILatencyTelemetrySnapshots(
	_ context.Context,
	snapshots []OpenAILatencyTelemetrySnapshot,
	heartbeat OpenAILatencyTelemetrySourceHeartbeat,
	_ time.Duration,
	_ time.Duration,
) error {
	if c.storeEntered != nil {
		c.storeOnce.Do(func() { close(c.storeEntered) })
		select {
		case <-c.storeRelease:
		case <-time.After(5 * time.Second):
			return errors.New("timed out waiting to release fake telemetry store")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storeCalls++
	copied := append([]OpenAILatencyTelemetrySnapshot(nil), snapshots...)
	c.stored = append(c.stored, copied)
	c.storedBeats = append(c.storedBeats, heartbeat)
	return c.storeErr
}

func (c *openAILatencyTelemetryCollectorFakeCache) ListOpenAILatencyTelemetryBuckets(
	ctx context.Context,
	before time.Time,
	limit int,
) ([]OpenAILatencyTelemetryBucket, error) {
	if c.listEntered != nil {
		c.listOnce.Do(func() { close(c.listEntered) })
		select {
		case <-c.listRelease:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]OpenAILatencyTelemetryBucket, 0, min(limit, len(c.buckets)))
	for _, bucket := range c.buckets {
		if bucket.EndTime().After(before) {
			continue
		}
		result = append(result, bucket)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (c *openAILatencyTelemetryCollectorFakeCache) LoadOpenAILatencyTelemetryBucket(
	_ context.Context,
	bucket OpenAILatencyTelemetryBucket,
	limit int,
) ([]OpenAILatencyTelemetrySnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	loaded := c.loads[bucket]
	if len(loaded) > limit {
		loaded = loaded[:limit]
	}
	return append([]OpenAILatencyTelemetrySnapshot(nil), loaded...), nil
}

func (c *openAILatencyTelemetryCollectorFakeCache) LoadOpenAILatencyTelemetryBucketBatch(
	_ context.Context,
	bucket OpenAILatencyTelemetryBucket,
	cursor uint64,
	limit int,
) ([]OpenAILatencyTelemetrySnapshot, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	loaded := c.loads[bucket]
	start := int(cursor)
	if start >= len(loaded) {
		return nil, 0, nil
	}
	end := min(start+limit, len(loaded))
	next := uint64(0)
	if end < len(loaded) {
		next = uint64(end)
	}
	return append([]OpenAILatencyTelemetrySnapshot(nil), loaded[start:end]...), next, nil
}

func (c *openAILatencyTelemetryCollectorFakeCache) ListOpenAILatencyTelemetrySourceHeartbeats(
	_ context.Context,
	since time.Time,
	limit int,
) ([]OpenAILatencyTelemetrySourceHeartbeat, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]OpenAILatencyTelemetrySourceHeartbeat, 0, min(limit, len(c.heartbeats)))
	for _, heartbeat := range c.heartbeats {
		if heartbeat.ObservedAt.Before(since) {
			continue
		}
		result = append(result, heartbeat)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (c *openAILatencyTelemetryCollectorFakeCache) DeleteOpenAILatencyTelemetryBucket(
	_ context.Context,
	bucket OpenAILatencyTelemetryBucket,
) error {
	c.mu.Lock()
	c.deleteBuckets = append(c.deleteBuckets, bucket)
	c.mu.Unlock()
	return nil
}

type openAILatencyTelemetryCollectorRetentionCall struct {
	bucketSeconds int
	before        time.Time
}

type openAILatencyTelemetryCollectorFakeRollups struct {
	mu sync.Mutex

	upsertCalls int
	rollups     []OpenAILatencyTelemetryRollup
	deletes     []openAILatencyTelemetryCollectorRetentionCall
}

func (r *openAILatencyTelemetryCollectorFakeRollups) UpsertOpenAILatencyTelemetryRollups(
	_ context.Context,
	rollups []OpenAILatencyTelemetryRollup,
) error {
	r.mu.Lock()
	r.upsertCalls++
	r.rollups = append(r.rollups, rollups...)
	r.mu.Unlock()
	return nil
}

func (r *openAILatencyTelemetryCollectorFakeRollups) DeleteOpenAILatencyTelemetryRollupsBefore(
	_ context.Context,
	bucketSeconds int,
	before time.Time,
) (int64, error) {
	r.mu.Lock()
	r.deletes = append(r.deletes, openAILatencyTelemetryCollectorRetentionCall{
		bucketSeconds: bucketSeconds,
		before:        before,
	})
	r.mu.Unlock()
	if bucketSeconds == OpenAILatencyTelemetry15MinuteRollupSeconds {
		return 2, nil
	}
	return 3, nil
}

type openAILatencyTelemetryCollectorFakeLeaderLock struct {
	mu    sync.Mutex
	owner string
}

func (l *openAILatencyTelemetryCollectorFakeLeaderLock) TryAcquireLeaderLock(
	_ context.Context,
	_ string,
	owner string,
	_ time.Duration,
) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.owner != "" {
		return false, nil
	}
	l.owner = owner
	return true, nil
}

func (l *openAILatencyTelemetryCollectorFakeLeaderLock) ReleaseLeaderLock(
	_ context.Context,
	_ string,
	owner string,
) error {
	l.mu.Lock()
	if l.owner == owner {
		l.owner = ""
	}
	l.mu.Unlock()
	return nil
}

func enabledOpenAILatencyTelemetryCollectorConfig(maxStreams int) config.OpenAILatencyShadowConfig {
	return config.OpenAILatencyShadowConfig{
		Enabled:                              true,
		TelemetryFlushIntervalSeconds:        5,
		TelemetryBucketSeconds:               60,
		TelemetryFinalizationIntervalSeconds: 60,
		TelemetryFinalizationGraceSeconds:    15,
		TelemetrySourceTTLSeconds:            30,
		TelemetryRedisRawRetentionSeconds:    2 * 60 * 60,
		MaxTelemetryStreams:                  maxStreams,
		MaxTelemetryContributions:            config.DefaultOpenAILatencyShadowMaxTelemetryContributions,
		Telemetry15MinuteRetentionDays:       30,
		TelemetryHourlyRetentionDays:         180,
	}
}

func newOpenAILatencyTelemetryCollectorForTest(
	t *testing.T,
	shadow config.OpenAILatencyShadowConfig,
	clock Clock,
	cache OpenAILatencyTelemetryCache,
	rollups OpenAILatencyTelemetryRollupRepository,
	leader LeaderLockCache,
	sourceID uint64,
) *OpenAILatencyTelemetryCollector {
	t.Helper()
	collector, err := NewOpenAILatencyTelemetryCollector(shadow, OpenAILatencyTelemetryCollectorDependencies{
		Cache:            cache,
		RollupRepository: rollups,
		LeaderLock:       leader,
		Clock:            clock,
		SourceID:         sourceID,
		Generation:       1,
	})
	require.NoError(t, err)
	return collector
}

func openAILatencyTelemetryCollectorTestKey(accountID int64) OpenAILatencyTelemetryKey {
	return OpenAILatencyTelemetryKey{
		AccountID:   accountID,
		Cohort:      UnknownCohortKey,
		AttemptRole: OpenAIAttemptRolePrimary,
	}
}

func openAILatencyTelemetryCollectorTestSnapshot(
	t *testing.T,
	key OpenAILatencyTelemetryKey,
	sourceID uint64,
	bucketStart time.Time,
) OpenAILatencyTelemetrySnapshot {
	t.Helper()
	aggregate, err := NewOpenAILatencyTelemetryAggregateWithIdentity(key, OpenAILatencyTelemetryIdentity{
		SourceID:       sourceID,
		Generation:     1,
		BucketStart:    bucketStart.UnixNano(),
		BucketDuration: int64(time.Minute),
	})
	require.NoError(t, err)
	require.NoError(t, aggregate.ObservePhase(OpenAILatencyPhaseTotal, 10*time.Second))
	return aggregate.Snapshot()
}

func TestOpenAILatencyTelemetryCollectorDisabledAllocatesNoRuntimeState(t *testing.T) {
	collector, err := NewOpenAILatencyTelemetryCollector(
		config.OpenAILatencyShadowConfig{Enabled: false},
		OpenAILatencyTelemetryCollectorDependencies{},
	)
	require.NoError(t, err)
	require.False(t, collector.Enabled())
	require.Nil(t, collector.registry)
	require.Nil(t, collector.stopCh)
	require.Nil(t, collector.doneCh)

	collector.Start()
	collector.Stop()
	require.Nil(t, collector.registry)
	require.Equal(t, OpenAILatencyTelemetryCollectorStats{Enabled: false}, collector.Stats())
}

func TestOpenAILatencyTelemetryCollectorBoundsStreamsAndDropsOverflow(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	clock := &openAILatencyTelemetryCollectorFakeClock{now: now}
	collector := newOpenAILatencyTelemetryCollectorForTest(
		t,
		enabledOpenAILatencyTelemetryCollectorConfig(1),
		clock,
		&openAILatencyTelemetryCollectorFakeCache{},
		&openAILatencyTelemetryCollectorFakeRollups{},
		nil,
		1,
	)

	require.NoError(t, collector.ObservePhase(openAILatencyTelemetryCollectorTestKey(11), OpenAILatencyPhaseTotal, time.Second))
	require.NoError(t, collector.ObservePhase(openAILatencyTelemetryCollectorTestKey(12), OpenAILatencyPhaseTotal, time.Second))
	stats := collector.Stats()
	require.Equal(t, 1, stats.Streams)
	require.Equal(t, uint64(1), stats.Observations)
	require.Equal(t, uint64(1), stats.DroppedObservations)
	require.Equal(t, uint64(1), stats.OverflowedStreams)
}

func TestOpenAILatencyTelemetryCollectorRotatesCurrentAndPreviousBuckets(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	clock := &openAILatencyTelemetryCollectorFakeClock{now: now}
	cache := &openAILatencyTelemetryCollectorFakeCache{}
	collector := newOpenAILatencyTelemetryCollectorForTest(
		t,
		enabledOpenAILatencyTelemetryCollectorConfig(4),
		clock,
		cache,
		&openAILatencyTelemetryCollectorFakeRollups{},
		nil,
		1,
	)
	key := openAILatencyTelemetryCollectorTestKey(21)

	require.NoError(t, collector.ObservePhase(key, OpenAILatencyPhaseTotal, time.Second))
	clock.Advance(time.Minute)
	require.NoError(t, collector.ObservePhase(key, OpenAILatencyPhaseTotal, 2*time.Second))
	require.Equal(t, 1, collector.Stats().PreviousBuckets)
	require.NoError(t, collector.Flush(context.Background()))
	require.Len(t, cache.stored, 1)
	require.Len(t, cache.stored[0], 2)
	require.Equal(t, now.UnixNano(), cache.stored[0][0].BucketStart)
	require.Equal(t, now.Add(time.Minute).UnixNano(), cache.stored[0][1].BucketStart)

	clock.Advance(time.Minute)
	require.NoError(t, collector.ObservePhase(key, OpenAILatencyPhaseTotal, 3*time.Second))
	require.Equal(t, uint64(1), collector.Stats().StaleBuckets)
	require.Equal(t, now.Add(2*time.Minute).UnixNano(), collector.CurrentOpenAILatencyTelemetrySnapshots()[0].BucketStart)
}

func TestOpenAILatencyTelemetryCollectorCacheFailureIsFailOpenAndFreshnessExpires(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	clock := &openAILatencyTelemetryCollectorFakeClock{now: now}
	cache := &openAILatencyTelemetryCollectorFakeCache{storeErr: errors.New("redis unavailable")}
	collector := newOpenAILatencyTelemetryCollectorForTest(
		t,
		enabledOpenAILatencyTelemetryCollectorConfig(4),
		clock,
		cache,
		&openAILatencyTelemetryCollectorFakeRollups{},
		nil,
		1,
	)
	key := openAILatencyTelemetryCollectorTestKey(31)
	require.NoError(t, collector.ObservePhase(key, OpenAILatencyPhaseTotal, time.Second))

	require.ErrorContains(t, collector.Flush(context.Background()), "redis unavailable")
	require.Equal(t, uint64(1), collector.CurrentOpenAILatencyTelemetrySnapshots()[0].Phases[OpenAILatencyPhaseTotal].Observations)
	require.True(t, collector.RecommendationSnapshot().Fresh)
	require.Equal(t, uint64(1), collector.Stats().FlushFailures)

	cache.mu.Lock()
	cache.storeErr = nil
	cache.mu.Unlock()
	clock.Advance(time.Second)
	require.NoError(t, collector.Flush(context.Background()))
	require.Equal(t, uint64(1), collector.Stats().FlushSuccesses)
	clock.Advance(31 * time.Second)
	require.False(t, collector.RecommendationSnapshot().Fresh)
}

func TestOpenAILatencyTelemetryCollectorOnlyOneConcurrentFinalizerLeads(t *testing.T) {
	now := time.Date(2026, 8, 10, 13, 0, 20, 0, time.UTC)
	clock := &openAILatencyTelemetryCollectorFakeClock{now: now}
	cache := &openAILatencyTelemetryCollectorFakeCache{
		loads:       make(map[OpenAILatencyTelemetryBucket][]OpenAILatencyTelemetrySnapshot),
		listEntered: make(chan struct{}),
		listRelease: make(chan struct{}),
	}
	rollups := &openAILatencyTelemetryCollectorFakeRollups{}
	leader := &openAILatencyTelemetryCollectorFakeLeaderLock{}
	shadow := enabledOpenAILatencyTelemetryCollectorConfig(4)
	first := newOpenAILatencyTelemetryCollectorForTest(t, shadow, clock, cache, rollups, leader, 1)
	second := newOpenAILatencyTelemetryCollectorForTest(t, shadow, clock, cache, rollups, leader, 2)

	firstResult := make(chan OpenAILatencyTelemetryFinalizationResult, 1)
	firstErr := make(chan error, 1)
	go func() {
		result, err := first.Finalize(context.Background())
		firstResult <- result
		firstErr <- err
	}()
	<-cache.listEntered

	secondResult, err := second.Finalize(context.Background())
	require.NoError(t, err)
	require.False(t, secondResult.Leader)
	close(cache.listRelease)
	require.NoError(t, <-firstErr)
	require.True(t, (<-firstResult).Leader)

	rollups.mu.Lock()
	require.Equal(t, 1, rollups.upsertCalls)
	rollups.mu.Unlock()
	require.Equal(t, uint64(1), second.Stats().FinalizationLeaderSkips)
}

func TestOpenAILatencyTelemetryCollectorFinalizes15MinuteAndHourlyRollupsAndRetention(t *testing.T) {
	hourStart := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	now := hourStart.Add(time.Hour + 20*time.Second)
	clock := &openAILatencyTelemetryCollectorFakeClock{now: now}
	cache := &openAILatencyTelemetryCollectorFakeCache{
		loads: make(map[OpenAILatencyTelemetryBucket][]OpenAILatencyTelemetrySnapshot),
		heartbeats: []OpenAILatencyTelemetrySourceHeartbeat{
			{SourceID: 1, Generation: 1, ObservedAt: now},
			{SourceID: 2, Generation: 1, ObservedAt: now},
		},
	}
	key := openAILatencyTelemetryCollectorTestKey(41)
	for minute := range 60 {
		start := hourStart.Add(time.Duration(minute) * time.Minute)
		bucket := OpenAILatencyTelemetryBucket{
			BucketStart:    start.UnixNano(),
			BucketDuration: int64(time.Minute),
		}
		cache.buckets = append(cache.buckets, bucket)
		cache.loads[bucket] = []OpenAILatencyTelemetrySnapshot{
			openAILatencyTelemetryCollectorTestSnapshot(t, key, 1, start),
			openAILatencyTelemetryCollectorTestSnapshot(t, key, 2, start),
		}
	}
	rollupRepo := &openAILatencyTelemetryCollectorFakeRollups{}
	collector := newOpenAILatencyTelemetryCollectorForTest(
		t,
		enabledOpenAILatencyTelemetryCollectorConfig(256),
		clock,
		cache,
		rollupRepo,
		&openAILatencyTelemetryCollectorFakeLeaderLock{},
		7,
	)

	result, err := collector.Finalize(context.Background())
	require.NoError(t, err)
	require.True(t, result.Leader)
	require.Equal(t, 60, result.BucketsLoaded)
	require.Equal(t, 120, result.SnapshotsLoaded)
	require.Equal(t, 5, result.RollupsPersisted)
	require.Equal(t, int64(5), result.RetentionRowsDeleted)

	rollupRepo.mu.Lock()
	rollups := append([]OpenAILatencyTelemetryRollup(nil), rollupRepo.rollups...)
	deletes := append([]openAILatencyTelemetryCollectorRetentionCall(nil), rollupRepo.deletes...)
	rollupRepo.mu.Unlock()
	require.Len(t, rollups, 5)
	sort.Slice(rollups, func(i, j int) bool {
		if rollups[i].BucketSeconds != rollups[j].BucketSeconds {
			return rollups[i].BucketSeconds < rollups[j].BucketSeconds
		}
		return rollups[i].BucketStart.Before(rollups[j].BucketStart)
	})
	for _, rollup := range rollups[:4] {
		require.Equal(t, OpenAILatencyTelemetry15MinuteRollupSeconds, rollup.BucketSeconds)
		require.Equal(t, 2, rollup.SourceCount)
		require.Equal(t, uint64(30), rollup.SampleCount)
		require.True(t, rollup.Complete)
		require.Equal(t, 15, rollup.CoveredBucketCount)
		require.Equal(t, 15, rollup.ExpectedBucketCount)
	}
	hourly := rollups[4]
	require.Equal(t, OpenAILatencyTelemetryHourlyRollupSeconds, hourly.BucketSeconds)
	require.Equal(t, 2, hourly.SourceCount)
	require.Equal(t, uint64(120), hourly.SampleCount)
	require.Equal(t, hourStart.Add(time.Hour), hourly.FreshThrough)
	require.True(t, hourly.Complete)
	require.Equal(t, 60, hourly.CoveredBucketCount)
	require.Equal(t, 60, hourly.ExpectedBucketCount)

	require.Equal(t, []openAILatencyTelemetryCollectorRetentionCall{
		{bucketSeconds: OpenAILatencyTelemetry15MinuteRollupSeconds, before: now.AddDate(0, 0, -30)},
		{bucketSeconds: OpenAILatencyTelemetryHourlyRollupSeconds, before: now.AddDate(0, 0, -180)},
	}, deletes)
}

func TestOpenAILatencyTelemetryCollectorMarksSparseAndPartialWindowsIncomplete(t *testing.T) {
	windowStart := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		minutes []int
	}{
		{name: "sparse", minutes: []int{0, 14}},
		{name: "partial", minutes: []int{0, 1, 2, 3, 4}},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			now := windowStart.Add(time.Hour + 20*time.Second)
			cache := &openAILatencyTelemetryCollectorFakeCache{
				loads: make(map[OpenAILatencyTelemetryBucket][]OpenAILatencyTelemetrySnapshot),
			}
			key := openAILatencyTelemetryCollectorTestKey(42)
			for _, minute := range testCase.minutes {
				start := windowStart.Add(time.Duration(minute) * time.Minute)
				bucket := OpenAILatencyTelemetryBucket{
					BucketStart:    start.UnixNano(),
					BucketDuration: int64(time.Minute),
				}
				cache.buckets = append(cache.buckets, bucket)
				cache.loads[bucket] = []OpenAILatencyTelemetrySnapshot{
					openAILatencyTelemetryCollectorTestSnapshot(t, key, 1, start),
				}
			}
			rollupRepo := &openAILatencyTelemetryCollectorFakeRollups{}
			collector := newOpenAILatencyTelemetryCollectorForTest(
				t,
				enabledOpenAILatencyTelemetryCollectorConfig(4),
				&openAILatencyTelemetryCollectorFakeClock{now: now},
				cache,
				rollupRepo,
				&openAILatencyTelemetryCollectorFakeLeaderLock{},
				1,
			)

			result, err := collector.Finalize(context.Background())
			require.NoError(t, err)
			require.Equal(t, 2, result.RollupsPersisted)

			rollupRepo.mu.Lock()
			rollups := append([]OpenAILatencyTelemetryRollup(nil), rollupRepo.rollups...)
			rollupRepo.mu.Unlock()
			require.Len(t, rollups, 2)
			for _, rollup := range rollups {
				require.False(t, rollup.Complete)
				require.Equal(t, len(testCase.minutes), rollup.CoveredBucketCount)
				switch rollup.BucketSeconds {
				case OpenAILatencyTelemetry15MinuteRollupSeconds:
					require.Equal(t, 15, rollup.ExpectedBucketCount)
				case OpenAILatencyTelemetryHourlyRollupSeconds:
					require.Equal(t, 60, rollup.ExpectedBucketCount)
				default:
					t.Fatalf("unexpected rollup duration: %d", rollup.BucketSeconds)
				}
			}
		})
	}
}

func TestOpenAILatencyTelemetryCollectorRecommendationDoesNotMutateConfigOrDependencies(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	clock := &openAILatencyTelemetryCollectorFakeClock{now: now}
	cache := &openAILatencyTelemetryCollectorFakeCache{}
	rollups := &openAILatencyTelemetryCollectorFakeRollups{}
	shadow := enabledOpenAILatencyTelemetryCollectorConfig(4)
	before := shadow
	collector := newOpenAILatencyTelemetryCollectorForTest(t, shadow, clock, cache, rollups, nil, 1)
	require.NoError(t, collector.ObservePhase(openAILatencyTelemetryCollectorTestKey(51), OpenAILatencyPhaseTotal, time.Second))

	recommendation := collector.RecommendationSnapshot()
	require.True(t, recommendation.Fresh)
	require.Equal(t, uint64(1), recommendation.SampleCount)
	require.True(t, reflect.DeepEqual(before, shadow))
	cache.mu.Lock()
	require.Zero(t, cache.storeCalls)
	cache.mu.Unlock()
	rollups.mu.Lock()
	require.Zero(t, rollups.upsertCalls)
	require.Empty(t, rollups.deletes)
	rollups.mu.Unlock()
}

func TestOpenAILatencyTelemetryCollectorStartUsesIndependentFinalizationCadence(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	clock := &openAILatencyTelemetryCollectorFakeClock{now: now}
	storeRelease := make(chan struct{})
	listRelease := make(chan struct{})
	close(listRelease)
	cache := &openAILatencyTelemetryCollectorFakeCache{
		loads:        make(map[OpenAILatencyTelemetryBucket][]OpenAILatencyTelemetrySnapshot),
		storeEntered: make(chan struct{}),
		storeRelease: storeRelease,
		listEntered:  make(chan struct{}),
		listRelease:  listRelease,
	}
	shadow := enabledOpenAILatencyTelemetryCollectorConfig(4)
	shadow.TelemetryFlushIntervalSeconds = 1
	shadow.TelemetryBucketSeconds = 2
	shadow.TelemetryFinalizationIntervalSeconds = 2
	collector := newOpenAILatencyTelemetryCollectorForTest(
		t,
		shadow,
		clock,
		cache,
		&openAILatencyTelemetryCollectorFakeRollups{},
		&openAILatencyTelemetryCollectorFakeLeaderLock{},
		1,
	)
	collector.Start()

	select {
	case <-cache.storeEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("periodic flush did not start")
	}
	select {
	case <-cache.listEntered:
		t.Fatal("finalization used the shorter flush cadence")
	case <-time.After(500 * time.Millisecond):
	}
	select {
	case <-cache.listEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("finalization did not run on its configured cadence while flush was blocked")
	}
	close(storeRelease)
	collector.Stop()
}

func TestOpenAILatencyTelemetryCollectorStopBeforeStartIsSafe(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	collector := newOpenAILatencyTelemetryCollectorForTest(
		t,
		enabledOpenAILatencyTelemetryCollectorConfig(4),
		&openAILatencyTelemetryCollectorFakeClock{now: now},
		&openAILatencyTelemetryCollectorFakeCache{},
		&openAILatencyTelemetryCollectorFakeRollups{},
		nil,
		1,
	)
	done := make(chan struct{})
	go func() {
		collector.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop before Start blocked")
	}
	collector.Start()
	collector.Stop()
}

func TestOpenAILatencyTelemetryCollectorPrunesExpiredTracksOnlyAfterSuccessfulFlush(t *testing.T) {
	start := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	clock := &openAILatencyTelemetryCollectorFakeClock{now: start}
	cache := &openAILatencyTelemetryCollectorFakeCache{storeErr: errors.New("redis unavailable")}
	shadow := enabledOpenAILatencyTelemetryCollectorConfig(4)
	collector := newOpenAILatencyTelemetryCollectorForTest(
		t,
		shadow,
		clock,
		cache,
		&openAILatencyTelemetryCollectorFakeRollups{},
		nil,
		1,
	)
	require.NoError(t, collector.ObservePhase(openAILatencyTelemetryCollectorTestKey(61), OpenAILatencyPhaseTotal, time.Second))
	clock.Advance(time.Duration(shadow.TelemetryRedisRawRetentionSeconds)*time.Second + 2*time.Minute)

	require.Error(t, collector.Flush(context.Background()))
	require.Equal(t, 1, collector.Stats().Streams, "a failed flush must retain local tracks")
	cache.mu.Lock()
	cache.storeErr = nil
	cache.mu.Unlock()
	require.NoError(t, collector.Flush(context.Background()))
	require.Zero(t, collector.Stats().Streams)
	cache.mu.Lock()
	require.Empty(t, cache.stored[len(cache.stored)-1], "expired snapshots must not be rewritten")
	cache.mu.Unlock()
}

func TestOpenAILatencyTelemetryCollectorUsesGlobalContributionLimitAndMarksPartialRollups(t *testing.T) {
	hourStart := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	now := hourStart.Add(time.Hour + 20*time.Second)
	bucket := OpenAILatencyTelemetryBucket{
		BucketStart:    hourStart.UnixNano(),
		BucketDuration: int64(time.Minute),
	}
	key := openAILatencyTelemetryCollectorTestKey(62)
	cache := &openAILatencyTelemetryCollectorFakeCache{
		buckets: []OpenAILatencyTelemetryBucket{bucket},
		loads: map[OpenAILatencyTelemetryBucket][]OpenAILatencyTelemetrySnapshot{
			bucket: {
				openAILatencyTelemetryCollectorTestSnapshot(t, key, 1, hourStart),
				openAILatencyTelemetryCollectorTestSnapshot(t, key, 2, hourStart),
				openAILatencyTelemetryCollectorTestSnapshot(t, key, 3, hourStart),
			},
		},
		heartbeats: []OpenAILatencyTelemetrySourceHeartbeat{
			{SourceID: 1, Generation: 1, ObservedAt: now},
			{SourceID: 2, Generation: 1, ObservedAt: now},
		},
	}
	rollups := &openAILatencyTelemetryCollectorFakeRollups{}
	shadow := enabledOpenAILatencyTelemetryCollectorConfig(1)
	shadow.MaxTelemetryContributions = 2
	collector := newOpenAILatencyTelemetryCollectorForTest(
		t,
		shadow,
		&openAILatencyTelemetryCollectorFakeClock{now: now},
		cache,
		rollups,
		&openAILatencyTelemetryCollectorFakeLeaderLock{},
		1,
	)

	result, err := collector.Finalize(context.Background())
	require.NoError(t, err)
	require.True(t, result.ContributionLimitReached)
	require.Equal(t, 2, result.ContributionsLoaded)
	require.Equal(t, 2, result.SnapshotsLoaded)
	rollups.mu.Lock()
	defer rollups.mu.Unlock()
	require.Len(t, rollups.rollups, 2)
	for _, rollup := range rollups.rollups {
		require.False(t, rollup.Complete)
		require.Equal(t, 1, rollup.CoveredBucketCount)
		require.Equal(t, hourStart, rollup.CoverageStart)
	}
}

func TestOpenAILatencyTelemetryCollectorMintsFreshGenerationForSameBucketRestart(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 30, 0, time.UTC)
	shadow := enabledOpenAILatencyTelemetryCollectorConfig(4)
	clock := &openAILatencyTelemetryCollectorFakeClock{now: now}
	newCollector := func() *OpenAILatencyTelemetryCollector {
		collector, err := NewOpenAILatencyTelemetryCollector(shadow, OpenAILatencyTelemetryCollectorDependencies{
			Clock:      clock,
			SourceID:   77,
			Generation: 0,
		})
		require.NoError(t, err)
		return collector
	}
	first := newCollector()
	second := newCollector()
	key := openAILatencyTelemetryCollectorTestKey(63)
	require.NoError(t, first.ObservePhase(key, OpenAILatencyPhaseTotal, time.Second))
	require.NoError(t, second.ObservePhase(key, OpenAILatencyPhaseTotal, time.Second))
	firstSnapshot := first.CurrentOpenAILatencyTelemetrySnapshots()[0]
	secondSnapshot := second.CurrentOpenAILatencyTelemetrySnapshots()[0]
	require.NotZero(t, firstSnapshot.Generation)
	require.NotZero(t, secondSnapshot.Generation)
	require.NotEqual(t, firstSnapshot.Generation, secondSnapshot.Generation)
	require.Equal(t, firstSnapshot.BucketStart, secondSnapshot.BucketStart)
}

func TestOpenAILatencyTelemetryCollectorUsesDatabaseFenceAfterRedisLease(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	lockID := hashAdvisoryLockID(openAILatencyTelemetryFinalizerLeaderKey)
	mock.ExpectQuery(`SELECT pg_try_advisory_lock`).
		WithArgs(lockID).
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	mock.ExpectExec(`SELECT pg_advisory_unlock`).
		WithArgs(lockID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	collector := newOpenAILatencyTelemetryCollectorForTest(
		t,
		enabledOpenAILatencyTelemetryCollectorConfig(4),
		&openAILatencyTelemetryCollectorFakeClock{now: now},
		&openAILatencyTelemetryCollectorFakeCache{loads: make(map[OpenAILatencyTelemetryBucket][]OpenAILatencyTelemetrySnapshot)},
		&openAILatencyTelemetryCollectorFakeRollups{},
		&openAILatencyTelemetryCollectorFakeLeaderLock{},
		1,
	)
	collector.db = db
	result, err := collector.Finalize(context.Background())
	require.NoError(t, err)
	require.True(t, result.Leader)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOpenAILatencyTelemetryCollectorFinalizationFailsClosedWithoutFence(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 20, 0, time.UTC)
	collector := newOpenAILatencyTelemetryCollectorForTest(
		t,
		enabledOpenAILatencyTelemetryCollectorConfig(4),
		&openAILatencyTelemetryCollectorFakeClock{now: now},
		&openAILatencyTelemetryCollectorFakeCache{loads: make(map[OpenAILatencyTelemetryBucket][]OpenAILatencyTelemetrySnapshot)},
		&openAILatencyTelemetryCollectorFakeRollups{},
		nil,
		1,
	)

	result, err := collector.Finalize(context.Background())
	require.ErrorContains(t, err, "requires a Redis or database coordination fence")
	require.False(t, result.Leader)
}
