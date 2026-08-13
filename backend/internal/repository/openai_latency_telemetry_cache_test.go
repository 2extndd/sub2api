//go:build unit

package repository

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newOpenAILatencyTelemetryRedisTestCache(
	t *testing.T,
) (*openAILatencyTelemetryCache, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(time.Date(2026, 8, 10, 9, 0, 0, 0, time.UTC))
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return &openAILatencyTelemetryCache{rdb: client}, client, server
}

type failOpenAILatencyTelemetryPipelineHook struct{}

func (failOpenAILatencyTelemetryPipelineHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (failOpenAILatencyTelemetryPipelineHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (failOpenAILatencyTelemetryPipelineHook) ProcessPipelineHook(redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(context.Context, []redis.Cmder) error {
		return errors.New("injected snapshot pipeline failure")
	}
}

func openAILatencyTelemetryRedisTestKey(accountID int64) service.OpenAILatencyTelemetryKey {
	return service.OpenAILatencyTelemetryKey{
		AccountID:   accountID,
		Cohort:      service.UnknownCohortKey,
		AttemptRole: service.OpenAIAttemptRolePrimary,
	}
}

func openAILatencyTelemetryRedisTestSnapshot(
	t *testing.T,
	key service.OpenAILatencyTelemetryKey,
	sourceID uint64,
	generation uint64,
	bucketStart time.Time,
	observations int,
	latency time.Duration,
) service.OpenAILatencyTelemetrySnapshot {
	t.Helper()
	aggregate, err := service.NewOpenAILatencyTelemetryAggregateWithIdentity(key, service.OpenAILatencyTelemetryIdentity{
		SourceID:       sourceID,
		Generation:     generation,
		BucketStart:    bucketStart.UnixNano(),
		BucketDuration: int64(time.Minute),
	})
	require.NoError(t, err)
	for range observations {
		require.NoError(t, aggregate.ObservePhase(service.OpenAILatencyPhaseTotal, latency))
	}
	return aggregate.Snapshot()
}

func storeOpenAILatencyTelemetryRedisTestSnapshot(
	t *testing.T,
	cache *openAILatencyTelemetryCache,
	snapshot service.OpenAILatencyTelemetrySnapshot,
	observedAt time.Time,
	rawRetention time.Duration,
	sourceTTL time.Duration,
) {
	t.Helper()
	require.NoError(t, cache.StoreOpenAILatencyTelemetrySnapshots(
		context.Background(),
		[]service.OpenAILatencyTelemetrySnapshot{snapshot},
		service.OpenAILatencyTelemetrySourceHeartbeat{
			SourceID:   snapshot.SourceID,
			Generation: snapshot.Generation,
			ObservedAt: observedAt,
		},
		rawRetention,
		sourceTTL,
	))
}

func TestOpenAILatencyTelemetryCachePublishesHeartbeatOnlyAfterSnapshotStore(t *testing.T) {
	cache, client, server := newOpenAILatencyTelemetryRedisTestCache(t)
	client.AddHook(failOpenAILatencyTelemetryPipelineHook{})
	bucketStart := time.Date(2026, 8, 10, 9, 0, 0, 0, time.UTC)
	snapshot := openAILatencyTelemetryRedisTestSnapshot(
		t,
		openAILatencyTelemetryRedisTestKey(40),
		1,
		1,
		bucketStart,
		1,
		time.Second,
	)
	err := cache.StoreOpenAILatencyTelemetrySnapshots(
		context.Background(),
		[]service.OpenAILatencyTelemetrySnapshot{snapshot},
		service.OpenAILatencyTelemetrySourceHeartbeat{
			SourceID:   snapshot.SourceID,
			Generation: snapshot.Generation,
			ObservedAt: bucketStart.Add(time.Minute),
		},
		2*time.Hour,
		time.Minute,
	)
	require.ErrorContains(t, err, "snapshot pipeline failure")
	require.False(t, server.Exists(openAILatencyTelemetrySourceHeartbeatKey))
}

func TestOpenAILatencyTelemetryCacheSequenceIsIdempotentAndMergesTwoSources(t *testing.T) {
	cache, _, server := newOpenAILatencyTelemetryRedisTestCache(t)
	ctx := context.Background()
	bucketStart := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	observedAt := bucketStart.Add(time.Minute)
	key := openAILatencyTelemetryRedisTestKey(41)
	first := openAILatencyTelemetryRedisTestSnapshot(t, key, 11, 3, bucketStart, 1, time.Second)
	storeOpenAILatencyTelemetryRedisTestSnapshot(t, cache, first, observedAt, time.Hour, time.Minute)

	identity := openAILatencyTelemetrySnapshotIdentity(first)
	absoluteJSON, err := server.Get(identity.snapshotKey())
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(absoluteJSON, "{"))
	var stored service.OpenAILatencyTelemetrySnapshot
	require.NoError(t, json.Unmarshal([]byte(absoluteJSON), &stored))
	require.Equal(t, first, stored)

	// Equal-sequence retry with different absolute data must not double count or
	// replace the accepted value.
	equalSequenceRetry := openAILatencyTelemetryRedisTestSnapshot(t, key, 11, 3, bucketStart, 1, 45*time.Second)
	require.Equal(t, first.Sequence, equalSequenceRetry.Sequence)
	storeOpenAILatencyTelemetryRedisTestSnapshot(t, cache, equalSequenceRetry, observedAt.Add(time.Second), time.Hour, time.Minute)
	loaded, err := cache.LoadOpenAILatencyTelemetryBucket(ctx, service.OpenAILatencyTelemetryBucket{
		BucketStart:    bucketStart.UnixNano(),
		BucketDuration: int64(time.Minute),
	}, 4)
	require.NoError(t, err)
	require.Equal(t, first, loaded[0])

	// A newer cumulative sequence replaces the absolute payload.
	newer := openAILatencyTelemetryRedisTestSnapshot(t, key, 11, 3, bucketStart, 2, 2*time.Second)
	storeOpenAILatencyTelemetryRedisTestSnapshot(t, cache, newer, observedAt.Add(2*time.Second), time.Hour, time.Minute)
	secondSource := openAILatencyTelemetryRedisTestSnapshot(t, key, 12, 1, bucketStart, 1, 3*time.Second)
	storeOpenAILatencyTelemetryRedisTestSnapshot(t, cache, secondSource, observedAt.Add(2*time.Second), time.Hour, time.Minute)

	loaded, err = cache.LoadOpenAILatencyTelemetryBucket(ctx, service.OpenAILatencyTelemetryBucket{
		BucketStart:    bucketStart.UnixNano(),
		BucketDuration: int64(time.Minute),
	}, 4)
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	require.Equal(t, uint64(2), loaded[0].Sequence)
	merger, err := service.NewOpenAILatencyTelemetryMerger(4)
	require.NoError(t, err)
	for _, snapshot := range loaded {
		accepted, mergeErr := merger.MergeCumulative(snapshot)
		require.NoError(t, mergeErr)
		require.True(t, accepted)
	}
	require.Equal(t, uint64(3), merger.Snapshot().Phases[service.OpenAILatencyPhaseTotal].Observations)
	require.Equal(t, 2, merger.Stats().Streams)

	buckets, err := cache.ListOpenAILatencyTelemetryBuckets(ctx, observedAt.Add(time.Minute), 4)
	require.NoError(t, err)
	require.Equal(t, []service.OpenAILatencyTelemetryBucket{{
		BucketStart:    bucketStart.UnixNano(),
		BucketDuration: int64(time.Minute),
	}}, buckets)
	heartbeats, err := cache.ListOpenAILatencyTelemetrySourceHeartbeats(ctx, observedAt, 4)
	require.NoError(t, err)
	require.Len(t, heartbeats, 2)
}

func TestOpenAILatencyTelemetryCacheComparesFullUint64Sequences(t *testing.T) {
	cache, _, _ := newOpenAILatencyTelemetryRedisTestCache(t)
	bucketStart := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	key := openAILatencyTelemetryRedisTestKey(42)
	snapshot := openAILatencyTelemetryRedisTestSnapshot(t, key, 11, 3, bucketStart, 1, time.Second)
	snapshot.Sequence = math.MaxUint64
	storeOpenAILatencyTelemetryRedisTestSnapshot(t, cache, snapshot, bucketStart.Add(time.Minute), time.Hour, time.Minute)

	older := snapshot
	older.Sequence = math.MaxUint64 - 1
	older.Phases[service.OpenAILatencyPhaseTotal].Observations = 99
	storeOpenAILatencyTelemetryRedisTestSnapshot(t, cache, older, bucketStart.Add(time.Minute+time.Second), time.Hour, time.Minute)
	loaded, err := cache.LoadOpenAILatencyTelemetryBucket(context.Background(), service.OpenAILatencyTelemetryBucket{
		BucketStart:    bucketStart.UnixNano(),
		BucketDuration: int64(time.Minute),
	}, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), loaded[0].Sequence)
	require.Equal(t, uint64(1), loaded[0].Phases[service.OpenAILatencyPhaseTotal].Observations)
}

func TestOpenAILatencyTelemetryCacheUsesNumericSecretSafeKeysAndBoundedLoads(t *testing.T) {
	cache, client, server := newOpenAILatencyTelemetryRedisTestCache(t)
	bucketStart := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	observedAt := bucketStart.Add(time.Minute)
	for sourceID := uint64(1); sourceID <= 2; sourceID++ {
		snapshot := openAILatencyTelemetryRedisTestSnapshot(
			t,
			openAILatencyTelemetryRedisTestKey(99),
			sourceID,
			1,
			bucketStart,
			1,
			time.Second,
		)
		storeOpenAILatencyTelemetryRedisTestSnapshot(t, cache, snapshot, observedAt, time.Hour, time.Minute)
	}

	numericSnapshotKey := regexp.MustCompile(`^openai:latency:telemetry:(snapshot|sequence):-?[0-9]+:[0-9]+:[0-9]+:[0-9]+:[0-9]+:[0-9]+:[0-9]+$`)
	numericBucketKey := regexp.MustCompile(`^openai:latency:telemetry:bucket:-?[0-9]+:[0-9]+$`)
	for _, key := range server.Keys() {
		require.NotContains(t, key, "sk-")
		switch {
		case strings.HasPrefix(key, openAILatencyTelemetrySnapshotPrefix),
			strings.HasPrefix(key, openAILatencyTelemetrySequencePrefix):
			require.Regexp(t, numericSnapshotKey, key)
		case strings.HasPrefix(key, openAILatencyTelemetryBucketPrefix):
			require.Regexp(t, numericBucketKey, key)
		}
	}

	bucket := service.OpenAILatencyTelemetryBucket{
		BucketStart:    bucketStart.UnixNano(),
		BucketDuration: int64(time.Minute),
	}
	var cursor uint64
	loadedCount := 0
	for {
		batch, next, err := cache.LoadOpenAILatencyTelemetryBucketBatch(
			context.Background(),
			bucket,
			cursor,
			1,
		)
		require.NoError(t, err)
		require.LessOrEqual(t, len(batch), 1)
		loadedCount += len(batch)
		if next == 0 {
			break
		}
		cursor = next
	}
	require.Equal(t, 2, loadedCount)

	_, err := cache.LoadOpenAILatencyTelemetryBucket(context.Background(), bucket, 1)
	require.ErrorIs(t, err, errOpenAILatencyTelemetryResultLimitExceeded)
	_, err = cache.ListOpenAILatencyTelemetrySourceHeartbeats(context.Background(), observedAt, 1)
	require.ErrorIs(t, err, errOpenAILatencyTelemetryResultLimitExceeded)

	require.NoError(t, cache.DeleteOpenAILatencyTelemetryBucket(context.Background(), bucket))
	members, err := client.ZRange(context.Background(), openAILatencyTelemetryBucketKey(bucket), 0, -1).Result()
	require.NoError(t, err)
	require.Empty(t, members)
	buckets, err := cache.ListOpenAILatencyTelemetryBuckets(context.Background(), observedAt.Add(time.Minute), 2)
	require.NoError(t, err)
	require.Empty(t, buckets)
}

func TestOpenAILatencyTelemetryCacheUsesStableZSetOffsetPaginationAndDeletesIndex(t *testing.T) {
	cache, client, _ := newOpenAILatencyTelemetryRedisTestCache(t)
	ctx := context.Background()
	bucketStart := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	observedAt := bucketStart.Add(time.Minute)
	bucket := service.OpenAILatencyTelemetryBucket{
		BucketStart:    bucketStart.UnixNano(),
		BucketDuration: int64(time.Minute),
	}
	indexKey := openAILatencyTelemetryBucketKey(bucket)

	snapshots := make([]service.OpenAILatencyTelemetrySnapshot, 0, 5)
	expectedKeys := make([]string, 0, 5)
	for sourceID := uint64(1); sourceID <= 5; sourceID++ {
		snapshot := openAILatencyTelemetryRedisTestSnapshot(
			t,
			openAILatencyTelemetryRedisTestKey(100),
			sourceID,
			1,
			bucketStart,
			1,
			time.Second,
		)
		snapshots = append(snapshots, snapshot)
		expectedKeys = append(expectedKeys, openAILatencyTelemetrySnapshotIdentity(snapshot).snapshotKey())
		storeOpenAILatencyTelemetryRedisTestSnapshot(t, cache, snapshot, observedAt, time.Hour, time.Minute)
	}

	// Retrying the same identity does not add a second bucket-index member.
	storeOpenAILatencyTelemetryRedisTestSnapshot(t, cache, snapshots[2], observedAt, time.Hour, time.Minute)

	indexType, err := client.Type(ctx, indexKey).Result()
	require.NoError(t, err)
	require.Equal(t, "zset", indexType)
	require.Equal(t, int64(len(expectedKeys)), client.ZCard(ctx, indexKey).Val())
	indexedKeys, err := client.ZRange(ctx, indexKey, 0, -1).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, expectedKeys, indexedKeys)
	for _, key := range expectedKeys {
		score, scoreErr := client.ZScore(ctx, indexKey, key).Result()
		require.NoError(t, scoreErr)
		require.Zero(t, score)
	}

	snapshotKeys := func(batch []service.OpenAILatencyTelemetrySnapshot) []string {
		keys := make([]string, 0, len(batch))
		for _, snapshot := range batch {
			keys = append(keys, openAILatencyTelemetrySnapshotIdentity(snapshot).snapshotKey())
		}
		return keys
	}

	var (
		cursor     uint64
		loadedKeys []string
		pageSizes  []int
		nexts      []uint64
	)
	for {
		batch, next, loadErr := cache.LoadOpenAILatencyTelemetryBucketBatch(ctx, bucket, cursor, 2)
		require.NoError(t, loadErr)
		require.LessOrEqual(t, len(batch), 2)
		pageSizes = append(pageSizes, len(batch))
		nexts = append(nexts, next)
		loadedKeys = append(loadedKeys, snapshotKeys(batch)...)
		if next == 0 {
			break
		}
		require.Greater(t, next, cursor)
		cursor = next
	}
	require.Equal(t, []int{2, 2, 1}, pageSizes)
	require.Equal(t, []uint64{2, 4, 0}, nexts)
	require.Len(t, loadedKeys, len(expectedKeys))
	require.ElementsMatch(t, expectedKeys, loadedKeys)
	uniqueLoadedKeys := make(map[string]struct{}, len(loadedKeys))
	for _, key := range loadedKeys {
		uniqueLoadedKeys[key] = struct{}{}
	}
	require.Len(t, uniqueLoadedKeys, len(expectedKeys))

	firstRetryBatch, firstRetryNext, err := cache.LoadOpenAILatencyTelemetryBucketBatch(ctx, bucket, 2, 2)
	require.NoError(t, err)
	secondRetryBatch, secondRetryNext, err := cache.LoadOpenAILatencyTelemetryBucketBatch(ctx, bucket, 2, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(4), firstRetryNext)
	require.Equal(t, firstRetryNext, secondRetryNext)
	require.Equal(t, snapshotKeys(firstRetryBatch), snapshotKeys(secondRetryBatch))

	exactBatch, exactNext, err := cache.LoadOpenAILatencyTelemetryBucketBatch(ctx, bucket, 3, 2)
	require.NoError(t, err)
	require.Len(t, exactBatch, 2)
	require.Zero(t, exactNext)

	loaded, err := cache.LoadOpenAILatencyTelemetryBucket(ctx, bucket, len(expectedKeys))
	require.NoError(t, err)
	require.Len(t, loaded, len(expectedKeys))
	_, err = cache.LoadOpenAILatencyTelemetryBucket(ctx, bucket, len(expectedKeys)-1)
	require.ErrorIs(t, err, errOpenAILatencyTelemetryResultLimitExceeded)

	require.NoError(t, cache.DeleteOpenAILatencyTelemetryBucket(ctx, bucket))
	require.Zero(t, client.Exists(ctx, indexKey).Val())
	for _, snapshot := range snapshots {
		identity := openAILatencyTelemetrySnapshotIdentity(snapshot)
		require.Zero(t, client.Exists(ctx, identity.snapshotKey(), identity.sequenceKey()).Val())
	}
	buckets, err := cache.ListOpenAILatencyTelemetryBuckets(ctx, observedAt.Add(time.Minute), 1)
	require.NoError(t, err)
	require.Empty(t, buckets)
}

func TestOpenAILatencyTelemetryCachePrunesStaleBucketAndSourceIndexes(t *testing.T) {
	cache, client, _ := newOpenAILatencyTelemetryRedisTestCache(t)
	ctx := context.Background()
	firstStart := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	first := openAILatencyTelemetryRedisTestSnapshot(t, openAILatencyTelemetryRedisTestKey(51), 1, 1, firstStart, 1, time.Second)
	storeOpenAILatencyTelemetryRedisTestSnapshot(t, cache, first, firstStart.Add(time.Minute), time.Hour, 30*time.Second)

	secondStart := firstStart.Add(2 * time.Hour)
	second := openAILatencyTelemetryRedisTestSnapshot(t, openAILatencyTelemetryRedisTestKey(52), 2, 1, secondStart, 1, time.Second)
	storeOpenAILatencyTelemetryRedisTestSnapshot(t, cache, second, secondStart.Add(time.Minute), time.Hour, 30*time.Second)

	buckets, err := cache.ListOpenAILatencyTelemetryBuckets(ctx, secondStart.Add(2*time.Minute), 4)
	require.NoError(t, err)
	require.Equal(t, []service.OpenAILatencyTelemetryBucket{{
		BucketStart:    secondStart.UnixNano(),
		BucketDuration: int64(time.Minute),
	}}, buckets)
	heartbeats, err := cache.ListOpenAILatencyTelemetrySourceHeartbeats(ctx, firstStart, 4)
	require.NoError(t, err)
	require.Equal(t, []service.OpenAILatencyTelemetrySourceHeartbeat{{
		SourceID:   2,
		Generation: 1,
		ObservedAt: secondStart.Add(time.Minute),
	}}, heartbeats)
	require.Greater(t, client.TTL(ctx, openAILatencyTelemetrySourceHeartbeatKey).Val(), time.Duration(0))
}

func TestRedisLatencyHealthTransitionGuardSerializesConcurrentReservations(t *testing.T) {
	_, client, _ := newOpenAILatencyTelemetryRedisTestCache(t)
	guard := NewRedisLatencyHealthTransitionGuard(client)
	ctx := context.Background()

	var accepted atomic.Int64
	var wg sync.WaitGroup
	errorsCh := make(chan error, 20)
	for accountID := int64(1); accountID <= 20; accountID++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reserved, err := guard.Reserve(ctx, service.LatencyHealthTransitionReservation{
				AccountID:      accountID,
				Transition:     service.LatencyHealthTransitionSoftQuarantine,
				PoolSnapshot:   service.PoolSnapshot{},
				QuarantineCap:  3,
				HardValidFloor: 0,
				OwnerToken:     uint64(accountID),
				TTL:            time.Minute,
			})
			if err != nil {
				errorsCh <- err
				return
			}
			if reserved {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}
	require.Equal(t, int64(3), accepted.Load())

	members, err := client.HKeys(ctx, openAILatencyHealthSoftReservationKey).Result()
	require.NoError(t, err)
	require.Len(t, members, 3)
	accountID, err := strconvParseFirstInt64(members)
	require.NoError(t, err)
	reserved, err := guard.Reserve(ctx, service.LatencyHealthTransitionReservation{
		AccountID:      accountID,
		Transition:     service.LatencyHealthTransitionSoftQuarantine,
		PoolSnapshot:   service.PoolSnapshot{QuarantinedAccounts: 100},
		QuarantineCap:  0,
		HardValidFloor: 0,
		OwnerToken:     uint64(accountID),
		TTL:            time.Minute,
	})
	require.NoError(t, err)
	require.True(t, reserved, "an existing account reservation must be renewable by its owner")
	renewed, err := guard.Renew(
		ctx,
		accountID,
		service.LatencyHealthTransitionSoftQuarantine,
		uint64(accountID),
		time.Minute,
	)
	require.NoError(t, err)
	require.True(t, renewed)
	require.NoError(t, guard.Release(ctx, accountID, service.LatencyHealthTransitionSoftQuarantine, uint64(accountID)+1))
	require.True(t, client.HExists(ctx, openAILatencyHealthSoftReservationKey, strconv.FormatInt(accountID, 10)).Val())
	require.NoError(t, guard.Release(ctx, accountID, service.LatencyHealthTransitionSoftQuarantine, uint64(accountID)))
	require.NoError(t, guard.Release(ctx, accountID, service.LatencyHealthTransitionSoftQuarantine, uint64(accountID)))

	for accountID := int64(101); accountID <= 103; accountID++ {
		reserved, err = guard.Reserve(ctx, service.LatencyHealthTransitionReservation{
			AccountID:      accountID,
			Transition:     service.LatencyHealthTransitionHardInvalid,
			PoolSnapshot:   service.PoolSnapshot{HardValidAccounts: 5},
			QuarantineCap:  0,
			HardValidFloor: 3,
			OwnerToken:     uint64(accountID),
			TTL:            time.Minute,
		})
		require.NoError(t, err)
		require.Equal(t, accountID <= 102, reserved)
	}
}

func TestRedisLatencyHealthTransitionGuardExpiresAndFencesOwners(t *testing.T) {
	_, client, _ := newOpenAILatencyTelemetryRedisTestCache(t)
	guard := NewRedisLatencyHealthTransitionGuard(client)
	ctx := context.Background()
	reservation := service.LatencyHealthTransitionReservation{
		AccountID:      81,
		Transition:     service.LatencyHealthTransitionSoftQuarantine,
		PoolSnapshot:   service.PoolSnapshot{},
		QuarantineCap:  1,
		HardValidFloor: 0,
		OwnerToken:     11,
		TTL:            20 * time.Millisecond,
	}
	reserved, err := guard.Reserve(ctx, reservation)
	require.NoError(t, err)
	require.True(t, reserved)

	other := reservation
	other.OwnerToken = 22
	reserved, err = guard.Reserve(ctx, other)
	require.NoError(t, err)
	require.False(t, reserved)

	time.Sleep(30 * time.Millisecond)
	reserved, err = guard.Reserve(ctx, other)
	require.NoError(t, err)
	require.True(t, reserved, "an expired reservation must be available to a new owner")
	renewed, err := guard.Renew(
		ctx,
		reservation.AccountID,
		reservation.Transition,
		reservation.OwnerToken,
		time.Minute,
	)
	require.NoError(t, err)
	require.False(t, renewed)
	require.NoError(t, guard.Release(ctx, reservation.AccountID, reservation.Transition, reservation.OwnerToken))
	owner, err := client.HGet(
		ctx,
		openAILatencyHealthSoftReservationKey,
		strconv.FormatInt(reservation.AccountID, 10),
	).Result()
	require.NoError(t, err)
	require.Equal(t, "22", owner)
}

func strconvParseFirstInt64(values []string) (int64, error) {
	if len(values) == 0 {
		return 0, nil
	}
	return strconv.ParseInt(values[0], 10, 64)
}
