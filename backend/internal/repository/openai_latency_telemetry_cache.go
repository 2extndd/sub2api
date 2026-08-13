package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const (
	openAILatencyTelemetryRedisPrefix           = "openai:latency:telemetry:"
	openAILatencyTelemetrySnapshotPrefix        = openAILatencyTelemetryRedisPrefix + "snapshot:"
	openAILatencyTelemetrySequencePrefix        = openAILatencyTelemetryRedisPrefix + "sequence:"
	openAILatencyTelemetryBucketPrefix          = openAILatencyTelemetryRedisPrefix + "bucket:"
	openAILatencyTelemetryBucketIndexKey        = openAILatencyTelemetryRedisPrefix + "buckets"
	openAILatencyTelemetrySourceHeartbeatKey    = openAILatencyTelemetryRedisPrefix + "sources"
	openAILatencyHealthSoftReservationKey       = "openai:latency:health:reservation:soft"
	openAILatencyHealthSoftReservationExpiryKey = "openai:latency:health:reservation:soft:expiry"
	openAILatencyHealthHardReservationKey       = "openai:latency:health:reservation:hard"
	openAILatencyHealthHardReservationExpiryKey = "openai:latency:health:reservation:hard:expiry"
	// One collector can retain current and previous buckets for every
	// configured stream, so a flush may contain twice the stream ceiling.
	openAILatencyTelemetryMaxStoreBatch   = 2 * 65536
	openAILatencyTelemetryMaxBucketList   = 4096
	openAILatencyTelemetryMaxSnapshotLoad = 65536
	openAILatencyTelemetryMaxSourceList   = 65536
)

var (
	errOpenAILatencyTelemetryResultLimitExceeded = errors.New("openai latency telemetry result exceeds the requested limit")

	// The payload key always contains the absolute JSON snapshot. Sequence state
	// is kept in a companion key so the Lua comparison remains exact for the full
	// uint64 range instead of passing through Lua's floating-point number type.
	openAILatencyTelemetryStoreScript = redis.NewScript(`
local function normalize_decimal(value)
  value = string.gsub(value, "^0+", "")
  if value == "" then
    return "0"
  end
  return value
end

local function compare_decimal(left, right)
  left = normalize_decimal(left)
  right = normalize_decimal(right)
  if string.len(left) < string.len(right) then
    return -1
  end
  if string.len(left) > string.len(right) then
    return 1
  end
  if left == right then
    return 0
  end
  if left < right then
    return -1
  end
  return 1
end

local current_sequence = redis.call("GET", KEYS[2])
local payload_exists = redis.call("EXISTS", KEYS[1])
local stored = 0
if not current_sequence or payload_exists == 0 or compare_decimal(ARGV[1], current_sequence) > 0 then
  redis.call("SET", KEYS[1], ARGV[2])
  redis.call("SET", KEYS[2], ARGV[1])
  stored = 1
end

-- Raw retention is anchored to bucket end, not to retries of an unchanged
-- cumulative sequence. This prevents an idle in-memory stream from extending
-- old Redis payloads forever merely because the collector keeps flushing.
redis.call("PEXPIREAT", KEYS[1], ARGV[3])
redis.call("PEXPIREAT", KEYS[2], ARGV[3])
redis.call("ZADD", KEYS[3], 0, KEYS[1])
redis.call("PEXPIREAT", KEYS[3], ARGV[3])
redis.call("ZADD", KEYS[4], ARGV[5], ARGV[4])
redis.call("ZREMRANGEBYSCORE", KEYS[4], "-inf", ARGV[6])
redis.call("PEXPIRE", KEYS[4], ARGV[7])
return stored
`)

	openAILatencyTelemetryHeartbeatScript = redis.NewScript(`
local current_score = redis.call("ZSCORE", KEYS[1], ARGV[1])
if not current_score or tonumber(current_score) < tonumber(ARGV[2]) then
  redis.call("ZADD", KEYS[1], ARGV[2], ARGV[1])
end
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", "(" .. ARGV[3])
redis.call("PEXPIRE", KEYS[1], ARGV[4])
return 1
`)

	openAILatencyHealthReserveScript = redis.NewScript(`
local expired = redis.call("ZRANGEBYSCORE", KEYS[2], "-inf", ARGV[7])
for _, account_id in ipairs(expired) do
  redis.call("HDEL", KEYS[1], account_id)
  redis.call("ZREM", KEYS[2], account_id)
end

local current_owner = redis.call("HGET", KEYS[1], ARGV[1])
if current_owner then
  if current_owner ~= ARGV[2] then
    return 0
  end
  redis.call("ZADD", KEYS[2], ARGV[8], ARGV[1])
  local latest = redis.call("ZRANGE", KEYS[2], -1, -1, "WITHSCORES")
  local key_expiry = tonumber(latest[2]) + tonumber(ARGV[9])
  redis.call("PEXPIREAT", KEYS[1], key_expiry)
  redis.call("PEXPIREAT", KEYS[2], key_expiry)
  return 1
end

local reservations = redis.call("HLEN", KEYS[1])
local max_accounts = tonumber(ARGV[3])
local pool_count = tonumber(ARGV[4])
local threshold = tonumber(ARGV[5])
if reservations >= max_accounts then
  return 0
end

if ARGV[6] == "soft" then
  if pool_count + reservations >= threshold then
    return 0
  end
elseif ARGV[6] == "hard" then
  if pool_count - reservations <= threshold then
    return 0
  end
else
  return redis.error_reply("invalid latency health transition")
end

redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[8], ARGV[1])
local latest = redis.call("ZRANGE", KEYS[2], -1, -1, "WITHSCORES")
local key_expiry = tonumber(latest[2]) + tonumber(ARGV[9])
redis.call("PEXPIREAT", KEYS[1], key_expiry)
redis.call("PEXPIREAT", KEYS[2], key_expiry)
return 1
`)

	openAILatencyHealthRenewScript = redis.NewScript(`
local expired = redis.call("ZRANGEBYSCORE", KEYS[2], "-inf", ARGV[3])
for _, account_id in ipairs(expired) do
  redis.call("HDEL", KEYS[1], account_id)
  redis.call("ZREM", KEYS[2], account_id)
end
if redis.call("HGET", KEYS[1], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call("ZADD", KEYS[2], ARGV[4], ARGV[1])
local latest = redis.call("ZRANGE", KEYS[2], -1, -1, "WITHSCORES")
local key_expiry = tonumber(latest[2]) + tonumber(ARGV[5])
redis.call("PEXPIREAT", KEYS[1], key_expiry)
redis.call("PEXPIREAT", KEYS[2], key_expiry)
return 1
`)

	openAILatencyHealthReleaseScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call("HDEL", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
return 1
`)
)

type openAILatencyTelemetryCache struct {
	rdb *redis.Client
}

var _ service.OpenAILatencyTelemetryCache = (*openAILatencyTelemetryCache)(nil)

// NewOpenAILatencyTelemetryCache creates the Redis-backed absolute snapshot
// store. Every caller-controlled key dimension is rendered as a base-10
// integer; raw model names, errors, request data, and credentials never enter
// Redis keys or members.
func NewOpenAILatencyTelemetryCache(rdb *redis.Client) service.OpenAILatencyTelemetryCache {
	return &openAILatencyTelemetryCache{rdb: rdb}
}

type openAILatencyTelemetrySnapshotRedisIdentity struct {
	bucketStart    int64
	bucketDuration int64
	accountID      int64
	cohort         uint64
	attemptRole    uint64
	sourceID       uint64
	generation     uint64
}

func openAILatencyTelemetrySnapshotIdentity(snapshot service.OpenAILatencyTelemetrySnapshot) openAILatencyTelemetrySnapshotRedisIdentity {
	return openAILatencyTelemetrySnapshotRedisIdentity{
		bucketStart:    snapshot.BucketStart,
		bucketDuration: snapshot.BucketDuration,
		accountID:      snapshot.Key.AccountID,
		cohort:         uint64(snapshot.Key.Cohort),
		attemptRole:    uint64(snapshot.Key.AttemptRole),
		sourceID:       snapshot.SourceID,
		generation:     snapshot.Generation,
	}
}

func (i openAILatencyTelemetrySnapshotRedisIdentity) suffix() string {
	return strconv.FormatInt(i.bucketStart, 10) + ":" +
		strconv.FormatInt(i.bucketDuration, 10) + ":" +
		strconv.FormatInt(i.accountID, 10) + ":" +
		strconv.FormatUint(i.cohort, 10) + ":" +
		strconv.FormatUint(i.attemptRole, 10) + ":" +
		strconv.FormatUint(i.sourceID, 10) + ":" +
		strconv.FormatUint(i.generation, 10)
}

func (i openAILatencyTelemetrySnapshotRedisIdentity) snapshotKey() string {
	return openAILatencyTelemetrySnapshotPrefix + i.suffix()
}

func (i openAILatencyTelemetrySnapshotRedisIdentity) sequenceKey() string {
	return openAILatencyTelemetrySequencePrefix + i.suffix()
}

func openAILatencyTelemetryBucketMember(bucket service.OpenAILatencyTelemetryBucket) string {
	return strconv.FormatInt(bucket.BucketStart, 10) + ":" + strconv.FormatInt(bucket.BucketDuration, 10)
}

func openAILatencyTelemetryBucketKey(bucket service.OpenAILatencyTelemetryBucket) string {
	return openAILatencyTelemetryBucketPrefix + openAILatencyTelemetryBucketMember(bucket)
}

func openAILatencyTelemetrySourceMember(sourceID, generation uint64) string {
	return strconv.FormatUint(sourceID, 10) + ":" + strconv.FormatUint(generation, 10)
}

func parseOpenAILatencyTelemetryBucketMember(member string) (service.OpenAILatencyTelemetryBucket, error) {
	parts := strings.Split(member, ":")
	if len(parts) != 2 {
		return service.OpenAILatencyTelemetryBucket{}, errors.New("invalid telemetry bucket index member")
	}
	start, startErr := strconv.ParseInt(parts[0], 10, 64)
	duration, durationErr := strconv.ParseInt(parts[1], 10, 64)
	if startErr != nil || durationErr != nil {
		return service.OpenAILatencyTelemetryBucket{}, errors.New("invalid telemetry bucket index member")
	}
	bucket := service.OpenAILatencyTelemetryBucket{BucketStart: start, BucketDuration: duration}
	if err := bucket.Validate(); err != nil {
		return service.OpenAILatencyTelemetryBucket{}, fmt.Errorf("validate telemetry bucket index member: %w", err)
	}
	return bucket, nil
}

func parseOpenAILatencyTelemetrySnapshotKey(key string) (openAILatencyTelemetrySnapshotRedisIdentity, error) {
	if !strings.HasPrefix(key, openAILatencyTelemetrySnapshotPrefix) {
		return openAILatencyTelemetrySnapshotRedisIdentity{}, errors.New("invalid telemetry snapshot index member")
	}
	parts := strings.Split(strings.TrimPrefix(key, openAILatencyTelemetrySnapshotPrefix), ":")
	if len(parts) != 7 {
		return openAILatencyTelemetrySnapshotRedisIdentity{}, errors.New("invalid telemetry snapshot index member")
	}
	start, errStart := strconv.ParseInt(parts[0], 10, 64)
	duration, errDuration := strconv.ParseInt(parts[1], 10, 64)
	accountID, errAccount := strconv.ParseInt(parts[2], 10, 64)
	cohort, errCohort := strconv.ParseUint(parts[3], 10, 16)
	role, errRole := strconv.ParseUint(parts[4], 10, 8)
	sourceID, errSource := strconv.ParseUint(parts[5], 10, 64)
	generation, errGeneration := strconv.ParseUint(parts[6], 10, 64)
	if errStart != nil || errDuration != nil || errAccount != nil || errCohort != nil ||
		errRole != nil || errSource != nil || errGeneration != nil {
		return openAILatencyTelemetrySnapshotRedisIdentity{}, errors.New("invalid telemetry snapshot index member")
	}
	return openAILatencyTelemetrySnapshotRedisIdentity{
		bucketStart:    start,
		bucketDuration: duration,
		accountID:      accountID,
		cohort:         cohort,
		attemptRole:    role,
		sourceID:       sourceID,
		generation:     generation,
	}, nil
}

func parseOpenAILatencyTelemetrySourceMember(member string) (uint64, uint64, error) {
	parts := strings.Split(member, ":")
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid telemetry source heartbeat member")
	}
	sourceID, sourceErr := strconv.ParseUint(parts[0], 10, 64)
	generation, generationErr := strconv.ParseUint(parts[1], 10, 64)
	if sourceErr != nil || generationErr != nil || sourceID == 0 || generation == 0 {
		return 0, 0, errors.New("invalid telemetry source heartbeat member")
	}
	return sourceID, generation, nil
}

func validateOpenAILatencyTelemetrySnapshotForRedis(snapshot service.OpenAILatencyTelemetrySnapshot) error {
	if snapshot.SchemaVersion != service.OpenAILatencyTelemetrySchemaVersion {
		return errors.New("telemetry snapshot schema version is unsupported")
	}
	if snapshot.Key.AccountID <= 0 {
		return errors.New("telemetry snapshot account id must be positive")
	}
	if snapshot.Key.Cohort != snapshot.Key.Cohort.Canonical() {
		return errors.New("telemetry snapshot cohort must be canonical")
	}
	if snapshot.Key.AttemptRole >= service.OpenAIAttemptRoleCardinality {
		return errors.New("telemetry snapshot attempt role must be canonical")
	}
	if snapshot.SourceID == 0 || snapshot.Generation == 0 || snapshot.Sequence == 0 {
		return errors.New("telemetry snapshot source, generation, and sequence must be positive")
	}
	if snapshot.BucketDuration <= 0 || snapshot.BucketStart%snapshot.BucketDuration != 0 {
		return errors.New("telemetry snapshot bucket must have a positive aligned duration")
	}
	return nil
}

func validateOpenAILatencyTelemetryCacheLimit(limit, maximum int) error {
	if limit <= 0 {
		return errors.New("openai latency telemetry limit must be positive")
	}
	if limit > maximum {
		return fmt.Errorf("openai latency telemetry limit exceeds maximum %d", maximum)
	}
	return nil
}

func openAILatencyTelemetryTTLMillis(ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		return 0, errors.New("openai latency telemetry TTL must be positive")
	}
	milliseconds := ttl.Milliseconds()
	if milliseconds == 0 {
		milliseconds = 1
	}
	return milliseconds, nil
}

func (c *openAILatencyTelemetryCache) validate() error {
	if c == nil || c.rdb == nil {
		return errors.New("openai latency telemetry Redis client is nil")
	}
	return nil
}

func (c *openAILatencyTelemetryCache) StoreOpenAILatencyTelemetrySnapshots(
	ctx context.Context,
	snapshots []service.OpenAILatencyTelemetrySnapshot,
	heartbeat service.OpenAILatencyTelemetrySourceHeartbeat,
	rawRetention time.Duration,
	sourceTTL time.Duration,
) error {
	if err := c.validate(); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("openai latency telemetry context is nil")
	}
	if len(snapshots) > openAILatencyTelemetryMaxStoreBatch {
		return errOpenAILatencyTelemetryResultLimitExceeded
	}
	if heartbeat.SourceID == 0 || heartbeat.Generation == 0 || heartbeat.ObservedAt.IsZero() {
		return errors.New("openai latency telemetry source heartbeat is incomplete")
	}
	rawRetentionMillis, err := openAILatencyTelemetryTTLMillis(rawRetention)
	if err != nil {
		return fmt.Errorf("validate telemetry raw retention: %w", err)
	}
	sourceTTLMillis, err := openAILatencyTelemetryTTLMillis(sourceTTL)
	if err != nil {
		return fmt.Errorf("validate telemetry source TTL: %w", err)
	}

	type preparedSnapshot struct {
		identity openAILatencyTelemetrySnapshotRedisIdentity
		bucket   service.OpenAILatencyTelemetryBucket
		sequence string
		payload  []byte
	}
	prepared := make([]preparedSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if err := validateOpenAILatencyTelemetrySnapshotForRedis(snapshot); err != nil {
			return fmt.Errorf("validate telemetry snapshot: %w", err)
		}
		if snapshot.SourceID != heartbeat.SourceID || snapshot.Generation != heartbeat.Generation {
			return errors.New("telemetry snapshot stream does not match source heartbeat")
		}
		payload, marshalErr := json.Marshal(snapshot)
		if marshalErr != nil {
			return fmt.Errorf("marshal telemetry snapshot: %w", marshalErr)
		}
		prepared = append(prepared, preparedSnapshot{
			identity: openAILatencyTelemetrySnapshotIdentity(snapshot),
			bucket: service.OpenAILatencyTelemetryBucket{
				BucketStart:    snapshot.BucketStart,
				BucketDuration: snapshot.BucketDuration,
			},
			sequence: strconv.FormatUint(snapshot.Sequence, 10),
			payload:  payload,
		})
	}

	observedAt := heartbeat.ObservedAt.UTC()
	bucketPruneBefore := observedAt.Add(-rawRetention).UnixMilli()
	sourcePruneBefore := observedAt.Add(-sourceTTL).UnixMilli()
	_, err = c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, snapshot := range prepared {
			bucketMember := openAILatencyTelemetryBucketMember(snapshot.bucket)
			openAILatencyTelemetryStoreScript.Eval(
				ctx,
				pipe,
				[]string{
					snapshot.identity.snapshotKey(),
					snapshot.identity.sequenceKey(),
					openAILatencyTelemetryBucketKey(snapshot.bucket),
					openAILatencyTelemetryBucketIndexKey,
				},
				snapshot.sequence,
				snapshot.payload,
				snapshot.bucket.EndTime().Add(rawRetention).UnixMilli(),
				bucketMember,
				snapshot.bucket.EndTime().UnixMilli(),
				bucketPruneBefore,
				rawRetentionMillis,
			)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("store openai latency telemetry snapshots in Redis: %w", err)
	}

	// Publish liveness only after every snapshot command has succeeded. A source
	// heartbeat must never tell the finalizer that a bucket is complete when its
	// cumulative payload failed to persist.
	if err := openAILatencyTelemetryHeartbeatScript.Run(
		ctx,
		c.rdb,
		[]string{openAILatencyTelemetrySourceHeartbeatKey},
		openAILatencyTelemetrySourceMember(heartbeat.SourceID, heartbeat.Generation),
		observedAt.UnixMilli(),
		sourcePruneBefore,
		sourceTTLMillis,
	).Err(); err != nil {
		return fmt.Errorf("store openai latency telemetry heartbeat in Redis: %w", err)
	}
	return nil
}

func (c *openAILatencyTelemetryCache) ListOpenAILatencyTelemetryBuckets(
	ctx context.Context,
	before time.Time,
	limit int,
) ([]service.OpenAILatencyTelemetryBucket, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, errors.New("openai latency telemetry context is nil")
	}
	if before.IsZero() {
		return nil, errors.New("telemetry bucket cutoff is required")
	}
	if err := validateOpenAILatencyTelemetryCacheLimit(limit, openAILatencyTelemetryMaxBucketList); err != nil {
		return nil, err
	}
	members, err := c.rdb.ZRangeByScore(ctx, openAILatencyTelemetryBucketIndexKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(before.UTC().UnixMilli(), 10),
		Count: int64(limit + 1),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("list telemetry bucket index: %w", err)
	}
	if len(members) > limit {
		return nil, errOpenAILatencyTelemetryResultLimitExceeded
	}
	buckets := make([]service.OpenAILatencyTelemetryBucket, 0, len(members))
	for _, member := range members {
		bucket, parseErr := parseOpenAILatencyTelemetryBucketMember(member)
		if parseErr != nil {
			return nil, parseErr
		}
		buckets = append(buckets, bucket)
	}
	return buckets, nil
}

func (c *openAILatencyTelemetryCache) scanBucketSnapshotKeys(
	ctx context.Context,
	bucket service.OpenAILatencyTelemetryBucket,
	limit int,
) ([]string, error) {
	indexKey := openAILatencyTelemetryBucketKey(bucket)
	count, err := c.rdb.ZCard(ctx, indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("count telemetry bucket snapshot index: %w", err)
	}
	if count > int64(limit) {
		return nil, errOpenAILatencyTelemetryResultLimitExceeded
	}
	if count == 0 {
		return nil, nil
	}

	keys, err := c.rdb.ZRange(ctx, indexKey, 0, count-1).Result()
	if err != nil {
		return nil, fmt.Errorf("scan telemetry bucket snapshot index: %w", err)
	}
	if int64(len(keys)) != count {
		return nil, errors.New("telemetry bucket snapshot index changed during full scan")
	}
	return keys, nil
}

func validateOpenAILatencyTelemetrySnapshotRedisIdentity(
	snapshot service.OpenAILatencyTelemetrySnapshot,
	identity openAILatencyTelemetrySnapshotRedisIdentity,
	bucket service.OpenAILatencyTelemetryBucket,
) error {
	if err := validateOpenAILatencyTelemetrySnapshotForRedis(snapshot); err != nil {
		return err
	}
	if identity.bucketStart != bucket.BucketStart || identity.bucketDuration != bucket.BucketDuration ||
		snapshot.BucketStart != bucket.BucketStart || snapshot.BucketDuration != bucket.BucketDuration ||
		identity.accountID != snapshot.Key.AccountID || identity.cohort != uint64(snapshot.Key.Cohort) ||
		identity.attemptRole != uint64(snapshot.Key.AttemptRole) || identity.sourceID != snapshot.SourceID ||
		identity.generation != snapshot.Generation {
		return errors.New("telemetry snapshot payload does not match its numeric Redis identity")
	}
	return nil
}

func sortOpenAILatencyTelemetrySnapshotsForRedis(snapshots []service.OpenAILatencyTelemetrySnapshot) {
	sort.Slice(snapshots, func(i, j int) bool {
		left, right := snapshots[i], snapshots[j]
		if left.Key.AccountID != right.Key.AccountID {
			return left.Key.AccountID < right.Key.AccountID
		}
		if left.Key.Cohort != right.Key.Cohort {
			return left.Key.Cohort < right.Key.Cohort
		}
		if left.Key.AttemptRole != right.Key.AttemptRole {
			return left.Key.AttemptRole < right.Key.AttemptRole
		}
		if left.SourceID != right.SourceID {
			return left.SourceID < right.SourceID
		}
		return left.Generation < right.Generation
	})
}

func (c *openAILatencyTelemetryCache) LoadOpenAILatencyTelemetryBucket(
	ctx context.Context,
	bucket service.OpenAILatencyTelemetryBucket,
	limit int,
) ([]service.OpenAILatencyTelemetrySnapshot, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, errors.New("openai latency telemetry context is nil")
	}
	if err := bucket.Validate(); err != nil {
		return nil, fmt.Errorf("validate telemetry bucket: %w", err)
	}
	if err := validateOpenAILatencyTelemetryCacheLimit(limit, openAILatencyTelemetryMaxSnapshotLoad); err != nil {
		return nil, err
	}
	keys, err := c.scanBucketSnapshotKeys(ctx, bucket, limit)
	if err != nil || len(keys) == 0 {
		return nil, err
	}

	return c.loadOpenAILatencyTelemetrySnapshotKeys(ctx, bucket, keys)
}

func (c *openAILatencyTelemetryCache) loadOpenAILatencyTelemetrySnapshotKeys(
	ctx context.Context,
	bucket service.OpenAILatencyTelemetryBucket,
	keys []string,
) ([]service.OpenAILatencyTelemetrySnapshot, error) {
	identities := make([]openAILatencyTelemetrySnapshotRedisIdentity, len(keys))
	for i, key := range keys {
		identity, parseErr := parseOpenAILatencyTelemetrySnapshotKey(key)
		if parseErr != nil {
			return nil, parseErr
		}
		if identity.bucketStart != bucket.BucketStart || identity.bucketDuration != bucket.BucketDuration {
			return nil, errors.New("telemetry snapshot index member belongs to another bucket")
		}
		identities[i] = identity
	}

	pipe := c.rdb.Pipeline()
	commands := make([]*redis.StringCmd, len(keys))
	for i, key := range keys {
		commands[i] = pipe.Get(ctx, key)
	}
	if _, execErr := pipe.Exec(ctx); execErr != nil && !errors.Is(execErr, redis.Nil) {
		return nil, fmt.Errorf("load telemetry snapshot pipeline: %w", execErr)
	}

	snapshots := make([]service.OpenAILatencyTelemetrySnapshot, 0, len(commands))
	for i, command := range commands {
		payload, commandErr := command.Bytes()
		if errors.Is(commandErr, redis.Nil) {
			return nil, errors.New("telemetry snapshot index references an expired payload")
		}
		if commandErr != nil {
			return nil, fmt.Errorf("read telemetry snapshot payload: %w", commandErr)
		}
		var snapshot service.OpenAILatencyTelemetrySnapshot
		if unmarshalErr := json.Unmarshal(payload, &snapshot); unmarshalErr != nil {
			return nil, fmt.Errorf("decode telemetry snapshot payload: %w", unmarshalErr)
		}
		if validateErr := validateOpenAILatencyTelemetrySnapshotRedisIdentity(snapshot, identities[i], bucket); validateErr != nil {
			return nil, fmt.Errorf("validate telemetry snapshot payload: %w", validateErr)
		}
		snapshots = append(snapshots, snapshot)
	}
	sortOpenAILatencyTelemetrySnapshotsForRedis(snapshots)
	return snapshots, nil
}

func (c *openAILatencyTelemetryCache) LoadOpenAILatencyTelemetryBucketBatch(
	ctx context.Context,
	bucket service.OpenAILatencyTelemetryBucket,
	cursor uint64,
	limit int,
) ([]service.OpenAILatencyTelemetrySnapshot, uint64, error) {
	if err := c.validate(); err != nil {
		return nil, 0, err
	}
	if ctx == nil {
		return nil, 0, errors.New("openai latency telemetry context is nil")
	}
	if err := bucket.Validate(); err != nil {
		return nil, 0, fmt.Errorf("validate telemetry bucket: %w", err)
	}
	if err := validateOpenAILatencyTelemetryCacheLimit(limit, openAILatencyTelemetryMaxSnapshotLoad); err != nil {
		return nil, 0, err
	}

	indexKey := openAILatencyTelemetryBucketKey(bucket)
	count, err := c.rdb.ZCard(ctx, indexKey).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("count telemetry bucket snapshot batch: %w", err)
	}
	if cursor >= uint64(count) {
		return nil, 0, nil
	}

	readCount := min(uint64(limit), uint64(count)-cursor)
	keys, err := c.rdb.ZRange(ctx, indexKey, int64(cursor), int64(cursor+readCount-1)).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("read telemetry bucket snapshot batch: %w", err)
	}
	if uint64(len(keys)) != readCount {
		return nil, 0, errors.New("telemetry bucket snapshot index changed during batch read")
	}

	snapshots, err := c.loadOpenAILatencyTelemetrySnapshotKeys(ctx, bucket, keys)
	if err != nil {
		return nil, 0, err
	}
	next := cursor + uint64(len(keys))
	if next == uint64(count) {
		next = 0
	}
	return snapshots, next, nil
}

func (c *openAILatencyTelemetryCache) ListOpenAILatencyTelemetrySourceHeartbeats(
	ctx context.Context,
	since time.Time,
	limit int,
) ([]service.OpenAILatencyTelemetrySourceHeartbeat, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, errors.New("openai latency telemetry context is nil")
	}
	if since.IsZero() {
		return nil, errors.New("telemetry source heartbeat cutoff is required")
	}
	if err := validateOpenAILatencyTelemetryCacheLimit(limit, openAILatencyTelemetryMaxSourceList); err != nil {
		return nil, err
	}
	members, err := c.rdb.ZRangeByScoreWithScores(ctx, openAILatencyTelemetrySourceHeartbeatKey, &redis.ZRangeBy{
		Min:   strconv.FormatInt(since.UTC().UnixMilli(), 10),
		Max:   "+inf",
		Count: int64(limit + 1),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("list telemetry source heartbeats: %w", err)
	}
	if len(members) > limit {
		return nil, errOpenAILatencyTelemetryResultLimitExceeded
	}
	heartbeats := make([]service.OpenAILatencyTelemetrySourceHeartbeat, 0, len(members))
	for _, member := range members {
		value, ok := member.Member.(string)
		if !ok {
			return nil, errors.New("invalid telemetry source heartbeat member")
		}
		sourceID, generation, parseErr := parseOpenAILatencyTelemetrySourceMember(value)
		if parseErr != nil {
			return nil, parseErr
		}
		heartbeats = append(heartbeats, service.OpenAILatencyTelemetrySourceHeartbeat{
			SourceID:   sourceID,
			Generation: generation,
			ObservedAt: time.UnixMilli(int64(member.Score)).UTC(),
		})
	}
	return heartbeats, nil
}

func (c *openAILatencyTelemetryCache) DeleteOpenAILatencyTelemetryBucket(
	ctx context.Context,
	bucket service.OpenAILatencyTelemetryBucket,
) error {
	if err := c.validate(); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("openai latency telemetry context is nil")
	}
	if err := bucket.Validate(); err != nil {
		return fmt.Errorf("validate telemetry bucket: %w", err)
	}
	keys, err := c.scanBucketSnapshotKeys(ctx, bucket, openAILatencyTelemetryMaxSnapshotLoad)
	if err != nil {
		return err
	}
	identities := make([]openAILatencyTelemetrySnapshotRedisIdentity, len(keys))
	for i, key := range keys {
		identity, parseErr := parseOpenAILatencyTelemetrySnapshotKey(key)
		if parseErr != nil {
			return parseErr
		}
		if identity.bucketStart != bucket.BucketStart || identity.bucketDuration != bucket.BucketDuration {
			return errors.New("telemetry snapshot index member belongs to another bucket")
		}
		identities[i] = identity
	}

	pipe := c.rdb.Pipeline()
	for i, key := range keys {
		pipe.Del(ctx, key, identities[i].sequenceKey())
	}
	pipe.Del(ctx, openAILatencyTelemetryBucketKey(bucket))
	pipe.ZRem(ctx, openAILatencyTelemetryBucketIndexKey, openAILatencyTelemetryBucketMember(bucket))
	if _, execErr := pipe.Exec(ctx); execErr != nil {
		return fmt.Errorf("delete telemetry bucket pipeline: %w", execErr)
	}
	return nil
}

type redisLatencyHealthTransitionGuard struct {
	rdb         *redis.Client
	maxAccounts int
}

var _ service.LatencyHealthTransitionGuard = (*redisLatencyHealthTransitionGuard)(nil)

// NewRedisLatencyHealthTransitionGuard creates a process-independent guard.
// Redis Lua serializes stale pool-snapshot checks with account reservations,
// while SISMEMBER makes aggregate/cohort repeats for one account idempotent.
func NewRedisLatencyHealthTransitionGuard(rdb *redis.Client) service.LatencyHealthTransitionGuard {
	return &redisLatencyHealthTransitionGuard{
		rdb:         rdb,
		maxAccounts: service.MaximumLatencyHealthTrackedAccounts,
	}
}

// NewLatencyHealthTransitionGuard is the repository-constructor alias used by
// callers that do not need to distinguish the Redis implementation by name.
func NewLatencyHealthTransitionGuard(rdb *redis.Client) service.LatencyHealthTransitionGuard {
	return NewRedisLatencyHealthTransitionGuard(rdb)
}

func (g *redisLatencyHealthTransitionGuard) validate() error {
	if g == nil || g.rdb == nil {
		return errors.New("latency health transition guard Redis client is nil")
	}
	if g.maxAccounts <= 0 || g.maxAccounts > service.MaximumLatencyHealthTrackedAccounts {
		return errors.New("latency health transition guard max accounts is invalid")
	}
	return nil
}

func openAILatencyHealthReservationKeys(
	transition service.LatencyHealthTransition,
) (string, string, error) {
	switch transition {
	case service.LatencyHealthTransitionSoftQuarantine:
		return openAILatencyHealthSoftReservationKey, openAILatencyHealthSoftReservationExpiryKey, nil
	case service.LatencyHealthTransitionHardInvalid:
		return openAILatencyHealthHardReservationKey, openAILatencyHealthHardReservationExpiryKey, nil
	default:
		return "", "", errors.New("latency health transition is invalid")
	}
}

func (g *redisLatencyHealthTransitionGuard) Reserve(
	ctx context.Context,
	reservation service.LatencyHealthTransitionReservation,
) (bool, error) {
	if err := g.validate(); err != nil {
		return false, err
	}
	if ctx == nil {
		return false, errors.New("latency health transition context is nil")
	}
	if reservation.AccountID <= 0 {
		return false, errors.New("transition reservation account id must be positive")
	}
	if reservation.OwnerToken == 0 {
		return false, errors.New("transition reservation owner token must be positive")
	}
	ttlMillis, err := openAILatencyTelemetryTTLMillis(reservation.TTL)
	if err != nil {
		return false, fmt.Errorf("validate transition reservation TTL: %w", err)
	}
	if err := reservation.PoolSnapshot.Validate(); err != nil {
		return false, fmt.Errorf("validate transition pool snapshot: %w", err)
	}

	key, expiryKey, err := openAILatencyHealthReservationKeys(reservation.Transition)
	if err != nil {
		return false, err
	}
	var mode string
	var poolCount, threshold int
	switch reservation.Transition {
	case service.LatencyHealthTransitionSoftQuarantine:
		if reservation.QuarantineCap < 0 {
			return false, errors.New("transition quarantine cap must be non-negative")
		}
		mode = "soft"
		poolCount = reservation.PoolSnapshot.QuarantinedAccounts
		threshold = reservation.QuarantineCap
	case service.LatencyHealthTransitionHardInvalid:
		if reservation.HardValidFloor < 0 {
			return false, errors.New("transition hard-valid floor must be non-negative")
		}
		mode = "hard"
		poolCount = reservation.PoolSnapshot.HardValidAccounts
		threshold = reservation.HardValidFloor
	default:
		return false, errors.New("latency health transition is invalid")
	}

	nowMillis := time.Now().UTC().UnixMilli()
	value, err := openAILatencyHealthReserveScript.Run(
		ctx,
		g.rdb,
		[]string{key, expiryKey},
		strconv.FormatInt(reservation.AccountID, 10),
		strconv.FormatUint(reservation.OwnerToken, 10),
		g.maxAccounts,
		poolCount,
		threshold,
		mode,
		nowMillis,
		nowMillis+ttlMillis,
		ttlMillis,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("reserve latency health transition: %w", err)
	}
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, errors.New("latency health transition reservation returned an invalid result")
	}
}

func (g *redisLatencyHealthTransitionGuard) Renew(
	ctx context.Context,
	accountID int64,
	transition service.LatencyHealthTransition,
	ownerToken uint64,
	ttl time.Duration,
) (bool, error) {
	if err := g.validate(); err != nil {
		return false, err
	}
	if ctx == nil {
		return false, errors.New("latency health transition context is nil")
	}
	if accountID <= 0 || ownerToken == 0 {
		return false, errors.New("transition renewal account and owner must be positive")
	}
	ttlMillis, err := openAILatencyTelemetryTTLMillis(ttl)
	if err != nil {
		return false, fmt.Errorf("validate transition renewal TTL: %w", err)
	}
	key, expiryKey, err := openAILatencyHealthReservationKeys(transition)
	if err != nil {
		return false, err
	}
	nowMillis := time.Now().UTC().UnixMilli()
	value, err := openAILatencyHealthRenewScript.Run(
		ctx,
		g.rdb,
		[]string{key, expiryKey},
		strconv.FormatInt(accountID, 10),
		strconv.FormatUint(ownerToken, 10),
		nowMillis,
		nowMillis+ttlMillis,
		ttlMillis,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("renew latency health transition: %w", err)
	}
	return value == 1, nil
}

func (g *redisLatencyHealthTransitionGuard) Release(
	ctx context.Context,
	accountID int64,
	transition service.LatencyHealthTransition,
	ownerToken uint64,
) error {
	if err := g.validate(); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("latency health transition context is nil")
	}
	if accountID <= 0 {
		return errors.New("transition release account id must be positive")
	}
	if ownerToken == 0 {
		return errors.New("transition release owner token must be positive")
	}
	key, expiryKey, err := openAILatencyHealthReservationKeys(transition)
	if err != nil {
		return err
	}
	if err := openAILatencyHealthReleaseScript.Run(
		ctx,
		g.rdb,
		[]string{key, expiryKey},
		strconv.FormatInt(accountID, 10),
		strconv.FormatUint(ownerToken, 10),
	).Err(); err != nil {
		return fmt.Errorf("release latency health transition: %w", err)
	}
	return nil
}
