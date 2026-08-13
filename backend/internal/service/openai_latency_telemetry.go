package service

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OpenAILatencyTelemetrySchemaVersion     uint16 = 5
	DefaultOpenAILatencyTelemetryMaxStreams int    = 256
)

var (
	openAILatencyTelemetrySourceSequence atomic.Uint64
	openAILatencyBootSequence            atomic.Uint64
)

func mintOpenAILatencyBootToken() uint64 {
	var random [8]byte
	if _, err := rand.Read(random[:]); err == nil {
		if token := binary.BigEndian.Uint64(random[:]); token != 0 {
			return token
		}
	}

	// crypto/rand failure must not make the recommendation-only subsystem
	// unavailable. The fallback combines wall time with a process-local sequence
	// so repeated constructors in one boot still receive distinct identities.
	token := uint64(time.Now().UnixNano()) ^ (openAILatencyBootSequence.Add(1) * 0x9e3779b97f4a7c15)
	if token == 0 {
		return openAILatencyBootSequence.Add(1)
	}
	return token
}

// OpenAIAttemptRole is an allowlisted attempt identity used in telemetry keys.
type OpenAIAttemptRole uint8

const (
	OpenAIAttemptRoleUnknown OpenAIAttemptRole = iota
	OpenAIAttemptRolePrimary
	OpenAIAttemptRoleHedge
	OpenAIAttemptRoleCardinality
)

// OpenAILatencyTelemetryKey is the complete fixed-cardinality telemetry
// identity. It contains no API key, request body, model string, or raw error.
type OpenAILatencyTelemetryKey struct {
	AccountID   int64
	Cohort      CohortKey
	AttemptRole OpenAIAttemptRole
}

func canonicalOpenAILatencyTelemetryKey(key OpenAILatencyTelemetryKey) (OpenAILatencyTelemetryKey, error) {
	if key.AccountID <= 0 {
		return OpenAILatencyTelemetryKey{}, errors.New("telemetry account id must be positive")
	}
	key.Cohort = key.Cohort.Canonical()
	key.AttemptRole = canonicalAttemptRole(key.AttemptRole)
	return key, nil
}

func validateOpenAILatencyTelemetryKey(key OpenAILatencyTelemetryKey) error {
	if key.AccountID <= 0 {
		return errors.New("telemetry account id must be positive")
	}
	if key.Cohort != key.Cohort.Canonical() {
		return errors.New("telemetry cohort key must be canonical")
	}
	if key.AttemptRole >= OpenAIAttemptRoleCardinality {
		return errors.New("telemetry attempt role must be canonical")
	}
	return nil
}

// OpenAILatencyPhase is an allowlisted, fixed-cardinality request phase.
type OpenAILatencyPhase uint8

const (
	OpenAILatencyPhaseUnknown OpenAILatencyPhase = iota
	OpenAILatencyPhaseAdmission
	OpenAILatencyPhaseQueue
	OpenAILatencyPhaseAccountSelection
	OpenAILatencyPhaseConnect
	OpenAILatencyPhaseFirstByte
	OpenAILatencyPhaseFirstSSE
	OpenAILatencyPhaseFirstMeaningful
	OpenAILatencyPhaseHedgeEligible
	OpenAILatencyPhaseSecondaryDispatch
	OpenAILatencyPhaseWinnerCommit
	OpenAILatencyPhaseLoserCancel
	OpenAILatencyPhaseUsage
	OpenAILatencyPhaseStream
	OpenAILatencyPhaseTerminal
	OpenAILatencyPhaseTotal
	OpenAILatencyPhaseCardinality
)

// OpenAIHedgeOutcome is an allowlisted hedge lifecycle result.
type OpenAIHedgeOutcome uint8

const (
	OpenAIHedgeOutcomeNone OpenAIHedgeOutcome = iota
	OpenAIHedgeOutcomeNotLaunched
	OpenAIHedgeOutcomeLaunched
	OpenAIHedgeOutcomePrimaryWon
	OpenAIHedgeOutcomeHedgeWon
	OpenAIHedgeOutcomeBothFailed
	OpenAIHedgeOutcomeCanceled
	OpenAIHedgeOutcomeCardinality
)

// OpenAIWinnerOutcome is an allowlisted winner-selection result.
type OpenAIWinnerOutcome uint8

const (
	OpenAIWinnerOutcomeUnknown OpenAIWinnerOutcome = iota
	OpenAIWinnerOutcomePrimary
	OpenAIWinnerOutcomeHedge
	OpenAIWinnerOutcomeNoWinner
	OpenAIWinnerOutcomeCardinality
)

// OpenAILoserCancelOutcome is an allowlisted loser-cancellation result.
type OpenAILoserCancelOutcome uint8

const (
	OpenAILoserCancelOutcomeUnknown OpenAILoserCancelOutcome = iota
	OpenAILoserCancelOutcomeSucceeded
	OpenAILoserCancelOutcomeAlreadyComplete
	OpenAILoserCancelOutcomeFailed
	OpenAILoserCancelOutcomeNotRequired
	OpenAILoserCancelOutcomeCardinality
)

// OpenAILoserLifecycleEvent is a fixed, allowlisted post-hedge lifecycle event.
// It makes late loser activity observable without retaining request details.
type OpenAILoserLifecycleEvent uint8

const (
	OpenAILoserLifecycleCancelRequested OpenAILoserLifecycleEvent = iota
	OpenAILoserLifecycleTransportClosed
	OpenAILoserLifecycleTerminalAfterCancel
	OpenAILoserLifecycleUsageAfterCancel
	OpenAILoserLifecycleBillingAfterCancel
	OpenAILoserLifecycleEventCardinality
)

// OpenAIUsageOutcome is an allowlisted upstream usage-capture result.
type OpenAIUsageOutcome uint8

const (
	OpenAIUsageOutcomeUnknown OpenAIUsageOutcome = iota
	OpenAIUsageOutcomeCaptured
	OpenAIUsageOutcomeMissing
	OpenAIUsageOutcomeInvalid
	OpenAIUsageOutcomeReconciled
	OpenAIUsageOutcomeCardinality
)

// OpenAICancelReason is an allowlisted cancellation origin.
type OpenAICancelReason uint8

const (
	OpenAICancelReasonNone OpenAICancelReason = iota
	OpenAICancelReasonClient
	OpenAICancelReasonDeadline
	OpenAICancelReasonHedgeLoser
	OpenAICancelReasonUpstream
	OpenAICancelReasonShutdown
	OpenAICancelReasonCardinality
)

// OpenAIBillingOutcome is an allowlisted billing terminal state.
type OpenAIBillingOutcome uint8

const (
	OpenAIBillingOutcomeUnknown OpenAIBillingOutcome = iota
	OpenAIBillingOutcomeNotBillable
	OpenAIBillingOutcomeCharged
	OpenAIBillingOutcomeRefunded
	OpenAIBillingOutcomeFailed
	OpenAIBillingOutcomeCardinality
)

// OpenAICapacityOutcome is an allowlisted pool-capacity observation.
type OpenAICapacityOutcome uint8

const (
	OpenAICapacityOutcomeUnknown OpenAICapacityOutcome = iota
	OpenAICapacityOutcomeAvailable
	OpenAICapacityOutcomeConstrained
	OpenAICapacityOutcomeExhausted
	OpenAICapacityOutcomeCardinality
)

// OpenAIPhaseTelemetryAggregate has a fixed histogram and no dynamic labels.
type OpenAIPhaseTelemetryAggregate struct {
	Observations        uint64
	LatencyMilliseconds uint64
	LatencyHistogram    LatencyHistogram
}

// OpenAIPhaseTelemetryAggregates is indexed by OpenAILatencyPhase.
type OpenAIPhaseTelemetryAggregates [OpenAILatencyPhaseCardinality]OpenAIPhaseTelemetryAggregate

// OpenAIHedgeTelemetryAggregate is indexed by OpenAIHedgeOutcome.
type OpenAIHedgeTelemetryAggregate [OpenAIHedgeOutcomeCardinality]uint64

// OpenAIWinnerTelemetryAggregate is indexed by OpenAIWinnerOutcome.
type OpenAIWinnerTelemetryAggregate [OpenAIWinnerOutcomeCardinality]uint64

// OpenAILoserCancelTelemetryAggregate is indexed by OpenAILoserCancelOutcome.
type OpenAILoserCancelTelemetryAggregate [OpenAILoserCancelOutcomeCardinality]uint64

// OpenAILoserLifecycleTelemetryAggregate is indexed by OpenAILoserLifecycleEvent.
type OpenAILoserLifecycleTelemetryAggregate [OpenAILoserLifecycleEventCardinality]uint64

// OpenAIUsageTelemetryAggregate is indexed by OpenAIUsageOutcome.
type OpenAIUsageTelemetryAggregate [OpenAIUsageOutcomeCardinality]uint64

// OpenAICancelTelemetryAggregate is indexed by OpenAICancelReason.
type OpenAICancelTelemetryAggregate [OpenAICancelReasonCardinality]uint64

// OpenAIBillingTelemetryAggregate is indexed by OpenAIBillingOutcome.
type OpenAIBillingTelemetryAggregate [OpenAIBillingOutcomeCardinality]uint64

// OpenAICapacityTelemetryAggregate is indexed by OpenAICapacityOutcome.
type OpenAICapacityTelemetryAggregate [OpenAICapacityOutcomeCardinality]uint64

// OpenAILatencyTelemetryIdentity identifies one cumulative source stream and
// one fixed bucket. BucketStart and BucketDuration are Unix nanoseconds and
// nanoseconds respectively, keeping the persistence identity numeric.
type OpenAILatencyTelemetryIdentity struct {
	SourceID       uint64
	Generation     uint64
	BucketStart    int64
	BucketDuration int64
}

// OpenAILatencyTelemetrySnapshot is cumulative and non-draining. Its key and
// retry identity are numeric and bounded; all measurements are fixed arrays
// with no caller labels, credentials, raw errors, request content, maps, or
// slices. SourceID zero is reserved for a composite aggregate.
type OpenAILatencyTelemetrySnapshot struct {
	SchemaVersion      uint16
	Key                OpenAILatencyTelemetryKey
	SourceID           uint64
	Generation         uint64
	Sequence           uint64
	BucketStart        int64
	BucketDuration     int64
	Phases             OpenAIPhaseTelemetryAggregates
	Hedges             OpenAIHedgeTelemetryAggregate
	Winners            OpenAIWinnerTelemetryAggregate
	LoserCancellations OpenAILoserCancelTelemetryAggregate
	LoserLifecycle     OpenAILoserLifecycleTelemetryAggregate
	Usage              OpenAIUsageTelemetryAggregate
	Cancellations      OpenAICancelTelemetryAggregate
	Billing            OpenAIBillingTelemetryAggregate
	LoserCostMicros    uint64
	Capacity           OpenAICapacityTelemetryAggregate
}

func (s OpenAILatencyTelemetrySnapshot) identity() OpenAILatencyTelemetryIdentity {
	return OpenAILatencyTelemetryIdentity{
		SourceID:       s.SourceID,
		Generation:     s.Generation,
		BucketStart:    s.BucketStart,
		BucketDuration: s.BucketDuration,
	}
}

func (s *OpenAILatencyTelemetrySnapshot) setIdentity(identity OpenAILatencyTelemetryIdentity, sequence uint64) {
	s.SourceID = identity.SourceID
	s.Generation = identity.Generation
	s.Sequence = sequence
	s.BucketStart = identity.BucketStart
	s.BucketDuration = identity.BucketDuration
}

func (i OpenAILatencyTelemetryIdentity) isZero() bool {
	return i == (OpenAILatencyTelemetryIdentity{})
}

func validateOpenAILatencyTelemetryIdentity(identity OpenAILatencyTelemetryIdentity) error {
	if identity.isZero() {
		return nil // Backward-compatible legacy snapshots remain readable.
	}
	if identity.Generation == 0 {
		return errors.New("telemetry generation must be positive")
	}
	if identity.BucketDuration <= 0 {
		return errors.New("telemetry bucket duration must be positive")
	}
	return nil
}

func sameOpenAILatencyTelemetryStream(left, right OpenAILatencyTelemetryIdentity) bool {
	return left.SourceID == right.SourceID &&
		left.Generation == right.Generation &&
		left.BucketStart == right.BucketStart &&
		left.BucketDuration == right.BucketDuration
}

func sameOpenAILatencyTelemetryBucket(left, right OpenAILatencyTelemetryIdentity) bool {
	return left.BucketStart == right.BucketStart && left.BucketDuration == right.BucketDuration
}

func mergeOpenAILatencyTelemetryIdentity(
	left OpenAILatencyTelemetrySnapshot,
	right OpenAILatencyTelemetrySnapshot,
) (OpenAILatencyTelemetryIdentity, uint64, error) {
	leftIdentity := left.identity()
	rightIdentity := right.identity()
	if err := validateOpenAILatencyTelemetryIdentity(leftIdentity); err != nil {
		return OpenAILatencyTelemetryIdentity{}, 0, fmt.Errorf("validate destination telemetry identity: %w", err)
	}
	if err := validateOpenAILatencyTelemetryIdentity(rightIdentity); err != nil {
		return OpenAILatencyTelemetryIdentity{}, 0, fmt.Errorf("validate source telemetry identity: %w", err)
	}
	if leftIdentity.isZero() {
		return rightIdentity, right.Sequence, nil
	}
	if rightIdentity.isZero() {
		return OpenAILatencyTelemetryIdentity{}, 0, errors.New("cannot add identified and legacy telemetry snapshots")
	}
	if !sameOpenAILatencyTelemetryBucket(leftIdentity, rightIdentity) {
		return OpenAILatencyTelemetryIdentity{}, 0, errors.New("telemetry bucket identities do not match")
	}
	if leftIdentity.SourceID != 0 && sameOpenAILatencyTelemetryStream(leftIdentity, rightIdentity) {
		return OpenAILatencyTelemetryIdentity{}, 0, errors.New("cannot add cumulative snapshots from the same source generation")
	}
	sequence, err := checkedAddTelemetry(maxTelemetryUint64(left.Sequence, right.Sequence), 1)
	if err != nil {
		return OpenAILatencyTelemetryIdentity{}, 0, fmt.Errorf("advance composite telemetry sequence: %w", err)
	}
	return OpenAILatencyTelemetryIdentity{
		SourceID:       0,
		Generation:     1,
		BucketStart:    leftIdentity.BucketStart,
		BucketDuration: leftIdentity.BucketDuration,
	}, sequence, nil
}

// Merge additively combines disjoint snapshots for the same key. It is
// transactional: overflow, a key mismatch, or a schema mismatch leaves the
// receiver unchanged. Persistence retries should use the idempotent Store
// interface or MergeCumulative rather than replaying Merge.
func (s *OpenAILatencyTelemetrySnapshot) Merge(other OpenAILatencyTelemetrySnapshot) error {
	if s == nil {
		return errors.New("openai latency telemetry snapshot is nil")
	}
	if err := validateTelemetrySchema(s.SchemaVersion, other.SchemaVersion); err != nil {
		return err
	}
	key, err := mergeTelemetryKey(s.Key, other.Key)
	if err != nil {
		return err
	}
	identity, sequence, err := mergeOpenAILatencyTelemetryIdentity(*s, other)
	if err != nil {
		return err
	}

	next := *s
	next.Key = key
	next.setIdentity(identity, sequence)
	if next.SchemaVersion == 0 {
		next.SchemaVersion = OpenAILatencyTelemetrySchemaVersion
	}
	for phaseIndex := range next.Phases {
		next.Phases[phaseIndex].Observations, err = checkedAddTelemetry(
			next.Phases[phaseIndex].Observations,
			other.Phases[phaseIndex].Observations,
		)
		if err != nil {
			return fmt.Errorf("merge phase %d observations: %w", phaseIndex, err)
		}
		next.Phases[phaseIndex].LatencyMilliseconds, err = checkedAddTelemetry(
			next.Phases[phaseIndex].LatencyMilliseconds,
			other.Phases[phaseIndex].LatencyMilliseconds,
		)
		if err != nil {
			return fmt.Errorf("merge phase %d latency: %w", phaseIndex, err)
		}
		for bucketIndex := range next.Phases[phaseIndex].LatencyHistogram {
			next.Phases[phaseIndex].LatencyHistogram[bucketIndex], err = checkedAddTelemetry(
				next.Phases[phaseIndex].LatencyHistogram[bucketIndex],
				other.Phases[phaseIndex].LatencyHistogram[bucketIndex],
			)
			if err != nil {
				return fmt.Errorf("merge phase %d histogram bucket %d: %w", phaseIndex, bucketIndex, err)
			}
		}
	}
	if err := mergeTelemetryCounters(next.Hedges[:], other.Hedges[:]); err != nil {
		return fmt.Errorf("merge hedge telemetry: %w", err)
	}
	if err := mergeTelemetryCounters(next.Winners[:], other.Winners[:]); err != nil {
		return fmt.Errorf("merge winner telemetry: %w", err)
	}
	if err := mergeTelemetryCounters(next.LoserCancellations[:], other.LoserCancellations[:]); err != nil {
		return fmt.Errorf("merge loser cancellation telemetry: %w", err)
	}
	if err := mergeTelemetryCounters(next.LoserLifecycle[:], other.LoserLifecycle[:]); err != nil {
		return fmt.Errorf("merge loser lifecycle telemetry: %w", err)
	}
	if err := mergeTelemetryCounters(next.Usage[:], other.Usage[:]); err != nil {
		return fmt.Errorf("merge usage telemetry: %w", err)
	}
	if err := mergeTelemetryCounters(next.Cancellations[:], other.Cancellations[:]); err != nil {
		return fmt.Errorf("merge cancellation telemetry: %w", err)
	}
	if err := mergeTelemetryCounters(next.Billing[:], other.Billing[:]); err != nil {
		return fmt.Errorf("merge billing telemetry: %w", err)
	}
	next.LoserCostMicros, err = checkedAddTelemetry(next.LoserCostMicros, other.LoserCostMicros)
	if err != nil {
		return fmt.Errorf("merge loser cost micros: %w", err)
	}
	if err := mergeTelemetryCounters(next.Capacity[:], other.Capacity[:]); err != nil {
		return fmt.Errorf("merge capacity telemetry: %w", err)
	}
	*s = next
	return nil
}

// MergeCumulative applies component-wise maxima for the same key. It is
// commutative and idempotent and is suitable for reconciling repeated
// cumulative snapshots from one logical source.
func (s *OpenAILatencyTelemetrySnapshot) MergeCumulative(other OpenAILatencyTelemetrySnapshot) error {
	if s == nil {
		return errors.New("openai latency telemetry snapshot is nil")
	}
	if err := validateTelemetrySchema(s.SchemaVersion, other.SchemaVersion); err != nil {
		return err
	}
	key, err := mergeTelemetryKey(s.Key, other.Key)
	if err != nil {
		return err
	}
	if *s == (OpenAILatencyTelemetrySnapshot{}) {
		if err := validateOpenAILatencyTelemetryIdentity(other.identity()); err != nil {
			return fmt.Errorf("validate source telemetry identity: %w", err)
		}
		next := other
		next.Key = key
		if next.SchemaVersion == 0 {
			next.SchemaVersion = OpenAILatencyTelemetrySchemaVersion
		}
		*s = next
		return nil
	}

	leftIdentity := s.identity()
	rightIdentity := other.identity()
	if !leftIdentity.isZero() || !rightIdentity.isZero() {
		if err := validateOpenAILatencyTelemetryIdentity(leftIdentity); err != nil {
			return fmt.Errorf("validate destination telemetry identity: %w", err)
		}
		if err := validateOpenAILatencyTelemetryIdentity(rightIdentity); err != nil {
			return fmt.Errorf("validate source telemetry identity: %w", err)
		}
		if leftIdentity.isZero() || rightIdentity.isZero() {
			return errors.New("cannot reconcile identified and legacy telemetry snapshots")
		}
		if !sameOpenAILatencyTelemetryBucket(leftIdentity, rightIdentity) {
			return errors.New("telemetry bucket identities do not match")
		}
		if !sameOpenAILatencyTelemetryStream(leftIdentity, rightIdentity) {
			return errors.New("different telemetry sources or generations require OpenAILatencyTelemetryMerger")
		}
		if other.Sequence <= s.Sequence {
			return nil
		}
		next := other
		next.Key = key
		if next.SchemaVersion == 0 {
			next.SchemaVersion = OpenAILatencyTelemetrySchemaVersion
		}
		*s = next
		return nil
	}

	if s.SchemaVersion == 0 {
		s.SchemaVersion = OpenAILatencyTelemetrySchemaVersion
	}
	s.Key = key
	for phaseIndex := range s.Phases {
		s.Phases[phaseIndex].Observations = maxTelemetryUint64(
			s.Phases[phaseIndex].Observations,
			other.Phases[phaseIndex].Observations,
		)
		s.Phases[phaseIndex].LatencyMilliseconds = maxTelemetryUint64(
			s.Phases[phaseIndex].LatencyMilliseconds,
			other.Phases[phaseIndex].LatencyMilliseconds,
		)
		for bucketIndex := range s.Phases[phaseIndex].LatencyHistogram {
			s.Phases[phaseIndex].LatencyHistogram[bucketIndex] = maxTelemetryUint64(
				s.Phases[phaseIndex].LatencyHistogram[bucketIndex],
				other.Phases[phaseIndex].LatencyHistogram[bucketIndex],
			)
		}
	}
	mergeTelemetryCounterMax(s.Hedges[:], other.Hedges[:])
	mergeTelemetryCounterMax(s.Winners[:], other.Winners[:])
	mergeTelemetryCounterMax(s.LoserCancellations[:], other.LoserCancellations[:])
	mergeTelemetryCounterMax(s.LoserLifecycle[:], other.LoserLifecycle[:])
	mergeTelemetryCounterMax(s.Usage[:], other.Usage[:])
	mergeTelemetryCounterMax(s.Cancellations[:], other.Cancellations[:])
	mergeTelemetryCounterMax(s.Billing[:], other.Billing[:])
	mergeTelemetryCounterMax(s.Capacity[:], other.Capacity[:])
	return nil
}

type openAILatencyTelemetryStreamKey struct {
	sourceID   uint64
	generation uint64
}

// OpenAILatencyTelemetryMerger keeps one latest cumulative contribution per
// source generation. The bounded map is outside the fixed-shape persisted
// snapshot and prevents interleaved retries from being counted twice.
type OpenAILatencyTelemetryMerger struct {
	mu             sync.Mutex
	maxStreams     int
	key            OpenAILatencyTelemetryKey
	bucketStart    int64
	bucketDuration int64
	contributions  map[openAILatencyTelemetryStreamKey]OpenAILatencyTelemetrySnapshot
	snapshot       OpenAILatencyTelemetrySnapshot
}

type OpenAILatencyTelemetryMergerStats struct {
	Streams    int
	MaxStreams int
	Sequence   uint64
}

func NewOpenAILatencyTelemetryMerger(maxStreams int) (*OpenAILatencyTelemetryMerger, error) {
	if maxStreams <= 0 {
		return nil, errors.New("openai latency telemetry max streams must be positive")
	}
	return &OpenAILatencyTelemetryMerger{maxStreams: maxStreams}, nil
}

// MergeCumulative accepts a new source contribution, ignores same/older
// retries, and transactionally replaces a prior contribution on a newer
// sequence. Distinct sources and restarted generations remain additive.
func (m *OpenAILatencyTelemetryMerger) MergeCumulative(
	incoming OpenAILatencyTelemetrySnapshot,
) (bool, error) {
	if m == nil {
		return false, errors.New("openai latency telemetry merger is nil")
	}
	if err := validateTelemetrySchema(0, incoming.SchemaVersion); err != nil {
		return false, err
	}
	canonicalKey, err := canonicalOpenAILatencyTelemetryKey(incoming.Key)
	if err != nil {
		return false, fmt.Errorf("validate telemetry merge key: %w", err)
	}
	identity := incoming.identity()
	if identity.SourceID == 0 {
		return false, errors.New("telemetry merger requires a positive source id")
	}
	if err := validateOpenAILatencyTelemetryIdentity(identity); err != nil {
		return false, fmt.Errorf("validate telemetry merge identity: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.maxStreams <= 0 {
		m.maxStreams = DefaultOpenAILatencyTelemetryMaxStreams
	}
	if m.key != (OpenAILatencyTelemetryKey{}) && m.key != canonicalKey {
		return false, errors.New("telemetry merger keys do not match")
	}
	if m.bucketDuration != 0 &&
		(m.bucketStart != identity.BucketStart || m.bucketDuration != identity.BucketDuration) {
		return false, errors.New("telemetry merger bucket identities do not match")
	}

	stream := openAILatencyTelemetryStreamKey{
		sourceID:   identity.SourceID,
		generation: identity.Generation,
	}
	if current, ok := m.contributions[stream]; ok && incoming.Sequence <= current.Sequence {
		return false, nil
	}
	if _, ok := m.contributions[stream]; !ok && len(m.contributions) >= m.maxStreams {
		return false, errors.New("openai latency telemetry source limit reached")
	}
	if m.snapshot.Sequence == math.MaxUint64 {
		return false, errors.New("openai latency telemetry merger sequence overflow")
	}

	var combined OpenAILatencyTelemetrySnapshot
	for existingStream, contribution := range m.contributions {
		if existingStream == stream {
			continue
		}
		if err := mergeOpenAILatencyTelemetryPayload(&combined, contribution); err != nil {
			return false, fmt.Errorf("merge existing telemetry contribution: %w", err)
		}
	}
	if err := mergeOpenAILatencyTelemetryPayload(&combined, incoming); err != nil {
		return false, fmt.Errorf("merge incoming telemetry contribution: %w", err)
	}
	combined.SchemaVersion = OpenAILatencyTelemetrySchemaVersion
	combined.Key = canonicalKey
	combined.setIdentity(OpenAILatencyTelemetryIdentity{
		SourceID:       0,
		Generation:     1,
		BucketStart:    identity.BucketStart,
		BucketDuration: identity.BucketDuration,
	}, m.snapshot.Sequence+1)

	if m.contributions == nil {
		m.contributions = make(map[openAILatencyTelemetryStreamKey]OpenAILatencyTelemetrySnapshot)
	}
	incoming.SchemaVersion = OpenAILatencyTelemetrySchemaVersion
	incoming.Key = canonicalKey
	m.contributions[stream] = incoming
	m.key = canonicalKey
	m.bucketStart = identity.BucketStart
	m.bucketDuration = identity.BucketDuration
	m.snapshot = combined
	return true, nil
}

func (m *OpenAILatencyTelemetryMerger) Snapshot() OpenAILatencyTelemetrySnapshot {
	if m == nil {
		return OpenAILatencyTelemetrySnapshot{SchemaVersion: OpenAILatencyTelemetrySchemaVersion}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshot
}

func (m *OpenAILatencyTelemetryMerger) Stats() OpenAILatencyTelemetryMergerStats {
	if m == nil {
		return OpenAILatencyTelemetryMergerStats{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	maxStreams := m.maxStreams
	if maxStreams <= 0 {
		maxStreams = DefaultOpenAILatencyTelemetryMaxStreams
	}
	return OpenAILatencyTelemetryMergerStats{
		Streams:    len(m.contributions),
		MaxStreams: maxStreams,
		Sequence:   m.snapshot.Sequence,
	}
}

func mergeOpenAILatencyTelemetryPayload(
	destination *OpenAILatencyTelemetrySnapshot,
	source OpenAILatencyTelemetrySnapshot,
) error {
	destination.setIdentity(OpenAILatencyTelemetryIdentity{}, 0)
	source.setIdentity(OpenAILatencyTelemetryIdentity{}, 0)
	return destination.Merge(source)
}

// OpenAILatencyTelemetrySink is the persistence boundary for a future slice.
// Implementations must make storing an identical cumulative snapshot idempotent.
type OpenAILatencyTelemetrySink interface {
	StoreOpenAILatencyTelemetrySnapshot(context.Context, OpenAILatencyTelemetrySnapshot) error
}

// OpenAILatencyTelemetrySnapshotter is the read side needed by a future flusher.
type OpenAILatencyTelemetrySnapshotter interface {
	SnapshotOpenAILatencyTelemetry() OpenAILatencyTelemetrySnapshot
}

// OpenAILatencyTelemetryAggregate is a concurrency-safe, fixed-cardinality
// in-memory aggregate for exactly one telemetry key. Snapshot calls are
// cumulative, non-draining value copies, so repeated calls without intervening
// records are idempotent.
type OpenAILatencyTelemetryAggregate struct {
	mu       sync.Mutex
	snapshot OpenAILatencyTelemetrySnapshot
}

func NewOpenAILatencyTelemetryAggregate(key OpenAILatencyTelemetryKey) (*OpenAILatencyTelemetryAggregate, error) {
	sourceID := openAILatencyTelemetrySourceSequence.Add(1)
	if sourceID == 0 {
		return nil, errors.New("openai latency telemetry source id exhausted")
	}
	return NewOpenAILatencyTelemetryAggregateWithIdentity(key, OpenAILatencyTelemetryIdentity{
		SourceID:       sourceID,
		Generation:     1,
		BucketStart:    0,
		BucketDuration: 1,
	})
}

func NewOpenAILatencyTelemetryAggregateWithIdentity(
	key OpenAILatencyTelemetryKey,
	identity OpenAILatencyTelemetryIdentity,
) (*OpenAILatencyTelemetryAggregate, error) {
	canonicalKey, err := canonicalOpenAILatencyTelemetryKey(key)
	if err != nil {
		return nil, err
	}
	if identity.SourceID == 0 {
		return nil, errors.New("openai latency telemetry source id must be positive")
	}
	if err := validateOpenAILatencyTelemetryIdentity(identity); err != nil {
		return nil, fmt.Errorf("validate openai latency telemetry identity: %w", err)
	}
	snapshot := OpenAILatencyTelemetrySnapshot{
		SchemaVersion: OpenAILatencyTelemetrySchemaVersion,
		Key:           canonicalKey,
	}
	snapshot.setIdentity(identity, 0)
	return &OpenAILatencyTelemetryAggregate{snapshot: snapshot}, nil
}

func (a *OpenAILatencyTelemetryAggregate) ObservePhase(phase OpenAILatencyPhase, latency time.Duration) error {
	if a == nil {
		return errors.New("openai latency telemetry aggregate is nil")
	}
	if latency < 0 {
		return errors.New("phase latency cannot be negative")
	}
	phase = canonicalTelemetryPhase(phase)
	latencyMilliseconds := uint64(latency / time.Millisecond)

	a.mu.Lock()
	defer a.mu.Unlock()
	aggregate := &a.snapshot.Phases[phase]
	if a.snapshot.Sequence == math.MaxUint64 {
		return errors.New("telemetry sequence overflow")
	}
	if aggregate.Observations == math.MaxUint64 {
		return errors.New("phase observation counter overflow")
	}
	if math.MaxUint64-aggregate.LatencyMilliseconds < latencyMilliseconds {
		return errors.New("phase latency counter overflow")
	}
	bucket := latencyHistogramBucket(latency)
	if aggregate.LatencyHistogram[bucket] == math.MaxUint64 {
		return errors.New("phase histogram counter overflow")
	}
	aggregate.Observations++
	aggregate.LatencyMilliseconds += latencyMilliseconds
	aggregate.LatencyHistogram[bucket]++
	a.snapshot.Sequence++
	return nil
}

func (a *OpenAILatencyTelemetryAggregate) ObserveHedge(outcome OpenAIHedgeOutcome) error {
	if a == nil {
		return errors.New("openai latency telemetry aggregate is nil")
	}
	return a.increment(func(snapshot *OpenAILatencyTelemetrySnapshot) *uint64 {
		return &snapshot.Hedges[canonicalHedgeOutcome(outcome)]
	})
}

func (a *OpenAILatencyTelemetryAggregate) ObserveWinner(outcome OpenAIWinnerOutcome) error {
	if a == nil {
		return errors.New("openai latency telemetry aggregate is nil")
	}
	return a.increment(func(snapshot *OpenAILatencyTelemetrySnapshot) *uint64 {
		return &snapshot.Winners[canonicalWinnerOutcome(outcome)]
	})
}

func (a *OpenAILatencyTelemetryAggregate) ObserveLoserCancellation(outcome OpenAILoserCancelOutcome) error {
	if a == nil {
		return errors.New("openai latency telemetry aggregate is nil")
	}
	return a.increment(func(snapshot *OpenAILatencyTelemetrySnapshot) *uint64 {
		return &snapshot.LoserCancellations[canonicalLoserCancelOutcome(outcome)]
	})
}

func (a *OpenAILatencyTelemetryAggregate) ObserveLoserLifecycle(event OpenAILoserLifecycleEvent) error {
	if a == nil {
		return errors.New("openai latency telemetry aggregate is nil")
	}
	if event >= OpenAILoserLifecycleEventCardinality {
		return errors.New("openai loser lifecycle event is invalid")
	}
	return a.increment(func(snapshot *OpenAILatencyTelemetrySnapshot) *uint64 {
		return &snapshot.LoserLifecycle[event]
	})
}

// ObserveLoserLifecycleEvent is an explicit-name alias for callers recording a
// single fixed loser lifecycle event.
func (a *OpenAILatencyTelemetryAggregate) ObserveLoserLifecycleEvent(event OpenAILoserLifecycleEvent) error {
	return a.ObserveLoserLifecycle(event)
}

func (a *OpenAILatencyTelemetryAggregate) ObserveUsage(outcome OpenAIUsageOutcome) error {
	if a == nil {
		return errors.New("openai latency telemetry aggregate is nil")
	}
	return a.increment(func(snapshot *OpenAILatencyTelemetrySnapshot) *uint64 {
		return &snapshot.Usage[canonicalUsageOutcome(outcome)]
	})
}

func (a *OpenAILatencyTelemetryAggregate) ObserveCancellation(reason OpenAICancelReason) error {
	if a == nil {
		return errors.New("openai latency telemetry aggregate is nil")
	}
	return a.increment(func(snapshot *OpenAILatencyTelemetrySnapshot) *uint64 {
		return &snapshot.Cancellations[canonicalCancelReason(reason)]
	})
}

func (a *OpenAILatencyTelemetryAggregate) ObserveBilling(outcome OpenAIBillingOutcome) error {
	if a == nil {
		return errors.New("openai latency telemetry aggregate is nil")
	}
	return a.increment(func(snapshot *OpenAILatencyTelemetrySnapshot) *uint64 {
		return &snapshot.Billing[canonicalBillingOutcome(outcome)]
	})
}

func (a *OpenAILatencyTelemetryAggregate) ObserveLoserCostMicros(amount uint64) error {
	if a == nil {
		return errors.New("openai latency telemetry aggregate is nil")
	}
	return a.incrementBy(amount, func(snapshot *OpenAILatencyTelemetrySnapshot) *uint64 {
		return &snapshot.LoserCostMicros
	})
}

func (a *OpenAILatencyTelemetryAggregate) ObserveCapacity(outcome OpenAICapacityOutcome) error {
	if a == nil {
		return errors.New("openai latency telemetry aggregate is nil")
	}
	return a.increment(func(snapshot *OpenAILatencyTelemetrySnapshot) *uint64 {
		return &snapshot.Capacity[canonicalCapacityOutcome(outcome)]
	})
}

// Merge additively merges a disjoint fixed-cardinality snapshot for this key.
func (a *OpenAILatencyTelemetryAggregate) Merge(snapshot OpenAILatencyTelemetrySnapshot) error {
	if a == nil {
		return errors.New("openai latency telemetry aggregate is nil")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.snapshot.Merge(snapshot)
}

// Snapshot returns a cumulative value copy and never resets counters.
func (a *OpenAILatencyTelemetryAggregate) Snapshot() OpenAILatencyTelemetrySnapshot {
	if a == nil {
		return OpenAILatencyTelemetrySnapshot{SchemaVersion: OpenAILatencyTelemetrySchemaVersion}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.snapshot
}

func (a *OpenAILatencyTelemetryAggregate) SnapshotOpenAILatencyTelemetry() OpenAILatencyTelemetrySnapshot {
	return a.Snapshot()
}

func (a *OpenAILatencyTelemetryAggregate) increment(counter func(*OpenAILatencyTelemetrySnapshot) *uint64) error {
	return a.incrementBy(1, counter)
}

func (a *OpenAILatencyTelemetryAggregate) incrementBy(amount uint64, counter func(*OpenAILatencyTelemetrySnapshot) *uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	value := counter(&a.snapshot)
	if a.snapshot.Sequence == math.MaxUint64 {
		return errors.New("telemetry sequence overflow")
	}
	if math.MaxUint64-*value < amount {
		return errors.New("telemetry counter overflow")
	}
	*value += amount
	a.snapshot.Sequence++
	return nil
}

func mergeTelemetryKey(left, right OpenAILatencyTelemetryKey) (OpenAILatencyTelemetryKey, error) {
	if err := validateOpenAILatencyTelemetryKey(right); err != nil {
		return OpenAILatencyTelemetryKey{}, fmt.Errorf("validate source telemetry key: %w", err)
	}
	if left == (OpenAILatencyTelemetryKey{}) {
		return right, nil
	}
	if err := validateOpenAILatencyTelemetryKey(left); err != nil {
		return OpenAILatencyTelemetryKey{}, fmt.Errorf("validate destination telemetry key: %w", err)
	}
	if left != right {
		return OpenAILatencyTelemetryKey{}, errors.New("telemetry keys do not match")
	}
	return left, nil
}

func validateTelemetrySchema(left, right uint16) error {
	if left != 0 && left != OpenAILatencyTelemetrySchemaVersion {
		return fmt.Errorf("unsupported destination telemetry schema version %d", left)
	}
	if right != 0 && right != OpenAILatencyTelemetrySchemaVersion {
		return fmt.Errorf("unsupported source telemetry schema version %d", right)
	}
	return nil
}

func checkedAddTelemetry(left, right uint64) (uint64, error) {
	if math.MaxUint64-left < right {
		return 0, errors.New("counter overflow")
	}
	return left + right, nil
}

func mergeTelemetryCounters(destination, source []uint64) error {
	for index := range destination {
		value, err := checkedAddTelemetry(destination[index], source[index])
		if err != nil {
			return fmt.Errorf("counter %d: %w", index, err)
		}
		destination[index] = value
	}
	return nil
}

func mergeTelemetryCounterMax(destination, source []uint64) {
	for index := range destination {
		destination[index] = maxTelemetryUint64(destination[index], source[index])
	}
}

func maxTelemetryUint64(left, right uint64) uint64 {
	if left >= right {
		return left
	}
	return right
}

func canonicalAttemptRole(role OpenAIAttemptRole) OpenAIAttemptRole {
	if role >= OpenAIAttemptRoleCardinality {
		return OpenAIAttemptRoleUnknown
	}
	return role
}

func canonicalTelemetryPhase(phase OpenAILatencyPhase) OpenAILatencyPhase {
	if phase >= OpenAILatencyPhaseCardinality {
		return OpenAILatencyPhaseUnknown
	}
	return phase
}

func canonicalHedgeOutcome(outcome OpenAIHedgeOutcome) OpenAIHedgeOutcome {
	if outcome >= OpenAIHedgeOutcomeCardinality {
		return OpenAIHedgeOutcomeNone
	}
	return outcome
}

func canonicalWinnerOutcome(outcome OpenAIWinnerOutcome) OpenAIWinnerOutcome {
	if outcome >= OpenAIWinnerOutcomeCardinality {
		return OpenAIWinnerOutcomeUnknown
	}
	return outcome
}

func canonicalLoserCancelOutcome(outcome OpenAILoserCancelOutcome) OpenAILoserCancelOutcome {
	if outcome >= OpenAILoserCancelOutcomeCardinality {
		return OpenAILoserCancelOutcomeUnknown
	}
	return outcome
}

func canonicalUsageOutcome(outcome OpenAIUsageOutcome) OpenAIUsageOutcome {
	if outcome >= OpenAIUsageOutcomeCardinality {
		return OpenAIUsageOutcomeUnknown
	}
	return outcome
}

func canonicalCancelReason(reason OpenAICancelReason) OpenAICancelReason {
	if reason >= OpenAICancelReasonCardinality {
		return OpenAICancelReasonNone
	}
	return reason
}

func canonicalBillingOutcome(outcome OpenAIBillingOutcome) OpenAIBillingOutcome {
	if outcome >= OpenAIBillingOutcomeCardinality {
		return OpenAIBillingOutcomeUnknown
	}
	return outcome
}

func canonicalCapacityOutcome(outcome OpenAICapacityOutcome) OpenAICapacityOutcome {
	if outcome >= OpenAICapacityOutcomeCardinality {
		return OpenAICapacityOutcomeUnknown
	}
	return outcome
}
