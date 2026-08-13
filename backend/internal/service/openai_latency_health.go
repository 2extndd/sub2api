package service

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appconfig "github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/tidwall/gjson"
)

const (
	// OpenAILatencyIncidentWindow is the rolling provider-incident correlation window.
	OpenAILatencyIncidentWindow = time.Duration(appconfig.DefaultOpenAILatencyShadowIncidentWindowSeconds) * time.Second
	// OpenAILatencyIncidentMinAffectedAccounts is the minimum number of distinct
	// accounts that must report the same failure fingerprint.
	OpenAILatencyIncidentMinAffectedAccounts = appconfig.DefaultOpenAILatencyShadowIncidentMinAffectedAccounts
	// OpenAILatencyIncidentMinAffectedFraction is the minimum affected/sampled ratio.
	OpenAILatencyIncidentMinAffectedFraction = appconfig.DefaultOpenAILatencyShadowIncidentMinAffectedFraction

	OpenAILatencyPenalizedCooldown   = time.Duration(appconfig.DefaultOpenAILatencyShadowPenalizedCooldownSeconds) * time.Second
	OpenAILatencyQuarantinedCooldown = time.Duration(appconfig.DefaultOpenAILatencyShadowQuarantinedCooldownSeconds) * time.Second
	OpenAILatencyHardInvalidCooldown = time.Duration(appconfig.DefaultOpenAILatencyShadowHardInvalidCooldownSeconds) * time.Second

	// OpenAILatencyQuarantineCap is the maximum number of accounts the policy may
	// place in soft quarantine at once. The current count comes from PoolSnapshot.
	OpenAILatencyQuarantineCap = appconfig.DefaultOpenAILatencyShadowQuarantineCap
	// OpenAILatencyHardValidFloor is the number of hard-valid accounts that must
	// remain after hard invalidation. The current count comes from PoolSnapshot.
	OpenAILatencyHardValidFloor = appconfig.DefaultOpenAILatencyShadowHardValidFloor

	OpenAILatencyTimeBucketDuration = 15 * time.Second
	OpenAILatencyTimeBucketCount    = int(OpenAILatencyIncidentWindow / OpenAILatencyTimeBucketDuration)

	DefaultLatencyHealthMaxTrackedAccounts    = appconfig.DefaultOpenAILatencyShadowMaxTrackedAccounts
	DefaultLatencyHealthMaxCohortsPerAccount  = appconfig.DefaultOpenAILatencyShadowMaxCohortsPerAccount
	DefaultLatencyHealthMaxTracks             = appconfig.DefaultOpenAILatencyShadowMaxTracks
	MaximumLatencyHealthTrackedAccounts       = appconfig.MaximumOpenAILatencyShadowTrackedAccounts
	MaximumLatencyHealthCohortsPerAccount     = appconfig.MaximumOpenAILatencyShadowCohortsPerAccount
	MaximumLatencyHealthTracks                = appconfig.MaximumOpenAILatencyShadowTracks
	DefaultLatencyHealthWeightFloor           = appconfig.DefaultOpenAILatencyShadowWeightFloor
	DefaultLatencyHealthQuarantineCap         = appconfig.DefaultOpenAILatencyShadowQuarantineCap
	DefaultLatencyHealthHardValidFloor        = appconfig.DefaultOpenAILatencyShadowHardValidFloor
	DefaultLatencyHealthIncidentWindow        = time.Duration(appconfig.DefaultOpenAILatencyShadowIncidentWindowSeconds) * time.Second
	DefaultLatencyHealthIncidentMinAccounts   = appconfig.DefaultOpenAILatencyShadowIncidentMinAffectedAccounts
	DefaultLatencyHealthIncidentMinFraction   = appconfig.DefaultOpenAILatencyShadowIncidentMinAffectedFraction
	DefaultLatencyHealthPenalizedCooldown     = time.Duration(appconfig.DefaultOpenAILatencyShadowPenalizedCooldownSeconds) * time.Second
	DefaultLatencyHealthQuarantinedCooldown   = time.Duration(appconfig.DefaultOpenAILatencyShadowQuarantinedCooldownSeconds) * time.Second
	DefaultLatencyHealthHardInvalidCooldown   = time.Duration(appconfig.DefaultOpenAILatencyShadowHardInvalidCooldownSeconds) * time.Second
	DefaultLatencyHealthLatencyHalfLife       = time.Duration(appconfig.DefaultOpenAILatencyShadowLatencyHalfLifeSeconds) * time.Second
	DefaultLatencyHealthErrorHalfLife         = time.Duration(appconfig.DefaultOpenAILatencyShadowErrorHalfLifeSeconds) * time.Second
	OpenAILatencyHistogramBucketCount         = 16
	openAILatencyHistogramFiniteBoundaryCount = OpenAILatencyHistogramBucketCount - 1
)

var (
	ErrLatencyHealthAccountLimit = errors.New("openai latency health account limit reached")
	ErrLatencyHealthTrackLimit   = errors.New("openai latency health track limit reached")
	ErrLatencyHealthNotFound     = errors.New("openai latency health state not found")
	ErrLatencyHealthInvalidState = errors.New("openai latency health state does not allow this operation")

	openAILatencyHistogramUpperBounds = [openAILatencyHistogramFiniteBoundaryCount]time.Duration{
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
	}
)

// Clock is injected into latency health so policy and rolling-window behavior
// can be tested without sleeping.
type Clock interface {
	Now() time.Time
}

// SystemClock can be explicitly injected by production wiring in a later slice.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

// LatencyHealthConfig owns bounded-cardinality controls and shadow-only policy.
// No field is wired into active account selection in this package.
type LatencyHealthConfig struct {
	Enabled                     bool
	MaxTrackedAccounts          int
	MaxCohortsPerAccount        int
	MaxTracks                   int
	WeightFloor                 float64
	QuarantineCap               int
	HardValidFloor              int
	IncidentWindow              time.Duration
	IncidentMinAffectedAccounts int
	IncidentMinAffectedFraction float64
	PenalizedCooldown           time.Duration
	QuarantinedCooldown         time.Duration
	HardInvalidCooldown         time.Duration
	LatencyHalfLife             time.Duration
	ErrorHalfLife               time.Duration
}

// DefaultLatencyHealthConfig is disabled by default but carries a complete,
// validated policy so enabling it does not silently activate zero values.
func DefaultLatencyHealthConfig() LatencyHealthConfig {
	return LatencyHealthConfig{
		Enabled:                     false,
		MaxTrackedAccounts:          DefaultLatencyHealthMaxTrackedAccounts,
		MaxCohortsPerAccount:        DefaultLatencyHealthMaxCohortsPerAccount,
		MaxTracks:                   DefaultLatencyHealthMaxTracks,
		WeightFloor:                 DefaultLatencyHealthWeightFloor,
		QuarantineCap:               DefaultLatencyHealthQuarantineCap,
		HardValidFloor:              DefaultLatencyHealthHardValidFloor,
		IncidentWindow:              DefaultLatencyHealthIncidentWindow,
		IncidentMinAffectedAccounts: DefaultLatencyHealthIncidentMinAccounts,
		IncidentMinAffectedFraction: DefaultLatencyHealthIncidentMinFraction,
		PenalizedCooldown:           DefaultLatencyHealthPenalizedCooldown,
		QuarantinedCooldown:         DefaultLatencyHealthQuarantinedCooldown,
		HardInvalidCooldown:         DefaultLatencyHealthHardInvalidCooldown,
		LatencyHalfLife:             DefaultLatencyHealthLatencyHalfLife,
		ErrorHalfLife:               DefaultLatencyHealthErrorHalfLife,
	}
}

// Validate returns configuration errors instead of panicking. Cardinality
// values are still validated while disabled so a bad config cannot become live
// merely by flipping Enabled later.
func (c LatencyHealthConfig) Validate() error {
	minimumCardinality := 0
	minimumPositivePolicy := time.Duration(0)
	minimumPositiveCount := 0
	if c.Enabled {
		minimumCardinality = 1
		minimumPositivePolicy = 1
		minimumPositiveCount = 1
	}
	if c.MaxTrackedAccounts < minimumCardinality || c.MaxTrackedAccounts > MaximumLatencyHealthTrackedAccounts {
		return fmt.Errorf("max tracked accounts must be between %d and %d", minimumCardinality, MaximumLatencyHealthTrackedAccounts)
	}
	if c.MaxCohortsPerAccount < minimumCardinality || c.MaxCohortsPerAccount > MaximumLatencyHealthCohortsPerAccount {
		return fmt.Errorf("max cohorts per account must be between %d and %d", minimumCardinality, MaximumLatencyHealthCohortsPerAccount)
	}
	if c.MaxTracks < minimumCardinality || c.MaxTracks > MaximumLatencyHealthTracks {
		return fmt.Errorf("max tracks must be between %d and %d", minimumCardinality, MaximumLatencyHealthTracks)
	}
	if c.Enabled && c.MaxTracks < 2*c.MaxTrackedAccounts {
		return fmt.Errorf("max tracks must reserve aggregate and overflow tracks for every account (at least %d)", 2*c.MaxTrackedAccounts)
	}
	if math.IsNaN(c.WeightFloor) || math.IsInf(c.WeightFloor, 0) || c.WeightFloor > 1 ||
		(c.Enabled && c.WeightFloor <= 0) || (!c.Enabled && c.WeightFloor < 0) {
		return errors.New("weight floor must be finite and in (0, 1] when enabled")
	}
	if c.QuarantineCap < 0 {
		return errors.New("quarantine cap must be non-negative")
	}
	if c.HardValidFloor < 0 {
		return errors.New("hard-valid floor must be non-negative")
	}
	if c.IncidentWindow < minimumPositivePolicy {
		return errors.New("incident window must be positive when enabled")
	}
	if c.IncidentMinAffectedAccounts < minimumPositiveCount {
		return errors.New("incident minimum affected accounts must be positive when enabled")
	}
	if math.IsNaN(c.IncidentMinAffectedFraction) || math.IsInf(c.IncidentMinAffectedFraction, 0) ||
		c.IncidentMinAffectedFraction > 1 ||
		(c.Enabled && c.IncidentMinAffectedFraction <= 0) || (!c.Enabled && c.IncidentMinAffectedFraction < 0) {
		return errors.New("incident minimum affected fraction must be finite and in (0, 1] when enabled")
	}
	policyDurations := []struct {
		name  string
		value time.Duration
	}{
		{name: "penalized cooldown", value: c.PenalizedCooldown},
		{name: "quarantined cooldown", value: c.QuarantinedCooldown},
		{name: "hard-invalid cooldown", value: c.HardInvalidCooldown},
		{name: "latency half-life", value: c.LatencyHalfLife},
		{name: "error half-life", value: c.ErrorHalfLife},
	}
	for _, policyDuration := range policyDurations {
		if policyDuration.value < minimumPositivePolicy {
			return fmt.Errorf("%s must be positive when enabled", policyDuration.name)
		}
	}
	return nil
}

// ValidateLatencyHealthConfig is the package-level validation entry point for
// config adapters that do not own a concrete service.
func ValidateLatencyHealthConfig(c LatencyHealthConfig) error { return c.Validate() }

// OpenAIEndpointClass is a bounded endpoint dimension for CohortKey.
type OpenAIEndpointClass uint8

const (
	OpenAIEndpointUnknown OpenAIEndpointClass = iota
	OpenAIEndpointResponses
	OpenAIEndpointChatCompletions
	OpenAIEndpointEmbeddings
	OpenAIEndpointImages
	OpenAIEndpointAudio
	OpenAIEndpointBatches
	openAIEndpointClassCount
)

// OpenAIModelClass is a bounded model dimension for CohortKey.
type OpenAIModelClass uint8

const (
	OpenAIModelUnknown OpenAIModelClass = iota
	OpenAIModelText
	OpenAIModelReasoning
	OpenAIModelEmbedding
	OpenAIModelImage
	OpenAIModelAudio
	openAIModelClassCount
)

// OpenAIInputSizeClass avoids putting raw token counts into cohort identities.
type OpenAIInputSizeClass uint8

const (
	OpenAIInputSizeUnknown OpenAIInputSizeClass = iota
	OpenAIInputSizeSmall
	OpenAIInputSizeMedium
	OpenAIInputSizeLarge
	openAIInputSizeClassCount
)

// OpenAIReasoningClass bounds request-level reasoning configuration without
// retaining the raw reasoning-effort string.
type OpenAIReasoningClass uint8

const (
	OpenAIReasoningUnknown OpenAIReasoningClass = iota
	OpenAIReasoningNone
	OpenAIReasoningMinimal
	OpenAIReasoningLow
	OpenAIReasoningMedium
	OpenAIReasoningHigh
	OpenAIReasoningXHigh
	openAIReasoningClassCount
)

// OpenAIToolUseClass bounds the number of exposed tools. A negative count maps
// to unknown so callers can distinguish unavailable metadata from an explicit
// empty tool list.
type OpenAIToolUseClass uint8

const (
	OpenAIToolUseUnknown OpenAIToolUseClass = iota
	OpenAIToolUseNone
	OpenAIToolUseOne
	OpenAIToolUseTwoToFour
	OpenAIToolUseFivePlus
	openAIToolUseClassCount
)

// CohortKey is a canonical, comparable, fixed-width workload identity. It never
// retains raw endpoint/model/reasoning strings. Bits 0-2 are endpoint, 3-5
// model, 6-7 input size, bit 8 streaming, 9-11 reasoning, and 12-14 tool use.
type CohortKey uint16

const (
	UnknownCohortKey  CohortKey = 0
	OverflowCohortKey CohortKey = CohortKey(math.MaxUint16)
)

// NewCohortKey builds a canonical key from bounded dimensions.
func NewCohortKey(
	endpoint OpenAIEndpointClass,
	model OpenAIModelClass,
	streaming bool,
	inputSize OpenAIInputSizeClass,
	reasoning OpenAIReasoningClass,
	toolUse OpenAIToolUseClass,
) CohortKey {
	if endpoint >= openAIEndpointClassCount {
		endpoint = OpenAIEndpointUnknown
	}
	if model >= openAIModelClassCount {
		model = OpenAIModelUnknown
	}
	if inputSize >= openAIInputSizeClassCount {
		inputSize = OpenAIInputSizeUnknown
	}
	if reasoning >= openAIReasoningClassCount {
		reasoning = OpenAIReasoningUnknown
	}
	if toolUse >= openAIToolUseClassCount {
		toolUse = OpenAIToolUseUnknown
	}

	key := CohortKey(endpoint) |
		CohortKey(model)<<3 |
		CohortKey(inputSize)<<6 |
		CohortKey(reasoning)<<9 |
		CohortKey(toolUse)<<12
	if streaming {
		key |= 1 << 8
	}
	return key
}

// CanonicalCohortKey classifies raw request attributes into finite dimensions;
// no caller-provided string becomes a stored map key.
func CanonicalCohortKey(
	endpoint, model string,
	streaming bool,
	inputTokens int64,
	reasoningEffort string,
	toolCount int,
) CohortKey {
	return NewCohortKey(
		classifyOpenAIEndpoint(endpoint),
		classifyOpenAIModel(model),
		streaming,
		classifyOpenAIInputSize(inputTokens),
		classifyOpenAIReasoning(reasoningEffort),
		classifyOpenAIToolUse(toolCount),
	)
}

// CanonicalOpenAIResponsesCohort reduces a native request to fixed dimensions.
// It never returns model names, body content, tool schemas, or reasoning text.
func CanonicalOpenAIResponsesCohort(body []byte, model string) CohortKey {
	streaming := gjson.GetBytes(body, "stream").Bool()
	reasoningEffort := strings.TrimSpace(gjson.GetBytes(body, "reasoning.effort").String())
	if reasoningEffort == "" {
		reasoningEffort = strings.TrimSpace(gjson.GetBytes(body, "reasoning_effort").String())
	}
	toolCount := -1
	if tools := gjson.GetBytes(body, "tools"); tools.Exists() && tools.IsArray() {
		toolCount = len(tools.Array())
	}
	// Before upstream usage is available, body bytes are a stable bounded proxy
	// for input class. It is intentionally coarse and never persisted verbatim.
	inputSize := OpenAIInputSizeUnknown
	switch {
	case len(body) == 0:
	case len(body) <= 16*1024:
		inputSize = OpenAIInputSizeSmall
	case len(body) <= 128*1024:
		inputSize = OpenAIInputSizeMedium
	default:
		inputSize = OpenAIInputSizeLarge
	}
	return NewCohortKey(
		OpenAIEndpointResponses,
		classifyOpenAIModel(model),
		streaming,
		inputSize,
		classifyOpenAIReasoning(reasoningEffort),
		classifyOpenAIToolUse(toolCount),
	)
}

// ClassifyAdaptiveLatencyFailure converts runtime errors to a secret-safe,
// fixed-cardinality fingerprint. Raw error strings are never retained.
func ClassifyAdaptiveLatencyFailure(err error, statusCode int, streamStarted bool) FailureFingerprint {
	if failure, ok := ClassifyHTTPFailure(statusCode); ok {
		return failure
	}
	if err == nil {
		return FailureFingerprintNone
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return FailureFingerprintTimeout
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		if networkError.Timeout() {
			return FailureFingerprintTimeout
		}
		return FailureFingerprintConnection
	}
	if streamStarted {
		return FailureFingerprintStream
	}
	return FailureFingerprintOther
}

// Canonical rejects non-canonical high bits and clamps invalid dimensions.
func (k CohortKey) Canonical() CohortKey {
	if k == OverflowCohortKey {
		return k
	}
	if k > (1<<15)-1 {
		return UnknownCohortKey
	}
	return NewCohortKey(
		OpenAIEndpointClass(k&0b111),
		OpenAIModelClass((k>>3)&0b111),
		k&(1<<8) != 0,
		OpenAIInputSizeClass((k>>6)&0b11),
		OpenAIReasoningClass((k>>9)&0b111),
		OpenAIToolUseClass((k>>12)&0b111),
	)
}

func (k CohortKey) String() string {
	if k == OverflowCohortKey {
		return "overflow"
	}
	k = k.Canonical()
	endpoint := [...]string{"unknown", "responses", "chat", "embeddings", "images", "audio", "batches"}
	model := [...]string{"unknown", "text", "reasoning", "embedding", "image", "audio"}
	size := [...]string{"unknown", "small", "medium", "large"}
	reasoning := [...]string{"reasoning-unknown", "reasoning-none", "reasoning-minimal", "reasoning-low", "reasoning-medium", "reasoning-high", "reasoning-xhigh"}
	toolUse := [...]string{"tools-unknown", "tools-none", "tools-one", "tools-2-4", "tools-5+"}
	stream := "nonstream"
	if k&(1<<8) != 0 {
		stream = "stream"
	}
	return endpoint[int(k&0b111)] + "/" +
		model[int((k>>3)&0b111)] + "/" +
		size[int((k>>6)&0b11)] + "/" +
		stream + "/" +
		reasoning[int((k>>9)&0b111)] + "/" +
		toolUse[int((k>>12)&0b111)]
}

func classifyOpenAIEndpoint(endpoint string) OpenAIEndpointClass {
	normalized := strings.ToLower(strings.TrimSpace(endpoint))
	if cut := strings.IndexAny(normalized, "?#"); cut >= 0 {
		normalized = normalized[:cut]
	}
	normalized = strings.TrimRight(normalized, "/")

	switch {
	case strings.HasSuffix(normalized, "/responses") || normalized == "responses":
		return OpenAIEndpointResponses
	case strings.HasSuffix(normalized, "/chat/completions") || normalized == "chat/completions":
		return OpenAIEndpointChatCompletions
	case strings.HasSuffix(normalized, "/embeddings") || normalized == "embeddings":
		return OpenAIEndpointEmbeddings
	case strings.Contains(normalized, "/images/") || normalized == "images":
		return OpenAIEndpointImages
	case strings.Contains(normalized, "/audio/") || normalized == "audio":
		return OpenAIEndpointAudio
	case strings.HasSuffix(normalized, "/batches") || normalized == "batches":
		return OpenAIEndpointBatches
	default:
		return OpenAIEndpointUnknown
	}
}

func classifyOpenAIModel(model string) OpenAIModelClass {
	normalized := strings.ToLower(strings.TrimSpace(model))
	if normalized == "" {
		return OpenAIModelUnknown
	}
	switch {
	case strings.Contains(normalized, "embedding"):
		return OpenAIModelEmbedding
	case strings.Contains(normalized, "image"), strings.HasPrefix(normalized, "dall-e"):
		return OpenAIModelImage
	case strings.Contains(normalized, "audio"), strings.HasPrefix(normalized, "whisper"), strings.HasPrefix(normalized, "tts"):
		return OpenAIModelAudio
	case strings.HasPrefix(normalized, "o1"), strings.HasPrefix(normalized, "o3"), strings.HasPrefix(normalized, "o4"), strings.Contains(normalized, "reasoning"):
		return OpenAIModelReasoning
	default:
		return OpenAIModelText
	}
}

func classifyOpenAIInputSize(tokens int64) OpenAIInputSizeClass {
	switch {
	case tokens <= 0:
		return OpenAIInputSizeUnknown
	case tokens <= 4_096:
		return OpenAIInputSizeSmall
	case tokens <= 32_768:
		return OpenAIInputSizeMedium
	default:
		return OpenAIInputSizeLarge
	}
}

func classifyOpenAIReasoning(effort string) OpenAIReasoningClass {
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "none":
		return OpenAIReasoningNone
	case "minimal":
		return OpenAIReasoningMinimal
	case "low":
		return OpenAIReasoningLow
	case "medium":
		return OpenAIReasoningMedium
	case "high":
		return OpenAIReasoningHigh
	case "xhigh":
		return OpenAIReasoningXHigh
	default:
		return OpenAIReasoningUnknown
	}
}

func classifyOpenAIToolUse(toolCount int) OpenAIToolUseClass {
	switch {
	case toolCount < 0:
		return OpenAIToolUseUnknown
	case toolCount == 0:
		return OpenAIToolUseNone
	case toolCount == 1:
		return OpenAIToolUseOne
	case toolCount <= 4:
		return OpenAIToolUseTwoToFour
	default:
		return OpenAIToolUseFivePlus
	}
}

// FailureFingerprint is a fixed-cardinality, secret-safe failure identity.
// HTTP 500-599 occupy 100 stable values; actionable authentication statuses
// and runtime failures use allowlisted constants. Raw errors and provider
// response bodies are never retained.
type FailureFingerprint uint8

const (
	FailureFingerprintNone FailureFingerprint = iota
	failureFingerprintHTTP5xxBase
	// Values 1 through 100 are reserved for HTTP 500 through 599.
	failureFingerprintHTTP5xxLast  FailureFingerprint = failureFingerprintHTTP5xxBase + 99
	FailureFingerprintTimeout      FailureFingerprint = failureFingerprintHTTP5xxLast + 1
	FailureFingerprintConnection   FailureFingerprint = failureFingerprintHTTP5xxLast + 2
	FailureFingerprintStream       FailureFingerprint = failureFingerprintHTTP5xxLast + 3
	FailureFingerprintProtocol     FailureFingerprint = failureFingerprintHTTP5xxLast + 4
	FailureFingerprintCapacity     FailureFingerprint = failureFingerprintHTTP5xxLast + 5
	FailureFingerprintUnauthorized FailureFingerprint = failureFingerprintHTTP5xxLast + 6
	FailureFingerprintForbidden    FailureFingerprint = failureFingerprintHTTP5xxLast + 7
	FailureFingerprintOther        FailureFingerprint = failureFingerprintHTTP5xxLast + 8
	failureFingerprintCardinality  FailureFingerprint = failureFingerprintHTTP5xxLast + 9
)

// ClassifyHTTPFailure classifies authentication-invalid statuses and every
// HTTP 500-599 status. Other 4xx responses remain neutral policy outcomes.
func ClassifyHTTPFailure(statusCode int) (FailureFingerprint, bool) {
	switch statusCode {
	case 401:
		return FailureFingerprintUnauthorized, true
	case 403:
		return FailureFingerprintForbidden, true
	}
	if statusCode < 500 || statusCode > 599 {
		return FailureFingerprintNone, false
	}
	return failureFingerprintHTTP5xxBase + FailureFingerprint(statusCode-500), true
}

// HTTPStatus returns the original status for an HTTP fingerprint.
func (f FailureFingerprint) HTTPStatus() (int, bool) {
	switch f {
	case FailureFingerprintUnauthorized:
		return 401, true
	case FailureFingerprintForbidden:
		return 403, true
	}
	if f < failureFingerprintHTTP5xxBase || f > failureFingerprintHTTP5xxLast {
		return 0, false
	}
	return 500 + int(f-failureFingerprintHTTP5xxBase), true
}

func (f FailureFingerprint) canonical() FailureFingerprint {
	if f < failureFingerprintCardinality {
		return f
	}
	return FailureFingerprintOther
}

func (f FailureFingerprint) String() string {
	if status, ok := f.HTTPStatus(); ok {
		return "http_" + strconv.Itoa(status)
	}
	switch f.canonical() {
	case FailureFingerprintNone:
		return "none"
	case FailureFingerprintTimeout:
		return "timeout"
	case FailureFingerprintConnection:
		return "connection"
	case FailureFingerprintStream:
		return "stream"
	case FailureFingerprintProtocol:
		return "protocol"
	case FailureFingerprintCapacity:
		return "capacity"
	case FailureFingerprintUnauthorized:
		return "http_401"
	case FailureFingerprintForbidden:
		return "http_403"
	default:
		return "other"
	}
}

// HealthState is an explicit account/cohort policy state.
type HealthState uint8

const (
	HealthStateHealthy HealthState = iota
	HealthStatePenalized
	HealthStateQuarantined
	HealthStateProbation
	HealthStateHardInvalid
)

func (s HealthState) String() string {
	switch s {
	case HealthStateHealthy:
		return "healthy"
	case HealthStatePenalized:
		return "penalized"
	case HealthStateQuarantined:
		return "quarantined"
	case HealthStateProbation:
		return "probation"
	case HealthStateHardInvalid:
		return "hard-invalid"
	default:
		return "unknown"
	}
}

// PoolSnapshot is supplied by the scheduler snapshot owner. Latency health does
// not query a repository or infer global pool safety from local observations.
type PoolSnapshot struct {
	QuarantinedAccounts int
	HardValidAccounts   int
}

// LatencyHealthPoolSnapshot is a domain-specific alias for callers that prefer
// an explicit name.
type LatencyHealthPoolSnapshot = PoolSnapshot

func (p PoolSnapshot) Validate() error {
	if p.QuarantinedAccounts < 0 {
		return errors.New("quarantined account count cannot be negative")
	}
	if p.HardValidAccounts < 0 {
		return errors.New("hard-valid account count cannot be negative")
	}
	return nil
}

// LatencyHealthTransition identifies an account-level policy reservation.
type LatencyHealthTransition uint8

const (
	LatencyHealthTransitionSoftQuarantine LatencyHealthTransition = iota + 1
	LatencyHealthTransitionHardInvalid

	DefaultLatencyHealthTransitionReservationTTL = 2 * time.Minute
)

// LatencyHealthTransitionReservation contains only the bounded inputs needed by
// a transition guard. AccountID makes repeated aggregate/cohort reservations
// idempotent for one account.
type LatencyHealthTransitionReservation struct {
	AccountID      int64
	Transition     LatencyHealthTransition
	PoolSnapshot   PoolSnapshot
	QuarantineCap  int
	HardValidFloor int
	OwnerToken     uint64
	TTL            time.Duration
}

// LatencyHealthTransitionGuard is the account-level atomic reservation port.
// A later global implementation can use the context for Redis operations while
// this slice keeps the default implementation process-local and bounded.
type LatencyHealthTransitionGuard interface {
	Reserve(context.Context, LatencyHealthTransitionReservation) (bool, error)
	Renew(context.Context, int64, LatencyHealthTransition, uint64, time.Duration) (bool, error)
	Release(context.Context, int64, LatencyHealthTransition, uint64) error
}

// LatencyHealthTransitionGuardStats exposes bounded local reservation counts.
type LatencyHealthTransitionGuardStats struct {
	MaxAccounts     int
	SoftQuarantined int
	HardInvalid     int
}

// LocalLatencyHealthTransitionGuard serializes stale PoolSnapshot checks with
// local reservations so concurrent transitions cannot overshoot a cap/floor.
type latencyHealthTransitionLease struct {
	ownerToken uint64
	expiresAt  time.Time
}

type LocalLatencyHealthTransitionGuard struct {
	mu              sync.Mutex
	maxAccounts     int
	softQuarantined map[int64]latencyHealthTransitionLease
	hardInvalid     map[int64]latencyHealthTransitionLease
}

func NewLocalLatencyHealthTransitionGuard(maxAccounts int) (*LocalLatencyHealthTransitionGuard, error) {
	if maxAccounts <= 0 || maxAccounts > MaximumLatencyHealthTrackedAccounts {
		return nil, fmt.Errorf("local transition guard max accounts must be between 1 and %d", MaximumLatencyHealthTrackedAccounts)
	}
	return &LocalLatencyHealthTransitionGuard{
		maxAccounts:     maxAccounts,
		softQuarantined: make(map[int64]latencyHealthTransitionLease),
		hardInvalid:     make(map[int64]latencyHealthTransitionLease),
	}, nil
}

func (g *LocalLatencyHealthTransitionGuard) Reserve(ctx context.Context, reservation LatencyHealthTransitionReservation) (bool, error) {
	if g == nil {
		return false, errors.New("local latency health transition guard is nil")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if reservation.AccountID <= 0 {
		return false, errors.New("transition reservation account id must be positive")
	}
	if reservation.OwnerToken == 0 {
		return false, errors.New("transition reservation owner token must be positive")
	}
	if reservation.TTL <= 0 {
		return false, errors.New("transition reservation TTL must be positive")
	}
	if err := reservation.PoolSnapshot.Validate(); err != nil {
		return false, fmt.Errorf("validate transition pool snapshot: %w", err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	g.pruneExpiredLocked(now)
	expiresAt := now.Add(reservation.TTL)

	switch reservation.Transition {
	case LatencyHealthTransitionSoftQuarantine:
		if lease, exists := g.softQuarantined[reservation.AccountID]; exists {
			if lease.ownerToken != reservation.OwnerToken {
				return false, nil
			}
			lease.expiresAt = expiresAt
			g.softQuarantined[reservation.AccountID] = lease
			return true, nil
		}
		if reservation.QuarantineCap < 0 {
			return false, errors.New("transition quarantine cap must be non-negative")
		}
		if len(g.softQuarantined) >= g.maxAccounts ||
			reservation.PoolSnapshot.QuarantinedAccounts+len(g.softQuarantined) >= reservation.QuarantineCap {
			return false, nil
		}
		g.softQuarantined[reservation.AccountID] = latencyHealthTransitionLease{
			ownerToken: reservation.OwnerToken,
			expiresAt:  expiresAt,
		}
		return true, nil
	case LatencyHealthTransitionHardInvalid:
		if lease, exists := g.hardInvalid[reservation.AccountID]; exists {
			if lease.ownerToken != reservation.OwnerToken {
				return false, nil
			}
			lease.expiresAt = expiresAt
			g.hardInvalid[reservation.AccountID] = lease
			return true, nil
		}
		if reservation.HardValidFloor < 0 {
			return false, errors.New("transition hard-valid floor must be non-negative")
		}
		if len(g.hardInvalid) >= g.maxAccounts ||
			reservation.PoolSnapshot.HardValidAccounts-len(g.hardInvalid) <= reservation.HardValidFloor {
			return false, nil
		}
		g.hardInvalid[reservation.AccountID] = latencyHealthTransitionLease{
			ownerToken: reservation.OwnerToken,
			expiresAt:  expiresAt,
		}
		return true, nil
	default:
		return false, errors.New("latency health transition is invalid")
	}
}

func (g *LocalLatencyHealthTransitionGuard) pruneExpiredLocked(now time.Time) {
	for accountID, lease := range g.softQuarantined {
		if !lease.expiresAt.After(now) {
			delete(g.softQuarantined, accountID)
		}
	}
	for accountID, lease := range g.hardInvalid {
		if !lease.expiresAt.After(now) {
			delete(g.hardInvalid, accountID)
		}
	}
}

func (g *LocalLatencyHealthTransitionGuard) Renew(
	ctx context.Context,
	accountID int64,
	transition LatencyHealthTransition,
	ownerToken uint64,
	ttl time.Duration,
) (bool, error) {
	if g == nil {
		return false, errors.New("local latency health transition guard is nil")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if accountID <= 0 || ownerToken == 0 || ttl <= 0 {
		return false, errors.New("transition renewal account, owner, and TTL must be positive")
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	g.pruneExpiredLocked(now)
	var leases map[int64]latencyHealthTransitionLease
	switch transition {
	case LatencyHealthTransitionSoftQuarantine:
		leases = g.softQuarantined
	case LatencyHealthTransitionHardInvalid:
		leases = g.hardInvalid
	default:
		return false, errors.New("latency health transition is invalid")
	}
	lease, exists := leases[accountID]
	if !exists || lease.ownerToken != ownerToken {
		return false, nil
	}
	lease.expiresAt = now.Add(ttl)
	leases[accountID] = lease
	return true, nil
}

func (g *LocalLatencyHealthTransitionGuard) Release(
	ctx context.Context,
	accountID int64,
	transition LatencyHealthTransition,
	ownerToken uint64,
) error {
	if g == nil {
		return errors.New("local latency health transition guard is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if accountID <= 0 {
		return errors.New("transition release account id must be positive")
	}
	if ownerToken == 0 {
		return errors.New("transition release owner token must be positive")
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	var leases map[int64]latencyHealthTransitionLease
	switch transition {
	case LatencyHealthTransitionSoftQuarantine:
		leases = g.softQuarantined
	case LatencyHealthTransitionHardInvalid:
		leases = g.hardInvalid
	default:
		return errors.New("latency health transition is invalid")
	}
	if lease, exists := leases[accountID]; exists && lease.ownerToken == ownerToken {
		delete(leases, accountID)
	}
	return nil
}

func (g *LocalLatencyHealthTransitionGuard) Stats() LatencyHealthTransitionGuardStats {
	if g == nil {
		return LatencyHealthTransitionGuardStats{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pruneExpiredLocked(time.Now())
	return LatencyHealthTransitionGuardStats{
		MaxAccounts:     g.maxAccounts,
		SoftQuarantined: len(g.softQuarantined),
		HardInvalid:     len(g.hardInvalid),
	}
}

var _ LatencyHealthTransitionGuard = (*LocalLatencyHealthTransitionGuard)(nil)

// OpenAILatencyObservation contains only bounded health dimensions and numeric
// account identity. HTTP 5xx classification overrides Failure so callers cannot
// accidentally treat a 5xx as healthy.
type OpenAILatencyObservation struct {
	AccountID    int64
	Cohort       CohortKey
	Latency      time.Duration
	HTTPStatus   int
	Failure      FailureFingerprint
	PoolSnapshot PoolSnapshot
}

// LatencyHistogram is a fixed histogram. The final bucket is +Inf.
type LatencyHistogram [OpenAILatencyHistogramBucketCount]uint64

// OpenAILatencyHistogramUpperBounds returns a copy so global bucket policy
// cannot be mutated by a caller.
func OpenAILatencyHistogramUpperBounds() [openAILatencyHistogramFiniteBoundaryCount]time.Duration {
	return openAILatencyHistogramUpperBounds
}

// LatencyHealthTimeBucketSnapshot is one fixed 15-second bucket.
type LatencyHealthTimeBucketSnapshot struct {
	Start               time.Time
	Requests            uint64
	Successes           uint64
	Neutral             uint64
	Failures            uint64
	HTTP5xx             uint64
	LatencyMilliseconds uint64
	LatencyHistogram    LatencyHistogram
}

// LatencyHealthWindowSnapshot is the current fixed 90-second metric window.
type LatencyHealthWindowSnapshot struct {
	Requests            uint64
	Successes           uint64
	Neutral             uint64
	Failures            uint64
	HTTP5xx             uint64
	LatencyMilliseconds uint64
	LatencyHistogram    LatencyHistogram
	Buckets             [OpenAILatencyTimeBucketCount]LatencyHealthTimeBucketSnapshot
}

// LatencyHealthSnapshot is an immutable account or account+cohort view.
type LatencyHealthSnapshot struct {
	AccountID                     int64
	Cohort                        CohortKey
	AccountAggregate              bool
	Overflow                      bool
	State                         HealthState
	StateSince                    time.Time
	CooldownUntil                 time.Time
	CooldownRemaining             time.Duration
	RevalidationDue               bool
	PenaltyFingerprint            FailureFingerprint
	TotalRequests                 uint64
	TotalSuccesses                uint64
	TotalNeutral                  uint64
	TotalFailures                 uint64
	TotalHTTP5xx                  uint64
	TotalLatencyMilliseconds      uint64
	DecayedLatencySumMilliseconds float64
	DecayedLatencySamples         float64
	DecayedErrors                 float64
	DecayedErrorSamples           float64
	LatencyHistogram              LatencyHistogram
	Window                        LatencyHealthWindowSnapshot
	GuardrailBlockedTransitions   uint64
	HardRevalidationSuccesses     uint64
	HardRevalidationFailures      uint64
	IncidentLiftedPenalties       uint64
}

// HealthSnapshot keeps the prior generic name available without changing the
// corrected value semantics.
type HealthSnapshot = LatencyHealthSnapshot

// AccountLatencyHealthSnapshot includes the aggregate plus bounded cohort data.
type AccountLatencyHealthSnapshot struct {
	AccountID      int64
	Aggregate      LatencyHealthSnapshot
	Cohorts        []LatencyHealthSnapshot
	TrackedCohorts int
	OverflowUsed   bool
}

// IncidentCorrelationSnapshot exposes only aggregate counts and the canonical
// fingerprint; it contains no account identities.
type IncidentCorrelationSnapshot struct {
	Fingerprint      FailureFingerprint
	SampledAccounts  int
	AffectedAccounts int
	AffectedFraction float64
	Active           bool
	Generation       uint64
}

// LatencyHealthDecision makes a policy decision observable to future wiring.
type LatencyHealthDecision struct {
	Disabled            bool
	Cohort              CohortKey
	UsedOverflow        bool
	TrackLimitOverflow  bool
	ObservationDropped  bool
	Failure             FailureFingerprint
	ProviderIncident    bool
	PenaltySuppressed   bool
	LiftedSoftPenalties int
	GuardrailBlocked    bool
	AccountState        HealthState
	CohortState         HealthState
}

// LatencyHealthServiceStats proves the configured cardinality envelope without
// exposing internal maps.
type LatencyHealthServiceStats struct {
	Enabled                 bool
	Accounts                int
	MaxAccounts             int
	TrackedCohorts          int
	MaxCohortsPerAccount    int
	OverflowAccounts        int
	Tracks                  int
	MaxTracks               int
	TrackLimitOverflows     uint64
	DroppedObservations     uint64
	IncidentSampleEntries   int
	IncidentAffectedEntries int
}

type latencyHealthTimeBucket struct {
	epoch               int64
	initialized         bool
	requests            uint64
	successes           uint64
	neutral             uint64
	failures            uint64
	http5xx             uint64
	latencyMilliseconds uint64
	histogram           LatencyHistogram
}

type latencyHealthTrack struct {
	state              HealthState
	stateSince         time.Time
	cooldownUntil      time.Time
	penaltyFingerprint FailureFingerprint

	totalRequests                 uint64
	totalSuccesses                uint64
	totalNeutral                  uint64
	totalFailures                 uint64
	totalHTTP5xx                  uint64
	totalLatencyMilliseconds      uint64
	decayedLatencySumMilliseconds float64
	decayedLatencySampleWeight    float64
	decayedErrorSum               float64
	decayedErrorSampleWeight      float64
	decayedAt                     time.Time
	histogram                     LatencyHistogram
	buckets                       [OpenAILatencyTimeBucketCount]latencyHealthTimeBucket

	guardrailBlockedTransitions uint64
	hardRevalidationSuccesses   uint64
	hardRevalidationFailures    uint64
	incidentLiftedPenalties     uint64
}

type latencyAccountHealth struct {
	mu sync.Mutex

	aggregate            latencyHealthTrack
	cohorts              map[CohortKey]*latencyHealthTrack
	overflow             latencyHealthTrack
	overflowUsed         bool
	softReservationHeld  bool
	hardReservationHeld  bool
	softReservationOwner uint64
	hardReservationOwner uint64
}

type latencyIncidentExpiry struct {
	accountID  int64
	observedAt time.Time
	index      int
}

type latencyIncidentExpiryHeap []*latencyIncidentExpiry

func (h latencyIncidentExpiryHeap) Len() int { return len(h) }
func (h latencyIncidentExpiryHeap) Less(i, j int) bool {
	return h[i].observedAt.Before(h[j].observedAt)
}
func (h latencyIncidentExpiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *latencyIncidentExpiryHeap) Push(value any) {
	entry := value.(*latencyIncidentExpiry)
	entry.index = len(*h)
	*h = append(*h, entry)
}
func (h *latencyIncidentExpiryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	entry := old[last]
	old[last] = nil
	entry.index = -1
	*h = old[:last]
	return entry
}

type latencyIncidentWindowSet struct {
	entries     map[int64]*latencyIncidentExpiry
	expirations latencyIncidentExpiryHeap
}

func (s *latencyIncidentWindowSet) observe(accountID int64, now time.Time, maxAccounts int) bool {
	if entry, ok := s.entries[accountID]; ok {
		if now.After(entry.observedAt) {
			entry.observedAt = now
			heap.Fix(&s.expirations, entry.index)
		}
		return true
	}
	if len(s.entries) >= maxAccounts {
		return false
	}
	if s.entries == nil {
		s.entries = make(map[int64]*latencyIncidentExpiry)
	}
	entry := &latencyIncidentExpiry{accountID: accountID, observedAt: now}
	s.entries[accountID] = entry
	heap.Push(&s.expirations, entry)
	return true
}

func (s *latencyIncidentWindowSet) expire(now time.Time, window time.Duration) {
	for len(s.expirations) > 0 {
		entry := s.expirations[0]
		if entry.observedAt.After(now) || now.Sub(entry.observedAt) <= window {
			return
		}
		heap.Pop(&s.expirations)
		delete(s.entries, entry.accountID)
	}
}

func (s *latencyIncidentWindowSet) len() int { return len(s.entries) }

type latencyIncidentFingerprintState struct {
	affected         latencyIncidentWindowSet
	active           bool
	generation       uint64
	liftedGeneration uint64
}

type latencyIncidentEvaluation struct {
	snapshot  IncidentCorrelationSnapshot
	activated bool
}

// OpenAILatencyHealthService is concurrency-safe. The map lock is held only for
// account lookup/insertion, each account serializes its own tracks, and incident
// correlation has an independent lock so unrelated observations do not share a
// global hot mutex. It owns no repository, scheduler, timer, or goroutine.
type OpenAILatencyHealthService struct {
	accountsMu           sync.RWMutex
	config               LatencyHealthConfig
	clock                Clock
	transitionGuard      LatencyHealthTransitionGuard
	transitionOwnerToken uint64
	accounts             map[int64]*latencyAccountHealth
	accountCount         atomic.Int64
	trackReservations    atomic.Int64
	trackLimitOverflows  atomic.Uint64
	droppedObservations  atomic.Uint64

	incidentMu           sync.Mutex
	incidentSampled      latencyIncidentWindowSet
	incidentFingerprints [failureFingerprintCardinality]latencyIncidentFingerprintState
}

// NewOpenAILatencyHealthService requires an injected clock and installs the
// bounded process-local transition guard. Disabled mode allocates no maps.
func NewOpenAILatencyHealthService(config LatencyHealthConfig, clock Clock) (*OpenAILatencyHealthService, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validate openai latency health config: %w", err)
	}
	if clock == nil {
		return nil, errors.New("openai latency health clock is required")
	}
	if !config.Enabled {
		return &OpenAILatencyHealthService{config: config, clock: clock}, nil
	}
	guard, err := NewLocalLatencyHealthTransitionGuard(config.MaxTrackedAccounts)
	if err != nil {
		return nil, fmt.Errorf("create local latency health transition guard: %w", err)
	}
	return newOpenAILatencyHealthService(config, clock, guard), nil
}

// NewOpenAILatencyHealthServiceWithTransitionGuard exposes the guard port for a
// later global implementation without wiring one into this slice.
func NewOpenAILatencyHealthServiceWithTransitionGuard(
	config LatencyHealthConfig,
	clock Clock,
	guard LatencyHealthTransitionGuard,
) (*OpenAILatencyHealthService, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validate openai latency health config: %w", err)
	}
	if clock == nil {
		return nil, errors.New("openai latency health clock is required")
	}
	if !config.Enabled {
		return &OpenAILatencyHealthService{config: config, clock: clock}, nil
	}
	if guard == nil {
		return nil, errors.New("openai latency health transition guard is required when enabled")
	}
	return newOpenAILatencyHealthService(config, clock, guard), nil
}

func newOpenAILatencyHealthService(
	config LatencyHealthConfig,
	clock Clock,
	guard LatencyHealthTransitionGuard,
) *OpenAILatencyHealthService {
	return &OpenAILatencyHealthService{
		config:               config,
		clock:                clock,
		transitionGuard:      guard,
		transitionOwnerToken: mintOpenAILatencyBootToken(),
		accounts:             make(map[int64]*latencyAccountHealth),
	}
}

// RecordObservation records an outcome and discards the detailed decision.
func (s *OpenAILatencyHealthService) RecordObservation(observation OpenAILatencyObservation) error {
	return s.RecordObservationContext(context.Background(), observation)
}

func (s *OpenAILatencyHealthService) RecordObservationContext(ctx context.Context, observation OpenAILatencyObservation) error {
	_, err := s.ObserveContext(ctx, observation)
	return err
}

// Observe records metrics and applies health policy. Provider incidents are
// derived internally from bounded incremental state. HTTP 5xx and runtime
// failures can only apply soft policy; 401/403 are the only hard-invalid path.
func (s *OpenAILatencyHealthService) Observe(observation OpenAILatencyObservation) (LatencyHealthDecision, error) {
	return s.ObserveContext(context.Background(), observation)
}

func (s *OpenAILatencyHealthService) ObserveContext(
	ctx context.Context,
	observation OpenAILatencyObservation,
) (LatencyHealthDecision, error) {
	if s == nil {
		return LatencyHealthDecision{}, errors.New("openai latency health service is nil")
	}
	if ctx == nil {
		return LatencyHealthDecision{}, errors.New("openai latency health context is required")
	}
	if !s.config.Enabled {
		return LatencyHealthDecision{Disabled: true, Cohort: observation.Cohort.Canonical()}, nil
	}
	if err := ctx.Err(); err != nil {
		return LatencyHealthDecision{}, err
	}
	if err := validateLatencyHealthObservation(observation); err != nil {
		return LatencyHealthDecision{}, err
	}

	now := s.clock.Now()
	cohort := observation.Cohort.Canonical()
	failure, classifiedHTTPFailure := ClassifyHTTPFailure(observation.HTTPStatus)
	failureClass := latencyHealthFailureNone
	if classifiedHTTPFailure {
		if observation.HTTPStatus == 401 || observation.HTTPStatus == 403 {
			failureClass = latencyHealthFailureHard
		} else {
			failureClass = latencyHealthFailureSoft
		}
	} else {
		failure = observation.Failure.canonical()
		if failure != FailureFingerprintNone {
			failureClass = latencyHealthFailureSoft
		}
	}
	isHTTP5xx := observation.HTTPStatus >= 500 && observation.HTTPStatus <= 599
	outcome := classifyLatencyHealthOutcome(observation.HTTPStatus, failure)

	account, err := s.account(observation.AccountID, now)
	if err != nil {
		return LatencyHealthDecision{
			Cohort:             cohort,
			Failure:            failure,
			ObservationDropped: true,
		}, err
	}
	initialIncident := s.recordIncidentObservation(
		observation.AccountID,
		failure,
		failureClass == latencyHealthFailureSoft,
		now,
	)

	account.mu.Lock()
	advanceLatencyAccountLocked(account, now)
	if err := s.syncTransitionReservationsLocked(ctx, observation.AccountID, account); err != nil {
		account.mu.Unlock()
		return LatencyHealthDecision{}, err
	}
	cohortTrack, storedCohort, overflow, trackLimitOverflow := s.cohortLocked(account, cohort, now)
	account.aggregate.record(now, observation.Latency, outcome, isHTTP5xx, s.config.LatencyHalfLife, s.config.ErrorHalfLife)
	cohortTrack.record(now, observation.Latency, outcome, isHTTP5xx, s.config.LatencyHalfLife, s.config.ErrorHalfLife)

	decision := LatencyHealthDecision{
		Cohort:             storedCohort,
		UsedOverflow:       overflow,
		TrackLimitOverflow: trackLimitOverflow,
		Failure:            failure,
	}
	activationGeneration := uint64(0)

	switch failureClass {
	case latencyHealthFailureSoft:
		// This recheck intentionally occurs after the account lock. A concurrent
		// activation cannot race a stale inactive result into a new penalty.
		currentIncident := s.currentIncident(failure, s.clock.Now())
		if currentIncident.snapshot.Active {
			decision.ProviderIncident = true
			decision.PenaltySuppressed = true
			if currentIncident.activated {
				activationGeneration = currentIncident.snapshot.Generation
			} else if initialIncident.activated &&
				initialIncident.snapshot.Generation == currentIncident.snapshot.Generation {
				activationGeneration = initialIncident.snapshot.Generation
			}
		} else {
			allowQuarantine := true
			if account.aggregate.needsSoftQuarantineReservation() || cohortTrack.needsSoftQuarantineReservation() {
				allowQuarantine, err = s.reserveTransitionLocked(
					ctx,
					observation.AccountID,
					account,
					LatencyHealthTransitionSoftQuarantine,
					observation.PoolSnapshot,
				)
				if err != nil {
					account.mu.Unlock()
					return decision, fmt.Errorf("reserve soft-quarantine transition: %w", err)
				}
			}
			accountBlocked := account.aggregate.applySoftFailure(now, failure, allowQuarantine, s.config)
			cohortBlocked := cohortTrack.applySoftFailure(now, failure, allowQuarantine, s.config)
			decision.GuardrailBlocked = accountBlocked || cohortBlocked
		}
	case latencyHealthFailureHard:
		allowHardInvalid := true
		if account.aggregate.needsHardInvalidReservation() || cohortTrack.needsHardInvalidReservation() {
			allowHardInvalid, err = s.reserveTransitionLocked(
				ctx,
				observation.AccountID,
				account,
				LatencyHealthTransitionHardInvalid,
				observation.PoolSnapshot,
			)
			if err != nil {
				account.mu.Unlock()
				return decision, fmt.Errorf("reserve hard-invalid transition: %w", err)
			}
		}
		accountBlocked := account.aggregate.applyHardFailure(now, failure, allowHardInvalid, s.config)
		cohortBlocked := cohortTrack.applyHardFailure(now, failure, allowHardInvalid, s.config)
		decision.GuardrailBlocked = accountBlocked || cohortBlocked
	default:
		if outcome == latencyHealthOutcomeSuccess {
			account.aggregate.applySuccess(now)
			cohortTrack.applySuccess(now)
		}
	}
	if err := s.syncTransitionReservationsLocked(ctx, observation.AccountID, account); err != nil {
		account.mu.Unlock()
		return decision, err
	}
	account.mu.Unlock()

	if activationGeneration != 0 {
		decision.LiftedSoftPenalties = s.liftMatchingSoftPenalties(failure, activationGeneration, s.clock.Now())
	}

	account.mu.Lock()
	decision.AccountState = account.aggregate.state
	decision.CohortState = cohortTrack.state
	account.mu.Unlock()
	return decision, nil
}

// RecordHardRevalidation records an explicit validation attempt for a
// hard-invalid account/cohort. A success enters probation; a failure restarts
// the configured hard-invalid revalidation cooldown.
func (s *OpenAILatencyHealthService) RecordHardRevalidation(accountID int64, cohort CohortKey, successful bool) error {
	if s == nil {
		return errors.New("openai latency health service is nil")
	}
	if !s.config.Enabled {
		return nil
	}
	if accountID <= 0 {
		return errors.New("account id must be positive")
	}

	account, ok := s.lookupAccount(accountID)
	if !ok {
		return ErrLatencyHealthNotFound
	}
	account.mu.Lock()
	defer account.mu.Unlock()

	track, ok := lookupCohortTrack(account, cohort.Canonical())
	if !ok {
		return ErrLatencyHealthNotFound
	}
	now := s.clock.Now()
	advanceLatencyAccountLocked(account, now)
	if err := s.syncTransitionReservationsLocked(context.Background(), accountID, account); err != nil {
		return err
	}

	changed := account.aggregate.applyHardRevalidation(now, successful, s.config)
	if track != &account.aggregate {
		changed = track.applyHardRevalidation(now, successful, s.config) || changed
	}
	if !changed {
		return ErrLatencyHealthInvalidState
	}
	return s.syncTransitionReservationsLocked(context.Background(), accountID, account)
}

// RecordSuccessfulHardRevalidation is the explicit success path required to
// restore a hard-invalid account to probation.
func (s *OpenAILatencyHealthService) RecordSuccessfulHardRevalidation(accountID int64, cohort CohortKey) error {
	return s.RecordHardRevalidation(accountID, cohort, true)
}

// GetAccountHealth returns the account aggregate snapshot.
func (s *OpenAILatencyHealthService) GetAccountHealth(accountID int64) (LatencyHealthSnapshot, bool) {
	if s == nil || !s.config.Enabled {
		return LatencyHealthSnapshot{}, false
	}
	account, ok := s.lookupAccount(accountID)
	if !ok {
		return LatencyHealthSnapshot{}, false
	}
	account.mu.Lock()
	defer account.mu.Unlock()
	now := s.clock.Now()
	advanceLatencyAccountLocked(account, now)
	_ = s.syncTransitionReservationsLocked(context.Background(), accountID, account)
	return account.aggregate.snapshot(now, accountID, UnknownCohortKey, true, false, s.config), true
}

// GetCohortHealth returns a canonical cohort snapshot. Overflow must be queried
// with OverflowCohortKey because raw high-cardinality keys are never retained.
func (s *OpenAILatencyHealthService) GetCohortHealth(accountID int64, cohort CohortKey) (LatencyHealthSnapshot, bool) {
	if s == nil || !s.config.Enabled {
		return LatencyHealthSnapshot{}, false
	}
	account, ok := s.lookupAccount(accountID)
	if !ok {
		return LatencyHealthSnapshot{}, false
	}
	account.mu.Lock()
	defer account.mu.Unlock()
	canonical := cohort.Canonical()
	track, ok := lookupCohortTrack(account, canonical)
	if !ok {
		return LatencyHealthSnapshot{}, false
	}
	now := s.clock.Now()
	advanceLatencyAccountLocked(account, now)
	_ = s.syncTransitionReservationsLocked(context.Background(), accountID, account)
	return track.snapshot(now, accountID, canonical, false, canonical == OverflowCohortKey, s.config), true
}

// SnapshotAccount returns deterministic cohort ordering and a copy of all data.
func (s *OpenAILatencyHealthService) SnapshotAccount(accountID int64) (AccountLatencyHealthSnapshot, bool) {
	if s == nil || !s.config.Enabled {
		return AccountLatencyHealthSnapshot{}, false
	}
	account, ok := s.lookupAccount(accountID)
	if !ok {
		return AccountLatencyHealthSnapshot{}, false
	}
	account.mu.Lock()
	defer account.mu.Unlock()
	now := s.clock.Now()
	advanceLatencyAccountLocked(account, now)
	_ = s.syncTransitionReservationsLocked(context.Background(), accountID, account)
	keys := make([]CohortKey, 0, len(account.cohorts))
	for key := range account.cohorts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	cohorts := make([]LatencyHealthSnapshot, 0, len(keys)+1)
	for _, key := range keys {
		cohorts = append(cohorts, account.cohorts[key].snapshot(now, accountID, key, false, false, s.config))
	}
	if account.overflowUsed {
		cohorts = append(cohorts, account.overflow.snapshot(now, accountID, OverflowCohortKey, false, true, s.config))
	}
	return AccountLatencyHealthSnapshot{
		AccountID:      accountID,
		Aggregate:      account.aggregate.snapshot(now, accountID, UnknownCohortKey, true, false, s.config),
		Cohorts:        cohorts,
		TrackedCohorts: len(account.cohorts),
		OverflowUsed:   account.overflowUsed,
	}, true
}

// IncidentCorrelation computes current rolling distinct-account correlation.
func (s *OpenAILatencyHealthService) IncidentCorrelation(failure FailureFingerprint) IncidentCorrelationSnapshot {
	failure = failure.canonical()
	if s == nil || !s.config.Enabled || failure == FailureFingerprintNone {
		return IncidentCorrelationSnapshot{Fingerprint: failure}
	}
	s.incidentMu.Lock()
	defer s.incidentMu.Unlock()
	return s.incidentCorrelationLocked(failure, s.clock.Now()).snapshot
}

// AnyProviderIncidentActive reports whether any allowlisted failure fingerprint
// currently meets provider-incident correlation. The scan is finite and does
// not expose account identities.
func (s *OpenAILatencyHealthService) AnyProviderIncidentActive() bool {
	if s == nil || !s.config.Enabled {
		return false
	}
	for failure := FailureFingerprint(1); failure < failureFingerprintCardinality; failure++ {
		if s.IncidentCorrelation(failure).Active {
			return true
		}
	}
	return false
}

// Stats reports bounded state shape without exposing account identifiers.
func (s *OpenAILatencyHealthService) Stats() LatencyHealthServiceStats {
	if s == nil {
		return LatencyHealthServiceStats{}
	}
	stats := LatencyHealthServiceStats{
		Enabled:              s.config.Enabled,
		MaxAccounts:          s.config.MaxTrackedAccounts,
		MaxCohortsPerAccount: s.config.MaxCohortsPerAccount,
		MaxTracks:            s.config.MaxTracks,
	}
	if !s.config.Enabled {
		return stats
	}

	stats.Accounts = int(s.accountCount.Load())
	stats.Tracks = int(s.trackReservations.Load())
	stats.TrackLimitOverflows = s.trackLimitOverflows.Load()
	stats.DroppedObservations = s.droppedObservations.Load()
	s.accountsMu.RLock()
	for _, account := range s.accounts {
		account.mu.Lock()
		stats.TrackedCohorts += len(account.cohorts)
		if account.overflowUsed {
			stats.OverflowAccounts++
		}
		account.mu.Unlock()
	}
	s.accountsMu.RUnlock()

	s.incidentMu.Lock()
	stats.IncidentSampleEntries = s.incidentSampled.len()
	for index := FailureFingerprint(1); index < failureFingerprintCardinality; index++ {
		stats.IncidentAffectedEntries += s.incidentFingerprints[index].affected.len()
	}
	s.incidentMu.Unlock()
	return stats
}

func validateLatencyHealthObservation(observation OpenAILatencyObservation) error {
	if observation.AccountID <= 0 {
		return errors.New("account id must be positive")
	}
	if observation.Latency < 0 {
		return errors.New("latency cannot be negative")
	}
	if observation.HTTPStatus < 0 || observation.HTTPStatus > 599 {
		return errors.New("http status must be zero or between 100 and 599")
	}
	if observation.HTTPStatus > 0 && observation.HTTPStatus < 100 {
		return errors.New("http status must be zero or between 100 and 599")
	}
	if err := observation.PoolSnapshot.Validate(); err != nil {
		return fmt.Errorf("validate pool snapshot: %w", err)
	}
	return nil
}

type latencyHealthOutcome uint8

const (
	latencyHealthOutcomeNeutral latencyHealthOutcome = iota
	latencyHealthOutcomeSuccess
	latencyHealthOutcomeFailure
)

type latencyHealthFailureClass uint8

const (
	latencyHealthFailureNone latencyHealthFailureClass = iota
	latencyHealthFailureSoft
	latencyHealthFailureHard
)

func classifyLatencyHealthOutcome(statusCode int, failure FailureFingerprint) latencyHealthOutcome {
	if failure != FailureFingerprintNone {
		return latencyHealthOutcomeFailure
	}
	if statusCode == 0 || statusCode == 200 || (statusCode >= 201 && statusCode < 400) {
		return latencyHealthOutcomeSuccess
	}
	return latencyHealthOutcomeNeutral
}

func (s *OpenAILatencyHealthService) lookupAccount(accountID int64) (*latencyAccountHealth, bool) {
	s.accountsMu.RLock()
	account, ok := s.accounts[accountID]
	s.accountsMu.RUnlock()
	return account, ok
}

func advanceLatencyAccountLocked(account *latencyAccountHealth, now time.Time) {
	account.aggregate.advance(now)
	for _, track := range account.cohorts {
		track.advance(now)
	}
	if account.overflowUsed {
		account.overflow.advance(now)
	}
}

func latencyAccountHasStateLocked(account *latencyAccountHealth, state HealthState) bool {
	if account.aggregate.state == state {
		return true
	}
	for _, track := range account.cohorts {
		if track.state == state {
			return true
		}
	}
	return account.overflowUsed && account.overflow.state == state
}

func (s *OpenAILatencyHealthService) reserveTransitionLocked(
	ctx context.Context,
	accountID int64,
	account *latencyAccountHealth,
	transition LatencyHealthTransition,
	pool PoolSnapshot,
) (bool, error) {
	switch transition {
	case LatencyHealthTransitionSoftQuarantine:
		if account.softReservationHeld {
			return s.transitionGuard.Renew(
				ctx,
				accountID,
				transition,
				account.softReservationOwner,
				DefaultLatencyHealthTransitionReservationTTL,
			)
		}
	case LatencyHealthTransitionHardInvalid:
		if account.hardReservationHeld {
			return s.transitionGuard.Renew(
				ctx,
				accountID,
				transition,
				account.hardReservationOwner,
				DefaultLatencyHealthTransitionReservationTTL,
			)
		}
	}
	reserved, err := s.transitionGuard.Reserve(ctx, LatencyHealthTransitionReservation{
		AccountID:      accountID,
		Transition:     transition,
		PoolSnapshot:   pool,
		QuarantineCap:  s.config.QuarantineCap,
		HardValidFloor: s.config.HardValidFloor,
		OwnerToken:     s.transitionOwnerToken,
		TTL:            DefaultLatencyHealthTransitionReservationTTL,
	})
	if err != nil || !reserved {
		return reserved, err
	}
	if transition == LatencyHealthTransitionSoftQuarantine {
		account.softReservationHeld = true
		account.softReservationOwner = s.transitionOwnerToken
	} else {
		account.hardReservationHeld = true
		account.hardReservationOwner = s.transitionOwnerToken
	}
	return true, nil
}

func (s *OpenAILatencyHealthService) syncTransitionReservationsLocked(
	ctx context.Context,
	accountID int64,
	account *latencyAccountHealth,
) error {
	syncLease := func(
		held *bool,
		owner *uint64,
		state HealthState,
		transition LatencyHealthTransition,
		label string,
	) error {
		if !*held {
			return nil
		}
		if *owner == 0 {
			*held = false
			return nil
		}
		if latencyAccountHasStateLocked(account, state) {
			renewed, err := s.transitionGuard.Renew(
				ctx,
				accountID,
				transition,
				*owner,
				DefaultLatencyHealthTransitionReservationTTL,
			)
			if err != nil {
				return fmt.Errorf("renew %s reservation: %w", label, err)
			}
			if !renewed {
				*held = false
				*owner = 0
			}
			return nil
		}
		if err := s.transitionGuard.Release(ctx, accountID, transition, *owner); err != nil {
			return fmt.Errorf("release %s reservation: %w", label, err)
		}
		*held = false
		*owner = 0
		return nil
	}
	if err := syncLease(
		&account.softReservationHeld,
		&account.softReservationOwner,
		HealthStateQuarantined,
		LatencyHealthTransitionSoftQuarantine,
		"soft-quarantine",
	); err != nil {
		return err
	}
	return syncLease(
		&account.hardReservationHeld,
		&account.hardReservationOwner,
		HealthStateHardInvalid,
		LatencyHealthTransitionHardInvalid,
		"hard-invalid",
	)
}

func (s *OpenAILatencyHealthService) account(accountID int64, now time.Time) (*latencyAccountHealth, error) {
	if account, ok := s.lookupAccount(accountID); ok {
		return account, nil
	}

	s.accountsMu.Lock()
	defer s.accountsMu.Unlock()
	if account, ok := s.accounts[accountID]; ok {
		return account, nil
	}
	if len(s.accounts) >= s.config.MaxTrackedAccounts {
		s.droppedObservations.Add(1)
		return nil, ErrLatencyHealthAccountLimit
	}
	// Aggregate and inline overflow capacity are reserved together. Cohort
	// pressure can therefore fall back without allocating another track.
	if !s.reserveTracks(2, int64(s.config.MaxTracks)) {
		s.droppedObservations.Add(1)
		return nil, ErrLatencyHealthTrackLimit
	}
	account := &latencyAccountHealth{
		aggregate: newLatencyHealthTrack(now),
		overflow:  newLatencyHealthTrack(now),
		cohorts:   make(map[CohortKey]*latencyHealthTrack),
	}
	s.accounts[accountID] = account
	s.accountCount.Add(1)
	return account, nil
}

func (s *OpenAILatencyHealthService) reserveTracks(count, limit int64) bool {
	for {
		current := s.trackReservations.Load()
		if count <= 0 || current > limit-count {
			return false
		}
		if s.trackReservations.CompareAndSwap(current, current+count) {
			return true
		}
	}
}

func (s *OpenAILatencyHealthService) reserveCohortTrack() bool {
	remainingAccounts := int64(s.config.MaxTrackedAccounts) - s.accountCount.Load()
	limit := int64(s.config.MaxTracks) - 2*remainingAccounts
	return s.reserveTracks(1, limit)
}

func (s *OpenAILatencyHealthService) cohortLocked(account *latencyAccountHealth, cohort CohortKey, now time.Time) (*latencyHealthTrack, CohortKey, bool, bool) {
	if cohort == OverflowCohortKey {
		account.overflowUsed = true
		return &account.overflow, OverflowCohortKey, true, false
	}
	if track, ok := account.cohorts[cohort]; ok {
		return track, cohort, false, false
	}
	if len(account.cohorts) < s.config.MaxCohortsPerAccount {
		if s.reserveCohortTrack() {
			track := newLatencyHealthTrack(now)
			account.cohorts[cohort] = &track
			return account.cohorts[cohort], cohort, false, false
		}
		s.trackLimitOverflows.Add(1)
		account.overflowUsed = true
		return &account.overflow, OverflowCohortKey, true, true
	}
	account.overflowUsed = true
	return &account.overflow, OverflowCohortKey, true, false
}

func lookupCohortTrack(account *latencyAccountHealth, cohort CohortKey) (*latencyHealthTrack, bool) {
	if cohort == OverflowCohortKey {
		if !account.overflowUsed {
			return nil, false
		}
		return &account.overflow, true
	}
	track, ok := account.cohorts[cohort]
	return track, ok
}

func (s *OpenAILatencyHealthService) recordIncidentObservation(
	accountID int64,
	failure FailureFingerprint,
	softFailure bool,
	now time.Time,
) latencyIncidentEvaluation {
	s.incidentMu.Lock()
	defer s.incidentMu.Unlock()

	s.incidentSampled.expire(now, s.config.IncidentWindow)
	s.incidentSampled.observe(accountID, now, s.config.MaxTrackedAccounts)
	if !softFailure || failure == FailureFingerprintNone {
		return latencyIncidentEvaluation{snapshot: IncidentCorrelationSnapshot{Fingerprint: failure}}
	}
	state := &s.incidentFingerprints[failure]
	state.affected.expire(now, s.config.IncidentWindow)
	state.affected.observe(accountID, now, s.config.MaxTrackedAccounts)
	return s.incidentCorrelationLocked(failure, now)
}

func (s *OpenAILatencyHealthService) incidentCorrelationLocked(failure FailureFingerprint, now time.Time) latencyIncidentEvaluation {
	snapshot := IncidentCorrelationSnapshot{Fingerprint: failure}
	if failure == FailureFingerprintNone {
		return latencyIncidentEvaluation{snapshot: snapshot}
	}

	s.incidentSampled.expire(now, s.config.IncidentWindow)
	state := &s.incidentFingerprints[failure]
	state.affected.expire(now, s.config.IncidentWindow)
	snapshot.SampledAccounts = s.incidentSampled.len()
	snapshot.AffectedAccounts = state.affected.len()
	if snapshot.SampledAccounts > 0 {
		snapshot.AffectedFraction = float64(snapshot.AffectedAccounts) / float64(snapshot.SampledAccounts)
	}
	snapshot.Active = snapshot.AffectedAccounts >= s.config.IncidentMinAffectedAccounts &&
		snapshot.AffectedFraction >= s.config.IncidentMinAffectedFraction
	activated := !state.active && snapshot.Active
	if activated {
		state.generation = saturatingAdd(state.generation, 1)
	}
	state.active = snapshot.Active
	snapshot.Generation = state.generation
	return latencyIncidentEvaluation{snapshot: snapshot, activated: activated}
}

func (s *OpenAILatencyHealthService) currentIncident(failure FailureFingerprint, now time.Time) latencyIncidentEvaluation {
	s.incidentMu.Lock()
	defer s.incidentMu.Unlock()
	return s.incidentCorrelationLocked(failure.canonical(), now)
}

func (s *OpenAILatencyHealthService) claimIncidentPenaltyLift(
	failure FailureFingerprint,
	generation uint64,
	now time.Time,
) bool {
	s.incidentMu.Lock()
	defer s.incidentMu.Unlock()
	evaluation := s.incidentCorrelationLocked(failure.canonical(), now)
	state := &s.incidentFingerprints[failure.canonical()]
	if !evaluation.snapshot.Active || evaluation.snapshot.Generation != generation ||
		state.liftedGeneration >= generation {
		return false
	}
	state.liftedGeneration = generation
	return true
}

func (s *OpenAILatencyHealthService) incidentGenerationActive(
	failure FailureFingerprint,
	generation uint64,
	now time.Time,
) bool {
	evaluation := s.currentIncident(failure, now)
	return evaluation.snapshot.Active && evaluation.snapshot.Generation == generation
}

func (s *OpenAILatencyHealthService) liftMatchingSoftPenalties(
	failure FailureFingerprint,
	generation uint64,
	now time.Time,
) int {
	if !s.claimIncidentPenaltyLift(failure, generation, now) {
		return 0
	}

	type accountEntry struct {
		id      int64
		account *latencyAccountHealth
	}
	s.accountsMu.RLock()
	accounts := make([]accountEntry, 0, len(s.accounts))
	for accountID, account := range s.accounts {
		accounts = append(accounts, accountEntry{id: accountID, account: account})
	}
	s.accountsMu.RUnlock()

	lifted := 0
	for _, entry := range accounts {
		entry.account.mu.Lock()
		// The generation check is deliberately inside the account lock. It
		// serializes each lift with a failure applying a new penalty.
		accountNow := s.clock.Now()
		if !s.incidentGenerationActive(failure, generation, accountNow) {
			entry.account.mu.Unlock()
			continue
		}
		if entry.account.aggregate.liftSoftPenalty(accountNow, failure) {
			lifted++
		}
		for _, track := range entry.account.cohorts {
			if track.liftSoftPenalty(accountNow, failure) {
				lifted++
			}
		}
		if entry.account.overflowUsed && entry.account.overflow.liftSoftPenalty(accountNow, failure) {
			lifted++
		}
		_ = s.syncTransitionReservationsLocked(context.Background(), entry.id, entry.account)
		entry.account.mu.Unlock()
	}
	return lifted
}

func newLatencyHealthTrack(now time.Time) latencyHealthTrack {
	return latencyHealthTrack{state: HealthStateHealthy, stateSince: now, decayedAt: now}
}

func (t *latencyHealthTrack) advance(now time.Time) {
	switch t.state {
	case HealthStatePenalized:
		if !now.Before(t.cooldownUntil) {
			t.setState(HealthStateHealthy, now, FailureFingerprintNone, 0)
		}
	case HealthStateQuarantined:
		if !now.Before(t.cooldownUntil) {
			t.setState(HealthStateProbation, now, FailureFingerprintNone, 0)
		}
	}
}

func (t *latencyHealthTrack) applySuccess(now time.Time) {
	t.advance(now)
	if t.state == HealthStateProbation {
		t.setState(HealthStateHealthy, now, FailureFingerprintNone, 0)
	}
}

func (t *latencyHealthTrack) needsSoftQuarantineReservation() bool {
	return t.state == HealthStatePenalized || t.state == HealthStateProbation
}

func (t *latencyHealthTrack) needsHardInvalidReservation() bool {
	return t.state != HealthStateHardInvalid
}

// applySoftFailure returns true when the account-level guard blocks escalation.
// Runtime and HTTP 5xx failures still have no hard-invalid transition.
func (t *latencyHealthTrack) applySoftFailure(now time.Time, failure FailureFingerprint, allowQuarantine bool, config LatencyHealthConfig) bool {
	t.advance(now)
	switch t.state {
	case HealthStateHealthy:
		t.setState(HealthStatePenalized, now, failure, config.PenalizedCooldown)
		return false
	case HealthStatePenalized, HealthStateProbation:
		if !allowQuarantine {
			t.guardrailBlockedTransitions = saturatingAdd(t.guardrailBlockedTransitions, 1)
			t.setState(HealthStatePenalized, now, failure, config.PenalizedCooldown)
			return true
		}
		t.setState(HealthStateQuarantined, now, failure, config.QuarantinedCooldown)
		return false
	case HealthStateQuarantined:
		t.setState(HealthStateQuarantined, now, failure, config.QuarantinedCooldown)
		return false
	case HealthStateHardInvalid:
		return false
	default:
		t.setState(HealthStatePenalized, now, failure, config.PenalizedCooldown)
		return false
	}
}

// applyHardFailure uses the account-level reservation result. A blocked
// transition leaves the current state unchanged and increments diagnostics.
func (t *latencyHealthTrack) applyHardFailure(now time.Time, failure FailureFingerprint, allowHardInvalid bool, config LatencyHealthConfig) bool {
	t.advance(now)
	if t.state == HealthStateHardInvalid {
		t.setState(HealthStateHardInvalid, now, failure, config.HardInvalidCooldown)
		return false
	}
	if !allowHardInvalid {
		t.guardrailBlockedTransitions = saturatingAdd(t.guardrailBlockedTransitions, 1)
		return true
	}
	t.setState(HealthStateHardInvalid, now, failure, config.HardInvalidCooldown)
	return false
}

func (t *latencyHealthTrack) applyHardRevalidation(now time.Time, successful bool, config LatencyHealthConfig) bool {
	t.advance(now)
	if t.state != HealthStateHardInvalid {
		return false
	}
	if successful {
		t.hardRevalidationSuccesses = saturatingAdd(t.hardRevalidationSuccesses, 1)
		t.setState(HealthStateProbation, now, FailureFingerprintNone, 0)
	} else {
		t.hardRevalidationFailures = saturatingAdd(t.hardRevalidationFailures, 1)
		t.cooldownUntil = now.Add(config.HardInvalidCooldown)
	}
	return true
}

func (t *latencyHealthTrack) liftSoftPenalty(now time.Time, failure FailureFingerprint) bool {
	t.advance(now)
	if t.penaltyFingerprint != failure {
		return false
	}
	if t.state != HealthStatePenalized && t.state != HealthStateQuarantined {
		return false
	}
	t.incidentLiftedPenalties = saturatingAdd(t.incidentLiftedPenalties, 1)
	t.setState(HealthStateHealthy, now, FailureFingerprintNone, 0)
	return true
}

func (t *latencyHealthTrack) setState(state HealthState, now time.Time, failure FailureFingerprint, cooldown time.Duration) {
	t.state = state
	t.stateSince = now
	t.penaltyFingerprint = failure
	if cooldown > 0 {
		t.cooldownUntil = now.Add(cooldown)
	} else {
		t.cooldownUntil = time.Time{}
	}
}

func (t *latencyHealthTrack) decayObservations(now time.Time, latencyHalfLife, errorHalfLife time.Duration) {
	if t.decayedAt.IsZero() {
		t.decayedAt = now
		return
	}
	elapsed := now.Sub(t.decayedAt)
	if elapsed <= 0 {
		return
	}
	if latencyHalfLife > 0 {
		factor := math.Exp2(-elapsed.Seconds() / latencyHalfLife.Seconds())
		t.decayedLatencySumMilliseconds *= factor
		t.decayedLatencySampleWeight *= factor
	}
	if errorHalfLife > 0 {
		factor := math.Exp2(-elapsed.Seconds() / errorHalfLife.Seconds())
		t.decayedErrorSum *= factor
		t.decayedErrorSampleWeight *= factor
	}
	t.decayedAt = now
}

func (t *latencyHealthTrack) record(
	now time.Time,
	latency time.Duration,
	outcome latencyHealthOutcome,
	http5xx bool,
	latencyHalfLife time.Duration,
	errorHalfLife time.Duration,
) {
	t.decayObservations(now, latencyHalfLife, errorHalfLife)
	t.totalRequests = saturatingAdd(t.totalRequests, 1)
	switch outcome {
	case latencyHealthOutcomeSuccess:
		t.totalSuccesses = saturatingAdd(t.totalSuccesses, 1)
	case latencyHealthOutcomeFailure:
		t.totalFailures = saturatingAdd(t.totalFailures, 1)
	default:
		t.totalNeutral = saturatingAdd(t.totalNeutral, 1)
	}
	if http5xx {
		t.totalHTTP5xx = saturatingAdd(t.totalHTTP5xx, 1)
	}
	latencyMilliseconds := uint64(latency / time.Millisecond)
	t.totalLatencyMilliseconds = saturatingAdd(t.totalLatencyMilliseconds, latencyMilliseconds)
	t.decayedLatencySumMilliseconds += float64(latency) / float64(time.Millisecond)
	t.decayedLatencySampleWeight++
	t.decayedErrorSampleWeight++
	if outcome == latencyHealthOutcomeFailure {
		t.decayedErrorSum++
	}
	histogramBucket := latencyHistogramBucket(latency)
	t.histogram[histogramBucket] = saturatingAdd(t.histogram[histogramBucket], 1)

	epoch := latencyHealthEpoch(now)
	index := latencyHealthBucketIndex(epoch)
	bucket := &t.buckets[index]
	if !bucket.initialized || bucket.epoch != epoch {
		*bucket = latencyHealthTimeBucket{epoch: epoch, initialized: true}
	}
	bucket.requests = saturatingAdd(bucket.requests, 1)
	switch outcome {
	case latencyHealthOutcomeSuccess:
		bucket.successes = saturatingAdd(bucket.successes, 1)
	case latencyHealthOutcomeFailure:
		bucket.failures = saturatingAdd(bucket.failures, 1)
	default:
		bucket.neutral = saturatingAdd(bucket.neutral, 1)
	}
	if http5xx {
		bucket.http5xx = saturatingAdd(bucket.http5xx, 1)
	}
	bucket.latencyMilliseconds = saturatingAdd(bucket.latencyMilliseconds, latencyMilliseconds)
	bucket.histogram[histogramBucket] = saturatingAdd(bucket.histogram[histogramBucket], 1)
}

func (t *latencyHealthTrack) snapshot(
	now time.Time,
	accountID int64,
	cohort CohortKey,
	accountAggregate bool,
	overflow bool,
	config LatencyHealthConfig,
) LatencyHealthSnapshot {
	t.advance(now)
	t.decayObservations(now, config.LatencyHalfLife, config.ErrorHalfLife)
	remaining := time.Duration(0)
	if now.Before(t.cooldownUntil) {
		remaining = t.cooldownUntil.Sub(now)
	}
	snapshot := LatencyHealthSnapshot{
		AccountID:                     accountID,
		Cohort:                        cohort,
		AccountAggregate:              accountAggregate,
		Overflow:                      overflow,
		State:                         t.state,
		StateSince:                    t.stateSince,
		CooldownUntil:                 t.cooldownUntil,
		CooldownRemaining:             remaining,
		RevalidationDue:               t.state == HealthStateHardInvalid && !now.Before(t.cooldownUntil),
		PenaltyFingerprint:            t.penaltyFingerprint,
		TotalRequests:                 t.totalRequests,
		TotalSuccesses:                t.totalSuccesses,
		TotalNeutral:                  t.totalNeutral,
		TotalFailures:                 t.totalFailures,
		TotalHTTP5xx:                  t.totalHTTP5xx,
		TotalLatencyMilliseconds:      t.totalLatencyMilliseconds,
		DecayedLatencySumMilliseconds: t.decayedLatencySumMilliseconds,
		DecayedLatencySamples:         t.decayedLatencySampleWeight,
		DecayedErrors:                 t.decayedErrorSum,
		DecayedErrorSamples:           t.decayedErrorSampleWeight,
		LatencyHistogram:              t.histogram,
		GuardrailBlockedTransitions:   t.guardrailBlockedTransitions,
		HardRevalidationSuccesses:     t.hardRevalidationSuccesses,
		HardRevalidationFailures:      t.hardRevalidationFailures,
		IncidentLiftedPenalties:       t.incidentLiftedPenalties,
	}
	snapshot.Window = t.windowSnapshot(now)
	return snapshot
}

func (t *latencyHealthTrack) windowSnapshot(now time.Time) LatencyHealthWindowSnapshot {
	var snapshot LatencyHealthWindowSnapshot
	currentEpoch := latencyHealthEpoch(now)
	for outputIndex := 0; outputIndex < OpenAILatencyTimeBucketCount; outputIndex++ {
		epoch := currentEpoch - int64(OpenAILatencyTimeBucketCount-1-outputIndex)
		start := time.Unix(0, epoch*int64(OpenAILatencyTimeBucketDuration))
		bucketSnapshot := LatencyHealthTimeBucketSnapshot{Start: start}
		bucket := t.buckets[latencyHealthBucketIndex(epoch)]
		if bucket.initialized && bucket.epoch == epoch {
			bucketSnapshot.Requests = bucket.requests
			bucketSnapshot.Successes = bucket.successes
			bucketSnapshot.Neutral = bucket.neutral
			bucketSnapshot.Failures = bucket.failures
			bucketSnapshot.HTTP5xx = bucket.http5xx
			bucketSnapshot.LatencyMilliseconds = bucket.latencyMilliseconds
			bucketSnapshot.LatencyHistogram = bucket.histogram
		}
		snapshot.Buckets[outputIndex] = bucketSnapshot
		snapshot.Requests = saturatingAdd(snapshot.Requests, bucketSnapshot.Requests)
		snapshot.Successes = saturatingAdd(snapshot.Successes, bucketSnapshot.Successes)
		snapshot.Neutral = saturatingAdd(snapshot.Neutral, bucketSnapshot.Neutral)
		snapshot.Failures = saturatingAdd(snapshot.Failures, bucketSnapshot.Failures)
		snapshot.HTTP5xx = saturatingAdd(snapshot.HTTP5xx, bucketSnapshot.HTTP5xx)
		snapshot.LatencyMilliseconds = saturatingAdd(snapshot.LatencyMilliseconds, bucketSnapshot.LatencyMilliseconds)
		for histogramIndex, count := range bucketSnapshot.LatencyHistogram {
			snapshot.LatencyHistogram[histogramIndex] = saturatingAdd(snapshot.LatencyHistogram[histogramIndex], count)
		}
	}
	return snapshot
}

func latencyHistogramBucket(latency time.Duration) int {
	for index, upperBound := range openAILatencyHistogramUpperBounds {
		if latency <= upperBound {
			return index
		}
	}
	return OpenAILatencyHistogramBucketCount - 1
}

func latencyHealthEpoch(now time.Time) int64 {
	return now.UnixNano() / int64(OpenAILatencyTimeBucketDuration)
}

func latencyHealthBucketIndex(epoch int64) int {
	index := epoch % int64(OpenAILatencyTimeBucketCount)
	if index < 0 {
		index += int64(OpenAILatencyTimeBucketCount)
	}
	return int(index)
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}
