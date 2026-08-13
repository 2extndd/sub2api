package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

var (
	ErrOpenAIHedgeDisabled               = errors.New("openai hedge coordinator is disabled")
	ErrOpenAIHedgeNoCandidate            = errors.New("no openai hedge candidate available")
	ErrOpenAIHedgeAttemptMissingTerminal = errors.New("openai hedge attempt ended without a terminal event")
	ErrOpenAIHedgeEventAfterTerminal     = errors.New("openai hedge event emitted after terminal")
	ErrOpenAIHedgeEventBufferExceeded    = errors.New("openai hedge pre-commit event buffer exceeded")
	ErrOpenAIHedgeAllAttemptsFailed      = errors.New("all openai hedge attempts failed")
	ErrOpenAIHedgeCleanupTimeout         = errors.New("openai hedge attempt cleanup timed out")
)

// OpenAIHedgeCostClass controls only the launch threshold. High and very heavy
// requests wait longer before duplicating potentially expensive work.
type OpenAIHedgeCostClass uint8

const (
	OpenAIHedgeCostStandard OpenAIHedgeCostClass = iota
	OpenAIHedgeCostHigh
	OpenAIHedgeCostVeryHeavy
	openAIHedgeCostClassCardinality
)

// OpenAIHedgeEventKind is deliberately semantic rather than transport-specific.
// Adapters may translate SSE frames into these events without exposing gateway
// response writers to both attempts.
type OpenAIHedgeEventKind uint8

const (
	OpenAIHedgeEventUnknown OpenAIHedgeEventKind = iota
	OpenAIHedgeEventPreamble
	OpenAIHedgeEventReasoning
	OpenAIHedgeEventText
	OpenAIHedgeEventTool
	OpenAIHedgeEventTerminalSuccess
	OpenAIHedgeEventTerminalFailure
	openAIHedgeEventKindCardinality
)

func (kind OpenAIHedgeEventKind) commitsWinner() bool {
	switch kind {
	case OpenAIHedgeEventReasoning,
		OpenAIHedgeEventText,
		OpenAIHedgeEventTool,
		OpenAIHedgeEventTerminalSuccess:
		return true
	default:
		return false
	}
}

func (kind OpenAIHedgeEventKind) terminal() bool {
	return kind == OpenAIHedgeEventTerminalSuccess || kind == OpenAIHedgeEventTerminalFailure
}

// OpenAIHedgeBillingObservation is optional upstream evidence. A loser that
// never supplies known billing evidence is finalized at its full potential
// cost; zero is still a known amount when Known is true.
type OpenAIHedgeBillingObservation struct {
	Known            bool
	ActualCostMicros uint64
}

// OpenAIHedgeEvent is copied before entering an attempt's bounded channel.
type OpenAIHedgeEvent struct {
	Kind    OpenAIHedgeEventKind
	Data    []byte
	Err     error
	Billing OpenAIHedgeBillingObservation
}

func (event OpenAIHedgeEvent) validate(maxBytes int) error {
	if event.Kind <= OpenAIHedgeEventUnknown || event.Kind >= openAIHedgeEventKindCardinality {
		return fmt.Errorf("invalid openai hedge event kind %d", event.Kind)
	}
	if maxBytes > 0 && len(event.Data) > maxBytes {
		return fmt.Errorf("%w: event has %d bytes, maximum is %d", ErrOpenAIHedgeEventBufferExceeded, len(event.Data), maxBytes)
	}
	if event.Kind == OpenAIHedgeEventTerminalFailure && event.Err == nil {
		return errors.New("openai hedge terminal failure requires an error")
	}
	if event.Kind != OpenAIHedgeEventTerminalFailure && event.Err != nil {
		return errors.New("only an openai hedge terminal failure may carry an error")
	}
	// uint64 is always non-negative, no validation needed for ActualCostMicros
	return nil
}

func cloneOpenAIHedgeEvent(event OpenAIHedgeEvent) OpenAIHedgeEvent {
	event.Data = append([]byte(nil), event.Data...)
	return event
}

// OpenAIHedgeAccount is the minimum account identity needed by the standalone
// coordinator. ParentAccountID is the credential owner for shadow accounts.
type OpenAIHedgeAccount struct {
	AccountID           int64
	ParentAccountID     *int64
	Provider            string
	PotentialCostMicros uint64
}

func cloneOpenAIHedgeAccount(account OpenAIHedgeAccount) OpenAIHedgeAccount {
	if account.ParentAccountID != nil {
		parentID := *account.ParentAccountID
		account.ParentAccountID = &parentID
	}
	return account
}

func (account OpenAIHedgeAccount) ownerID() int64 {
	if account.ParentAccountID != nil {
		return *account.ParentAccountID
	}
	return account.AccountID
}

func (account OpenAIHedgeAccount) validate() error {
	if account.AccountID <= 0 {
		return errors.New("openai hedge account id must be positive")
	}
	if account.ParentAccountID != nil {
		if *account.ParentAccountID <= 0 {
			return errors.New("openai hedge parent account id must be positive")
		}
		if *account.ParentAccountID == account.AccountID {
			return errors.New("openai hedge account cannot own itself")
		}
	}
	return nil
}

// OpenAIHedgeRequest contains only coordinator-level request traits and an
// opaque payload for the attempt adapter. Stateless, streaming requests with
// store=false and no previous response id are the only hedgeable shape.
type OpenAIHedgeRequest struct {
	Provider               string
	Model                  string
	ModelFamily            string
	Protocol               string
	Stream                 bool
	Stateless              bool
	Store                  bool
	PreviousResponseID     string
	FullInput              bool
	ClientVisibleCommitted bool
	CostClass              OpenAIHedgeCostClass
	Primary                OpenAIHedgeAccount
	Payload                []byte
}

func cloneOpenAIHedgeRequest(request OpenAIHedgeRequest) OpenAIHedgeRequest {
	request.Primary = cloneOpenAIHedgeAccount(request.Primary)
	request.Payload = append([]byte(nil), request.Payload...)
	return request
}

// OpenAIHedgeSelection is passed to candidate selection without the request
// payload, preventing policy ports from retaining prompts.
type OpenAIHedgeSelection struct {
	Provider    string
	Model       string
	ModelFamily string
	Protocol    string
	CostClass   OpenAIHedgeCostClass
	Primary     OpenAIHedgeAccount
}

// OpenAIHedgeAdmission contains the two candidate identities and bounded cost
// class needed by provider, headroom, and cost policy adapters.
type OpenAIHedgeAdmission struct {
	Provider    string
	Model       string
	ModelFamily string
	Protocol    string
	CostClass   OpenAIHedgeCostClass
	Primary     OpenAIHedgeAccount
	Candidate   OpenAIHedgeAccount
}

// OpenAIHedgeAdmissionDecision keeps the three dynamic checks explicit. The
// coordinator launches only when every field is true.
type OpenAIHedgeAdmissionDecision struct {
	ProviderEligible  bool
	HeadroomAvailable bool
	CostAllowed       bool
}

// OpenAIHedgeCandidatePort selects at most one secondary account.
type OpenAIHedgeCandidatePort interface {
	SelectOpenAIHedgeCandidate(context.Context, OpenAIHedgeSelection) (OpenAIHedgeAccount, error)
}

// OpenAIHedgeAdmissionPort owns provider capability, live headroom, and cost
// budget checks. Selection and admission remain separate so stale candidates
// cannot bypass a launch-time capacity check.
type OpenAIHedgeAdmissionPort interface {
	CheckOpenAIHedgeAdmission(context.Context, OpenAIHedgeAdmission) (OpenAIHedgeAdmissionDecision, error)
}

// OpenAIHedgeAttempt is immutable input to one upstream attempt.
type OpenAIHedgeAttempt struct {
	Role        OpenAIAttemptRole
	Provider    string
	Model       string
	ModelFamily string
	Protocol    string
	CostClass   OpenAIHedgeCostClass
	Account     OpenAIHedgeAccount
	Payload     []byte
}

// OpenAIHedgeEventEmitter is valid only for the duration of
// RunOpenAIHedgeAttempt. Emit blocks only on that attempt's bounded channel.
type OpenAIHedgeEventEmitter interface {
	EmitOpenAIHedgeEvent(OpenAIHedgeEvent) error
}

// OpenAIHedgeAttemptPort runs an upstream attempt until it emits one terminal
// event or returns. Returning without a terminal event is treated as failure.
type OpenAIHedgeAttemptPort interface {
	RunOpenAIHedgeAttempt(context.Context, OpenAIHedgeAttempt, OpenAIHedgeEventEmitter) error
}

// OpenAIHedgeSlot is released exactly once, and only after its attempt goroutine
// exits. A cleanup timeout may return to the caller but never frees a live slot.
type OpenAIHedgeSlot interface {
	Release()
}

// OpenAIHedgeSlotPort owns concurrency slot acquisition for both attempts.
type OpenAIHedgeSlotPort interface {
	AcquireOpenAIHedgeSlot(context.Context, OpenAIAttemptRole, OpenAIHedgeAccount) (OpenAIHedgeSlot, error)
}

// OpenAIHedgeOutput identifies the sole committed attempt for a sink event.
type OpenAIHedgeOutput struct {
	Role      OpenAIAttemptRole
	AccountID int64
	Event     OpenAIHedgeEvent
}

// OpenAIHedgeSink is called serially and only after an atomic winner commit.
// Preambles are retained per attempt and flushed only for the winner.
type OpenAIHedgeSink interface {
	WriteOpenAIHedgeEvent(context.Context, OpenAIHedgeOutput) error
}

// OpenAIHedgeLoserBillingMode states whether the loser supplied authoritative
// billing evidence before bounded cleanup ended.
type OpenAIHedgeLoserBillingMode uint8

const (
	OpenAIHedgeLoserBillingUnknown OpenAIHedgeLoserBillingMode = iota
	OpenAIHedgeLoserBillingActual
	OpenAIHedgeLoserBillingFullPotential
)

// OpenAIHedgeLoserBilling is emitted once when a two-attempt race has a winner.
type OpenAIHedgeLoserBilling struct {
	Role         OpenAIAttemptRole
	AccountID    int64
	Mode         OpenAIHedgeLoserBillingMode
	AmountMicros uint64
}

// OpenAIHedgeBillingPort records provider-cost exposure for the loser only.
// It MUST NOT debit user balance or usage quota. Winner user billing remains
// exclusively in the existing request path when integration is added.
type OpenAIHedgeBillingPort interface {
	FinalizeOpenAIHedgeLoserBilling(context.Context, OpenAIHedgeLoserBilling) error
}

// OpenAIHedgeEligibilityReason is bounded telemetry, never a raw policy error.
type OpenAIHedgeEligibilityReason uint8

const (
	OpenAIHedgeEligibilityEligible OpenAIHedgeEligibilityReason = iota
	OpenAIHedgeEligibilityDisabled
	OpenAIHedgeEligibilityNotStreaming
	OpenAIHedgeEligibilityStateful
	OpenAIHedgeEligibilityStoreEnabled
	OpenAIHedgeEligibilityPreviousResponse
	OpenAIHedgeEligibilityNotFullInput
	OpenAIHedgeEligibilityClientVisibleCommitted
	OpenAIHedgeEligibilityVeryHeavyDisabled
	OpenAIHedgeEligibilityProvider
	OpenAIHedgeEligibilityNoCandidate
	OpenAIHedgeEligibilitySameOwner
	OpenAIHedgeEligibilityHeadroom
	OpenAIHedgeEligibilityCost
	OpenAIHedgeEligibilitySlot
	OpenAIHedgeEligibilityPolicyError
)

// OpenAIHedgeTelemetryKind describes coordinator lifecycle callbacks.
type OpenAIHedgeTelemetryKind uint8

const (
	OpenAIHedgeTelemetryEligibility OpenAIHedgeTelemetryKind = iota
	OpenAIHedgeTelemetryAttemptStarted
	OpenAIHedgeTelemetryAttemptTerminal
	OpenAIHedgeTelemetryHedgeOutcome
	OpenAIHedgeTelemetryWinnerCommitted
	OpenAIHedgeTelemetryLoserLifecycle
	OpenAIHedgeTelemetryCancellation
	OpenAIHedgeTelemetryBillingFinalized
	OpenAIHedgeTelemetrySlotReleased
	OpenAIHedgeTelemetryCompleted
)

// OpenAIHedgeTelemetryEvent carries fixed-cardinality lifecycle values only.
type OpenAIHedgeTelemetryEvent struct {
	Kind               OpenAIHedgeTelemetryKind
	Role               OpenAIAttemptRole
	AccountID          int64
	EligibilityReason  OpenAIHedgeEligibilityReason
	HedgeOutcome       OpenAIHedgeOutcome
	WinnerOutcome      OpenAIWinnerOutcome
	LoserCancelOutcome OpenAILoserCancelOutcome
	LoserLifecycle     OpenAILoserLifecycleEvent
	CancelReason       OpenAICancelReason
	BillingMode        OpenAIHedgeLoserBillingMode
	BillingOutcome     OpenAIBillingOutcome
	CapacityOutcome    OpenAICapacityOutcome
	Phase              OpenAILatencyPhase
	Elapsed            time.Duration
	TerminalSuccess    bool
	TerminalFailure    bool
	CleanupTimedOut    bool
}

// OpenAIHedgeTelemetryPort receives lifecycle callbacks. Implementations must
// be concurrency-safe because slot release is observed by attempt goroutines.
type OpenAIHedgeTelemetryPort interface {
	RecordOpenAIHedgeTelemetry(OpenAIHedgeTelemetryEvent)
}

// OpenAIHedgeCoordinatorDependencies are standalone ports; no gateway type is
// referenced and this file performs no production wiring.
type OpenAIHedgeCoordinatorDependencies struct {
	Candidates OpenAIHedgeCandidatePort
	Admission  OpenAIHedgeAdmissionPort
	Attempts   OpenAIHedgeAttemptPort
	Slots      OpenAIHedgeSlotPort
	Sink       OpenAIHedgeSink
	Billing    OpenAIHedgeBillingPort
	Telemetry  OpenAIHedgeTelemetryPort
}

// OpenAIHedgeResult reports the single committed winner and terminal outcome.
type OpenAIHedgeResult struct {
	Winner              OpenAIAttemptRole
	HedgeLaunched       bool
	Terminal            OpenAIHedgeEvent
	LoserBilling        *OpenAIHedgeLoserBilling
	LoserBillingOutcome OpenAIBillingOutcome
}

type openAIHedgeCoordinatorConfig struct {
	enabled                bool
	standardThreshold      time.Duration
	highThreshold          time.Duration
	veryHeavyThreshold     time.Duration
	veryHeavyEnabled       bool
	eventChannelCapacity   int
	maxPrecommitEvents     int
	maxPrecommitBytes      int
	cancelDrainTimeout     time.Duration
	maxAttempts            int
	maxDuplicateCostMicros uint64
}

func newOpenAIHedgeCoordinatorConfig(source config.OpenAIHedgeConfig) (openAIHedgeCoordinatorConfig, error) {
	coordinatorConfig := openAIHedgeCoordinatorConfig{enabled: source.Enabled}
	if !source.Enabled {
		return coordinatorConfig, nil
	}

	values := []struct {
		name    string
		value   int
		maximum int
	}{
		{"standard threshold", source.StandardThresholdSeconds, config.MaximumOpenAIHedgeThresholdSeconds},
		{"high threshold", source.HighThresholdSeconds, config.MaximumOpenAIHedgeThresholdSeconds},
		{"very heavy threshold", source.VeryHeavyThresholdSeconds, config.MaximumOpenAIHedgeThresholdSeconds},
		{"event channel capacity", source.EventChannelCapacity, config.MaximumOpenAIHedgeEventChannelCapacity},
		{"max precommit events", source.MaxPrecommitEvents, config.MaximumOpenAIHedgeMaxPrecommitEvents},
		{"max precommit bytes", source.MaxPrecommitBytes, config.MaximumOpenAIHedgeMaxPrecommitBytes},
		{"cancel drain timeout", source.CancelDrainTimeoutSeconds, config.MaximumOpenAIHedgeCancelDrainTimeoutSeconds},
	}
	for _, value := range values {
		if value.value <= 0 {
			return coordinatorConfig, fmt.Errorf("openai hedge %s must be positive", value.name)
		}
		if value.value > value.maximum {
			return coordinatorConfig, fmt.Errorf("openai hedge %s must not exceed %d", value.name, value.maximum)
		}
	}
	if source.MaxAttempts != 2 {
		return coordinatorConfig, fmt.Errorf("openai hedge max attempts must be exactly 2, got %d", source.MaxAttempts)
	}
	if source.StandardThresholdSeconds > source.HighThresholdSeconds {
		return coordinatorConfig, errors.New("openai hedge high threshold must be at least standard threshold")
	}
	if source.HighThresholdSeconds > source.VeryHeavyThresholdSeconds {
		return coordinatorConfig, errors.New("openai hedge very heavy threshold must be at least high threshold")
	}
	if source.EventChannelCapacity > source.MaxPrecommitEvents {
		return coordinatorConfig, errors.New("openai hedge max precommit events must be at least event channel capacity")
	}

	coordinatorConfig.standardThreshold = time.Duration(source.StandardThresholdSeconds) * time.Second
	coordinatorConfig.highThreshold = time.Duration(source.HighThresholdSeconds) * time.Second
	coordinatorConfig.veryHeavyThreshold = time.Duration(source.VeryHeavyThresholdSeconds) * time.Second
	if source.NoProgressEnabled {
		return openAIHedgeCoordinatorConfig{}, errors.New("openai hedge no-progress trigger is not supported until shadow telemetry validates it")
	}
	coordinatorConfig.veryHeavyEnabled = source.VeryHeavyEnabled
	coordinatorConfig.eventChannelCapacity = source.EventChannelCapacity
	coordinatorConfig.maxPrecommitEvents = source.MaxPrecommitEvents
	coordinatorConfig.maxPrecommitBytes = source.MaxPrecommitBytes
	coordinatorConfig.cancelDrainTimeout = time.Duration(source.CancelDrainTimeoutSeconds) * time.Second
	coordinatorConfig.maxAttempts = source.MaxAttempts
	coordinatorConfig.maxDuplicateCostMicros = source.MaxDuplicateCostMicros
	return coordinatorConfig, nil
}

type openAIHedgeTimer interface {
	channel() <-chan time.Time
	reset(time.Duration)
	stop()
}

type systemOpenAIHedgeTimer struct {
	timer *time.Timer
}

func newSystemOpenAIHedgeTimer(duration time.Duration) openAIHedgeTimer {
	return &systemOpenAIHedgeTimer{timer: time.NewTimer(duration)}
}

func (timer *systemOpenAIHedgeTimer) channel() <-chan time.Time {
	return timer.timer.C
}

func (timer *systemOpenAIHedgeTimer) reset(duration time.Duration) {
	if !timer.timer.Stop() {
		select {
		case <-timer.timer.C:
		default:
		}
	}
	timer.timer.Reset(duration)
}

func (timer *systemOpenAIHedgeTimer) stop() {
	if timer == nil || timer.timer == nil {
		return
	}
	if !timer.timer.Stop() {
		select {
		case <-timer.timer.C:
		default:
		}
	}
}

// OpenAIHedgeCoordinator owns one primary and at most one hedge attempt.
type OpenAIHedgeCoordinator struct {
	config       openAIHedgeCoordinatorConfig
	dependencies OpenAIHedgeCoordinatorDependencies
	now          func() time.Time
	newTimer     func(time.Duration) openAIHedgeTimer
}

func NewOpenAIHedgeCoordinator(
	source config.OpenAIHedgeConfig,
	dependencies OpenAIHedgeCoordinatorDependencies,
) (*OpenAIHedgeCoordinator, error) {
	coordinatorConfig, err := newOpenAIHedgeCoordinatorConfig(source)
	if err != nil {
		return nil, err
	}
	coordinator := &OpenAIHedgeCoordinator{
		config:       coordinatorConfig,
		dependencies: dependencies,
		now:          time.Now,
		newTimer:     newSystemOpenAIHedgeTimer,
	}
	if !coordinatorConfig.enabled {
		return coordinator, nil
	}
	if isNilHedgePort(dependencies.Candidates) {
		return nil, errors.New("openai hedge candidate port is required")
	}
	if isNilHedgePort(dependencies.Admission) {
		return nil, errors.New("openai hedge admission port is required")
	}
	if isNilHedgePort(dependencies.Attempts) {
		return nil, errors.New("openai hedge attempt port is required")
	}
	if isNilHedgePort(dependencies.Slots) {
		return nil, errors.New("openai hedge slot port is required")
	}
	if isNilHedgePort(dependencies.Sink) {
		return nil, errors.New("openai hedge sink is required")
	}
	if isNilHedgePort(dependencies.Billing) {
		return nil, errors.New("openai hedge billing port is required")
	}
	return coordinator, nil
}

func isNilHedgePort(port any) bool {
	if port == nil {
		return true
	}
	value := reflect.ValueOf(port)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func callHedgePort[T any](operation string, call func() (T, error)) (result T, err error) {
	defer func() {
		if recover() != nil {
			// Never include adapter panic payloads: they may contain native request
			// data or credentials.
			err = fmt.Errorf("hedge %s panicked", operation)
		}
	}()
	return call()
}

func callHedgeErrorPort(operation string, call func() error) (err error) {
	_, err = callHedgePort(operation, func() (struct{}, error) {
		return struct{}{}, call()
	})
	return err
}

// Coordinate executes one request through the isolated hedge state machine.
func (coordinator *OpenAIHedgeCoordinator) Coordinate(
	ctx context.Context,
	request OpenAIHedgeRequest,
) (OpenAIHedgeResult, error) {
	if coordinator == nil || !coordinator.config.enabled {
		return OpenAIHedgeResult{}, ErrOpenAIHedgeDisabled
	}
	if ctx == nil {
		return OpenAIHedgeResult{}, errors.New("openai hedge context is nil")
	}
	request = cloneOpenAIHedgeRequest(request)
	if err := validateOpenAIHedgeRequest(request); err != nil {
		return OpenAIHedgeResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return OpenAIHedgeResult{}, err
	}

	state := &openAIHedgeCoordinateState{
		coordinator: coordinator,
		ctx:         ctx,
		request:     request,
		startedAt:   coordinator.now(),
	}
	primary, err := state.startAttempt(OpenAIAttemptRolePrimary, request.Primary)
	if err != nil {
		terminal := OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalFailure, Err: fmt.Errorf("acquire primary openai hedge slot: %w", err)}
		state.completeWithoutRuntime(OpenAIAttemptRolePrimary, request.Primary.AccountID, terminal, terminal.Err)
		return state.result, state.finalErr
	}
	state.primary = primary

	state.hedgeEligible, state.eligibilityReason = state.staticEligibility()
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:              OpenAIHedgeTelemetryEligibility,
		Role:              OpenAIAttemptRolePrimary,
		AccountID:         request.Primary.AccountID,
		EligibilityReason: state.eligibilityReason,
		Phase:             OpenAILatencyPhaseHedgeEligible,
		Elapsed:           coordinator.now().Sub(state.startedAt),
	})
	if state.hedgeEligible {
		state.timer = coordinator.newTimer(state.initialThreshold())
	} else {
		state.recordHedgeNotLaunched()
	}

	state.run()
	state.cleanup()
	state.finalizeLoserBilling()
	state.result.Winner = OpenAIAttemptRole(state.winner.Load())
	state.result.HedgeLaunched = state.hedge != nil
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:            OpenAIHedgeTelemetryCompleted,
		Role:            state.result.Winner,
		HedgeOutcome:    state.finalHedgeOutcome(),
		CleanupTimedOut: state.cleanupTimedOut,
		Elapsed:         coordinator.now().Sub(state.startedAt),
	})
	return state.result, state.finalErr
}

func validateOpenAIHedgeRequest(request OpenAIHedgeRequest) error {
	if err := request.Primary.validate(); err != nil {
		return fmt.Errorf("validate primary openai hedge account: %w", err)
	}
	for _, label := range []struct {
		name  string
		value string
	}{
		{name: "provider", value: request.Provider},
		{name: "model", value: request.Model},
		{name: "model family", value: request.ModelFamily},
		{name: "protocol", value: request.Protocol},
	} {
		value := strings.TrimSpace(label.value)
		if value == "" {
			return fmt.Errorf("openai hedge %s is required", label.name)
		}
		if len(value) > 128 || strings.ContainsAny(value, "\r\n\t") {
			return fmt.Errorf("openai hedge %s must be a bounded single-line label", label.name)
		}
	}
	if request.CostClass >= openAIHedgeCostClassCardinality {
		return fmt.Errorf("invalid openai hedge cost class %d", request.CostClass)
	}
	return nil
}

type openAIHedgeAttemptRuntime struct {
	attempt OpenAIHedgeAttempt
	ctx     context.Context
	cancel  context.CancelFunc
	lease   OpenAIHedgeSlot
	events  chan OpenAIHedgeEvent

	emitMu       sync.Mutex
	emittedFinal bool
	runErr       error
	releaseErr   error
	releaseOnce  sync.Once
	cancelReason atomic.Uint32

	closed           bool
	terminal         bool
	terminalSuccess  bool
	terminalEvent    OpenAIHedgeEvent
	failure          error
	pending          []OpenAIHedgeEvent
	pendingBytes     int
	billingKnown     bool
	actualCostMicros uint64
}

func (runtime *openAIHedgeAttemptRuntime) EmitOpenAIHedgeEvent(event OpenAIHedgeEvent) error {
	runtime.emitMu.Lock()
	defer runtime.emitMu.Unlock()
	if runtime.emittedFinal {
		return ErrOpenAIHedgeEventAfterTerminal
	}
	if err := event.validate(0); err != nil {
		return err
	}
	if event.Kind.terminal() {
		runtime.emittedFinal = true
	}
	event = cloneOpenAIHedgeEvent(event)
	select {
	case runtime.events <- event:
		return nil
	case <-runtime.ctx.Done():
		return runtime.ctx.Err()
	}
}

func (runtime *openAIHedgeAttemptRuntime) requestCancel(
	state *openAIHedgeCoordinateState,
	reason OpenAICancelReason,
) {
	firstCancellation := runtime.cancelReason.CompareAndSwap(uint32(OpenAICancelReasonNone), uint32(reason))
	runtime.cancel()
	if firstCancellation {
		state.record(OpenAIHedgeTelemetryEvent{
			Kind:         OpenAIHedgeTelemetryCancellation,
			Role:         runtime.attempt.Role,
			AccountID:    runtime.attempt.Account.AccountID,
			CancelReason: reason,
			Elapsed:      state.coordinator.now().Sub(state.startedAt),
		})
	}
}

func (runtime *openAIHedgeAttemptRuntime) release(state *openAIHedgeCoordinateState) {
	runtime.releaseOnce.Do(func() {
		runtime.lease.Release()
		state.record(OpenAIHedgeTelemetryEvent{
			Kind:      OpenAIHedgeTelemetrySlotReleased,
			Role:      runtime.attempt.Role,
			AccountID: runtime.attempt.Account.AccountID,
			Elapsed:   state.coordinator.now().Sub(state.startedAt),
		})
	})
}

func (runtime *openAIHedgeAttemptRuntime) releaseSafely(state *openAIHedgeCoordinateState) {
	defer func() {
		if recover() != nil {
			runtime.releaseErr = errors.New("hedge slot release panicked")
		}
	}()
	runtime.release(state)
}

type openAIHedgeCoordinateState struct {
	coordinator *OpenAIHedgeCoordinator
	ctx         context.Context
	request     OpenAIHedgeRequest
	startedAt   time.Time

	primary *openAIHedgeAttemptRuntime
	hedge   *openAIHedgeAttemptRuntime
	timer   openAIHedgeTimer

	hedgeEligible      bool
	eligibilityReason  OpenAIHedgeEligibilityReason
	hedgeLaunchTried   bool
	hedgeOutcomeStored bool
	winner             atomic.Uint32
	terminalWritten    atomic.Bool
	finished           bool
	cleanupTimedOut    bool
	result             OpenAIHedgeResult
	finalErr           error
	telemetryMu        sync.Mutex
}

func (state *openAIHedgeCoordinateState) record(event OpenAIHedgeTelemetryEvent) {
	telemetry := state.coordinator.dependencies.Telemetry
	if isNilHedgePort(telemetry) {
		return
	}
	state.telemetryMu.Lock()
	defer state.telemetryMu.Unlock()
	// Diagnostic callbacks fail open and cannot break routing or slot cleanup.
	defer func() { _ = recover() }()
	telemetry.RecordOpenAIHedgeTelemetry(event)
}

func (state *openAIHedgeCoordinateState) startAttempt(
	role OpenAIAttemptRole,
	account OpenAIHedgeAccount,
) (*openAIHedgeAttemptRuntime, error) {
	if err := state.ctx.Err(); err != nil {
		return nil, err
	}
	attemptContext, cancel := context.WithCancel(state.ctx)
	lease, err := callHedgePort("slot acquire", func() (OpenAIHedgeSlot, error) {
		return state.coordinator.dependencies.Slots.AcquireOpenAIHedgeSlot(attemptContext, role, account)
	})
	if err != nil {
		cancel()
		return nil, err
	}
	if isNilHedgePort(lease) {
		cancel()
		return nil, errors.New("openai hedge slot port returned a nil lease")
	}
	// A slot implementation may return success concurrently with cancellation.
	// Do not dispatch upstream work under a context already known to be dead.
	if err := attemptContext.Err(); err != nil {
		releaseErr := callHedgeErrorPort("slot release", func() error {
			lease.Release()
			return nil
		})
		cancel()
		return nil, errors.Join(err, releaseErr)
	}
	attempt := OpenAIHedgeAttempt{
		Role:        role,
		Provider:    state.request.Provider,
		Model:       state.request.Model,
		ModelFamily: state.request.ModelFamily,
		Protocol:    state.request.Protocol,
		CostClass:   state.request.CostClass,
		Account:     cloneOpenAIHedgeAccount(account),
		Payload:     append([]byte(nil), state.request.Payload...),
	}
	runtime := &openAIHedgeAttemptRuntime{
		attempt: attempt,
		ctx:     attemptContext,
		cancel:  cancel,
		lease:   lease,
		events:  make(chan OpenAIHedgeEvent, state.coordinator.config.eventChannelCapacity),
	}
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:      OpenAIHedgeTelemetryAttemptStarted,
		Role:      role,
		AccountID: account.AccountID,
		Elapsed:   state.coordinator.now().Sub(state.startedAt),
	})
	go func() {
		defer close(runtime.events)
		defer runtime.releaseSafely(state)
		defer func() {
			if recover() != nil {
				runtime.runErr = errors.Join(runtime.runErr, errors.New("hedge attempt adapter panicked"))
			}
		}()
		runtime.runErr = state.coordinator.dependencies.Attempts.RunOpenAIHedgeAttempt(attemptContext, attempt, &boundedOpenAIHedgeEmitter{
			runtime:  runtime,
			maxBytes: state.coordinator.config.maxPrecommitBytes,
		})
	}()
	return runtime, nil
}

type boundedOpenAIHedgeEmitter struct {
	runtime  *openAIHedgeAttemptRuntime
	maxBytes int
}

func (emitter *boundedOpenAIHedgeEmitter) EmitOpenAIHedgeEvent(event OpenAIHedgeEvent) error {
	if err := event.validate(emitter.maxBytes); err != nil {
		return err
	}
	return emitter.runtime.EmitOpenAIHedgeEvent(event)
}

func (state *openAIHedgeCoordinateState) staticEligibility() (bool, OpenAIHedgeEligibilityReason) {
	if !state.request.Stream {
		return false, OpenAIHedgeEligibilityNotStreaming
	}
	if !state.request.Stateless {
		return false, OpenAIHedgeEligibilityStateful
	}
	if state.request.Store {
		return false, OpenAIHedgeEligibilityStoreEnabled
	}
	if strings.TrimSpace(state.request.PreviousResponseID) != "" {
		return false, OpenAIHedgeEligibilityPreviousResponse
	}
	if !state.request.FullInput {
		return false, OpenAIHedgeEligibilityNotFullInput
	}
	if state.request.ClientVisibleCommitted {
		return false, OpenAIHedgeEligibilityClientVisibleCommitted
	}
	if state.request.CostClass == OpenAIHedgeCostVeryHeavy && !state.coordinator.config.veryHeavyEnabled {
		return false, OpenAIHedgeEligibilityVeryHeavyDisabled
	}
	provider := strings.TrimSpace(state.request.Provider)
	primaryProvider := strings.TrimSpace(state.request.Primary.Provider)
	if provider == "" || primaryProvider == "" || !strings.EqualFold(provider, primaryProvider) {
		return false, OpenAIHedgeEligibilityProvider
	}
	return true, OpenAIHedgeEligibilityEligible
}

func (state *openAIHedgeCoordinateState) initialThreshold() time.Duration {
	switch state.request.CostClass {
	case OpenAIHedgeCostHigh:
		return state.coordinator.config.highThreshold
	case OpenAIHedgeCostVeryHeavy:
		return state.coordinator.config.veryHeavyThreshold
	default:
		return state.coordinator.config.standardThreshold
	}
}

func (state *openAIHedgeCoordinateState) run() {
	for !state.finished {
		if err := state.ctx.Err(); err != nil {
			state.handleClientCancellation(err)
			break
		}

		var primaryEvents <-chan OpenAIHedgeEvent
		if state.primary != nil && !state.primary.closed {
			primaryEvents = state.primary.events
		}
		var hedgeEvents <-chan OpenAIHedgeEvent
		if state.hedge != nil && !state.hedge.closed {
			hedgeEvents = state.hedge.events
		}
		var threshold <-chan time.Time
		if state.timer != nil {
			threshold = state.timer.channel()
		}

		select {
		case <-state.ctx.Done():
			state.handleClientCancellation(state.ctx.Err())
		case event, open := <-primaryEvents:
			if !open {
				state.handleAttemptClosed(state.primary)
				continue
			}
			state.handleEvent(state.primary, event)
		case event, open := <-hedgeEvents:
			if !open {
				state.handleAttemptClosed(state.hedge)
				continue
			}
			state.handleEvent(state.hedge, event)
		case <-threshold:
			state.stopThresholdTimer()
			state.tryLaunchHedge()
			state.maybeCompleteFailures()
		}
	}
	state.stopThresholdTimer()
}

func (state *openAIHedgeCoordinateState) handleEvent(runtime *openAIHedgeAttemptRuntime, event OpenAIHedgeEvent) {
	state.observeBilling(runtime, event)
	// Internal failures (for example a pre-commit buffer overflow) terminalize
	// an attempt before its producer necessarily stops. Preserve late billing,
	// but never let already-queued semantic frames resurrect that attempt.
	if runtime.terminal {
		return
	}
	winner := OpenAIAttemptRole(state.winner.Load())
	if winner != OpenAIAttemptRoleUnknown && winner != runtime.attempt.Role {
		state.observeLateLoserEvent(runtime, event)
		if event.Kind.terminal() {
			state.markTerminal(runtime, event)
		}
		return
	}

	switch event.Kind {
	case OpenAIHedgeEventPreamble:
		if winner == runtime.attempt.Role {
			state.writeWinnerEvent(runtime, event)
			return
		}
		if err := state.bufferPreamble(runtime, event); err != nil {
			failure := OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalFailure, Err: err}
			state.markTerminal(runtime, failure)
			runtime.requestCancel(state, OpenAICancelReasonUpstream)
			state.maybeCompleteFailures()
			return
		}
	case OpenAIHedgeEventReasoning, OpenAIHedgeEventText, OpenAIHedgeEventTool:
		if winner == runtime.attempt.Role {
			state.writeWinnerEvent(runtime, event)
			return
		}
		state.commitWinner(runtime, event)
	case OpenAIHedgeEventTerminalSuccess:
		state.markTerminal(runtime, event)
		if winner == runtime.attempt.Role {
			state.writeWinnerEvent(runtime, event)
			state.finish(nil)
			return
		}
		state.commitWinner(runtime, event)
		if !state.finished {
			state.finish(nil)
		}
	case OpenAIHedgeEventTerminalFailure:
		state.markTerminal(runtime, event)
		if winner == runtime.attempt.Role {
			state.writeWinnerEvent(runtime, event)
			state.finish(event.Err)
			return
		}
		state.maybeCompleteFailures()
	}
}

func (state *openAIHedgeCoordinateState) observeBilling(runtime *openAIHedgeAttemptRuntime, event OpenAIHedgeEvent) {
	if !event.Billing.Known {
		return
	}
	runtime.billingKnown = true
	runtime.actualCostMicros = event.Billing.ActualCostMicros
	if OpenAIAttemptRole(state.winner.Load()) != OpenAIAttemptRoleUnknown &&
		OpenAIAttemptRole(state.winner.Load()) != runtime.attempt.Role {
		state.record(OpenAIHedgeTelemetryEvent{
			Kind:           OpenAIHedgeTelemetryLoserLifecycle,
			Role:           runtime.attempt.Role,
			AccountID:      runtime.attempt.Account.AccountID,
			LoserLifecycle: OpenAILoserLifecycleBillingAfterCancel,
			Elapsed:        state.coordinator.now().Sub(state.startedAt),
		})
	}
}

func (state *openAIHedgeCoordinateState) observeLateLoserEvent(runtime *openAIHedgeAttemptRuntime, event OpenAIHedgeEvent) {
	lifecycle := OpenAILoserLifecycleTerminalAfterCancel
	if event.Billing.Known {
		lifecycle = OpenAILoserLifecycleBillingAfterCancel
	}
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:           OpenAIHedgeTelemetryLoserLifecycle,
		Role:           runtime.attempt.Role,
		AccountID:      runtime.attempt.Account.AccountID,
		LoserLifecycle: lifecycle,
		Elapsed:        state.coordinator.now().Sub(state.startedAt),
	})
}

func (state *openAIHedgeCoordinateState) bufferPreamble(runtime *openAIHedgeAttemptRuntime, event OpenAIHedgeEvent) error {
	if len(runtime.pending) >= state.coordinator.config.maxPrecommitEvents {
		return fmt.Errorf("%w: attempt %d event count", ErrOpenAIHedgeEventBufferExceeded, runtime.attempt.Role)
	}
	if len(event.Data) > state.coordinator.config.maxPrecommitBytes-runtime.pendingBytes {
		return fmt.Errorf("%w: attempt %d byte count", ErrOpenAIHedgeEventBufferExceeded, runtime.attempt.Role)
	}
	runtime.pending = append(runtime.pending, cloneOpenAIHedgeEvent(event))
	runtime.pendingBytes += len(event.Data)
	return nil
}

func (state *openAIHedgeCoordinateState) commitWinner(runtime *openAIHedgeAttemptRuntime, event OpenAIHedgeEvent) {
	if !event.Kind.commitsWinner() {
		return
	}
	if !state.winner.CompareAndSwap(uint32(OpenAIAttemptRoleUnknown), uint32(runtime.attempt.Role)) {
		if OpenAIAttemptRole(state.winner.Load()) == runtime.attempt.Role {
			state.writeWinnerEvent(runtime, event)
		}
		return
	}
	state.stopThresholdTimer()
	state.result.Winner = runtime.attempt.Role
	winnerOutcome := OpenAIWinnerOutcomePrimary
	hedgeOutcome := OpenAIHedgeOutcomePrimaryWon
	if runtime.attempt.Role == OpenAIAttemptRoleHedge {
		winnerOutcome = OpenAIWinnerOutcomeHedge
		hedgeOutcome = OpenAIHedgeOutcomeHedgeWon
	}
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:          OpenAIHedgeTelemetryWinnerCommitted,
		Role:          runtime.attempt.Role,
		AccountID:     runtime.attempt.Account.AccountID,
		WinnerOutcome: winnerOutcome,
		HedgeOutcome:  hedgeOutcome,
		Phase:         OpenAILatencyPhaseWinnerCommit,
		Elapsed:       state.coordinator.now().Sub(state.startedAt),
	})
	state.cancelLoser(runtime)
	for _, pending := range runtime.pending {
		if !state.writeWinnerEvent(runtime, pending) {
			return
		}
	}
	runtime.pending = nil
	runtime.pendingBytes = 0
	state.writeWinnerEvent(runtime, event)
}

func (state *openAIHedgeCoordinateState) cancelLoser(winner *openAIHedgeAttemptRuntime) {
	loser := state.primary
	if winner == state.primary {
		loser = state.hedge
	}
	if loser == nil {
		state.recordHedgeNotLaunched()
		state.record(OpenAIHedgeTelemetryEvent{
			Kind:               OpenAIHedgeTelemetryLoserLifecycle,
			Role:               OpenAIAttemptRoleUnknown,
			LoserCancelOutcome: OpenAILoserCancelOutcomeNotRequired,
			Elapsed:            state.coordinator.now().Sub(state.startedAt),
		})
		return
	}
	if loser.closed || loser.terminal {
		state.record(OpenAIHedgeTelemetryEvent{
			Kind:               OpenAIHedgeTelemetryLoserLifecycle,
			Role:               loser.attempt.Role,
			AccountID:          loser.attempt.Account.AccountID,
			LoserCancelOutcome: OpenAILoserCancelOutcomeAlreadyComplete,
			Elapsed:            state.coordinator.now().Sub(state.startedAt),
		})
		return
	}
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:               OpenAIHedgeTelemetryLoserLifecycle,
		Role:               loser.attempt.Role,
		AccountID:          loser.attempt.Account.AccountID,
		LoserCancelOutcome: OpenAILoserCancelOutcomeSucceeded,
		LoserLifecycle:     OpenAILoserLifecycleCancelRequested,
		Phase:              OpenAILatencyPhaseLoserCancel,
		Elapsed:            state.coordinator.now().Sub(state.startedAt),
	})
	loser.requestCancel(state, OpenAICancelReasonHedgeLoser)
}

func (state *openAIHedgeCoordinateState) writeWinnerEvent(
	runtime *openAIHedgeAttemptRuntime,
	event OpenAIHedgeEvent,
) bool {
	if state.finished {
		return false
	}
	if event.Kind.terminal() && !state.terminalWritten.CompareAndSwap(false, true) {
		state.finish(errors.New("openai hedge terminal sink event already written"))
		return false
	}
	output := OpenAIHedgeOutput{
		Role:      runtime.attempt.Role,
		AccountID: runtime.attempt.Account.AccountID,
		Event:     cloneOpenAIHedgeEvent(event),
	}
	if event.Kind.terminal() {
		state.result.Terminal = cloneOpenAIHedgeEvent(event)
	}
	if err := callHedgeErrorPort("winner sink write", func() error {
		return state.coordinator.dependencies.Sink.WriteOpenAIHedgeEvent(state.ctx, output)
	}); err != nil {
		state.finalErr = errors.Join(state.finalErr, fmt.Errorf("write committed openai hedge event: %w", err))
		if !event.Kind.terminal() {
			state.writeTerminalWithoutWinner(runtime.attempt.Role, runtime.attempt.Account.AccountID, OpenAIHedgeEvent{
				Kind: OpenAIHedgeEventTerminalFailure,
				Err:  state.finalErr,
			})
		}
		state.finish(state.finalErr)
		return false
	}
	return true
}

func (state *openAIHedgeCoordinateState) markTerminal(runtime *openAIHedgeAttemptRuntime, event OpenAIHedgeEvent) {
	if runtime.terminal {
		return
	}
	runtime.terminal = true
	runtime.terminalSuccess = event.Kind == OpenAIHedgeEventTerminalSuccess
	runtime.terminalEvent = cloneOpenAIHedgeEvent(event)
	if event.Kind == OpenAIHedgeEventTerminalFailure {
		runtime.failure = event.Err
	}
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:            OpenAIHedgeTelemetryAttemptTerminal,
		Role:            runtime.attempt.Role,
		AccountID:       runtime.attempt.Account.AccountID,
		TerminalSuccess: runtime.terminalSuccess,
		TerminalFailure: !runtime.terminalSuccess,
		Phase:           OpenAILatencyPhaseTerminal,
		Elapsed:         state.coordinator.now().Sub(state.startedAt),
	})
}

func (state *openAIHedgeCoordinateState) handleAttemptClosed(runtime *openAIHedgeAttemptRuntime) {
	if runtime == nil || runtime.closed {
		return
	}
	runtime.closed = true
	// A closed attempt channel and client cancellation may become ready in the
	// same scheduler turn. Cancellation owns that race: never synthesize a
	// missing-terminal provider failure after the caller has gone away.
	if err := state.ctx.Err(); err != nil {
		state.handleClientCancellation(err)
		return
	}
	if runtime.releaseErr != nil {
		state.finalErr = errors.Join(state.finalErr, runtime.releaseErr)
	}
	if OpenAICancelReason(runtime.cancelReason.Load()) == OpenAICancelReasonHedgeLoser {
		state.record(OpenAIHedgeTelemetryEvent{
			Kind:           OpenAIHedgeTelemetryLoserLifecycle,
			Role:           runtime.attempt.Role,
			AccountID:      runtime.attempt.Account.AccountID,
			LoserLifecycle: OpenAILoserLifecycleTransportClosed,
			Elapsed:        state.coordinator.now().Sub(state.startedAt),
		})
	}
	if runtime.terminal {
		return
	}
	cancelReason := OpenAICancelReason(runtime.cancelReason.Load())
	winner := OpenAIAttemptRole(state.winner.Load())
	if cancelReason != OpenAICancelReasonNone && (state.finished || (winner != OpenAIAttemptRoleUnknown && winner != runtime.attempt.Role)) {
		return
	}
	failure := runtime.runErr
	if failure == nil {
		failure = ErrOpenAIHedgeAttemptMissingTerminal
	} else {
		failure = errors.Join(ErrOpenAIHedgeAttemptMissingTerminal, failure)
	}
	terminal := OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalFailure, Err: failure}
	// Route this through normal terminal handling. If this runtime had already
	// committed, the client still needs exactly one terminal failure and the
	// coordinator must finish rather than wait forever.
	state.handleEvent(runtime, terminal)
}

func (state *openAIHedgeCoordinateState) tryLaunchHedge() {
	if state.cancelHedgeLaunchIfCallerDone() || !state.hedgeEligible || state.hedgeLaunchTried || state.finished || state.winner.Load() != uint32(OpenAIAttemptRoleUnknown) {
		return
	}
	state.hedgeLaunchTried = true
	selection := OpenAIHedgeSelection{
		Provider:    state.request.Provider,
		Model:       state.request.Model,
		ModelFamily: state.request.ModelFamily,
		Protocol:    state.request.Protocol,
		CostClass:   state.request.CostClass,
		Primary:     cloneOpenAIHedgeAccount(state.request.Primary),
	}
	candidate, err := callHedgePort("candidate selection", func() (OpenAIHedgeAccount, error) {
		return state.coordinator.dependencies.Candidates.SelectOpenAIHedgeCandidate(state.ctx, selection)
	})
	if state.cancelHedgeLaunchIfCallerDone() {
		return
	}
	if err != nil {
		reason := OpenAIHedgeEligibilityNoCandidate
		if !errors.Is(err, ErrOpenAIHedgeNoCandidate) {
			reason = OpenAIHedgeEligibilityPolicyError
		}
		state.rejectHedge(reason, OpenAICapacityOutcomeUnknown)
		return
	}
	candidate = cloneOpenAIHedgeAccount(candidate)
	if err := candidate.validate(); err != nil {
		state.rejectHedge(OpenAIHedgeEligibilityPolicyError, OpenAICapacityOutcomeUnknown)
		return
	}
	primaryAccountID := state.request.Primary.AccountID
	primaryOwnerID := state.request.Primary.ownerID()
	candidateAccountID := candidate.AccountID
	candidateOwnerID := candidate.ownerID()
	if candidateAccountID == primaryAccountID || candidateAccountID == primaryOwnerID ||
		candidateOwnerID == primaryAccountID || candidateOwnerID == primaryOwnerID {
		state.rejectHedge(OpenAIHedgeEligibilitySameOwner, OpenAICapacityOutcomeUnknown)
		return
	}
	provider := strings.TrimSpace(state.request.Provider)
	if strings.TrimSpace(candidate.Provider) == "" || !strings.EqualFold(provider, strings.TrimSpace(candidate.Provider)) {
		state.rejectHedge(OpenAIHedgeEligibilityProvider, OpenAICapacityOutcomeUnknown)
		return
	}
	admissionRequest := OpenAIHedgeAdmission{
		Provider:    state.request.Provider,
		Model:       state.request.Model,
		ModelFamily: state.request.ModelFamily,
		Protocol:    state.request.Protocol,
		CostClass:   state.request.CostClass,
		Primary:     cloneOpenAIHedgeAccount(state.request.Primary),
		Candidate:   cloneOpenAIHedgeAccount(candidate),
	}
	admission, err := callHedgePort("admission", func() (OpenAIHedgeAdmissionDecision, error) {
		return state.coordinator.dependencies.Admission.CheckOpenAIHedgeAdmission(state.ctx, admissionRequest)
	})
	if state.cancelHedgeLaunchIfCallerDone() {
		return
	}
	if err != nil {
		state.rejectHedge(OpenAIHedgeEligibilityPolicyError, OpenAICapacityOutcomeUnknown)
		return
	}
	if !admission.ProviderEligible {
		state.rejectHedge(OpenAIHedgeEligibilityProvider, OpenAICapacityOutcomeUnknown)
		return
	}
	if !admission.HeadroomAvailable {
		state.rejectHedge(OpenAIHedgeEligibilityHeadroom, OpenAICapacityOutcomeConstrained)
		return
	}
	if !admission.CostAllowed {
		state.rejectHedge(OpenAIHedgeEligibilityCost, OpenAICapacityOutcomeAvailable)
		return
	}

	if state.cancelHedgeLaunchIfCallerDone() {
		return
	}
	hedge, err := state.startAttempt(OpenAIAttemptRoleHedge, candidate)
	if err != nil {
		state.rejectHedge(OpenAIHedgeEligibilitySlot, OpenAICapacityOutcomeExhausted)
		return
	}
	state.hedge = hedge
	state.result.HedgeLaunched = true
	state.hedgeOutcomeStored = true
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:            OpenAIHedgeTelemetryHedgeOutcome,
		Role:            OpenAIAttemptRoleHedge,
		AccountID:       candidate.AccountID,
		HedgeOutcome:    OpenAIHedgeOutcomeLaunched,
		CapacityOutcome: OpenAICapacityOutcomeAvailable,
		Phase:           OpenAILatencyPhaseSecondaryDispatch,
		Elapsed:         state.coordinator.now().Sub(state.startedAt),
	})
}

func (state *openAIHedgeCoordinateState) rejectHedge(
	reason OpenAIHedgeEligibilityReason,
	capacity OpenAICapacityOutcome,
) {
	state.eligibilityReason = reason
	state.hedgeOutcomeStored = true
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:              OpenAIHedgeTelemetryHedgeOutcome,
		Role:              OpenAIAttemptRoleHedge,
		EligibilityReason: reason,
		HedgeOutcome:      OpenAIHedgeOutcomeNotLaunched,
		CapacityOutcome:   capacity,
		Elapsed:           state.coordinator.now().Sub(state.startedAt),
	})
}

func (state *openAIHedgeCoordinateState) recordHedgeNotLaunched() {
	if state.hedgeOutcomeStored {
		return
	}
	state.hedgeOutcomeStored = true
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:              OpenAIHedgeTelemetryHedgeOutcome,
		Role:              OpenAIAttemptRoleHedge,
		EligibilityReason: state.eligibilityReason,
		HedgeOutcome:      OpenAIHedgeOutcomeNotLaunched,
		Elapsed:           state.coordinator.now().Sub(state.startedAt),
	})
}

func (state *openAIHedgeCoordinateState) maybeCompleteFailures() {
	if state.finished || state.winner.Load() != uint32(OpenAIAttemptRoleUnknown) || state.primary == nil || !state.primary.terminal || state.primary.terminalSuccess {
		return
	}
	if state.hedgeEligible && !state.hedgeLaunchTried {
		state.stopThresholdTimer()
		state.tryLaunchHedge()
	}
	if state.hedge != nil && (!state.hedge.terminal || state.hedge.terminalSuccess) {
		return
	}
	if state.hedgeEligible && !state.hedgeLaunchTried {
		return
	}

	terminal := cloneOpenAIHedgeEvent(state.primary.terminalEvent)
	role := OpenAIAttemptRolePrimary
	accountID := state.primary.attempt.Account.AccountID
	failure := state.primary.failure
	if state.hedge != nil {
		failure = errors.Join(
			ErrOpenAIHedgeAllAttemptsFailed,
			fmt.Errorf("primary: %w", state.primary.failure),
			fmt.Errorf("hedge: %w", state.hedge.failure),
		)
		terminal = OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalFailure, Err: failure}
		state.record(OpenAIHedgeTelemetryEvent{
			Kind:          OpenAIHedgeTelemetryHedgeOutcome,
			HedgeOutcome:  OpenAIHedgeOutcomeBothFailed,
			WinnerOutcome: OpenAIWinnerOutcomeNoWinner,
			Elapsed:       state.coordinator.now().Sub(state.startedAt),
		})
	} else {
		state.recordHedgeNotLaunched()
	}
	state.writeTerminalWithoutWinner(role, accountID, terminal)
	state.finish(failure)
}

func (state *openAIHedgeCoordinateState) writeTerminalWithoutWinner(
	role OpenAIAttemptRole,
	accountID int64,
	event OpenAIHedgeEvent,
) {
	if !state.terminalWritten.CompareAndSwap(false, true) {
		return
	}
	state.result.Terminal = cloneOpenAIHedgeEvent(event)
	output := OpenAIHedgeOutput{Role: role, AccountID: accountID, Event: cloneOpenAIHedgeEvent(event)}
	writeContext := state.ctx
	if state.ctx.Err() != nil {
		writeContext = context.WithoutCancel(state.ctx)
	}
	if err := callHedgeErrorPort("terminal sink write", func() error {
		return state.coordinator.dependencies.Sink.WriteOpenAIHedgeEvent(writeContext, output)
	}); err != nil {
		state.finalErr = errors.Join(state.finalErr, fmt.Errorf("write openai hedge terminal event: %w", err))
	}
}

func (state *openAIHedgeCoordinateState) cancelHedgeLaunchIfCallerDone() bool {
	if err := state.ctx.Err(); err != nil {
		state.handleClientCancellation(err)
		return true
	}
	return false
}

func (state *openAIHedgeCoordinateState) handleClientCancellation(cause error) {
	if state.finished {
		return
	}
	reason := OpenAICancelReasonClient
	if errors.Is(cause, context.DeadlineExceeded) {
		reason = OpenAICancelReasonDeadline
	}
	if state.primary != nil {
		state.primary.requestCancel(state, reason)
	}
	if state.hedge != nil {
		state.hedge.requestCancel(state, reason)
	}
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:         OpenAIHedgeTelemetryHedgeOutcome,
		HedgeOutcome: OpenAIHedgeOutcomeCanceled,
		Elapsed:      state.coordinator.now().Sub(state.startedAt),
	})
	role, accountID := state.cancellationTerminalIdentity()
	terminal := OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalFailure, Err: cause}
	state.writeTerminalWithoutWinner(role, accountID, terminal)
	state.finish(cause)
}

func (state *openAIHedgeCoordinateState) cancellationTerminalIdentity() (OpenAIAttemptRole, int64) {
	winner := OpenAIAttemptRole(state.winner.Load())
	switch winner {
	case OpenAIAttemptRolePrimary:
		return winner, state.request.Primary.AccountID
	case OpenAIAttemptRoleHedge:
		if state.hedge != nil {
			return winner, state.hedge.attempt.Account.AccountID
		}
	}
	return OpenAIAttemptRolePrimary, state.request.Primary.AccountID
}

func (state *openAIHedgeCoordinateState) finish(err error) {
	if state.finished {
		if err != nil {
			state.finalErr = errors.Join(state.finalErr, err)
		}
		return
	}
	state.finished = true
	if err != nil {
		state.finalErr = errors.Join(state.finalErr, err)
	}
}

func (state *openAIHedgeCoordinateState) completeWithoutRuntime(
	role OpenAIAttemptRole,
	accountID int64,
	terminal OpenAIHedgeEvent,
	failure error,
) {
	state.writeTerminalWithoutWinner(role, accountID, terminal)
	state.finish(failure)
}

func (state *openAIHedgeCoordinateState) stopThresholdTimer() {
	if state.timer == nil {
		return
	}
	state.timer.stop()
	state.timer = nil
}

func (state *openAIHedgeCoordinateState) cleanup() {
	if state.primary != nil {
		state.primary.requestCancel(state, OpenAICancelReasonUpstream)
	}
	if state.hedge != nil {
		state.hedge.requestCancel(state, OpenAICancelReasonUpstream)
	}
	if state.allAttemptsClosed() {
		return
	}

	timer := time.NewTimer(state.coordinator.config.cancelDrainTimeout)
	defer timer.Stop()
	for !state.allAttemptsClosed() {
		var primaryEvents <-chan OpenAIHedgeEvent
		if state.primary != nil && !state.primary.closed {
			primaryEvents = state.primary.events
		}
		var hedgeEvents <-chan OpenAIHedgeEvent
		if state.hedge != nil && !state.hedge.closed {
			hedgeEvents = state.hedge.events
		}
		select {
		case event, open := <-primaryEvents:
			if !open {
				state.handleAttemptClosed(state.primary)
				continue
			}
			state.handleCleanupEvent(state.primary, event)
		case event, open := <-hedgeEvents:
			if !open {
				state.handleAttemptClosed(state.hedge)
				continue
			}
			state.handleCleanupEvent(state.hedge, event)
		case <-timer.C:
			state.cleanupTimedOut = true
			state.finalErr = errors.Join(state.finalErr, ErrOpenAIHedgeCleanupTimeout)
			return
		}
	}
}

func (state *openAIHedgeCoordinateState) handleCleanupEvent(runtime *openAIHedgeAttemptRuntime, event OpenAIHedgeEvent) {
	state.observeBilling(runtime, event)
	if event.Kind.terminal() {
		state.observeLateLoserEvent(runtime, event)
		state.markTerminal(runtime, event)
	}
}

func (state *openAIHedgeCoordinateState) allAttemptsClosed() bool {
	return (state.primary == nil || state.primary.closed) && (state.hedge == nil || state.hedge.closed)
}

func (state *openAIHedgeCoordinateState) finalizeLoserBilling() {
	winner := OpenAIAttemptRole(state.winner.Load())
	if winner == OpenAIAttemptRoleUnknown || state.hedge == nil {
		return
	}
	loser := state.primary
	if winner == OpenAIAttemptRolePrimary {
		loser = state.hedge
	}
	billing := OpenAIHedgeLoserBilling{
		Role:         loser.attempt.Role,
		AccountID:    loser.attempt.Account.AccountID,
		Mode:         OpenAIHedgeLoserBillingFullPotential,
		AmountMicros: loser.attempt.Account.PotentialCostMicros,
	}
	if loser.billingKnown {
		billing.Mode = OpenAIHedgeLoserBillingActual
		billing.AmountMicros = loser.actualCostMicros
	}
	state.result.LoserBilling = &billing
	billingContext, cancel := context.WithTimeout(context.WithoutCancel(state.ctx), state.coordinator.config.cancelDrainTimeout)
	defer cancel()
	// This callback records duplicate provider-cost exposure; users are billed
	// only by the existing winner path.
	billingOutcome := OpenAIBillingOutcomeNotBillable
	if err := callHedgeErrorPort("loser cost exposure", func() error {
		return state.coordinator.dependencies.Billing.FinalizeOpenAIHedgeLoserBilling(billingContext, billing)
	}); err != nil {
		// Exposure recording is operational bookkeeping, never user charging and
		// never a reason to invalidate an already committed client response.
		billingOutcome = OpenAIBillingOutcomeFailed
	}
	state.result.LoserBillingOutcome = billingOutcome
	state.record(OpenAIHedgeTelemetryEvent{
		Kind:            OpenAIHedgeTelemetryBillingFinalized,
		Role:            loser.attempt.Role,
		AccountID:       loser.attempt.Account.AccountID,
		BillingMode:     billing.Mode,
		BillingOutcome:  billingOutcome,
		HedgeOutcome:    state.finalHedgeOutcome(),
		WinnerOutcome:   winnerOutcome(winner),
		CapacityOutcome: OpenAICapacityOutcomeAvailable,
		Elapsed:         state.coordinator.now().Sub(state.startedAt),
	})
}

func (state *openAIHedgeCoordinateState) finalHedgeOutcome() OpenAIHedgeOutcome {
	winner := OpenAIAttemptRole(state.winner.Load())
	switch winner {
	case OpenAIAttemptRolePrimary:
		return OpenAIHedgeOutcomePrimaryWon
	case OpenAIAttemptRoleHedge:
		return OpenAIHedgeOutcomeHedgeWon
	default:
		if state.ctx.Err() != nil {
			return OpenAIHedgeOutcomeCanceled
		}
		if state.hedge != nil && state.primary != nil && state.primary.failure != nil && state.hedge.failure != nil {
			return OpenAIHedgeOutcomeBothFailed
		}
		return OpenAIHedgeOutcomeNotLaunched
	}
}

func winnerOutcome(role OpenAIAttemptRole) OpenAIWinnerOutcome {
	switch role {
	case OpenAIAttemptRolePrimary:
		return OpenAIWinnerOutcomePrimary
	case OpenAIAttemptRoleHedge:
		return OpenAIWinnerOutcomeHedge
	default:
		return OpenAIWinnerOutcomeNoWinner
	}
}
