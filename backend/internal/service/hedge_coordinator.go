package service

import (
	"context"
	"errors"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// Provider-neutral hedge API.
//
// The race kernel understands only semantic stream events and attempt
// lifecycle. A provider adapter is responsible for translating its native
// protocol (for example OpenAI Responses, Anthropic Messages, or Gemini
// generateContent) into these types and for setting the request portability
// fields before invoking Coordinate. This deliberately keeps provider state,
// native frame parsing, credentials, and model-family policy out of the race
// state machine.

var (
	ErrHedgeDisabled               = ErrOpenAIHedgeDisabled
	ErrHedgeNoCandidate            = ErrOpenAIHedgeNoCandidate
	ErrHedgeAttemptMissingTerminal = ErrOpenAIHedgeAttemptMissingTerminal
	ErrHedgeEventAfterTerminal     = ErrOpenAIHedgeEventAfterTerminal
	ErrHedgeEventBufferExceeded    = ErrOpenAIHedgeEventBufferExceeded
	ErrHedgeAllAttemptsFailed      = ErrOpenAIHedgeAllAttemptsFailed
	ErrHedgeCleanupTimeout         = ErrOpenAIHedgeCleanupTimeout
)

type HedgeAttemptRole = OpenAIAttemptRole

const (
	HedgeAttemptRoleUnknown = OpenAIAttemptRoleUnknown
	HedgeAttemptRolePrimary = OpenAIAttemptRolePrimary
	HedgeAttemptRoleHedge   = OpenAIAttemptRoleHedge
)

type HedgeEventKind = OpenAIHedgeEventKind

const (
	HedgeEventPreamble        = OpenAIHedgeEventPreamble
	HedgeEventReasoning       = OpenAIHedgeEventReasoning
	HedgeEventText            = OpenAIHedgeEventText
	HedgeEventTool            = OpenAIHedgeEventTool
	HedgeEventTerminalSuccess = OpenAIHedgeEventTerminalSuccess
	HedgeEventTerminalFailure = OpenAIHedgeEventTerminalFailure
)

type HedgeEvent = OpenAIHedgeEvent
type HedgeCostClass = OpenAIHedgeCostClass

const (
	HedgeCostStandard  = OpenAIHedgeCostStandard
	HedgeCostHigh      = OpenAIHedgeCostHigh
	HedgeCostVeryHeavy = OpenAIHedgeCostVeryHeavy
)

type HedgeAccount = OpenAIHedgeAccount
type HedgeRequest = OpenAIHedgeRequest
type HedgeSelection = OpenAIHedgeSelection
type HedgeAdmission = OpenAIHedgeAdmission
type HedgeAdmissionDecision = OpenAIHedgeAdmissionDecision
type HedgeAttempt = OpenAIHedgeAttempt
type HedgeOutput = OpenAIHedgeOutput
type HedgeResult = OpenAIHedgeResult
type HedgeLoserBilling = OpenAIHedgeLoserBilling
type HedgeLoserBillingMode = OpenAIHedgeLoserBillingMode

const (
	HedgeLoserBillingUnknown       = OpenAIHedgeLoserBillingUnknown
	HedgeLoserBillingActual        = OpenAIHedgeLoserBillingActual
	HedgeLoserBillingFullPotential = OpenAIHedgeLoserBillingFullPotential
)

type HedgeEligibilityReason = OpenAIHedgeEligibilityReason

const (
	HedgeEligibilityEligible               = OpenAIHedgeEligibilityEligible
	HedgeEligibilityDisabled               = OpenAIHedgeEligibilityDisabled
	HedgeEligibilityNotStreaming           = OpenAIHedgeEligibilityNotStreaming
	HedgeEligibilityStateful               = OpenAIHedgeEligibilityStateful
	HedgeEligibilityStoreEnabled           = OpenAIHedgeEligibilityStoreEnabled
	HedgeEligibilityPreviousResponse       = OpenAIHedgeEligibilityPreviousResponse
	HedgeEligibilityNotFullInput           = OpenAIHedgeEligibilityNotFullInput
	HedgeEligibilityClientVisibleCommitted = OpenAIHedgeEligibilityClientVisibleCommitted
	HedgeEligibilityVeryHeavyDisabled      = OpenAIHedgeEligibilityVeryHeavyDisabled
	HedgeEligibilityProvider               = OpenAIHedgeEligibilityProvider
	HedgeEligibilityNoCandidate            = OpenAIHedgeEligibilityNoCandidate
	HedgeEligibilitySameOwner              = OpenAIHedgeEligibilitySameOwner
	HedgeEligibilityHeadroom               = OpenAIHedgeEligibilityHeadroom
	HedgeEligibilityCost                   = OpenAIHedgeEligibilityCost
	HedgeEligibilitySlot                   = OpenAIHedgeEligibilitySlot
	HedgeEligibilityPolicyError            = OpenAIHedgeEligibilityPolicyError
)

type HedgeTelemetryEvent = OpenAIHedgeTelemetryEvent
type HedgeTelemetryKind = OpenAIHedgeTelemetryKind

const (
	HedgeTelemetryEligibility      = OpenAIHedgeTelemetryEligibility
	HedgeTelemetryAttemptStarted   = OpenAIHedgeTelemetryAttemptStarted
	HedgeTelemetryAttemptTerminal  = OpenAIHedgeTelemetryAttemptTerminal
	HedgeTelemetryHedgeOutcome     = OpenAIHedgeTelemetryHedgeOutcome
	HedgeTelemetryWinnerCommitted  = OpenAIHedgeTelemetryWinnerCommitted
	HedgeTelemetryLoserLifecycle   = OpenAIHedgeTelemetryLoserLifecycle
	HedgeTelemetryCancellation     = OpenAIHedgeTelemetryCancellation
	HedgeTelemetryBillingFinalized = OpenAIHedgeTelemetryBillingFinalized
	HedgeTelemetrySlotReleased     = OpenAIHedgeTelemetrySlotReleased
	HedgeTelemetryCompleted        = OpenAIHedgeTelemetryCompleted
)

type HedgeOutcome = OpenAIHedgeOutcome

const (
	HedgeOutcomeNone        = OpenAIHedgeOutcomeNone
	HedgeOutcomeNotLaunched = OpenAIHedgeOutcomeNotLaunched
	HedgeOutcomeLaunched    = OpenAIHedgeOutcomeLaunched
	HedgeOutcomePrimaryWon  = OpenAIHedgeOutcomePrimaryWon
	HedgeOutcomeHedgeWon    = OpenAIHedgeOutcomeHedgeWon
	HedgeOutcomeBothFailed  = OpenAIHedgeOutcomeBothFailed
	HedgeOutcomeCanceled    = OpenAIHedgeOutcomeCanceled
)

type HedgeWinnerOutcome = OpenAIWinnerOutcome

const (
	HedgeWinnerUnknown   = OpenAIWinnerOutcomeUnknown
	HedgeWinnerPrimary   = OpenAIWinnerOutcomePrimary
	HedgeWinnerSecondary = OpenAIWinnerOutcomeHedge
	HedgeWinnerNone      = OpenAIWinnerOutcomeNoWinner
)

type HedgeLoserCancelOutcome = OpenAILoserCancelOutcome

const (
	HedgeLoserCancelUnknown         = OpenAILoserCancelOutcomeUnknown
	HedgeLoserCancelSucceeded       = OpenAILoserCancelOutcomeSucceeded
	HedgeLoserCancelAlreadyComplete = OpenAILoserCancelOutcomeAlreadyComplete
	HedgeLoserCancelFailed          = OpenAILoserCancelOutcomeFailed
	HedgeLoserCancelNotRequired     = OpenAILoserCancelOutcomeNotRequired
)

type HedgeLoserLifecycleEvent = OpenAILoserLifecycleEvent

const (
	HedgeLoserCancelRequested     = OpenAILoserLifecycleCancelRequested
	HedgeLoserTransportClosed     = OpenAILoserLifecycleTransportClosed
	HedgeLoserTerminalAfterCancel = OpenAILoserLifecycleTerminalAfterCancel
	HedgeLoserUsageAfterCancel    = OpenAILoserLifecycleUsageAfterCancel
	HedgeLoserBillingAfterCancel  = OpenAILoserLifecycleBillingAfterCancel
)

type HedgeCancelReason = OpenAICancelReason

const (
	HedgeCancelNone     = OpenAICancelReasonNone
	HedgeCancelClient   = OpenAICancelReasonClient
	HedgeCancelDeadline = OpenAICancelReasonDeadline
	HedgeCancelLoser    = OpenAICancelReasonHedgeLoser
	HedgeCancelUpstream = OpenAICancelReasonUpstream
	HedgeCancelShutdown = OpenAICancelReasonShutdown
)

type HedgeBillingOutcome = OpenAIBillingOutcome

const (
	HedgeBillingUnknown     = OpenAIBillingOutcomeUnknown
	HedgeBillingNotBillable = OpenAIBillingOutcomeNotBillable
	HedgeBillingFailed      = OpenAIBillingOutcomeFailed
)

type HedgeCapacityOutcome = OpenAICapacityOutcome

const (
	HedgeCapacityUnknown     = OpenAICapacityOutcomeUnknown
	HedgeCapacityAvailable   = OpenAICapacityOutcomeAvailable
	HedgeCapacityConstrained = OpenAICapacityOutcomeConstrained
	HedgeCapacityExhausted   = OpenAICapacityOutcomeExhausted
)

type HedgeLatencyPhase = OpenAILatencyPhase

const (
	HedgePhaseEligible          = OpenAILatencyPhaseHedgeEligible
	HedgePhaseSecondaryDispatch = OpenAILatencyPhaseSecondaryDispatch
	HedgePhaseWinnerCommit      = OpenAILatencyPhaseWinnerCommit
	HedgePhaseLoserCancel       = OpenAILatencyPhaseLoserCancel
	HedgePhaseTerminal          = OpenAILatencyPhaseTerminal
)

// HedgeEventEmitter is scoped to one isolated upstream attempt.
type HedgeEventEmitter interface {
	EmitHedgeEvent(HedgeEvent) error
}

// HedgeCandidatePort must exclude the primary account and its credential
// owner. The kernel validates both exclusions again before dispatch.
type HedgeCandidatePort interface {
	SelectHedgeCandidate(context.Context, HedgeSelection) (HedgeAccount, error)
}

// HedgeAdmissionPort owns provider/model-family eligibility, live capacity,
// and cost policy. Provider adapters must reject native state that cannot be
// replayed safely on another credential.
type HedgeAdmissionPort interface {
	CheckHedgeAdmission(context.Context, HedgeAdmission) (HedgeAdmissionDecision, error)
}

// HedgeAttemptPort translates one provider-native stream into semantic events.
// It must honor ctx cancellation and emit at most one terminal event.
type HedgeAttemptPort interface {
	RunHedgeAttempt(context.Context, HedgeAttempt, HedgeEventEmitter) error
}

type HedgeSlot = OpenAIHedgeSlot

type HedgeSlotPort interface {
	AcquireHedgeSlot(context.Context, HedgeAttemptRole, HedgeAccount) (HedgeSlot, error)
}

type HedgeSinkPort interface {
	WriteHedgeEvent(context.Context, HedgeOutput) error
}

// HedgeBillingPort records loser provider-cost exposure only. It must never
// debit a user; winner charging remains outside the race kernel.
type HedgeBillingPort interface {
	FinalizeHedgeLoserBilling(context.Context, HedgeLoserBilling) error
}

type HedgeTelemetryPort interface {
	RecordHedgeTelemetry(HedgeTelemetryEvent)
}

// HedgeConfig is protocol neutral. Thresholds remain integer seconds because
// they are operational configuration, not transport timers.
type HedgeConfig struct {
	Enabled                   bool
	StandardThresholdSeconds  int
	HighThresholdSeconds      int
	VeryHeavyThresholdSeconds int
	VeryHeavyEnabled          bool
	NoProgressEnabled         bool
	EventChannelCapacity      int
	MaxPrecommitEvents        int
	MaxPrecommitBytes         int
	CancelDrainTimeoutSeconds int
	MaxAttempts               int
	MaxDuplicateCostMicros    uint64
}

// DefaultHedgeConfig returns conservative, disabled rollout defaults.
func DefaultHedgeConfig() HedgeConfig {
	return HedgeConfig{
		Enabled:                   false,
		StandardThresholdSeconds:  config.DefaultOpenAIHedgeStandardThresholdSeconds,
		HighThresholdSeconds:      config.DefaultOpenAIHedgeHighThresholdSeconds,
		VeryHeavyThresholdSeconds: config.DefaultOpenAIHedgeVeryHeavyThresholdSeconds,
		VeryHeavyEnabled:          false,
		NoProgressEnabled:         false,
		EventChannelCapacity:      config.DefaultOpenAIHedgeEventChannelCapacity,
		MaxPrecommitEvents:        config.DefaultOpenAIHedgeMaxPrecommitEvents,
		MaxPrecommitBytes:         config.DefaultOpenAIHedgeMaxPrecommitBytes,
		CancelDrainTimeoutSeconds: config.DefaultOpenAIHedgeCancelDrainTimeoutSeconds,
		MaxAttempts:               2,
		MaxDuplicateCostMicros:    config.DefaultOpenAIHedgeMaxDuplicateCostMicros,
	}
}

func (source HedgeConfig) openAICompatibilityConfig() config.OpenAIHedgeConfig {
	return config.OpenAIHedgeConfig{
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

type HedgeCoordinatorDependencies struct {
	Candidates HedgeCandidatePort
	Admission  HedgeAdmissionPort
	Attempts   HedgeAttemptPort
	Slots      HedgeSlotPort
	Sink       HedgeSinkPort
	Billing    HedgeBillingPort
	Telemetry  HedgeTelemetryPort
}

// HedgeCoordinator is a provider-neutral facade over the single semantic race
// kernel. It never parses native protocol frames and never writes from attempt
// goroutines directly to the client sink.
type HedgeCoordinator struct {
	core *OpenAIHedgeCoordinator
}

func NewHedgeCoordinator(source HedgeConfig, dependencies HedgeCoordinatorDependencies) (*HedgeCoordinator, error) {
	if source.Enabled {
		switch {
		case isNilHedgePort(dependencies.Candidates):
			return nil, errors.New("hedge candidate port is required")
		case isNilHedgePort(dependencies.Admission):
			return nil, errors.New("hedge admission port is required")
		case isNilHedgePort(dependencies.Attempts):
			return nil, errors.New("hedge attempt port is required")
		case isNilHedgePort(dependencies.Slots):
			return nil, errors.New("hedge slot port is required")
		case isNilHedgePort(dependencies.Sink):
			return nil, errors.New("hedge sink is required")
		case isNilHedgePort(dependencies.Billing):
			return nil, errors.New("hedge billing port is required")
		}
	}
	core, err := NewOpenAIHedgeCoordinator(source.openAICompatibilityConfig(), OpenAIHedgeCoordinatorDependencies{
		Candidates: hedgeCandidateCompatibilityAdapter{port: dependencies.Candidates},
		Admission:  hedgeAdmissionCompatibilityAdapter{port: dependencies.Admission},
		Attempts:   hedgeAttemptCompatibilityAdapter{port: dependencies.Attempts},
		Slots:      hedgeSlotCompatibilityAdapter{port: dependencies.Slots},
		Sink:       hedgeSinkCompatibilityAdapter{port: dependencies.Sink},
		Billing:    hedgeBillingCompatibilityAdapter{port: dependencies.Billing},
		Telemetry:  hedgeTelemetryCompatibilityAdapter{port: dependencies.Telemetry},
	})
	if err != nil {
		return nil, err
	}
	return &HedgeCoordinator{core: core}, nil
}

func (coordinator *HedgeCoordinator) Coordinate(ctx context.Context, request HedgeRequest) (HedgeResult, error) {
	if coordinator == nil || coordinator.core == nil {
		return HedgeResult{}, ErrHedgeDisabled
	}
	return coordinator.core.Coordinate(ctx, request)
}

type hedgeCandidateCompatibilityAdapter struct{ port HedgeCandidatePort }

func (adapter hedgeCandidateCompatibilityAdapter) SelectOpenAIHedgeCandidate(ctx context.Context, selection OpenAIHedgeSelection) (OpenAIHedgeAccount, error) {
	return adapter.port.SelectHedgeCandidate(ctx, selection)
}

type hedgeAdmissionCompatibilityAdapter struct{ port HedgeAdmissionPort }

func (adapter hedgeAdmissionCompatibilityAdapter) CheckOpenAIHedgeAdmission(ctx context.Context, admission OpenAIHedgeAdmission) (OpenAIHedgeAdmissionDecision, error) {
	return adapter.port.CheckHedgeAdmission(ctx, admission)
}

type hedgeAttemptCompatibilityAdapter struct{ port HedgeAttemptPort }

func (adapter hedgeAttemptCompatibilityAdapter) RunOpenAIHedgeAttempt(ctx context.Context, attempt OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
	return adapter.port.RunHedgeAttempt(ctx, attempt, hedgeEmitterCompatibilityAdapter{emitter: emitter})
}

type hedgeEmitterCompatibilityAdapter struct{ emitter OpenAIHedgeEventEmitter }

func (adapter hedgeEmitterCompatibilityAdapter) EmitHedgeEvent(event HedgeEvent) error {
	return adapter.emitter.EmitOpenAIHedgeEvent(event)
}

type hedgeSlotCompatibilityAdapter struct{ port HedgeSlotPort }

func (adapter hedgeSlotCompatibilityAdapter) AcquireOpenAIHedgeSlot(ctx context.Context, role OpenAIAttemptRole, account OpenAIHedgeAccount) (OpenAIHedgeSlot, error) {
	return adapter.port.AcquireHedgeSlot(ctx, role, account)
}

type hedgeSinkCompatibilityAdapter struct{ port HedgeSinkPort }

func (adapter hedgeSinkCompatibilityAdapter) WriteOpenAIHedgeEvent(ctx context.Context, output OpenAIHedgeOutput) error {
	return adapter.port.WriteHedgeEvent(ctx, output)
}

type hedgeBillingCompatibilityAdapter struct{ port HedgeBillingPort }

func (adapter hedgeBillingCompatibilityAdapter) FinalizeOpenAIHedgeLoserBilling(ctx context.Context, billing OpenAIHedgeLoserBilling) error {
	return adapter.port.FinalizeHedgeLoserBilling(ctx, billing)
}

type hedgeTelemetryCompatibilityAdapter struct{ port HedgeTelemetryPort }

func (adapter hedgeTelemetryCompatibilityAdapter) RecordOpenAIHedgeTelemetry(event OpenAIHedgeTelemetryEvent) {
	if adapter.port != nil {
		adapter.port.RecordHedgeTelemetry(event)
	}
}
