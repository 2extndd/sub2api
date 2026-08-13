package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type syntheticNativeFrame struct {
	kind string
	data string
}

type syntheticNativeRequest struct {
	provider       string
	payload        []byte
	store          bool
	continuationID string
	cachedState    bool
}

type syntheticProtocolAdapter interface {
	request(syntheticNativeRequest) HedgeRequest
	translate(syntheticNativeFrame) (HedgeEvent, error)
}

type anthropicSyntheticAdapter struct{}

func (anthropicSyntheticAdapter) request(native syntheticNativeRequest) HedgeRequest {
	return HedgeRequest{
		Provider:           native.provider,
		Model:              "claude-synthetic",
		ModelFamily:        "claude",
		Protocol:           "messages-v1",
		Stream:             true,
		Stateless:          !native.cachedState,
		Store:              native.store,
		PreviousResponseID: native.continuationID,
		FullInput:          native.continuationID == "" && !native.cachedState,
		Primary: HedgeAccount{
			AccountID:           11,
			Provider:            native.provider,
			PotentialCostMicros: 1_100_001,
		},
		Payload: append([]byte(nil), native.payload...),
	}
}

func (anthropicSyntheticAdapter) translate(frame syntheticNativeFrame) (HedgeEvent, error) {
	kinds := map[string]HedgeEventKind{
		"message_start":  HedgeEventPreamble,
		"thinking_delta": HedgeEventReasoning,
		"text_delta":     HedgeEventText,
		"tool_use_delta": HedgeEventTool,
		"message_stop":   HedgeEventTerminalSuccess,
		"message_error":  HedgeEventTerminalFailure,
	}
	kind, ok := kinds[frame.kind]
	if !ok {
		return HedgeEvent{}, errors.New("unsupported synthetic Anthropic frame")
	}
	return HedgeEvent{Kind: kind, Data: []byte(frame.data)}, nil
}

type geminiSyntheticAdapter struct{}

func (geminiSyntheticAdapter) request(native syntheticNativeRequest) HedgeRequest {
	return HedgeRequest{
		Provider:           native.provider,
		Model:              "gemini-synthetic",
		ModelFamily:        "gemini",
		Protocol:           "generate-content-v1beta",
		Stream:             true,
		Stateless:          !native.cachedState,
		Store:              native.store,
		PreviousResponseID: native.continuationID,
		FullInput:          native.continuationID == "" && !native.cachedState,
		Primary: HedgeAccount{
			AccountID:           21,
			Provider:            native.provider,
			PotentialCostMicros: 2_200_003,
		},
		Payload: append([]byte(nil), native.payload...),
	}
}

func (geminiSyntheticAdapter) translate(frame syntheticNativeFrame) (HedgeEvent, error) {
	kinds := map[string]HedgeEventKind{
		"candidate_metadata": HedgeEventPreamble,
		"thought":            HedgeEventReasoning,
		"candidate_text":     HedgeEventText,
		"function_call":      HedgeEventTool,
		"finish_reason":      HedgeEventTerminalSuccess,
		"blocked":            HedgeEventTerminalFailure,
	}
	kind, ok := kinds[frame.kind]
	if !ok {
		return HedgeEvent{}, errors.New("unsupported synthetic Gemini frame")
	}
	return HedgeEvent{Kind: kind, Data: []byte(frame.data)}, nil
}

type syntheticHedgeCandidatePort struct {
	mu        sync.Mutex
	candidate HedgeAccount
	selection HedgeSelection
	calls     atomic.Int32
}

func (port *syntheticHedgeCandidatePort) SelectHedgeCandidate(_ context.Context, selection HedgeSelection) (HedgeAccount, error) {
	port.calls.Add(1)
	port.mu.Lock()
	port.selection = selection
	port.mu.Unlock()
	return port.candidate, nil
}

func (port *syntheticHedgeCandidatePort) selectionSnapshot() HedgeSelection {
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.selection
}

type syntheticHedgeAdmissionPort struct {
	mu        sync.Mutex
	admission HedgeAdmission
}

func (port *syntheticHedgeAdmissionPort) CheckHedgeAdmission(_ context.Context, admission HedgeAdmission) (HedgeAdmissionDecision, error) {
	port.mu.Lock()
	port.admission = admission
	port.mu.Unlock()
	return HedgeAdmissionDecision{ProviderEligible: true, HeadroomAvailable: true, CostAllowed: true}, nil
}

func (port *syntheticHedgeAdmissionPort) snapshot() HedgeAdmission {
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.admission
}

type syntheticHedgeAttemptPort struct {
	mu            sync.Mutex
	adapter       syntheticProtocolAdapter
	primaryFrames []syntheticNativeFrame
	hedgeFrames   []syntheticNativeFrame
	calls         []HedgeAttempt
	active        atomic.Int32
	maximum       atomic.Int32
}

func (port *syntheticHedgeAttemptPort) RunHedgeAttempt(ctx context.Context, attempt HedgeAttempt, emitter HedgeEventEmitter) error {
	attempt.Payload = append([]byte(nil), attempt.Payload...)
	port.mu.Lock()
	port.calls = append(port.calls, attempt)
	port.mu.Unlock()
	active := port.active.Add(1)
	defer port.active.Add(-1)
	for {
		maximum := port.maximum.Load()
		if active <= maximum || port.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	frames := port.primaryFrames
	if attempt.Role == HedgeAttemptRoleHedge {
		frames = port.hedgeFrames
	}
	if frames == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	for _, frame := range frames {
		event, err := port.adapter.translate(frame)
		if err != nil {
			return err
		}
		if err := emitter.EmitHedgeEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (port *syntheticHedgeAttemptPort) snapshot() []HedgeAttempt {
	port.mu.Lock()
	defer port.mu.Unlock()
	return append([]HedgeAttempt(nil), port.calls...)
}

type syntheticHedgeLease struct {
	released atomic.Int32
}

func (lease *syntheticHedgeLease) Release() { lease.released.Add(1) }

type syntheticHedgeSlotPort struct {
	mu     sync.Mutex
	leases map[HedgeAttemptRole][]*syntheticHedgeLease
}

func (port *syntheticHedgeSlotPort) AcquireHedgeSlot(_ context.Context, role HedgeAttemptRole, _ HedgeAccount) (HedgeSlot, error) {
	lease := &syntheticHedgeLease{}
	port.mu.Lock()
	if port.leases == nil {
		port.leases = make(map[HedgeAttemptRole][]*syntheticHedgeLease)
	}
	port.leases[role] = append(port.leases[role], lease)
	port.mu.Unlock()
	return lease, nil
}

func (port *syntheticHedgeSlotPort) allReleased() bool {
	port.mu.Lock()
	defer port.mu.Unlock()
	for _, leases := range port.leases {
		for _, lease := range leases {
			if lease.released.Load() != 1 {
				return false
			}
		}
	}
	return true
}

type syntheticHedgeSink struct {
	mu      sync.Mutex
	outputs []HedgeOutput
}

func (sink *syntheticHedgeSink) WriteHedgeEvent(_ context.Context, output HedgeOutput) error {
	output.Event.Data = append([]byte(nil), output.Event.Data...)
	sink.mu.Lock()
	sink.outputs = append(sink.outputs, output)
	sink.mu.Unlock()
	return nil
}

func (sink *syntheticHedgeSink) snapshot() []HedgeOutput {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]HedgeOutput(nil), sink.outputs...)
}

type syntheticHedgeBillingPort struct {
	mu      sync.Mutex
	records []HedgeLoserBilling
}

func (port *syntheticHedgeBillingPort) FinalizeHedgeLoserBilling(_ context.Context, billing HedgeLoserBilling) error {
	port.mu.Lock()
	port.records = append(port.records, billing)
	port.mu.Unlock()
	return nil
}

func (port *syntheticHedgeBillingPort) snapshot() []HedgeLoserBilling {
	port.mu.Lock()
	defer port.mu.Unlock()
	return append([]HedgeLoserBilling(nil), port.records...)
}

func TestHedgeCoordinatorRejectsTypedNilRequiredPort(t *testing.T) {
	coordinatorConfig := DefaultHedgeConfig()
	coordinatorConfig.Enabled = true
	var candidates *syntheticHedgeCandidatePort
	_, err := NewHedgeCoordinator(coordinatorConfig, HedgeCoordinatorDependencies{Candidates: candidates})
	require.ErrorContains(t, err, "candidate port is required")
}

func TestHedgeCoordinatorReusesSemanticKernelAcrossProtocolFamilies(t *testing.T) {
	tests := []struct {
		name           string
		provider       string
		adapter        syntheticProtocolAdapter
		hedgeFrames    []syntheticNativeFrame
		meaningfulKind HedgeEventKind
		primaryID      int64
		primaryCost    uint64
	}{
		{
			name:           "anthropic thinking stream",
			provider:       "anthropic",
			adapter:        anthropicSyntheticAdapter{},
			hedgeFrames:    []syntheticNativeFrame{{kind: "message_start"}, {kind: "thinking_delta", data: "thinking"}, {kind: "message_stop"}},
			meaningfulKind: HedgeEventReasoning,
			primaryID:      11,
			primaryCost:    1_100_001,
		},
		{
			name:           "gemini family through modelshare provider",
			provider:       "modelshare",
			adapter:        geminiSyntheticAdapter{},
			hedgeFrames:    []syntheticNativeFrame{{kind: "candidate_metadata"}, {kind: "function_call", data: "tool"}, {kind: "finish_reason"}},
			meaningfulKind: HedgeEventTool,
			primaryID:      21,
			primaryCost:    2_200_003,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			request := testCase.adapter.request(syntheticNativeRequest{provider: testCase.provider, payload: []byte("opaque-native-request")})
			candidate := &syntheticHedgeCandidatePort{candidate: HedgeAccount{
				AccountID:           testCase.primaryID + 1,
				Provider:            testCase.provider,
				PotentialCostMicros: testCase.primaryCost + 10,
			}}
			admission := &syntheticHedgeAdmissionPort{}
			attempts := &syntheticHedgeAttemptPort{adapter: testCase.adapter, hedgeFrames: testCase.hedgeFrames}
			slots := &syntheticHedgeSlotPort{}
			sink := &syntheticHedgeSink{}
			billing := &syntheticHedgeBillingPort{}
			coordinatorConfig := DefaultHedgeConfig()
			coordinatorConfig.Enabled = true
			coordinator, err := NewHedgeCoordinator(coordinatorConfig, HedgeCoordinatorDependencies{
				Candidates: candidate,
				Admission:  admission,
				Attempts:   attempts,
				Slots:      slots,
				Sink:       sink,
				Billing:    billing,
			})
			require.NoError(t, err)
			timers := &openAIHedgeManualTimerFactory{created: make(chan *openAIHedgeManualTimer, 8)}
			coordinator.core.newTimer = timers.newTimer

			resultChannel := make(chan openAIHedgeCoordinateResult, 1)
			go func() {
				result, coordinateErr := coordinator.Coordinate(context.Background(), request)
				resultChannel <- openAIHedgeCoordinateResult{result: result, err: coordinateErr}
			}()
			timer := timers.next(t)
			require.Equal(t, 25*time.Second, timer.duration)
			timer.fire()
			coordinateResult := awaitOpenAIHedgeResult(t, resultChannel)
			require.NoError(t, coordinateResult.err)
			require.Equal(t, HedgeAttemptRoleHedge, coordinateResult.result.Winner)
			require.LessOrEqual(t, attempts.maximum.Load(), int32(2))
			require.GreaterOrEqual(t, attempts.maximum.Load(), int32(1))
			require.Equal(t, int32(1), candidate.calls.Load())
			selection := candidate.selectionSnapshot()
			require.Equal(t, request.Provider, selection.Provider)
			require.Equal(t, request.Model, selection.Model)
			require.Equal(t, request.ModelFamily, selection.ModelFamily)
			require.Equal(t, request.Protocol, selection.Protocol)
			admissionCall := admission.snapshot()
			require.Equal(t, request.Model, admissionCall.Model)
			require.Equal(t, request.ModelFamily, admissionCall.ModelFamily)
			require.Equal(t, request.Protocol, admissionCall.Protocol)
			attemptCalls := attempts.snapshot()
			require.Len(t, attemptCalls, 2)
			for _, attempt := range attemptCalls {
				require.Equal(t, request.Provider, attempt.Provider)
				require.Equal(t, request.Model, attempt.Model)
				require.Equal(t, request.ModelFamily, attempt.ModelFamily)
				require.Equal(t, request.Protocol, attempt.Protocol)
				require.Equal(t, request.Payload, attempt.Payload)
			}

			outputs := sink.snapshot()
			require.NotEmpty(t, outputs)
			terminalCount := 0
			meaningfulSeen := false
			for _, output := range outputs {
				require.Equal(t, HedgeAttemptRoleHedge, output.Role)
				require.Equal(t, testCase.primaryID+1, output.AccountID)
				if output.Event.Kind == testCase.meaningfulKind {
					meaningfulSeen = true
				}
				if output.Event.Kind == HedgeEventTerminalSuccess || output.Event.Kind == HedgeEventTerminalFailure {
					terminalCount++
				}
			}
			require.True(t, meaningfulSeen)
			require.Equal(t, 1, terminalCount)
			require.Equal(t, []HedgeLoserBilling{{
				Role:         HedgeAttemptRolePrimary,
				AccountID:    testCase.primaryID,
				Mode:         HedgeLoserBillingFullPotential,
				AmountMicros: testCase.primaryCost,
			}}, billing.snapshot())
			require.Eventually(t, slots.allReleased, time.Second, time.Millisecond)
		})
	}
}

func TestProtocolAdapterOwnsNativePortabilityDecision(t *testing.T) {
	adapter := geminiSyntheticAdapter{}
	request := adapter.request(syntheticNativeRequest{
		provider:    "gemini",
		payload:     []byte("cached-content-reference"),
		cachedState: true,
	})
	require.False(t, request.Stateless)
	require.False(t, request.FullInput)

	candidate := &syntheticHedgeCandidatePort{candidate: HedgeAccount{AccountID: 22, Provider: "gemini"}}
	attempts := &syntheticHedgeAttemptPort{
		adapter:       adapter,
		primaryFrames: []syntheticNativeFrame{{kind: "candidate_text", data: "primary"}, {kind: "finish_reason"}},
	}
	slots := &syntheticHedgeSlotPort{}
	sink := &syntheticHedgeSink{}
	billing := &syntheticHedgeBillingPort{}
	coordinatorConfig := DefaultHedgeConfig()
	coordinatorConfig.Enabled = true
	coordinator, err := NewHedgeCoordinator(coordinatorConfig, HedgeCoordinatorDependencies{
		Candidates: candidate,
		Admission:  &syntheticHedgeAdmissionPort{},
		Attempts:   attempts,
		Slots:      slots,
		Sink:       sink,
		Billing:    billing,
	})
	require.NoError(t, err)
	result, err := coordinator.Coordinate(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, HedgeAttemptRolePrimary, result.Winner)
	require.False(t, result.HedgeLaunched)
	require.Zero(t, candidate.calls.Load())
	require.Empty(t, billing.snapshot())
	require.Eventually(t, slots.allReleased, time.Second, time.Millisecond)
}
