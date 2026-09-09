package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type openAIHedgeAttemptScript func(context.Context, OpenAIHedgeAttempt, OpenAIHedgeEventEmitter) error

type openAIHedgeTestAttemptPort struct {
	mu      sync.Mutex
	scripts map[OpenAIAttemptRole]openAIHedgeAttemptScript
	calls   []OpenAIHedgeAttempt
	started chan OpenAIAttemptRole
	active  atomic.Int32
	maximum atomic.Int32
}

func newOpenAIHedgeTestAttemptPort(scripts map[OpenAIAttemptRole]openAIHedgeAttemptScript) *openAIHedgeTestAttemptPort {
	return &openAIHedgeTestAttemptPort{
		scripts: scripts,
		started: make(chan OpenAIAttemptRole, 256),
	}
}

func (port *openAIHedgeTestAttemptPort) RunOpenAIHedgeAttempt(
	ctx context.Context,
	attempt OpenAIHedgeAttempt,
	emitter OpenAIHedgeEventEmitter,
) error {
	current := port.active.Add(1)
	defer port.active.Add(-1)
	for {
		maximum := port.maximum.Load()
		if current <= maximum || port.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	port.mu.Lock()
	port.calls = append(port.calls, attempt)
	script := port.scripts[attempt.Role]
	port.mu.Unlock()
	port.started <- attempt.Role
	if script == nil {
		return fmt.Errorf("missing test script for attempt role %d", attempt.Role)
	}
	return script(ctx, attempt, emitter)
}

func (port *openAIHedgeTestAttemptPort) callCount(role OpenAIAttemptRole) int {
	port.mu.Lock()
	defer port.mu.Unlock()
	count := 0
	for _, call := range port.calls {
		if call.Role == role {
			count++
		}
	}
	return count
}

type openAIHedgeTestCandidatePort struct {
	mu        sync.Mutex
	candidate OpenAIHedgeAccount
	err       error
	calls     int
	selectFn  func(context.Context, OpenAIHedgeSelection) (OpenAIHedgeAccount, error)
}

func (port *openAIHedgeTestCandidatePort) SelectOpenAIHedgeCandidate(
	ctx context.Context,
	selection OpenAIHedgeSelection,
) (OpenAIHedgeAccount, error) {
	port.mu.Lock()
	port.calls++
	selectFn := port.selectFn
	candidate := cloneOpenAIHedgeAccount(port.candidate)
	err := port.err
	port.mu.Unlock()
	if selectFn != nil {
		return selectFn(ctx, selection)
	}
	return candidate, err
}

func (port *openAIHedgeTestCandidatePort) callCount() int {
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.calls
}

type openAIHedgeTestAdmissionPort struct {
	mu       sync.Mutex
	decision OpenAIHedgeAdmissionDecision
	err      error
	calls    int
	checkFn  func(context.Context, OpenAIHedgeAdmission) (OpenAIHedgeAdmissionDecision, error)
}

func (port *openAIHedgeTestAdmissionPort) CheckOpenAIHedgeAdmission(
	ctx context.Context,
	admission OpenAIHedgeAdmission,
) (OpenAIHedgeAdmissionDecision, error) {
	port.mu.Lock()
	port.calls++
	checkFn := port.checkFn
	decision := port.decision
	err := port.err
	port.mu.Unlock()
	if checkFn != nil {
		return checkFn(ctx, admission)
	}
	return decision, err
}

func (port *openAIHedgeTestAdmissionPort) callCount() int {
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.calls
}

type openAIHedgeTestLease struct {
	onRelease func()
	once      sync.Once
}

func (lease *openAIHedgeTestLease) Release() {
	lease.once.Do(lease.onRelease)
}

type openAIHedgeTestSlotPort struct {
	mu           sync.Mutex
	acquireErr   map[OpenAIAttemptRole]error
	releasePanic map[OpenAIAttemptRole]bool
	acquired     map[OpenAIAttemptRole]int
	released     map[OpenAIAttemptRole]int
}

func newOpenAIHedgeTestSlotPort() *openAIHedgeTestSlotPort {
	return &openAIHedgeTestSlotPort{
		acquireErr:   make(map[OpenAIAttemptRole]error),
		releasePanic: make(map[OpenAIAttemptRole]bool),
		acquired:     make(map[OpenAIAttemptRole]int),
		released:     make(map[OpenAIAttemptRole]int),
	}
}

func (port *openAIHedgeTestSlotPort) AcquireOpenAIHedgeSlot(
	_ context.Context,
	role OpenAIAttemptRole,
	_ OpenAIHedgeAccount,
) (OpenAIHedgeSlot, error) {
	port.mu.Lock()
	defer port.mu.Unlock()
	if err := port.acquireErr[role]; err != nil {
		return nil, err
	}
	port.acquired[role]++
	panicOnRelease := port.releasePanic[role]
	return &openAIHedgeTestLease{onRelease: func() {
		if panicOnRelease {
			panic("synthetic slot release panic")
		}
		port.mu.Lock()
		port.released[role]++
		port.mu.Unlock()
	}}, nil
}

func (port *openAIHedgeTestSlotPort) counts(role OpenAIAttemptRole) (int, int) {
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.acquired[role], port.released[role]
}

type openAIHedgeTestSink struct {
	mu      sync.Mutex
	events  []OpenAIHedgeOutput
	failure func(OpenAIHedgeOutput) error
}

func (sink *openAIHedgeTestSink) WriteOpenAIHedgeEvent(_ context.Context, output OpenAIHedgeOutput) error {
	output.Event = cloneOpenAIHedgeEvent(output.Event)
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.events = append(sink.events, output)
	if sink.failure != nil {
		return sink.failure(output)
	}
	return nil
}

func (sink *openAIHedgeTestSink) snapshot() []OpenAIHedgeOutput {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	result := make([]OpenAIHedgeOutput, len(sink.events))
	copy(result, sink.events)
	return result
}

type openAIHedgeTestBillingPort struct {
	mu      sync.Mutex
	records []OpenAIHedgeLoserBilling
	err     error
}

func (port *openAIHedgeTestBillingPort) FinalizeOpenAIHedgeLoserBilling(
	_ context.Context,
	billing OpenAIHedgeLoserBilling,
) error {
	port.mu.Lock()
	defer port.mu.Unlock()
	port.records = append(port.records, billing)
	return port.err
}

func (port *openAIHedgeTestBillingPort) snapshot() []OpenAIHedgeLoserBilling {
	port.mu.Lock()
	defer port.mu.Unlock()
	return append([]OpenAIHedgeLoserBilling(nil), port.records...)
}

type openAIHedgeTestTelemetryPort struct {
	mu     sync.Mutex
	events []OpenAIHedgeTelemetryEvent
	notify chan OpenAIHedgeTelemetryEvent
}

func newOpenAIHedgeTestTelemetryPort() *openAIHedgeTestTelemetryPort {
	return &openAIHedgeTestTelemetryPort{notify: make(chan OpenAIHedgeTelemetryEvent, 1024)}
}

func (port *openAIHedgeTestTelemetryPort) RecordOpenAIHedgeTelemetry(event OpenAIHedgeTelemetryEvent) {
	port.mu.Lock()
	port.events = append(port.events, event)
	port.mu.Unlock()
	port.notify <- event
}

type panickingOpenAIHedgeTelemetryPort struct{}

func (panickingOpenAIHedgeTelemetryPort) RecordOpenAIHedgeTelemetry(OpenAIHedgeTelemetryEvent) {
	panic("synthetic telemetry panic with secret payload")
}

type panickingOpenAIHedgeBillingPort struct{}

func (panickingOpenAIHedgeBillingPort) FinalizeOpenAIHedgeLoserBilling(context.Context, OpenAIHedgeLoserBilling) error {
	panic("synthetic exposure panic with secret payload")
}

func (port *openAIHedgeTestTelemetryPort) contains(predicate func(OpenAIHedgeTelemetryEvent) bool) bool {
	port.mu.Lock()
	defer port.mu.Unlock()
	for _, event := range port.events {
		if predicate(event) {
			return true
		}
	}
	return false
}

func (port *openAIHedgeTestTelemetryPort) waitFor(
	t *testing.T,
	predicate func(OpenAIHedgeTelemetryEvent) bool,
) OpenAIHedgeTelemetryEvent {
	t.Helper()
	for {
		select {
		case event := <-port.notify:
			if predicate(event) {
				return event
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for openai hedge telemetry")
		}
	}
}

type openAIHedgeManualTimer struct {
	mu       sync.Mutex
	ch       chan time.Time
	duration time.Duration
	resets   chan time.Duration
	stopped  bool
}

func (timer *openAIHedgeManualTimer) channel() <-chan time.Time {
	return timer.ch
}

func (timer *openAIHedgeManualTimer) reset(duration time.Duration) {
	timer.mu.Lock()
	timer.duration = duration
	timer.stopped = false
	timer.mu.Unlock()
	timer.resets <- duration
}

func (timer *openAIHedgeManualTimer) stop() {
	timer.mu.Lock()
	timer.stopped = true
	timer.mu.Unlock()
}

func (timer *openAIHedgeManualTimer) fire() {
	timer.ch <- time.Unix(1, 0)
}

type openAIHedgeManualTimerFactory struct {
	created chan *openAIHedgeManualTimer
}

func newOpenAIHedgeManualTimerFactory() *openAIHedgeManualTimerFactory {
	return &openAIHedgeManualTimerFactory{created: make(chan *openAIHedgeManualTimer, 128)}
}

func (factory *openAIHedgeManualTimerFactory) newTimer(duration time.Duration) openAIHedgeTimer {
	timer := &openAIHedgeManualTimer{
		ch:       make(chan time.Time, 1),
		duration: duration,
		resets:   make(chan time.Duration, 16),
	}
	factory.created <- timer
	return timer
}

func (factory *openAIHedgeManualTimerFactory) next(t *testing.T) *openAIHedgeManualTimer {
	t.Helper()
	select {
	case timer := <-factory.created:
		return timer
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for openai hedge timer")
		return nil
	}
}

type openAIHedgeTestHarness struct {
	coordinator *OpenAIHedgeCoordinator
	attempts    *openAIHedgeTestAttemptPort
	candidates  *openAIHedgeTestCandidatePort
	admission   *openAIHedgeTestAdmissionPort
	slots       *openAIHedgeTestSlotPort
	sink        *openAIHedgeTestSink
	billing     *openAIHedgeTestBillingPort
	telemetry   *openAIHedgeTestTelemetryPort
	timers      *openAIHedgeManualTimerFactory
}

func enabledOpenAIHedgeTestConfig() config.OpenAIHedgeConfig {
	return config.OpenAIHedgeConfig{
		Enabled:                   true,
		StandardThresholdSeconds:  config.DefaultOpenAIHedgeStandardThresholdSeconds,
		HighThresholdSeconds:      config.DefaultOpenAIHedgeHighThresholdSeconds,
		VeryHeavyThresholdSeconds: config.DefaultOpenAIHedgeVeryHeavyThresholdSeconds,
		VeryHeavyEnabled:          true,
		NoProgressEnabled:         false,
		EventChannelCapacity:      config.DefaultOpenAIHedgeEventChannelCapacity,
		MaxPrecommitEvents:        config.DefaultOpenAIHedgeMaxPrecommitEvents,
		MaxPrecommitBytes:         config.DefaultOpenAIHedgeMaxPrecommitBytes,
		CancelDrainTimeoutSeconds: config.DefaultOpenAIHedgeCancelDrainTimeoutSeconds,
		MaxAttempts:               2,
	}
}

func newOpenAIHedgeTestHarness(
	t *testing.T,
	scripts map[OpenAIAttemptRole]openAIHedgeAttemptScript,
	mutate func(*openAIHedgeTestHarness, *config.OpenAIHedgeConfig),
) *openAIHedgeTestHarness {
	t.Helper()
	harness := &openAIHedgeTestHarness{
		attempts: newOpenAIHedgeTestAttemptPort(scripts),
		candidates: &openAIHedgeTestCandidatePort{candidate: OpenAIHedgeAccount{
			AccountID:           2,
			Provider:            "openai",
			PotentialCostMicros: 7000000,
		}},
		admission: &openAIHedgeTestAdmissionPort{decision: OpenAIHedgeAdmissionDecision{
			ProviderEligible:  true,
			HeadroomAvailable: true,
			CostAllowed:       true,
		}},
		slots:     newOpenAIHedgeTestSlotPort(),
		sink:      &openAIHedgeTestSink{},
		billing:   &openAIHedgeTestBillingPort{},
		telemetry: newOpenAIHedgeTestTelemetryPort(),
		timers:    newOpenAIHedgeManualTimerFactory(),
	}
	coordinatorConfig := enabledOpenAIHedgeTestConfig()
	if mutate != nil {
		mutate(harness, &coordinatorConfig)
	}
	coordinator, err := NewOpenAIHedgeCoordinator(coordinatorConfig, OpenAIHedgeCoordinatorDependencies{
		Candidates: harness.candidates,
		Admission:  harness.admission,
		Attempts:   harness.attempts,
		Slots:      harness.slots,
		Sink:       harness.sink,
		Billing:    harness.billing,
		Telemetry:  harness.telemetry,
	})
	require.NoError(t, err)
	fixedNow := time.Unix(100, 0)
	coordinator.now = func() time.Time { return fixedNow }
	coordinator.newTimer = harness.timers.newTimer
	harness.coordinator = coordinator
	return harness
}

func eligibleOpenAIHedgeRequest() OpenAIHedgeRequest {
	return OpenAIHedgeRequest{
		Provider:               "openai",
		Model:                  "gpt-test",
		ModelFamily:            "gpt",
		Protocol:               "responses",
		Stream:                 true,
		Stateless:              true,
		FullInput:              true,
		ClientVisibleCommitted: false,
		Primary: OpenAIHedgeAccount{
			AccountID:           1,
			Provider:            "openai",
			PotentialCostMicros: 4000000,
		},
		Payload: []byte(`{"model":"gpt-test","stream":true,"store":false}`),
	}
}

type openAIHedgeCoordinateResult struct {
	result OpenAIHedgeResult
	err    error
}

func coordinateOpenAIHedgeAsync(
	coordinator *OpenAIHedgeCoordinator,
	ctx context.Context,
	request OpenAIHedgeRequest,
) <-chan openAIHedgeCoordinateResult {
	result := make(chan openAIHedgeCoordinateResult, 1)
	go func() {
		coordinateResult, err := coordinator.Coordinate(ctx, request)
		result <- openAIHedgeCoordinateResult{result: coordinateResult, err: err}
	}()
	return result
}

func awaitOpenAIHedgeResult(t *testing.T, result <-chan openAIHedgeCoordinateResult) openAIHedgeCoordinateResult {
	t.Helper()
	select {
	case coordinateResult := <-result:
		return coordinateResult
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for openai hedge result")
		return openAIHedgeCoordinateResult{}
	}
}

func waitOpenAIHedgeAttemptStarted(t *testing.T, port *openAIHedgeTestAttemptPort, role OpenAIAttemptRole) {
	t.Helper()
	for {
		select {
		case startedRole := <-port.started:
			if startedRole == role {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for attempt role %d", role)
		}
	}
}

func emitOpenAIHedgeSuccess(kind OpenAIHedgeEventKind, data string) openAIHedgeAttemptScript {
	return func(ctx context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
		if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: kind, Data: []byte(data)}); err != nil {
			return err
		}
		if kind == OpenAIHedgeEventTerminalSuccess {
			return nil
		}
		if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalSuccess}); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		return nil
	}
}

func emitOpenAIHedgeFailure(message string) openAIHedgeAttemptScript {
	return func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
		return emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{
			Kind: OpenAIHedgeEventTerminalFailure,
			Err:  errors.New(message),
		})
	}
}

func terminalOpenAIHedgeEventCount(events []OpenAIHedgeOutput) int {
	count := 0
	for _, event := range events {
		if event.Event.Kind.terminal() {
			count++
		}
	}
	return count
}

func requireOpenAIHedgeSlotsReleased(t *testing.T, slots *openAIHedgeTestSlotPort, roles ...OpenAIAttemptRole) {
	t.Helper()
	for _, role := range roles {
		acquired, released := slots.counts(role)
		require.Equal(t, acquired, released, "attempt role %d leaked a slot", role)
	}
}

func TestOpenAIHedgeCoordinatorDisabledIsInert(t *testing.T) {
	coordinator, err := NewOpenAIHedgeCoordinator(config.OpenAIHedgeConfig{}, OpenAIHedgeCoordinatorDependencies{})
	require.NoError(t, err)
	_, err = coordinator.Coordinate(context.Background(), eligibleOpenAIHedgeRequest())
	require.ErrorIs(t, err, ErrOpenAIHedgeDisabled)
}

func TestOpenAIHedgeCoordinatorRequiresBoundedRoutingLabels(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*OpenAIHedgeRequest)
	}{
		{name: "provider", mutate: func(request *OpenAIHedgeRequest) { request.Provider = "" }},
		{name: "model", mutate: func(request *OpenAIHedgeRequest) { request.Model = "" }},
		{name: "model family", mutate: func(request *OpenAIHedgeRequest) { request.ModelFamily = "" }},
		{name: "protocol", mutate: func(request *OpenAIHedgeRequest) { request.Protocol = "" }},
		{name: "multiline", mutate: func(request *OpenAIHedgeRequest) { request.ModelFamily = "claude\nsecret" }},
		{name: "oversized", mutate: func(request *OpenAIHedgeRequest) { request.Model = strings.Repeat("m", 129) }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newOpenAIHedgeTestHarness(t, nil, nil)
			request := eligibleOpenAIHedgeRequest()
			testCase.mutate(&request)
			_, err := harness.coordinator.Coordinate(context.Background(), request)
			require.Error(t, err)
			require.Zero(t, harness.attempts.callCount(OpenAIAttemptRolePrimary))
		})
	}
}

func TestOpenAIHedgeCoordinatorStaticEligibility(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*OpenAIHedgeRequest)
		reason OpenAIHedgeEligibilityReason
	}{
		{name: "non-stream", mutate: func(r *OpenAIHedgeRequest) { r.Stream = false }, reason: OpenAIHedgeEligibilityNotStreaming},
		{name: "stateful", mutate: func(r *OpenAIHedgeRequest) { r.Stateless = false }, reason: OpenAIHedgeEligibilityStateful},
		{name: "store true", mutate: func(r *OpenAIHedgeRequest) { r.Store = true }, reason: OpenAIHedgeEligibilityStoreEnabled},
		{name: "previous response", mutate: func(r *OpenAIHedgeRequest) { r.PreviousResponseID = "resp_previous" }, reason: OpenAIHedgeEligibilityPreviousResponse},
		{name: "partial input", mutate: func(r *OpenAIHedgeRequest) { r.FullInput = false }, reason: OpenAIHedgeEligibilityNotFullInput},
		{name: "client-visible committed", mutate: func(r *OpenAIHedgeRequest) { r.ClientVisibleCommitted = true }, reason: OpenAIHedgeEligibilityClientVisibleCommitted},
		{name: "provider mismatch", mutate: func(r *OpenAIHedgeRequest) { r.Provider = "azure" }, reason: OpenAIHedgeEligibilityProvider},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
				OpenAIAttemptRolePrimary: emitOpenAIHedgeFailure("primary failed"),
			}, nil)
			request := eligibleOpenAIHedgeRequest()
			testCase.mutate(&request)
			coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), request))
			require.ErrorContains(t, coordinateResult.err, "primary failed")
			require.False(t, coordinateResult.result.HedgeLaunched)
			require.Zero(t, harness.candidates.callCount())
			require.Zero(t, harness.admission.callCount())
			require.True(t, harness.telemetry.contains(func(event OpenAIHedgeTelemetryEvent) bool {
				return event.Kind == OpenAIHedgeTelemetryEligibility && event.EligibilityReason == testCase.reason
			}))
			require.Len(t, harness.sink.snapshot(), 1)
			requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary)
		})
	}
}

func TestOpenAIHedgeCoordinatorDynamicAdmissionAndOwnerRejection(t *testing.T) {
	parentID := int64(10)
	tests := []struct {
		name          string
		mutateHarness func(*openAIHedgeTestHarness, *config.OpenAIHedgeConfig)
		mutateRequest func(*OpenAIHedgeRequest)
		reason        OpenAIHedgeEligibilityReason
	}{
		{name: "same account", mutateHarness: func(h *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) { h.candidates.candidate.AccountID = 1 }, reason: OpenAIHedgeEligibilitySameOwner},
		{name: "candidate shares primary owner", mutateHarness: func(h *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
			h.candidates.candidate.ParentAccountID = pointerToInt64(1)
		}, reason: OpenAIHedgeEligibilitySameOwner},
		{
			name: "primary shadow shares candidate owner",
			mutateHarness: func(h *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
				h.candidates.candidate.AccountID = parentID
			},
			mutateRequest: func(request *OpenAIHedgeRequest) { request.Primary.ParentAccountID = &parentID },
			reason:        OpenAIHedgeEligibilitySameOwner,
		},
		{
			name: "candidate account is primary credential owner",
			mutateHarness: func(h *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
				h.candidates.candidate.AccountID = parentID
				h.candidates.candidate.ParentAccountID = pointerToInt64(20)
			},
			mutateRequest: func(request *OpenAIHedgeRequest) { request.Primary.ParentAccountID = &parentID },
			reason:        OpenAIHedgeEligibilitySameOwner,
		},
		{
			name: "candidate credential owner is primary account",
			mutateHarness: func(h *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
				h.candidates.candidate.ParentAccountID = pointerToInt64(1)
			},
			mutateRequest: func(request *OpenAIHedgeRequest) { request.Primary.ParentAccountID = &parentID },
			reason:        OpenAIHedgeEligibilitySameOwner,
		},
		{name: "candidate provider mismatch", mutateHarness: func(h *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
			h.candidates.candidate.Provider = "azure"
		}, reason: OpenAIHedgeEligibilityProvider},
		{name: "provider policy", mutateHarness: func(h *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
			h.admission.decision.ProviderEligible = false
		}, reason: OpenAIHedgeEligibilityProvider},
		{name: "headroom policy", mutateHarness: func(h *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
			h.admission.decision.HeadroomAvailable = false
		}, reason: OpenAIHedgeEligibilityHeadroom},
		{name: "cost policy", mutateHarness: func(h *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) { h.admission.decision.CostAllowed = false }, reason: OpenAIHedgeEligibilityCost},
		{name: "selection error", mutateHarness: func(h *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
			h.candidates.err = ErrOpenAIHedgeNoCandidate
		}, reason: OpenAIHedgeEligibilityNoCandidate},
		{name: "slot exhausted", mutateHarness: func(h *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
			h.slots.acquireErr[OpenAIAttemptRoleHedge] = errors.New("full")
		}, reason: OpenAIHedgeEligibilitySlot},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
				OpenAIAttemptRolePrimary: emitOpenAIHedgeFailure("primary failed"),
			}, testCase.mutateHarness)
			request := eligibleOpenAIHedgeRequest()
			if testCase.mutateRequest != nil {
				testCase.mutateRequest(&request)
			}
			coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), request))
			require.ErrorContains(t, coordinateResult.err, "primary failed")
			require.False(t, coordinateResult.result.HedgeLaunched)
			require.Zero(t, harness.attempts.callCount(OpenAIAttemptRoleHedge))
			require.True(t, harness.telemetry.contains(func(event OpenAIHedgeTelemetryEvent) bool {
				return event.Kind == OpenAIHedgeTelemetryHedgeOutcome && event.EligibilityReason == testCase.reason
			}))
			requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary, OpenAIAttemptRoleHedge)
		})
	}
}

func TestOpenAIHedgeCoordinatorThresholds(t *testing.T) {
	t.Run("standard launches at 25 seconds", func(t *testing.T) {
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: func(ctx context.Context, _ OpenAIHedgeAttempt, _ OpenAIHedgeEventEmitter) error {
				<-ctx.Done()
				return nil
			},
			OpenAIAttemptRoleHedge: emitOpenAIHedgeSuccess(OpenAIHedgeEventTerminalSuccess, ""),
		}, nil)
		result := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest())
		timer := harness.timers.next(t)
		require.Equal(t, 25*time.Second, timer.duration)
		timer.fire()
		coordinateResult := awaitOpenAIHedgeResult(t, result)
		require.NoError(t, coordinateResult.err)
		require.Equal(t, OpenAIAttemptRoleHedge, coordinateResult.result.Winner)
		require.Equal(t, 1, harness.candidates.callCount())
	})

	t.Run("preamble postpones standard launch to 40 seconds", func(t *testing.T) {
		preambleSent := make(chan struct{})
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: func(ctx context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
				if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventPreamble, Data: []byte("primary-preamble")}); err != nil {
					return err
				}
				close(preambleSent)
				<-ctx.Done()
				return nil
			},
			OpenAIAttemptRoleHedge: emitOpenAIHedgeSuccess(OpenAIHedgeEventTerminalSuccess, ""),
		}, nil)
		result := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest())
		timer := harness.timers.next(t)
		require.Equal(t, 25*time.Second, timer.duration)
		<-preambleSent
		select {
		case reset := <-timer.resets:
			t.Fatalf("preamble unexpectedly reset acquisition threshold to %s", reset)
		default:
		}
		require.Zero(t, harness.candidates.callCount())
		timer.fire()
		coordinateResult := awaitOpenAIHedgeResult(t, result)
		require.NoError(t, coordinateResult.err)
		require.Equal(t, OpenAIAttemptRoleHedge, coordinateResult.result.Winner)
		for _, output := range harness.sink.snapshot() {
			require.NotEqual(t, "primary-preamble", string(output.Event.Data))
		}
	})

	for _, costClass := range []OpenAIHedgeCostClass{OpenAIHedgeCostHigh, OpenAIHedgeCostVeryHeavy} {
		t.Run(fmt.Sprintf("class_%d_waits_appropriate_threshold", costClass), func(t *testing.T) {
			harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
				OpenAIAttemptRolePrimary: func(ctx context.Context, _ OpenAIHedgeAttempt, _ OpenAIHedgeEventEmitter) error {
					<-ctx.Done()
					return nil
				},
				OpenAIAttemptRoleHedge: emitOpenAIHedgeSuccess(OpenAIHedgeEventTerminalSuccess, ""),
			}, nil)
			request := eligibleOpenAIHedgeRequest()
			request.CostClass = costClass
			result := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), request)
			timer := harness.timers.next(t)
			expected := 40 * time.Second
			if costClass == OpenAIHedgeCostVeryHeavy {
				expected = 60 * time.Second
			}
			require.Equal(t, expected, timer.duration)
			timer.fire()
			coordinateResult := awaitOpenAIHedgeResult(t, result)
			require.NoError(t, coordinateResult.err)
			require.Equal(t, OpenAIAttemptRoleHedge, coordinateResult.result.Winner)
		})
	}
}

func TestOpenAIHedgeCoordinatorCommitEvents(t *testing.T) {
	for _, kind := range []OpenAIHedgeEventKind{
		OpenAIHedgeEventReasoning,
		OpenAIHedgeEventText,
		OpenAIHedgeEventTool,
		OpenAIHedgeEventTerminalSuccess,
	} {
		t.Run(fmt.Sprintf("kind_%d", kind), func(t *testing.T) {
			harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
				OpenAIAttemptRolePrimary: emitOpenAIHedgeSuccess(kind, "commit"),
			}, nil)
			request := eligibleOpenAIHedgeRequest()
			request.Stream = false
			coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), request))
			require.NoError(t, coordinateResult.err)
			require.Equal(t, OpenAIAttemptRolePrimary, coordinateResult.result.Winner)
			events := harness.sink.snapshot()
			require.NotEmpty(t, events)
			require.Equal(t, kind, events[0].Event.Kind)
			require.Equal(t, 1, terminalOpenAIHedgeEventCount(events))
		})
	}
}

func TestOpenAIHedgeCoordinatorPreambleNeverCommitsAndFailureWaitsForOther(t *testing.T) {
	primaryMayFail := make(chan struct{})
	hedgeMayWin := make(chan struct{})
	harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
		OpenAIAttemptRolePrimary: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
			if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventPreamble, Data: []byte("primary-preamble")}); err != nil {
				return err
			}
			<-primaryMayFail
			return emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalFailure, Err: errors.New("primary failed")})
		},
		OpenAIAttemptRoleHedge: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
			if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventPreamble, Data: []byte("hedge-preamble")}); err != nil {
				return err
			}
			<-hedgeMayWin
			if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventReasoning, Data: []byte("hedge-reasoning")}); err != nil {
				return err
			}
			return emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalSuccess})
		},
	}, nil)
	result := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest())
	timer := harness.timers.next(t)
	// Preamble no longer resets the timer; fire immediately after preamble is sent
	waitOpenAIHedgeAttemptStarted(t, harness.attempts, OpenAIAttemptRolePrimary)
	timer.fire()
	waitOpenAIHedgeAttemptStarted(t, harness.attempts, OpenAIAttemptRoleHedge)
	close(primaryMayFail)
	harness.telemetry.waitFor(t, func(event OpenAIHedgeTelemetryEvent) bool {
		return event.Kind == OpenAIHedgeTelemetryAttemptTerminal && event.Role == OpenAIAttemptRolePrimary
	})
	require.Empty(t, harness.sink.snapshot(), "preamble and a single failed attempt must not commit the sink")
	close(hedgeMayWin)
	coordinateResult := awaitOpenAIHedgeResult(t, result)
	require.NoError(t, coordinateResult.err)
	require.Equal(t, OpenAIAttemptRoleHedge, coordinateResult.result.Winner)
	events := harness.sink.snapshot()
	require.Equal(t, []OpenAIHedgeEventKind{
		OpenAIHedgeEventPreamble,
		OpenAIHedgeEventReasoning,
		OpenAIHedgeEventTerminalSuccess,
	}, []OpenAIHedgeEventKind{events[0].Event.Kind, events[1].Event.Kind, events[2].Event.Kind})
	for _, event := range events {
		require.Equal(t, OpenAIAttemptRoleHedge, event.Role)
	}
}

func TestOpenAIHedgeCoordinatorBothFailuresProduceOneTerminal(t *testing.T) {
	primaryFail := make(chan struct{})
	harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
		OpenAIAttemptRolePrimary: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
			<-primaryFail
			return emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalFailure, Err: errors.New("primary failed")})
		},
		OpenAIAttemptRoleHedge: emitOpenAIHedgeFailure("hedge failed"),
	}, nil)
	result := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest())
	harness.timers.next(t).fire()
	waitOpenAIHedgeAttemptStarted(t, harness.attempts, OpenAIAttemptRoleHedge)
	close(primaryFail)
	coordinateResult := awaitOpenAIHedgeResult(t, result)
	require.ErrorIs(t, coordinateResult.err, ErrOpenAIHedgeAllAttemptsFailed)
	require.Equal(t, OpenAIAttemptRoleUnknown, coordinateResult.result.Winner)
	outputs := harness.sink.snapshot()
	require.Equal(t, 1, terminalOpenAIHedgeEventCount(outputs))
	require.Len(t, outputs, 1)
	require.Equal(t, OpenAIAttemptRolePrimary, outputs[0].Role)
	require.Equal(t, int64(1), outputs[0].AccountID)
	requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary, OpenAIAttemptRoleHedge)
}

func TestOpenAIHedgeCoordinatorClientCancellationCancelsBoth(t *testing.T) {
	canceled := make(chan OpenAIAttemptRole, 2)
	waitForCancel := func(ctx context.Context, attempt OpenAIHedgeAttempt, _ OpenAIHedgeEventEmitter) error {
		<-ctx.Done()
		canceled <- attempt.Role
		return nil
	}
	harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
		OpenAIAttemptRolePrimary: waitForCancel,
		OpenAIAttemptRoleHedge:   waitForCancel,
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	result := coordinateOpenAIHedgeAsync(harness.coordinator, ctx, eligibleOpenAIHedgeRequest())
	harness.timers.next(t).fire()
	waitOpenAIHedgeAttemptStarted(t, harness.attempts, OpenAIAttemptRoleHedge)
	cancel()
	coordinateResult := awaitOpenAIHedgeResult(t, result)
	require.ErrorIs(t, coordinateResult.err, context.Canceled)
	roles := map[OpenAIAttemptRole]bool{}
	for range 2 {
		select {
		case role := <-canceled:
			roles[role] = true
		case <-time.After(2 * time.Second):
			t.Fatal("both attempts were not canceled")
		}
	}
	require.True(t, roles[OpenAIAttemptRolePrimary])
	require.True(t, roles[OpenAIAttemptRoleHedge])
	outputs := harness.sink.snapshot()
	require.Equal(t, 1, terminalOpenAIHedgeEventCount(outputs))
	require.Len(t, outputs, 1)
	require.Equal(t, OpenAIAttemptRolePrimary, outputs[0].Role)
	require.Equal(t, int64(1), outputs[0].AccountID)
	requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary, OpenAIAttemptRoleHedge)
}

func TestOpenAIHedgeCoordinatorCancellationAfterCommitUsesWinnerIdentity(t *testing.T) {
	harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
		OpenAIAttemptRolePrimary: func(ctx context.Context, _ OpenAIHedgeAttempt, _ OpenAIHedgeEventEmitter) error {
			<-ctx.Done()
			return ctx.Err()
		},
		OpenAIAttemptRoleHedge: func(ctx context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
			if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventText, Data: []byte("winner")}); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	result := coordinateOpenAIHedgeAsync(harness.coordinator, ctx, eligibleOpenAIHedgeRequest())
	harness.timers.next(t).fire()
	require.Eventually(t, func() bool {
		outputs := harness.sink.snapshot()
		return len(outputs) == 1 && outputs[0].Event.Kind == OpenAIHedgeEventText
	}, time.Second, time.Millisecond)
	cancel()
	coordinateResult := awaitOpenAIHedgeResult(t, result)
	require.ErrorIs(t, coordinateResult.err, context.Canceled)
	require.Equal(t, OpenAIAttemptRoleHedge, coordinateResult.result.Winner)
	outputs := harness.sink.snapshot()
	require.Equal(t, 1, terminalOpenAIHedgeEventCount(outputs))
	for _, output := range outputs {
		require.Equal(t, OpenAIAttemptRoleHedge, output.Role)
		require.Equal(t, int64(2), output.AccountID)
	}
	requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary, OpenAIAttemptRoleHedge)
}

func TestOpenAIHedgeCoordinatorCancellationOwnsClosedChannelRace(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		ctx, cancel := context.WithCancel(context.Background())
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: func(context.Context, OpenAIHedgeAttempt, OpenAIHedgeEventEmitter) error {
				cancel()
				return nil
			},
		}, nil)
		coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, ctx, eligibleOpenAIHedgeRequest()))
		require.ErrorIs(t, coordinateResult.err, context.Canceled)
		require.NotErrorIs(t, coordinateResult.err, ErrOpenAIHedgeAttemptMissingTerminal)
		outputs := harness.sink.snapshot()
		require.Len(t, outputs, 1)
		require.Equal(t, OpenAIAttemptRolePrimary, outputs[0].Role)
		require.Equal(t, int64(1), outputs[0].AccountID)
		requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary)
	}
}

func TestOpenAIHedgeCoordinatorNeverDispatchesHedgeAfterCancellation(t *testing.T) {
	for _, stage := range []string{"selection", "admission"} {
		t.Run(stage, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
				OpenAIAttemptRolePrimary: func(ctx context.Context, _ OpenAIHedgeAttempt, _ OpenAIHedgeEventEmitter) error {
					<-ctx.Done()
					return ctx.Err()
				},
			}, func(harness *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
				if stage == "selection" {
					harness.candidates.selectFn = func(context.Context, OpenAIHedgeSelection) (OpenAIHedgeAccount, error) {
						close(entered)
						<-release
						return cloneOpenAIHedgeAccount(harness.candidates.candidate), nil
					}
					return
				}
				harness.admission.checkFn = func(context.Context, OpenAIHedgeAdmission) (OpenAIHedgeAdmissionDecision, error) {
					close(entered)
					<-release
					return OpenAIHedgeAdmissionDecision{ProviderEligible: true, HeadroomAvailable: true, CostAllowed: true}, nil
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			result := coordinateOpenAIHedgeAsync(harness.coordinator, ctx, eligibleOpenAIHedgeRequest())
			harness.timers.next(t).fire()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatalf("%s policy stage was not entered", stage)
			}
			cancel()
			close(release)
			coordinateResult := awaitOpenAIHedgeResult(t, result)
			require.ErrorIs(t, coordinateResult.err, context.Canceled)
			require.Zero(t, harness.attempts.callCount(OpenAIAttemptRoleHedge))
			outputs := harness.sink.snapshot()
			require.Len(t, outputs, 1)
			require.Equal(t, OpenAIAttemptRolePrimary, outputs[0].Role)
			require.Equal(t, int64(1), outputs[0].AccountID)
			requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary)
		})
	}
}

func TestOpenAIHedgeCoordinatorCancelsLoserAndBillsUnknownAtFullPotential(t *testing.T) {
	hedgeStarted := make(chan struct{})
	hedgeCanceled := make(chan struct{})
	harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
		OpenAIAttemptRolePrimary: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
			<-hedgeStarted
			if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventText, Data: []byte("primary")}); err != nil {
				return err
			}
			return emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalSuccess})
		},
		OpenAIAttemptRoleHedge: func(ctx context.Context, _ OpenAIHedgeAttempt, _ OpenAIHedgeEventEmitter) error {
			close(hedgeStarted)
			<-ctx.Done()
			close(hedgeCanceled)
			return nil
		},
	}, nil)
	result := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest())
	harness.timers.next(t).fire()
	coordinateResult := awaitOpenAIHedgeResult(t, result)
	require.NoError(t, coordinateResult.err)
	require.Equal(t, OpenAIAttemptRolePrimary, coordinateResult.result.Winner)
	require.Equal(t, OpenAIBillingOutcomeNotBillable, coordinateResult.result.LoserBillingOutcome)
	select {
	case <-hedgeCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("loser was not canceled on winner commit")
	}
	billing := harness.billing.snapshot()
	require.Equal(t, []OpenAIHedgeLoserBilling{{
		Role:         OpenAIAttemptRoleHedge,
		AccountID:    2,
		Mode:         OpenAIHedgeLoserBillingFullPotential,
		AmountMicros: 7000000,
	}}, billing)
	requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary, OpenAIAttemptRoleHedge)
	for _, predicate := range []func(OpenAIHedgeTelemetryEvent) bool{
		func(event OpenAIHedgeTelemetryEvent) bool {
			return event.Kind == OpenAIHedgeTelemetryHedgeOutcome && event.HedgeOutcome == OpenAIHedgeOutcomeLaunched
		},
		func(event OpenAIHedgeTelemetryEvent) bool {
			return event.Kind == OpenAIHedgeTelemetryWinnerCommitted && event.Role == OpenAIAttemptRolePrimary
		},
		func(event OpenAIHedgeTelemetryEvent) bool {
			return event.LoserLifecycle == OpenAILoserLifecycleCancelRequested
		},
		func(event OpenAIHedgeTelemetryEvent) bool {
			return event.Kind == OpenAIHedgeTelemetryBillingFinalized && event.BillingMode == OpenAIHedgeLoserBillingFullPotential
		},
		func(event OpenAIHedgeTelemetryEvent) bool {
			return event.Kind == OpenAIHedgeTelemetrySlotReleased && event.Role == OpenAIAttemptRoleHedge
		},
	} {
		require.True(t, harness.telemetry.contains(predicate))
	}
}

func TestOpenAIHedgeCoordinatorUsesKnownLoserBilling(t *testing.T) {
	hedgeBilled := make(chan struct{})
	harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
		OpenAIAttemptRolePrimary: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
			<-hedgeBilled
			return emitOpenAIHedgeSuccess(OpenAIHedgeEventText, "primary")(context.Background(), OpenAIHedgeAttempt{}, emitter)
		},
		OpenAIAttemptRoleHedge: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
			err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{
				Kind:    OpenAIHedgeEventTerminalFailure,
				Err:     errors.New("hedge failed"),
				Billing: OpenAIHedgeBillingObservation{Known: true, ActualCostMicros: 1250000},
			})
			close(hedgeBilled)
			return err
		},
	}, nil)
	result := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest())
	harness.timers.next(t).fire()
	coordinateResult := awaitOpenAIHedgeResult(t, result)
	require.NoError(t, coordinateResult.err)
	require.Equal(t, OpenAIAttemptRolePrimary, coordinateResult.result.Winner)
	require.Equal(t, []OpenAIHedgeLoserBilling{{
		Role:         OpenAIAttemptRoleHedge,
		AccountID:    2,
		Mode:         OpenAIHedgeLoserBillingActual,
		AmountMicros: 1250000,
	}}, harness.billing.snapshot())
	require.Equal(t, OpenAIBillingOutcomeNotBillable, coordinateResult.result.LoserBillingOutcome)
}

func TestOpenAIHedgeCoordinatorCostExposureFailureIsObservableButDoesNotFailWinner(t *testing.T) {
	hedgeStarted := make(chan struct{})
	harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
		OpenAIAttemptRolePrimary: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
			<-hedgeStarted
			return emitOpenAIHedgeSuccess(OpenAIHedgeEventText, "winner")(context.Background(), OpenAIHedgeAttempt{}, emitter)
		},
		OpenAIAttemptRoleHedge: func(ctx context.Context, _ OpenAIHedgeAttempt, _ OpenAIHedgeEventEmitter) error {
			close(hedgeStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	}, func(harness *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
		harness.billing.err = errors.New("exposure store unavailable")
	})
	result := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest())
	harness.timers.next(t).fire()
	coordinateResult := awaitOpenAIHedgeResult(t, result)
	require.NoError(t, coordinateResult.err)
	require.Equal(t, OpenAIAttemptRolePrimary, coordinateResult.result.Winner)
	require.Equal(t, OpenAIBillingOutcomeFailed, coordinateResult.result.LoserBillingOutcome)
	require.True(t, harness.telemetry.contains(func(event OpenAIHedgeTelemetryEvent) bool {
		return event.Kind == OpenAIHedgeTelemetryBillingFinalized && event.BillingOutcome == OpenAIBillingOutcomeFailed
	}))
	requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary, OpenAIAttemptRoleHedge)
}

func TestOpenAIHedgeCoordinatorBoundsAndIsolatesAttemptBuffers(t *testing.T) {
	t.Run("precommit event count overflow fails attempt", func(t *testing.T) {
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: func(ctx context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
				for range 3 {
					if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventPreamble, Data: []byte("p")}); err != nil {
						return err
					}
				}
				<-ctx.Done()
				return nil
			},
		}, func(_ *openAIHedgeTestHarness, coordinatorConfig *config.OpenAIHedgeConfig) {
			coordinatorConfig.EventChannelCapacity = 2
			coordinatorConfig.MaxPrecommitEvents = 2
		})
		request := eligibleOpenAIHedgeRequest()
		request.Stream = false
		coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), request))
		require.ErrorIs(t, coordinateResult.err, ErrOpenAIHedgeEventBufferExceeded)
		require.Equal(t, 1, terminalOpenAIHedgeEventCount(harness.sink.snapshot()))
		requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary)
	})

	t.Run("loser queue cannot block winner queue", func(t *testing.T) {
		hedgeStarted := make(chan struct{})
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
				<-hedgeStarted
				return emitOpenAIHedgeSuccess(OpenAIHedgeEventText, "primary")(context.Background(), OpenAIHedgeAttempt{}, emitter)
			},
			OpenAIAttemptRoleHedge: func(ctx context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
				close(hedgeStarted)
				for range 4 {
					if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventPreamble, Data: []byte("loser")}); err != nil {
						return nil
					}
				}
				<-ctx.Done()
				return nil
			},
		}, func(_ *openAIHedgeTestHarness, coordinatorConfig *config.OpenAIHedgeConfig) {
			coordinatorConfig.EventChannelCapacity = 1
			coordinatorConfig.MaxPrecommitEvents = 4
		})
		result := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest())
		harness.timers.next(t).fire()
		coordinateResult := awaitOpenAIHedgeResult(t, result)
		require.NoError(t, coordinateResult.err)
		require.Equal(t, OpenAIAttemptRolePrimary, coordinateResult.result.Winner)
		for _, event := range harness.sink.snapshot() {
			require.Equal(t, OpenAIAttemptRolePrimary, event.Role)
		}
	})
}

func TestOpenAIHedgeCoordinatorAtomicWinnerUnderRace(t *testing.T) {
	for iteration := range 1000 {
		t.Run(fmt.Sprintf("iteration_%d", iteration), func(t *testing.T) {
			ready := make(chan struct{}, 2)
			release := make(chan struct{})
			script := func(ctx context.Context, attempt OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
				ready <- struct{}{}
				<-release
				if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventText, Data: fmt.Appendf(nil, "role-%d", attempt.Role)}); err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return err
				}
				if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalSuccess}); err != nil && ctx.Err() == nil {
					return err
				}
				return nil
			}
			harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
				OpenAIAttemptRolePrimary: script,
				OpenAIAttemptRoleHedge:   script,
			}, nil)
			result := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest())
			harness.timers.next(t).fire()
			for range 2 {
				<-ready
			}
			close(release)
			coordinateResult := awaitOpenAIHedgeResult(t, result)
			require.NoError(t, coordinateResult.err)
			require.Contains(t, []OpenAIAttemptRole{OpenAIAttemptRolePrimary, OpenAIAttemptRoleHedge}, coordinateResult.result.Winner)
			events := harness.sink.snapshot()
			require.Equal(t, 1, terminalOpenAIHedgeEventCount(events))
			for _, event := range events {
				require.Equal(t, coordinateResult.result.Winner, event.Role)
			}
			require.Equal(t, 1, harness.candidates.callCount())
			require.Equal(t, int32(2), harness.attempts.maximum.Load())
			require.Len(t, harness.billing.snapshot(), 1)
			requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary, OpenAIAttemptRoleHedge)
		})
	}
}

func TestOpenAIHedgeCoordinatorCommittedWinnerMissingTerminalFinishes(t *testing.T) {
	harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
		OpenAIAttemptRolePrimary: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
			return emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventText, Data: []byte("committed")})
		},
	}, nil)
	coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest()))
	require.ErrorIs(t, coordinateResult.err, ErrOpenAIHedgeAttemptMissingTerminal)
	require.Equal(t, OpenAIAttemptRolePrimary, coordinateResult.result.Winner)
	outputs := harness.sink.snapshot()
	require.Len(t, outputs, 2)
	require.Equal(t, OpenAIHedgeEventText, outputs[0].Event.Kind)
	require.Equal(t, OpenAIHedgeEventTerminalFailure, outputs[1].Event.Kind)
	require.Equal(t, 1, terminalOpenAIHedgeEventCount(outputs))
	for _, output := range outputs {
		require.Equal(t, OpenAIAttemptRolePrimary, output.Role)
		require.Equal(t, int64(1), output.AccountID)
	}
	requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary)
}

func TestOpenAIHedgeCoordinatorTerminalizedAttemptCannotBeResurrectedByQueuedEvents(t *testing.T) {
	harness := newOpenAIHedgeTestHarness(t, nil, nil)
	harness.coordinator.config.maxPrecommitEvents = 1
	attemptContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &openAIHedgeAttemptRuntime{
		attempt: OpenAIHedgeAttempt{Role: OpenAIAttemptRolePrimary, Account: cloneOpenAIHedgeAccount(eligibleOpenAIHedgeRequest().Primary)},
		ctx:     attemptContext,
		cancel:  cancel,
	}
	state := &openAIHedgeCoordinateState{
		coordinator: harness.coordinator,
		ctx:         context.Background(),
		request:     eligibleOpenAIHedgeRequest(),
		startedAt:   time.Now(),
		primary:     runtime,
	}
	state.handleEvent(runtime, OpenAIHedgeEvent{Kind: OpenAIHedgeEventPreamble, Data: []byte("one")})
	state.handleEvent(runtime, OpenAIHedgeEvent{Kind: OpenAIHedgeEventPreamble, Data: []byte("overflow")})
	require.True(t, runtime.terminal)
	require.ErrorIs(t, runtime.failure, ErrOpenAIHedgeEventBufferExceeded)
	state.handleEvent(runtime, OpenAIHedgeEvent{Kind: OpenAIHedgeEventText, Data: []byte("must-not-win")})
	require.Equal(t, uint32(OpenAIAttemptRoleUnknown), state.winner.Load())
	outputs := harness.sink.snapshot()
	require.Len(t, outputs, 1)
	require.Equal(t, OpenAIHedgeEventTerminalFailure, outputs[0].Event.Kind)
	require.NotContains(t, string(outputs[0].Event.Data), "must-not-win")
}

func TestOpenAIHedgeCoordinatorRejectsUnimplementedNoProgressTrigger(t *testing.T) {
	coordinatorConfig := enabledOpenAIHedgeTestConfig()
	coordinatorConfig.NoProgressEnabled = true
	_, err := NewOpenAIHedgeCoordinator(coordinatorConfig, OpenAIHedgeCoordinatorDependencies{})
	require.ErrorContains(t, err, "no-progress trigger is not supported")
}

func TestOpenAIHedgeCoordinatorMissingTerminalAndSinkFailure(t *testing.T) {
	t.Run("missing terminal is explicit", func(t *testing.T) {
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: func(context.Context, OpenAIHedgeAttempt, OpenAIHedgeEventEmitter) error { return nil },
		}, nil)
		request := eligibleOpenAIHedgeRequest()
		request.Stream = false
		coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), request))
		require.ErrorIs(t, coordinateResult.err, ErrOpenAIHedgeAttemptMissingTerminal)
	})

	t.Run("sink error cancels request", func(t *testing.T) {
		sinkErr := errors.New("sink unavailable")
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: emitOpenAIHedgeSuccess(OpenAIHedgeEventText, "primary"),
		}, func(harness *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
			harness.sink.failure = func(OpenAIHedgeOutput) error { return sinkErr }
		})
		request := eligibleOpenAIHedgeRequest()
		request.Stream = false
		coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), request))
		require.ErrorIs(t, coordinateResult.err, sinkErr)
		requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary)
	})
}

func TestOpenAIHedgeCoordinatorPrimarySlotFailureWritesOneBoundedTerminal(t *testing.T) {
	slotErr := errors.New("primary slot unavailable")
	harness := newOpenAIHedgeTestHarness(t, nil, func(harness *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
		harness.slots.acquireErr[OpenAIAttemptRolePrimary] = slotErr
	})
	result, err := harness.coordinator.Coordinate(context.Background(), eligibleOpenAIHedgeRequest())
	require.ErrorIs(t, err, slotErr)
	require.Equal(t, OpenAIAttemptRoleUnknown, result.Winner)
	require.Zero(t, harness.attempts.callCount(OpenAIAttemptRolePrimary))
	outputs := harness.sink.snapshot()
	require.Len(t, outputs, 1)
	require.Equal(t, OpenAIAttemptRolePrimary, outputs[0].Role)
	require.Equal(t, int64(1), outputs[0].AccountID)
	require.Equal(t, OpenAIHedgeEventTerminalFailure, outputs[0].Event.Kind)
}

func TestOpenAIHedgeCoordinatorAttemptAndTelemetryPanicsFailSafely(t *testing.T) {
	t.Run("attempt panic becomes a terminal failure", func(t *testing.T) {
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: func(context.Context, OpenAIHedgeAttempt, OpenAIHedgeEventEmitter) error {
				panic("synthetic attempt panic")
			},
		}, nil)
		request := eligibleOpenAIHedgeRequest()
		request.Stream = false
		coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), request))
		require.ErrorIs(t, coordinateResult.err, ErrOpenAIHedgeAttemptMissingTerminal)
		require.ErrorContains(t, coordinateResult.err, "attempt adapter panicked")
		require.Equal(t, 1, terminalOpenAIHedgeEventCount(harness.sink.snapshot()))
		requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary)
	})

	t.Run("telemetry panic cannot break routing", func(t *testing.T) {
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: emitOpenAIHedgeSuccess(OpenAIHedgeEventTerminalSuccess, ""),
		}, nil)
		harness.coordinator.dependencies.Telemetry = panickingOpenAIHedgeTelemetryPort{}
		result, err := harness.coordinator.Coordinate(context.Background(), eligibleOpenAIHedgeRequest())
		require.NoError(t, err)
		require.Equal(t, OpenAIAttemptRolePrimary, result.Winner)
		require.Equal(t, 1, terminalOpenAIHedgeEventCount(harness.sink.snapshot()))
		requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary)
	})

	for _, stage := range []string{"candidate", "admission"} {
		t.Run(stage+" panic rejects hedge without crashing", func(t *testing.T) {
			policyEntered := make(chan struct{})
			harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
				OpenAIAttemptRolePrimary: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
					<-policyEntered
					return emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalSuccess})
				},
			}, func(harness *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
				if stage == "candidate" {
					harness.candidates.selectFn = func(context.Context, OpenAIHedgeSelection) (OpenAIHedgeAccount, error) {
						close(policyEntered)
						panic("candidate secret payload")
					}
					return
				}
				harness.admission.checkFn = func(context.Context, OpenAIHedgeAdmission) (OpenAIHedgeAdmissionDecision, error) {
					close(policyEntered)
					panic("admission secret payload")
				}
			})
			resultChannel := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest())
			harness.timers.next(t).fire()
			coordinateResult := awaitOpenAIHedgeResult(t, resultChannel)
			require.NoError(t, coordinateResult.err)
			require.Equal(t, OpenAIAttemptRolePrimary, coordinateResult.result.Winner)
			require.False(t, coordinateResult.result.HedgeLaunched)
			require.Zero(t, harness.attempts.callCount(OpenAIAttemptRoleHedge))
			requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary)
		})
	}

	t.Run("sink panic is bounded and does not expose panic payload", func(t *testing.T) {
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: emitOpenAIHedgeSuccess(OpenAIHedgeEventText, "winner"),
		}, func(harness *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
			harness.sink.failure = func(OpenAIHedgeOutput) error {
				panic("sink secret payload")
			}
		})
		coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest()))
		require.ErrorContains(t, coordinateResult.err, "hedge winner sink write panicked")
		require.NotContains(t, coordinateResult.err.Error(), "secret payload")
	})

	t.Run("cost exposure panic is fail-open and bounded", func(t *testing.T) {
		hedgeStarted := make(chan struct{})
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
				<-hedgeStarted
				return emitOpenAIHedgeSuccess(OpenAIHedgeEventText, "winner")(context.Background(), OpenAIHedgeAttempt{}, emitter)
			},
			OpenAIAttemptRoleHedge: func(ctx context.Context, _ OpenAIHedgeAttempt, _ OpenAIHedgeEventEmitter) error {
				close(hedgeStarted)
				<-ctx.Done()
				return ctx.Err()
			},
		}, nil)
		harness.coordinator.dependencies.Billing = panickingOpenAIHedgeBillingPort{}
		resultChannel := coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest())
		harness.timers.next(t).fire()
		coordinateResult := awaitOpenAIHedgeResult(t, resultChannel)
		require.NoError(t, coordinateResult.err)
		require.Equal(t, OpenAIBillingOutcomeFailed, coordinateResult.result.LoserBillingOutcome)
		requireOpenAIHedgeSlotsReleased(t, harness.slots, OpenAIAttemptRolePrimary, OpenAIAttemptRoleHedge)
	})

	t.Run("slot release panic is surfaced without crashing", func(t *testing.T) {
		harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
			OpenAIAttemptRolePrimary: emitOpenAIHedgeSuccess(OpenAIHedgeEventTerminalSuccess, ""),
		}, func(harness *openAIHedgeTestHarness, _ *config.OpenAIHedgeConfig) {
			harness.slots.releasePanic[OpenAIAttemptRolePrimary] = true
		})
		coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), eligibleOpenAIHedgeRequest()))
		require.ErrorContains(t, coordinateResult.err, "slot release panic")
		require.Equal(t, OpenAIAttemptRolePrimary, coordinateResult.result.Winner)
		require.Equal(t, 1, terminalOpenAIHedgeEventCount(harness.sink.snapshot()))
	})
}

func TestOpenAIHedgeCoordinatorCleanupTimeoutDoesNotReleaseLiveAttemptSlot(t *testing.T) {
	unblock := make(chan struct{})
	harness := newOpenAIHedgeTestHarness(t, map[OpenAIAttemptRole]openAIHedgeAttemptScript{
		OpenAIAttemptRolePrimary: func(_ context.Context, _ OpenAIHedgeAttempt, emitter OpenAIHedgeEventEmitter) error {
			if err := emitter.EmitOpenAIHedgeEvent(OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalSuccess}); err != nil {
				return err
			}
			<-unblock
			return nil
		},
	}, nil)
	harness.coordinator.config.cancelDrainTimeout = 20 * time.Millisecond
	request := eligibleOpenAIHedgeRequest()
	request.Stream = false
	coordinateResult := awaitOpenAIHedgeResult(t, coordinateOpenAIHedgeAsync(harness.coordinator, context.Background(), request))
	require.ErrorIs(t, coordinateResult.err, ErrOpenAIHedgeCleanupTimeout)
	acquired, released := harness.slots.counts(OpenAIAttemptRolePrimary)
	require.Equal(t, 1, acquired)
	require.Zero(t, released, "a live upstream attempt must retain its concurrency slot")
	close(unblock)
	require.Eventually(t, func() bool {
		acquired, released = harness.slots.counts(OpenAIAttemptRolePrimary)
		return acquired == 1 && released == 1
	}, time.Second, time.Millisecond)
}

func pointerToInt64(value int64) *int64 {
	return &value
}
