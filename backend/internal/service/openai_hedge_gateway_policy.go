package service

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/tidwall/gjson"
)

// OpenAIHedgeGatewayPolicy is request-scoped. It selects a distinct native
// Responses account through the production scheduler, applies incident/cost
// admission, and acquires an immediate secondary concurrency slot.
type OpenAIHedgeGatewayPolicy struct {
	Gateway            *OpenAIGatewayService
	Runtime            *AdaptiveLatencyRuntime
	Config             config.OpenAIHedgeConfig
	GroupID            *int64
	Model              string
	Platform           string
	Body               []byte
	Primary            *Account
	RequiredCapability OpenAIEndpointCapability

	mu                 sync.Mutex
	candidate          *Account
	candidateBody      []byte
	candidateSelection *AccountSelectionResult
	leases             map[OpenAIAttemptRole]OpenAIHedgeSlot
}

func NewOpenAIHedgeGatewayPolicy(
	gateway *OpenAIGatewayService,
	runtime *AdaptiveLatencyRuntime,
	cfg config.OpenAIHedgeConfig,
	groupID *int64,
	model, platform string,
	body []byte,
	primary *Account,
) (*OpenAIHedgeGatewayPolicy, error) {
	if gateway == nil || runtime == nil || primary == nil {
		return nil, errors.New("openai hedge gateway policy dependencies are incomplete")
	}
	return &OpenAIHedgeGatewayPolicy{
		Gateway: gateway, Runtime: runtime, Config: cfg, GroupID: groupID,
		Model: model, Platform: platform, Body: append([]byte(nil), body...), Primary: primary,
		RequiredCapability: OpenAIEndpointCapabilityChatCompletions,
		leases:             make(map[OpenAIAttemptRole]OpenAIHedgeSlot, 2),
	}, nil
}

func (policy *OpenAIHedgeGatewayPolicy) SelectOpenAIHedgeCandidate(
	ctx context.Context,
	_ OpenAIHedgeSelection,
) (OpenAIHedgeAccount, error) {
	excluded := map[int64]struct{}{policy.Primary.ID: {}}
	if policy.Primary.ParentAccountID != nil {
		excluded[*policy.Primary.ParentAccountID] = struct{}{}
	}
	selection, _, err := policy.Gateway.SelectAccountWithSchedulerForCapability(
		ctx,
		policy.GroupID,
		"",
		"", // never mutate or consume a sticky binding for speculative work
		policy.Model,
		excluded,
		OpenAIUpstreamTransportHTTPSSE,
		policy.RequiredCapability,
		false,
		false,
		true,
		policy.Platform,
	)
	if err != nil || selection == nil || selection.Account == nil {
		return OpenAIHedgeAccount{}, ErrOpenAIHedgeNoCandidate
	}
	// Scheduler probes may acquire a slot. Candidate selection must not retain it
	// across admission/cancellation; dispatch performs a fresh immediate acquire.
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
	candidate := selection.Account
	policy.mu.Lock()
	policy.candidate = candidate
	policy.candidateBody = append([]byte(nil), policy.Body...)
	policy.candidateSelection = selection
	policy.mu.Unlock()
	return hedgeAccountFromGatewayAccount(candidate, policy.estimatePotentialCostMicros(candidate)), nil
}

func (policy *OpenAIHedgeGatewayPolicy) CheckOpenAIHedgeAdmission(
	_ context.Context,
	admission OpenAIHedgeAdmission,
) (OpenAIHedgeAdmissionDecision, error) {
	potential := admission.Candidate.PotentialCostMicros
	policy.mu.Lock()
	candidate := policy.candidate
	policy.mu.Unlock()
	return OpenAIHedgeAdmissionDecision{
		ProviderEligible:  candidate != nil && candidate.SupportsOpenAIEndpointCapability(policy.RequiredCapability),
		HeadroomAvailable: policy.Runtime.Health() == nil || !policy.Runtime.Health().AnyProviderIncidentActive(),
		CostAllowed:       potential > 0 && potential <= policy.Config.MaxDuplicateCostMicros,
	}, nil
}

func (policy *OpenAIHedgeGatewayPolicy) AcquireOpenAIHedgeSlot(
	ctx context.Context,
	role OpenAIAttemptRole,
	_ OpenAIHedgeAccount,
) (OpenAIHedgeSlot, error) {
	policy.mu.Lock()
	if lease := policy.leases[role]; lease != nil {
		delete(policy.leases, role)
		policy.mu.Unlock()
		return lease, nil
	}
	candidate := policy.candidate
	selection := policy.candidateSelection
	policy.mu.Unlock()
	if role == OpenAIAttemptRolePrimary {
		return noopOpenAIHedgeSlot{}, nil
	}
	if candidate == nil {
		return nil, ErrOpenAIHedgeNoCandidate
	}
	acquire := func(account *Account) (*AcquireResult, error) {
		return policy.Gateway.tryAcquireAccountSlot(ctx, account.ID, account.Concurrency)
	}
	result, err := acquire(candidate)
	if err != nil {
		return nil, err
	}
	if result == nil || !result.Acquired || result.ReleaseFunc == nil {
		return nil, errors.New("openai hedge secondary capacity unavailable")
	}
	profitContext := ContextWithSelectionProfitGate(ctx, selection)
	latest, vetoed, _ := policy.Gateway.ProfitControlVetoLatest(profitContext, candidate)
	if vetoed || latest == nil {
		result.ReleaseFunc()
		return nil, errors.New("openai hedge secondary rejected by profit admission")
	}
	if latest.ID != candidate.ID || openAIHedgeGatewayAccountsOverlap(policy.Primary, latest) ||
		!policy.postAcquireAccountAllowed(latest) {
		result.ReleaseFunc()
		return nil, errors.New("openai hedge secondary changed during admission")
	}
	if latest.Concurrency != candidate.Concurrency {
		result.ReleaseFunc()
		result, err = acquire(latest)
		if err != nil || result == nil || !result.Acquired || result.ReleaseFunc == nil {
			return nil, errors.New("openai hedge secondary capacity changed")
		}
		rechecked, recheckedVetoed, _ := policy.Gateway.ProfitControlVetoLatest(profitContext, latest)
		if recheckedVetoed || rechecked == nil || rechecked.ID != latest.ID ||
			openAIHedgeGatewayAccountsOverlap(policy.Primary, rechecked) ||
			rechecked.Concurrency != latest.Concurrency || !policy.postAcquireAccountAllowed(rechecked) {
			result.ReleaseFunc()
			return nil, errors.New("openai hedge secondary changed after capacity reacquisition")
		}
		latest = rechecked
	}
	policy.mu.Lock()
	policy.candidate = latest
	policy.mu.Unlock()
	return &releaseFuncOpenAIHedgeSlot{release: result.ReleaseFunc}, nil
}

func (policy *OpenAIHedgeGatewayPolicy) Candidate() (*Account, []byte) {
	policy.mu.Lock()
	defer policy.mu.Unlock()
	return policy.candidate, append([]byte(nil), policy.candidateBody...)
}

func (policy *OpenAIHedgeGatewayPolicy) TransferPrimaryLease(release func()) error {
	if release == nil {
		return errors.New("openai hedge primary lease is required")
	}
	policy.mu.Lock()
	defer policy.mu.Unlock()
	if _, exists := policy.leases[OpenAIAttemptRolePrimary]; exists {
		return errors.New("openai hedge primary lease already transferred")
	}
	policy.leases[OpenAIAttemptRolePrimary] = &releaseFuncOpenAIHedgeSlot{release: release}
	return nil
}

func (policy *OpenAIHedgeGatewayPolicy) ReleasePendingLeases() {
	if policy == nil {
		return
	}
	policy.mu.Lock()
	leases := make([]OpenAIHedgeSlot, 0, len(policy.leases))
	for role, lease := range policy.leases {
		leases = append(leases, lease)
		delete(policy.leases, role)
	}
	policy.mu.Unlock()
	for _, lease := range leases {
		if lease != nil {
			lease.Release()
		}
	}
}

func (policy *OpenAIHedgeGatewayPolicy) PrimaryHedgeAccount() OpenAIHedgeAccount {
	return hedgeAccountFromGatewayAccount(policy.Primary, policy.estimatePotentialCostMicros(policy.Primary))
}

func (policy *OpenAIHedgeGatewayPolicy) estimatePotentialCostMicros(account *Account) uint64 {
	if policy == nil || policy.Gateway == nil || policy.Gateway.billingService == nil || account == nil {
		return 0
	}
	// One byte/token deliberately overestimates ordinary JSON text and remains
	// conservative for non-ASCII payloads. Explicit max_output_tokens is bounded.
	inputTokens := len(policy.Body)
	outputTokens := int(gjson.GetBytes(policy.Body, "max_output_tokens").Int())
	if outputTokens <= 0 {
		outputTokens = 4096
	}
	if outputTokens > 32768 {
		outputTokens = 32768
	}
	cost, err := policy.Gateway.billingService.CalculateCost(
		policy.Model,
		UsageTokens{InputTokens: inputTokens, OutputTokens: outputTokens},
		account.BillingRateMultiplier(),
	)
	if err != nil || cost == nil || cost.ActualCost <= 0 || math.IsNaN(cost.ActualCost) || math.IsInf(cost.ActualCost, 0) {
		return 0
	}
	micros := cost.ActualCost * 1_000_000
	if micros >= math.MaxUint64 {
		return math.MaxUint64
	}
	return uint64(math.Ceil(micros))
}

func (policy *OpenAIHedgeGatewayPolicy) postAcquireAccountAllowed(account *Account) bool {
	if policy == nil || account == nil || !account.IsSchedulable() ||
		normalizeOpenAICompatiblePlatform(account.Platform) != normalizeOpenAICompatiblePlatform(policy.Platform) ||
		!account.SupportsOpenAIEndpointCapability(policy.RequiredCapability) {
		return false
	}
	if policy.Runtime != nil && policy.Runtime.Health() != nil && policy.Runtime.Health().AnyProviderIncidentActive() {
		return false
	}
	potential := policy.estimatePotentialCostMicros(account)
	return potential > 0 && potential <= policy.Config.MaxDuplicateCostMicros
}

func openAIHedgeGatewayAccountsOverlap(primary, candidate *Account) bool {
	if primary == nil || candidate == nil {
		return false
	}
	primaryOwnerID := primary.ID
	if primary.ParentAccountID != nil {
		primaryOwnerID = *primary.ParentAccountID
	}
	candidateOwnerID := candidate.ID
	if candidate.ParentAccountID != nil {
		candidateOwnerID = *candidate.ParentAccountID
	}
	return candidate.ID == primary.ID || candidate.ID == primaryOwnerID ||
		candidateOwnerID == primary.ID || candidateOwnerID == primaryOwnerID
}

func hedgeAccountFromGatewayAccount(account *Account, potentialCostMicros uint64) OpenAIHedgeAccount {
	if account == nil {
		return OpenAIHedgeAccount{}
	}
	return OpenAIHedgeAccount{
		AccountID:           account.ID,
		ParentAccountID:     account.ParentAccountID,
		Provider:            account.Platform,
		PotentialCostMicros: potentialCostMicros,
	}
}

type noopOpenAIHedgeSlot struct{}

func (noopOpenAIHedgeSlot) Release() {}

type releaseFuncOpenAIHedgeSlot struct {
	once    sync.Once
	release func()
}

func (slot *releaseFuncOpenAIHedgeSlot) Release() {
	if slot == nil {
		return
	}
	slot.once.Do(slot.release)
}

var _ OpenAIHedgeCandidatePort = (*OpenAIHedgeGatewayPolicy)(nil)
var _ OpenAIHedgeAdmissionPort = (*OpenAIHedgeGatewayPolicy)(nil)
var _ OpenAIHedgeSlotPort = (*OpenAIHedgeGatewayPolicy)(nil)
