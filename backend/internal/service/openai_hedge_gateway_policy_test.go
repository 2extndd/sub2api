package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestOpenAIHedgeGatewayAccountsOverlapRejectsCredentialOwnerAliases(t *testing.T) {
	primaryParentID := int64(10)
	candidateParentID := int64(20)
	tests := []struct {
		name      string
		primary   *Account
		candidate *Account
		overlaps  bool
	}{
		{name: "same account", primary: &Account{ID: 10}, candidate: &Account{ID: 10}, overlaps: true},
		{name: "candidate is primary parent", primary: &Account{ID: 11, ParentAccountID: &primaryParentID}, candidate: &Account{ID: 10}, overlaps: true},
		{name: "candidate child owns primary", primary: &Account{ID: 10}, candidate: &Account{ID: 21, ParentAccountID: &primaryParentID}, overlaps: true},
		{name: "siblings share owner", primary: &Account{ID: 11, ParentAccountID: &primaryParentID}, candidate: &Account{ID: 12, ParentAccountID: &primaryParentID}, overlaps: true},
		{name: "distinct owners", primary: &Account{ID: 11, ParentAccountID: &primaryParentID}, candidate: &Account{ID: 21, ParentAccountID: &candidateParentID}, overlaps: false},
		{name: "nil primary", primary: nil, candidate: &Account{ID: 21}, overlaps: false},
		{name: "nil candidate", primary: &Account{ID: 11}, candidate: nil, overlaps: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.overlaps, openAIHedgeGatewayAccountsOverlap(test.primary, test.candidate))
		})
	}
}

func TestOpenAIHedgeGatewayPolicyTransferredPrimaryLeaseReleasesExactlyOnce(t *testing.T) {
	policy := &OpenAIHedgeGatewayPolicy{leases: make(map[OpenAIAttemptRole]OpenAIHedgeSlot, 2)}
	var releases atomic.Int64
	require.NoError(t, policy.TransferPrimaryLease(func() { releases.Add(1) }))

	policy.ReleasePendingLeases()
	policy.ReleasePendingLeases()

	require.Equal(t, int64(1), releases.Load())
}

type openAIHedgePolicySequenceSnapshotCache struct {
	SchedulerCache
	mu       sync.Mutex
	accounts []*Account
}

func (cache *openAIHedgePolicySequenceSnapshotCache) GetAccount(context.Context, int64) (*Account, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.accounts) == 0 {
		return nil, nil
	}
	account := cache.accounts[0]
	cache.accounts = cache.accounts[1:]
	return account, nil
}

type openAIHedgePolicyCountingConcurrencyCache struct {
	ConcurrencyCache
	acquires atomic.Int64
	releases atomic.Int64
}

func (cache *openAIHedgePolicyCountingConcurrencyCache) AcquireAccountSlot(context.Context, int64, int, string) (bool, error) {
	cache.acquires.Add(1)
	return true, nil
}

func (cache *openAIHedgePolicyCountingConcurrencyCache) ReleaseAccountSlot(context.Context, int64, string) error {
	cache.releases.Add(1)
	return nil
}

func TestOpenAIHedgeGatewayPolicyRechecksProfitAfterCapacityReacquisition(t *testing.T) {
	candidate := &Account{
		ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Status: StatusActive, Schedulable: true, Concurrency: 2, RateMultiplier: float64Ptr(1),
	}
	firstRefresh := *candidate
	firstRefresh.Concurrency = 3
	secondRefresh := firstRefresh
	secondRefresh.Concurrency = 4
	snapshot := NewSchedulerSnapshotService(
		&openAIHedgePolicySequenceSnapshotCache{accounts: []*Account{&firstRefresh, &secondRefresh}},
		nil,
		nil,
		nil,
		nil,
	)
	concurrencyCache := &openAIHedgePolicyCountingConcurrencyCache{}
	cfg := &config.Config{}
	gateway := &OpenAIGatewayService{
		cfg:                cfg,
		schedulerSnapshot:  snapshot,
		concurrencyService: NewConcurrencyService(concurrencyCache),
		billingService:     NewBillingService(cfg, nil),
	}
	gate := &openAIProfitControlGate{threshold: 10}
	policy := &OpenAIHedgeGatewayPolicy{
		Gateway:            gateway,
		Config:             config.OpenAIHedgeConfig{MaxDuplicateCostMicros: config.MaximumOpenAIHedgeMaxDuplicateCostMicros},
		Model:              "gpt-5.4",
		Platform:           PlatformOpenAI,
		Body:               []byte(`{"model":"gpt-5.4","max_output_tokens":16,"input":"hello"}`),
		Primary:            &Account{ID: 1},
		RequiredCapability: OpenAIEndpointCapabilityChatCompletions,
		candidate:          candidate,
		candidateSelection: &AccountSelectionResult{
			Account:    candidate,
			profitGate: gate,
		},
		leases: make(map[OpenAIAttemptRole]OpenAIHedgeSlot, 2),
	}

	slot, err := policy.AcquireOpenAIHedgeSlot(context.Background(), OpenAIAttemptRoleHedge, OpenAIHedgeAccount{AccountID: 2})

	require.Error(t, err)
	require.Nil(t, slot)
	require.Equal(t, int64(2), concurrencyCache.acquires.Load())
	require.Equal(t, int64(2), concurrencyCache.releases.Load())
}

func TestOpenAIHedgeGatewayPolicyPostAcquireAccountAdmission(t *testing.T) {
	cfg := &config.Config{}
	gateway := &OpenAIGatewayService{cfg: cfg, billingService: NewBillingService(cfg, nil)}
	policy := &OpenAIHedgeGatewayPolicy{
		Gateway:            gateway,
		Config:             config.OpenAIHedgeConfig{MaxDuplicateCostMicros: config.MaximumOpenAIHedgeMaxDuplicateCostMicros},
		Model:              "gpt-5.4",
		Platform:           PlatformOpenAI,
		Body:               []byte(`{"model":"gpt-5.4","max_output_tokens":16,"input":"hello"}`),
		Primary:            &Account{ID: 1},
		RequiredCapability: OpenAIEndpointCapabilityChatCompletions,
	}
	valid := Account{
		ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Status: StatusActive, Schedulable: true, Concurrency: 2, RateMultiplier: float64Ptr(1),
	}

	tests := []struct {
		name    string
		mutate  func(*Account)
		allowed bool
	}{
		{name: "valid", allowed: true},
		{name: "unschedulable", mutate: func(account *Account) { account.Schedulable = false }},
		{name: "wrong platform", mutate: func(account *Account) { account.Platform = PlatformGrok }},
		{name: "capability removed", mutate: func(account *Account) {
			account.Credentials = map[string]any{openAIEndpointCapabilitiesCredentialKey: []string{string(OpenAIEndpointCapabilityResponses)}}
		}},
		{name: "duplicate cost over budget", mutate: func(account *Account) {
			account.RateMultiplier = float64Ptr(1_000_000_000)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			account := valid
			if test.mutate != nil {
				test.mutate(&account)
			}
			require.Equal(t, test.allowed, policy.postAcquireAccountAllowed(&account))
		})
	}
}

func TestReleaseFuncOpenAIHedgeSlotIsIdempotent(t *testing.T) {
	var releases atomic.Int64
	slot := &releaseFuncOpenAIHedgeSlot{release: func() { releases.Add(1) }}

	slot.Release()
	slot.Release()

	require.Equal(t, int64(1), releases.Load())
}
