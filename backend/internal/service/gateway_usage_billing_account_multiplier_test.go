package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type accountMultiplierBillingRepo struct {
	command *UsageBillingCommand
}

func (r *accountMultiplierBillingRepo) Apply(_ context.Context, command *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	r.command = command
	return &UsageBillingApplyResult{Applied: false}, nil
}

func (*accountMultiplierBillingRepo) ReserveBatchImageBalance(context.Context, *BatchImageBalanceHoldCommand) (*BatchImageBalanceHoldResult, error) {
	return nil, nil
}

func (*accountMultiplierBillingRepo) CaptureBatchImageBalance(context.Context, *BatchImageBalanceHoldCommand) (*BatchImageBalanceHoldResult, error) {
	return nil, nil
}

func (*accountMultiplierBillingRepo) ReleaseBatchImageBalance(context.Context, *BatchImageBalanceHoldCommand) (*BatchImageBalanceHoldResult, error) {
	return nil, nil
}

type accountMultiplierQuotaUpdater struct{}

func (accountMultiplierQuotaUpdater) UpdateQuotaUsed(context.Context, int64, float64) error {
	return nil
}

func (accountMultiplierQuotaUpdater) UpdateRateLimitUsage(context.Context, int64, float64) error {
	return nil
}

func TestApplyUsageBillingCombinesGroupAndSuccessfulAccountMultiplierOnce(t *testing.T) {
	accountMultiplier := 2.0
	cost := &CostBreakdown{
		TotalCost:  10.0, // official model cost
		ActualCost: 15.0, // group/user multiplier already applied: 10 x 1.5
	}
	usageLog := &UsageLog{
		RequestID:  "req-account-normalization",
		Model:      "gpt-test",
		TotalCost:  cost.TotalCost,
		ActualCost: cost.ActualCost,
	}
	apiKey := &APIKey{ID: 22, Quota: 100}
	account := &Account{ID: 33, UsageBillingMultiplier: &accountMultiplier}
	repo := &accountMultiplierBillingRepo{}

	applied, err := applyUsageBilling(
		context.Background(),
		usageLog.RequestID,
		usageLog,
		&postUsageBillingParams{
			Cost:          cost,
			User:          &User{ID: 11},
			APIKey:        apiKey,
			Account:       account,
			APIKeyService: accountMultiplierQuotaUpdater{},
		},
		&billingDeps{deferredService: &DeferredService{}},
		repo,
	)

	require.NoError(t, err)
	require.False(t, applied, "fake repository reports a deduplicated command")
	require.NotNil(t, repo.command)
	require.InDelta(t, 10.0, cost.TotalCost, 1e-12)
	require.InDelta(t, 30.0, cost.ActualCost, 1e-12)
	require.InDelta(t, 30.0, usageLog.ActualCost, 1e-12)
	require.NotNil(t, usageLog.UsageBillingMultiplier)
	require.InDelta(t, 2.0, *usageLog.UsageBillingMultiplier, 1e-12)
	require.InDelta(t, 30.0, repo.command.BalanceCost, 1e-12)
	require.InDelta(t, 30.0, repo.command.APIKeyQuotaCost, 1e-12)

	applyAccountUsageBillingMultiplier(cost, usageLog, account)
	require.InDelta(t, 30.0, cost.ActualCost, 1e-12, "snapshot marker prevents double application")
}
