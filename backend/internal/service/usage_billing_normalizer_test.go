package service

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyAccountUsageBillingMultiplier(t *testing.T) {
	tests := []struct {
		name       string
		account    *Account
		actualCost float64
		wantCost   float64
		wantRate   float64
	}{
		{
			name:       "legacy account defaults to one",
			account:    &Account{},
			actualCost: 25,
			wantCost:   25,
			wantRate:   1,
		},
		{
			name:       "api2cn inverse normalizes source debit",
			account:    &Account{UsageBillingMultiplier: float64Ptr(1 / 0.22)},
			actualCost: 5.5,
			wantCost:   25,
			wantRate:   1 / 0.22,
		},
		{
			name:       "black coding inverse normalizes source debit",
			account:    &Account{UsageBillingMultiplier: float64Ptr(1 / 1.4)},
			actualCost: 35,
			wantCost:   25,
			wantRate:   1 / 1.4,
		},
		{
			name:       "nil account defaults to one",
			account:    nil,
			actualCost: 7.25,
			wantCost:   7.25,
			wantRate:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := &CostBreakdown{TotalCost: tt.actualCost, ActualCost: tt.actualCost}
			usage := &UsageLog{}

			applyAccountUsageBillingMultiplier(cost, usage, tt.account)
			applyAccountUsageBillingMultiplier(cost, usage, tt.account)

			require.InDelta(t, tt.wantCost, cost.ActualCost, 1e-9, "must apply exactly once")
			require.Equal(t, tt.actualCost, cost.TotalCost)
			require.NotNil(t, usage.UsageBillingMultiplier)
			require.InDelta(t, tt.wantRate, *usage.UsageBillingMultiplier, 1e-9)
			require.InDelta(t, tt.wantCost, usage.ActualCost, 1e-9)
		})
	}
}

func TestApplyAccountUsageBillingMultiplierRetryWithNewUsageLogDoesNotDoubleCharge(t *testing.T) {
	multiplier := 2.0
	account := &Account{UsageBillingMultiplier: &multiplier}
	cost := &CostBreakdown{TotalCost: 10, ActualCost: 15}

	applyAccountUsageBillingMultiplier(cost, nil, account)
	retryLog := &UsageLog{}
	applyAccountUsageBillingMultiplier(cost, retryLog, account)

	require.InDelta(t, 30, cost.ActualCost, 1e-12)
	require.InDelta(t, 10, cost.TotalCost, 1e-12)
	require.InDelta(t, 30, retryLog.ActualCost, 1e-12)
	require.NotNil(t, retryLog.UsageBillingMultiplier)
	require.InDelta(t, 2, *retryLog.UsageBillingMultiplier, 1e-12)
}

func TestValidateUsageBillingMultiplier(t *testing.T) {
	require.NoError(t, ValidateUsageBillingMultiplier(nil))
	for _, value := range []float64{0.7, 1, MaxUsageBillingMultiplier} {
		value := value
		require.NoError(t, ValidateUsageBillingMultiplier(&value))
	}
	for _, value := range []float64{0, -1, MaxUsageBillingMultiplier + 1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		value := value
		require.Error(t, ValidateUsageBillingMultiplier(&value))
	}
}

func TestAccountUsageBillingMultiplierRejectsInvalidPersistedValues(t *testing.T) {
	for _, value := range []float64{0, -1, MaxUsageBillingMultiplier + 1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		account := &Account{UsageBillingMultiplier: float64Ptr(value)}
		require.Equal(t, 1.0, account.EffectiveUsageBillingMultiplier())
	}
}
