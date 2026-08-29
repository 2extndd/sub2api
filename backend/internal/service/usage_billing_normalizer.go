package service

import (
	"fmt"
	"math"
)

const MaxUsageBillingMultiplier = 1000000.0

// ValidateUsageBillingMultiplier validates an optional admin write. nil means
// preserve/default; persisted values must be finite, positive, and bounded.
func ValidateUsageBillingMultiplier(value *float64) error {
	if value == nil {
		return nil
	}
	if *value <= 0 || *value > MaxUsageBillingMultiplier || math.IsNaN(*value) || math.IsInf(*value, 0) {
		return fmt.Errorf("usage_billing_multiplier must be finite, > 0, and <= %g", MaxUsageBillingMultiplier)
	}
	return nil
}

// applyAccountUsageBillingMultiplier applies the final successful routing
// account's customer billing multiplier exactly once, immediately before the
// shared transactional billing command is built. TotalCost and component costs
// remain the pre-account-normalization audit basis; ActualCost is the amount
// propagated to user balance, API-key quota, subscription usage, dedup, exports,
// and customer-facing reports.
func applyAccountUsageBillingMultiplier(cost *CostBreakdown, usageLog *UsageLog, account *Account) {
	if cost == nil {
		return
	}
	// CostBreakdown owns the in-memory application marker because callers may
	// rebuild or omit a usage log while retrying the same billing operation.
	if !cost.usageBillingMultiplierApplied {
		cost.appliedUsageBillingMultiplier = account.EffectiveUsageBillingMultiplier()
		cost.ActualCost *= cost.appliedUsageBillingMultiplier
		cost.usageBillingMultiplierApplied = true
	}
	if usageLog != nil {
		multiplier := cost.appliedUsageBillingMultiplier
		usageLog.ActualCost = cost.ActualCost
		usageLog.UsageBillingMultiplier = &multiplier
	}
}
