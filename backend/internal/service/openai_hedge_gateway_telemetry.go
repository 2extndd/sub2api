package service

import "context"

// OpenAIHedgeRuntimeTelemetryAdapter bridges one request's generic hedge
// lifecycle into shared bounded telemetry. It stores no payload or raw error.
type OpenAIHedgeRuntimeTelemetryAdapter struct {
	Runtime *AdaptiveLatencyRuntime
	Cohort  CohortKey
}

func (adapter *OpenAIHedgeRuntimeTelemetryAdapter) RecordOpenAIHedgeTelemetry(event OpenAIHedgeTelemetryEvent) {
	if adapter == nil || adapter.Runtime == nil || adapter.Runtime.telemetry == nil || !adapter.Runtime.telemetry.Enabled() {
		return
	}
	accountID := event.AccountID
	if accountID <= 0 {
		return
	}
	role := event.Role
	if role == OpenAIAttemptRoleUnknown {
		role = OpenAIAttemptRolePrimary
	}
	key := OpenAILatencyTelemetryKey{AccountID: accountID, Cohort: adapter.Cohort, AttemptRole: role}
	if event.Phase != OpenAILatencyPhaseUnknown && event.Elapsed >= 0 {
		_ = adapter.Runtime.telemetry.ObservePhase(key, event.Phase, event.Elapsed)
	}
	if event.HedgeOutcome != OpenAIHedgeOutcomeNone {
		_ = adapter.Runtime.telemetry.ObserveHedge(key, event.HedgeOutcome)
	}
	if event.WinnerOutcome != OpenAIWinnerOutcomeUnknown {
		_ = adapter.Runtime.telemetry.ObserveWinner(key, event.WinnerOutcome)
	}
	if event.LoserCancelOutcome != OpenAILoserCancelOutcomeUnknown {
		_ = adapter.Runtime.telemetry.ObserveLoserCancellation(key, event.LoserCancelOutcome)
	}
	if event.LoserLifecycle < OpenAILoserLifecycleEventCardinality {
		_ = adapter.Runtime.telemetry.ObserveLoserLifecycle(key, event.LoserLifecycle)
	}
	if event.CancelReason != OpenAICancelReasonNone {
		_ = adapter.Runtime.telemetry.ObserveCancellation(key, event.CancelReason)
	}
	if event.BillingOutcome != OpenAIBillingOutcomeUnknown {
		_ = adapter.Runtime.telemetry.ObserveBilling(key, event.BillingOutcome)
	}
	if event.CapacityOutcome != OpenAICapacityOutcomeUnknown {
		_ = adapter.Runtime.telemetry.ObserveCapacity(key, event.CapacityOutcome)
	}
}

// OpenAIHedgeRuntimeExposurePort records exact conservative loser exposure and
// never invokes user billing.
type OpenAIHedgeRuntimeExposurePort struct {
	Runtime *AdaptiveLatencyRuntime
	Cohort  CohortKey
}

func (port *OpenAIHedgeRuntimeExposurePort) FinalizeOpenAIHedgeLoserBilling(
	ctx context.Context,
	billing OpenAIHedgeLoserBilling,
) error {
	if port == nil || port.Runtime == nil {
		return nil
	}
	return port.Runtime.RecordHedgeLoserExposure(ctx, OpenAILatencyTelemetryKey{
		AccountID:   billing.AccountID,
		Cohort:      port.Cohort,
		AttemptRole: billing.Role,
	}, billing)
}

var _ OpenAIHedgeTelemetryPort = (*OpenAIHedgeRuntimeTelemetryAdapter)(nil)
var _ OpenAIHedgeBillingPort = (*OpenAIHedgeRuntimeExposurePort)(nil)
