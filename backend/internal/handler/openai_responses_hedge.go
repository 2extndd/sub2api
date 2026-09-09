package handler

import (
	"context"
	"errors"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

type openAIResponsesHedgeResult struct {
	result  *service.OpenAIForwardResult
	err     error
	account *service.Account
	context *gin.Context
}

func (h *OpenAIGatewayHandler) openAIResponsesHedgeEligible(
	c *gin.Context,
	body []byte,
	model string,
	primary *service.Account,
	reqStream, imageIntent, requireCompact bool,
) bool {
	if h == nil || h.adaptiveLatencyRuntime == nil || primary == nil || !reqStream || imageIntent || requireCompact {
		return false
	}
	_, enabled := h.adaptiveLatencyRuntime.HedgeConfig()
	if !enabled || service.GetOpenAIClientTransport(c) == service.OpenAIClientTransportWS {
		return false
	}
	if gjson.GetBytes(body, "store").Bool() || strings.TrimSpace(gjson.GetBytes(body, "previous_response_id").String()) != "" {
		return false
	}
	if gjson.GetBytes(body, "input.#(type==\"reasoning\").encrypted_content").Exists() ||
		strings.Contains(string(body), "\"encrypted_content\"") {
		return false
	}
	return primary.Platform == service.PlatformOpenAI &&
		primary.SupportsOpenAIEndpointCapability(service.OpenAIEndpointCapabilityChatCompletions) &&
		strings.TrimSpace(model) != ""
}

func (h *OpenAIGatewayHandler) tryForwardResponsesWithHedge(
	ctx context.Context,
	c *gin.Context,
	apiKey *service.APIKey,
	model, platform string,
	body []byte,
	primary *service.Account,
	primaryRelease func(),
	cohort service.CohortKey,
	reqStream, imageIntent, requireCompact bool,
) (openAIResponsesHedgeResult, bool) {
	if !h.openAIResponsesHedgeEligible(c, body, model, primary, reqStream, imageIntent, requireCompact) {
		return openAIResponsesHedgeResult{}, false
	}
	hedgeConfig, enabled := h.adaptiveLatencyRuntime.HedgeConfig()
	if !enabled {
		return openAIResponsesHedgeResult{}, false
	}
	if apiKey == nil {
		h.adaptiveLatencyRuntime.RecordHedgeCanaryDecision(primary.ID, cohort, false)
		return openAIResponsesHedgeResult{}, false
	}
	canarySelected := service.OpenAIHedgeCanarySelected(
		apiKey.ID,
		cohort,
		hedgeConfig.CanaryBasisPoints,
	)
	h.adaptiveLatencyRuntime.RecordHedgeCanaryDecision(primary.ID, cohort, canarySelected)
	if !canarySelected {
		return openAIResponsesHedgeResult{}, false
	}
	policy, err := service.NewOpenAIHedgeGatewayPolicy(
		h.gatewayService,
		h.adaptiveLatencyRuntime,
		hedgeConfig,
		apiKey.GroupID,
		model,
		platform,
		body,
		primary,
	)
	if err != nil || policy.PrimaryHedgeAccount().PotentialCostMicros == 0 ||
		policy.PrimaryHedgeAccount().PotentialCostMicros > hedgeConfig.MaxDuplicateCostMicros {
		return openAIResponsesHedgeResult{}, false
	}
	if err := policy.TransferPrimaryLease(primaryRelease); err != nil {
		return openAIResponsesHedgeResult{}, false
	}
	defer policy.ReleasePendingLeases()
	attempts, err := service.NewOpenAIHedgeGatewayAttemptPort(service.OpenAIHedgeGatewayAttemptInput{
		Gateway:        h.gatewayService,
		Request:        c.Request,
		GinKeys:        service.SnapshotOpenAIHedgeGinKeys(c),
		PrimaryAccount: primary,
		PrimaryBody:    body,
		HedgeInput:     policy.Candidate,
	})
	if err != nil {
		return openAIResponsesHedgeResult{err: err}, true
	}
	clientSink := &service.OpenAIHedgeClientSink{Writer: c.Writer, Attempts: attempts}
	coordinator, err := h.adaptiveLatencyRuntime.NewRequestHedgeCoordinator(service.OpenAIHedgeCoordinatorDependencies{
		Candidates: policy,
		Admission:  policy,
		Attempts:   attempts,
		Slots:      policy,
		Sink:       clientSink,
		Billing:    &service.OpenAIHedgeRuntimeExposurePort{Runtime: h.adaptiveLatencyRuntime, Cohort: cohort},
		Telemetry:  &service.OpenAIHedgeRuntimeTelemetryAdapter{Runtime: h.adaptiveLatencyRuntime, Cohort: cohort},
	})
	if err != nil {
		return openAIResponsesHedgeResult{err: err}, true
	}
	costClass := service.OpenAIHedgeCostStandard
	effort := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "reasoning.effort").String()))
	if effort == "high" {
		costClass = service.OpenAIHedgeCostHigh
	}
	if effort == "xhigh" || effort == "max" {
		costClass = service.OpenAIHedgeCostVeryHeavy
	}
	coordinateResult, coordinateErr := coordinator.Coordinate(ctx, service.OpenAIHedgeRequest{
		Provider:               primary.Platform,
		Model:                  model,
		ModelFamily:            "gpt",
		Protocol:               "responses-http-sse",
		Stream:                 true,
		Stateless:              true,
		Store:                  false,
		FullInput:              true,
		ClientVisibleCommitted: c.Writer.Written(),
		CostClass:              costClass,
		Primary:                policy.PrimaryHedgeAccount(),
		Payload:                nil,
	})
	winnerAccount := primary
	if coordinateResult.Winner == service.OpenAIAttemptRoleHedge {
		candidate, _ := policy.Candidate()
		winnerAccount = candidate
	}
	winnerRole := coordinateResult.Winner
	winnerResult := attempts.Result(winnerRole)
	winnerContext := attempts.Context(winnerRole)
	if coordinateErr != nil && errors.Is(coordinateErr, context.Canceled) {
		return openAIResponsesHedgeResult{result: winnerResult, err: coordinateErr, account: winnerAccount, context: winnerContext}, true
	}
	return openAIResponsesHedgeResult{result: winnerResult, err: coordinateErr, account: winnerAccount, context: winnerContext}, true
}
