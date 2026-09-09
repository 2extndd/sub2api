package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAIHedgeCanarySelected_BoundsAndDeterminism(t *testing.T) {
	cohort := NewCohortKey(
		OpenAIEndpointResponses,
		OpenAIModelText,
		true,
		OpenAIInputSizeSmall,
		OpenAIReasoningLow,
		OpenAIToolUseNone,
	)

	require.False(t, OpenAIHedgeCanarySelected(0, cohort, 10_000), "missing identity must fail closed")
	require.False(t, OpenAIHedgeCanarySelected(42, cohort, -1), "negative basis points must fail closed")
	require.False(t, OpenAIHedgeCanarySelected(42, cohort, 0), "zero basis points is decision-shadow only")
	require.False(t, OpenAIHedgeCanarySelected(42, cohort, 10_001), "out-of-range basis points must fail closed")
	require.True(t, OpenAIHedgeCanarySelected(42, cohort, 10_000), "full basis points must select a valid identity")

	first := OpenAIHedgeCanarySelected(42, cohort, 2_500)
	for range 100 {
		require.Equal(t, first, OpenAIHedgeCanarySelected(42, cohort, 2_500))
	}
}

func TestOpenAIHedgeCanarySelected_IsMonotonicAndApproximatelyBounded(t *testing.T) {
	cohort := NewCohortKey(
		OpenAIEndpointResponses,
		OpenAIModelText,
		true,
		OpenAIInputSizeSmall,
		OpenAIReasoningLow,
		OpenAIToolUseNone,
	)

	selectedAtTenPercent := 0
	for apiKeyID := int64(1); apiKeyID <= 10_000; apiKeyID++ {
		atTenPercent := OpenAIHedgeCanarySelected(apiKeyID, cohort, 1_000)
		atTwentyPercent := OpenAIHedgeCanarySelected(apiKeyID, cohort, 2_000)
		if atTenPercent {
			selectedAtTenPercent++
			require.True(t, atTwentyPercent, "raising the threshold must not evict an existing canary member")
		}
	}

	require.InDelta(t, 1_000, selectedAtTenPercent, 125, "stable hashing should keep a 10%% canary approximately bounded")
}
