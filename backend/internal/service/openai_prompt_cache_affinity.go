package service

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

var (
	openAIPromptCacheStablePrefixTotal     atomic.Int64
	openAIPromptCacheAnchoredFallbackTotal atomic.Int64
)

func openAIPromptCacheAffinityStats() (stablePrefixTotal, anchoredFallbackTotal int64) {
	return openAIPromptCacheStablePrefixTotal.Load(), openAIPromptCacheAnchoredFallbackTotal.Load()
}

// deriveOpenAIPromptCacheRoutingSeed groups reusable prompt prefixes within one
// downstream tenant and requested model. It deliberately leaves account choice
// to the existing scheduler: sticky escape, timeout exclusions, priorities, and
// failover remain unchanged.
func deriveOpenAIPromptCacheRoutingSeed(c *gin.Context, body []byte) string {
	apiKeyID := getAPIKeyIDFromContext(c)
	model := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "model").String()))
	if apiKeyID <= 0 || model == "" {
		return deriveOpenAIAnchoredPromptCacheFallback(body)
	}

	stablePrefix := deriveOpenAIStablePrefixSessionSeed(body)
	if stablePrefix != "" {
		openAIPromptCacheStablePrefixTotal.Add(1)
		return fmt.Sprintf("openai-prompt-cache:v1:%d:%s:%s", apiKeyID, model, stablePrefix)
	}

	return deriveOpenAIAnchoredPromptCacheFallback(body)
}

func deriveOpenAIAnchoredPromptCacheFallback(body []byte) string {
	seed := deriveOpenAIContentSessionSeed(body)
	if seed != "" {
		openAIPromptCacheAnchoredFallbackTotal.Add(1)
	}
	return seed
}
