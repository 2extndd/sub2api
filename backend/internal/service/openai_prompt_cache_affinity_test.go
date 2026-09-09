//go:build unit

package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newOpenAIPromptCacheAffinityContext(t *testing.T, apiKeyID int64) *gin.Context {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Set("api_key", &APIKey{ID: apiKeyID, Group: &Group{Platform: PlatformOpenAI}})
	return c
}

func TestOpenAIGatewayService_GenerateSessionHash_GroupsIndependentPromptsByReusablePrefix(t *testing.T) {
	svc := &OpenAIGatewayService{}
	first := []byte(`{
		"model":"gpt-5.6",
		"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],
		"messages":[
			{"role":"system","content":"Use the same long policy and tool instructions."},
			{"role":"user","content":"First independent task"}
		]
	}`)
	second := []byte(`{
		"model":"gpt-5.6",
		"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],
		"messages":[
			{"role":"system","content":"Use the same long policy and tool instructions."},
			{"role":"user","content":"Second independent task"}
		]
	}`)

	firstHash := svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), first)
	secondHash := svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), second)
	require.NotEmpty(t, firstHash)
	require.Equal(t, firstHash, secondHash, "same tenant/model/prefix should route to the same upstream cache")
}

func TestOpenAIGatewayService_GenerateSessionHash_ReusablePrefixIsolatesTenantAndModel(t *testing.T) {
	svc := &OpenAIGatewayService{}
	body := []byte(`{
		"model":"gpt-5.6",
		"messages":[
			{"role":"system","content":"Stable policy"},
			{"role":"user","content":"Task"}
		]
	}`)
	differentModel := []byte(`{
		"model":"gpt-5.6-codex",
		"messages":[
			{"role":"system","content":"Stable policy"},
			{"role":"user","content":"Task"}
		]
	}`)

	baseHash := svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), body)
	require.NotEqual(t, baseHash, svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 42), body))
	require.NotEqual(t, baseHash, svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), differentModel))
}

func TestOpenAIGatewayService_GenerateSessionHash_UserOnlyPromptsRemainSeparated(t *testing.T) {
	svc := &OpenAIGatewayService{}
	first := []byte(`{"model":"gpt-5.6","messages":[{"role":"user","content":"First unrelated prompt"}]}`)
	second := []byte(`{"model":"gpt-5.6","messages":[{"role":"user","content":"Second unrelated prompt"}]}`)

	firstHash := svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), first)
	secondHash := svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), second)
	require.NotEmpty(t, firstHash)
	require.NotEqual(t, firstHash, secondHash, "model-only requests must not collapse into one hot sticky key")
}

func TestOpenAIGatewayService_GenerateSessionHash_AnthropicSystemPrefixIsReusable(t *testing.T) {
	svc := &OpenAIGatewayService{}
	first := []byte(`{
		"model":"claude-sonnet-4-6",
		"system":"Stable Claude policy",
		"messages":[{"role":"user","content":"First task"}]
	}`)
	second := []byte(`{
		"model":"claude-sonnet-4-6",
		"system":"Stable Claude policy",
		"messages":[{"role":"user","content":"Second task"}]
	}`)

	firstHash := svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), first)
	secondHash := svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), second)
	require.Equal(t, firstHash, secondHash)
}

func TestOpenAIPromptCacheAffinity_MetricsCountRoutingSource(t *testing.T) {
	svc := &OpenAIGatewayService{}
	beforeStable, beforeAnchored := openAIPromptCacheAffinityStats()

	stable := []byte(`{"model":"gpt-5.6","messages":[{"role":"system","content":"Stable policy"},{"role":"user","content":"Task"}]}`)
	anchored := []byte(`{"model":"gpt-5.6","messages":[{"role":"user","content":"Standalone task"}]}`)
	require.NotEmpty(t, svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), stable))
	require.NotEmpty(t, svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), anchored))

	afterStable, afterAnchored := openAIPromptCacheAffinityStats()
	require.Equal(t, beforeStable+1, afterStable)
	require.Equal(t, beforeAnchored+1, afterAnchored)

	snapshot := SnapshotOpenAICompatibilityFallbackMetrics()
	require.Equal(t, afterStable, snapshot.PromptCacheStablePrefixTotal)
	require.Equal(t, afterAnchored, snapshot.PromptCacheAnchoredFallbackTotal)
}

func TestOpenAIPromptCacheAffinity_TimeoutExclusionSwitchesWithoutChangingPriorities(t *testing.T) {
	groupID := int64(10201)
	accounts := []Account{
		{ID: 22101, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 1, GroupIDs: []int64{groupID}},
		{ID: 22102, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 100, GroupIDs: []int64{groupID}},
	}
	cfg := &config.Config{}
	cfg.Gateway.OpenAIScheduler.StickyEscapeEnabled = true
	cfg.Gateway.OpenAIScheduler.StickyEscapeTTFTMs = 15000
	cfg.Gateway.OpenAIScheduler.StickyEscapeErrorRate = 0.5
	cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{}}
	svc := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
		cache:              cache,
		cfg:                cfg,
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true"),
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquireResults: map[int64]bool{22102: true}}),
		openaiAccountStats: newOpenAIAccountRuntimeStats(),
	}

	first := []byte(`{"model":"gpt-5.6","messages":[{"role":"system","content":"Stable policy"},{"role":"user","content":"First task"}]}`)
	second := []byte(`{"model":"gpt-5.6","messages":[{"role":"system","content":"Stable policy"},{"role":"user","content":"Second task"}]}`)
	firstHash := svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), first)
	secondHash := svc.GenerateSessionHash(newOpenAIPromptCacheAffinityContext(t, 41), second)
	require.Equal(t, firstHash, secondHash)
	cache.sessionBindings["openai:"+firstHash] = 22101

	slowTTFT := 20000
	for i := 0; i < 3; i++ {
		svc.openaiAccountStats.report(22101, true, &slowTTFT)
	}

	selection, decision, err := svc.SelectAccountWithScheduler(
		context.Background(),
		&groupID,
		"",
		secondHash,
		"gpt-5.6",
		map[int64]struct{}{22101: {}},
		OpenAIUpstreamTransportAny,
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, int64(22102), selection.Account.ID, "timeout-excluded sticky account must not block the existing scheduler")
	require.Equal(t, openAIAccountScheduleLayerLoadBalance, decision.Layer)
	require.False(t, decision.StickySessionHit)
	require.Equal(t, int64(22102), cache.sessionBindings["openai:"+firstHash], "timeout failover must preserve the existing winner-rebinding behavior")
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}
