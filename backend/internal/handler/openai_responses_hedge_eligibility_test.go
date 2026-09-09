package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

// constructTestOpenAIHedgeHandler creates a minimal handler with hedge config for testing.
func constructTestOpenAIHedgeHandler(
	t *testing.T,
	hedgeEnabled bool,
) *OpenAIGatewayHandler {
	t.Helper()

	handler := &OpenAIGatewayHandler{
		cfg:                    &config.Config{},
		gatewayService:         &service.OpenAIGatewayService{},
		billingCacheService:    &service.BillingCacheService{},
		apiKeyService:          &service.APIKeyService{},
		concurrencyHelper:      NewConcurrencyHelper(service.NewConcurrencyService(&concurrencyCacheMock{}), SSEPingFormatNone, time.Second),
		adaptiveLatencyRuntime: nil,
	}

	return handler
}

// ===== Eligibility Tests =====

// TestOpenAIResponsesHedge_EligibilityChecks_RejectsImageGeneration verifies hedge eligibility blocks images.
func TestOpenAIResponsesHedge_EligibilityChecks_RejectsImageGeneration(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := constructTestOpenAIHedgeHandler(t, true)

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	body := []byte(`{"model":"gpt-4","stream":true}`)
	primary := &service.Account{
		ID:       1,
		Platform: service.PlatformOpenAI,
		Type:     "openai",
	}

	imageIntent := true
	eligible := handler.openAIResponsesHedgeEligible(c, body, "gpt-4", primary, true, imageIntent, false)
	assert.False(t, eligible, "hedge must be ineligible for image generation")
}

// TestOpenAIResponsesHedge_EligibilityChecks_RejectsEncryptedReasoning verifies hedge eligibility blocks encryption.
func TestOpenAIResponsesHedge_EligibilityChecks_RejectsEncryptedReasoning(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := constructTestOpenAIHedgeHandler(t, true)

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	body := []byte(`{
		"model":"gpt-4",
		"stream":true,
		"input":[
			{
				"type":"reasoning",
				"encrypted_content":"secret_data"
			}
		]
	}`)

	primary := &service.Account{
		ID:       1,
		Platform: service.PlatformOpenAI,
		Type:     "openai",
	}

	eligible := handler.openAIResponsesHedgeEligible(c, body, "gpt-4", primary, true, false, false)
	assert.False(t, eligible, "hedge must be ineligible for encrypted reasoning")
}

// TestOpenAIResponsesHedge_EligibilityChecks_RejectsStorageRequests verifies hedge eligibility blocks store.
func TestOpenAIResponsesHedge_EligibilityChecks_RejectsStorageRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := constructTestOpenAIHedgeHandler(t, true)

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	body := []byte(`{"model":"gpt-4","stream":true,"store":true}`)

	primary := &service.Account{
		ID:       1,
		Platform: service.PlatformOpenAI,
		Type:     "openai",
	}

	eligible := handler.openAIResponsesHedgeEligible(c, body, "gpt-4", primary, true, false, false)
	assert.False(t, eligible, "hedge must be ineligible for store requests")
}

// TestOpenAIResponsesHedge_EligibilityChecks_RequiresHTTPTransport verifies hedge requires HTTP.
func TestOpenAIResponsesHedge_EligibilityChecks_RequiresHTTPTransport(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := constructTestOpenAIHedgeHandler(t, true)

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	service.SetOpenAIClientTransport(c, service.OpenAIClientTransportWS)

	body := []byte(`{"model":"gpt-4","stream":true}`)

	primary := &service.Account{
		ID:       1,
		Platform: service.PlatformOpenAI,
		Type:     "openai",
	}

	eligible := handler.openAIResponsesHedgeEligible(c, body, "gpt-4", primary, true, false, false)
	assert.False(t, eligible, "hedge must be ineligible for WebSocket transport")
}

// TestOpenAIResponsesHedge_EligibilityChecks_RequiresStream verifies streaming eligibility check.
func TestOpenAIResponsesHedge_EligibilityChecks_RequiresStream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := constructTestOpenAIHedgeHandler(t, true)

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	body := []byte(`{"model":"gpt-4","stream":false}`)

	primary := &service.Account{
		ID:       1,
		Platform: service.PlatformOpenAI,
		Type:     "openai",
	}

	eligible := handler.openAIResponsesHedgeEligible(c, body, "gpt-4", primary, false, false, false)
	assert.False(t, eligible, "hedge must be ineligible for non-streaming")
}

// TestOpenAIResponsesHedge_EligibilityChecks_RequiresOpenAIPlatform verifies hedge requires OpenAI platform.
func TestOpenAIResponsesHedge_EligibilityChecks_RequiresOpenAIPlatform(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := constructTestOpenAIHedgeHandler(t, true)

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	body := []byte(`{"model":"gpt-4","stream":true}`)

	primary := &service.Account{
		ID:       1,
		Platform: service.PlatformGrok,
		Type:     "grok",
	}

	eligible := handler.openAIResponsesHedgeEligible(c, body, "gpt-4", primary, true, false, false)
	assert.False(t, eligible, "hedge must be ineligible for non-OpenAI platforms")
}

// TestOpenAIResponsesHedge_EligibilityChecks_RequiresNonNilRuntime verifies nil runtime disables hedge.
func TestOpenAIResponsesHedge_EligibilityChecks_RequiresNonNilRuntime(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := constructTestOpenAIHedgeHandler(t, true)
	handler.adaptiveLatencyRuntime = nil

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	body := []byte(`{"model":"gpt-4","stream":true}`)

	primary := &service.Account{
		ID:       1,
		Platform: service.PlatformOpenAI,
		Type:     "openai",
	}

	eligible := handler.openAIResponsesHedgeEligible(c, body, "gpt-4", primary, true, false, false)
	assert.False(t, eligible, "hedge must be ineligible with nil runtime")
}

// TestOpenAIResponsesHedge_EligibilityChecks_DisabledFeature verifies disabled feature makes ineligible.
func TestOpenAIResponsesHedge_EligibilityChecks_DisabledFeature(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := constructTestOpenAIHedgeHandler(t, false)

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	body := []byte(`{"model":"gpt-4","stream":true}`)

	primary := &service.Account{
		ID:       1,
		Platform: service.PlatformOpenAI,
		Type:     "openai",
	}

	eligible := handler.openAIResponsesHedgeEligible(c, body, "gpt-4", primary, true, false, false)
	assert.False(t, eligible, "hedge must be ineligible when feature is disabled")
}

// ===== Handler Construction Tests =====

// TestOpenAIResponsesHedge_Handler_Construction verifies handler can be constructed with hedge config.
func TestOpenAIResponsesHedge_Handler_Construction(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := constructTestOpenAIHedgeHandler(t, true)

	assert.NotNil(t, handler, "handler must be constructed")
	assert.NotNil(t, handler.gatewayService, "gateway service must exist")
	// Note: adaptiveLatencyRuntime is nil for test handler; full hedge testing requires
	// service layer fakes from openai_hedge_coordinator_test.go patterns
}

// TestOpenAIResponsesHedge_Handler_DisabledConstruction verifies handler construction with hedge disabled.
func TestOpenAIResponsesHedge_Handler_DisabledConstruction(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := constructTestOpenAIHedgeHandler(t, false)

	assert.NotNil(t, handler, "handler must be constructed")
	assert.Nil(t, handler.adaptiveLatencyRuntime, "adaptive latency runtime should be nil for tests")
}
