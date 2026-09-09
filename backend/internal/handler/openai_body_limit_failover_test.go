package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIBodyLimitFailoverExhausted_ReturnsRedactedJSON413(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(nil))

	(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, bodyLimitFailoverTestError(), false)

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	var envelope map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope))
	errBody, ok := envelope["error"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "invalid_request_error", errBody["type"])
	require.Equal(t, "Request payload is too large", errBody["message"])
	require.NotContains(t, rec.Body.String(), "must-not-leak")
}

func TestOpenAIBodyLimitFailoverExhausted_ReturnsRedactedResponsesSSE(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(nil))

	(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, bodyLimitFailoverTestError(), true)

	body := rec.Body.String()
	require.True(t, strings.HasPrefix(body, "event: response.failed\n"))
	require.Contains(t, body, `"code":"invalid_request"`)
	require.Contains(t, body, `"message":"Request payload is too large"`)
	require.NotContains(t, body, "must-not-leak")
}

func bodyLimitFailoverTestError() *service.UpstreamFailoverError {
	return &service.UpstreamFailoverError{
		StatusCode:        http.StatusRequestEntityTooLarge,
		ResponseBody:      []byte(`{"error":{"message":"proxy limit secret=must-not-leak"}}`),
		Scope:             service.GatewayFailureScopeAccount,
		Reason:            service.GatewayFailureReason("openai_request_body_too_large"),
		NextAccountAction: service.NextAccountRetry,
		ClientStatusCode:  http.StatusRequestEntityTooLarge,
		ClientMessage:     "Request payload is too large",
	}
}

func TestOpenAIBodyLimitFailureDomain_BlocksSiblingRelayAccountsOnly(t *testing.T) {
	proxyID := int64(7)
	otherProxyID := int64(8)
	first := &service.Account{
		Platform: service.PlatformOpenAI,
		Type:     service.AccountTypeAPIKey,
		ProxyID:  &proxyID,
		Credentials: map[string]any{
			"base_url": "https://relay.example.test/v1/",
		},
	}
	sibling := &service.Account{
		Platform: service.PlatformOpenAI,
		Type:     service.AccountTypeAPIKey,
		ProxyID:  &proxyID,
		Credentials: map[string]any{
			"base_url": "https://relay.example.test/v1",
		},
	}
	differentRelay := &service.Account{
		Platform: service.PlatformOpenAI,
		Type:     service.AccountTypeAPIKey,
		ProxyID:  &proxyID,
		Credentials: map[string]any{
			"base_url": "https://fallback.example.test/v1",
		},
	}
	differentProxy := &service.Account{
		Platform: service.PlatformOpenAI,
		Type:     service.AccountTypeAPIKey,
		ProxyID:  &otherProxyID,
		Credentials: map[string]any{
			"base_url": "https://relay.example.test/v1",
		},
	}

	domains := make(map[string]struct{})
	recordOpenAIBodyLimitFailureDomain(domains, first, bodyLimitFailoverTestError())

	require.True(t, openAIBodyLimitFailureDomainBlocked(domains, sibling))
	require.False(t, openAIBodyLimitFailureDomainBlocked(domains, differentRelay))
	require.False(t, openAIBodyLimitFailureDomainBlocked(domains, differentProxy))
}

func TestOpenAIBodyLimitFailureDomain_IgnoresOtherErrorsAndOAuth(t *testing.T) {
	apiKeyAccount := &service.Account{
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeAPIKey,
		Credentials: map[string]any{"base_url": "https://relay.example.test/v1"},
	}
	oauthAccount := &service.Account{
		Platform: service.PlatformOpenAI,
		Type:     service.AccountTypeOAuth,
	}
	domains := make(map[string]struct{})

	recordOpenAIBodyLimitFailureDomain(domains, apiKeyAccount, &service.UpstreamFailoverError{StatusCode: http.StatusBadGateway})
	recordOpenAIBodyLimitFailureDomain(domains, oauthAccount, bodyLimitFailoverTestError())

	require.Empty(t, domains)
	require.False(t, openAIBodyLimitFailureDomainBlocked(domains, apiKeyAccount))
}
