package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestFailoverOpenAIUpstreamHTTPErrorAppliesGatewayPolicyBeforeOpsAppend(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	body := []byte(`{"error":{"message":"upstream unavailable"}}`)
	resp := &http.Response{StatusCode: http.StatusBadGateway, Header: make(http.Header)}

	failoverErr := svc.failoverOpenAIUpstreamHTTPError(
		context.Background(),
		c,
		account,
		resp,
		body,
		"upstream unavailable",
		"gpt-5.6",
	)

	require.NotNil(t, failoverErr)
	assertSingleOpsEventUsesPolicy(t, c, ClassifyUpstreamHTTPFailure(resp.StatusCode, body, resp.Header))
}

func TestHandleFailoverErrorResponsePassthroughAppliesGatewayPolicyBeforeOpsAppend(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 8, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	body := []byte(`{"error":{"message":"upstream unavailable"}}`)
	resp := &http.Response{StatusCode: http.StatusBadGateway, Header: make(http.Header)}

	failoverErr := svc.handleFailoverErrorResponsePassthrough(
		context.Background(),
		resp,
		c,
		account,
		[]byte(`{"model":"gpt-5.6","input":"ok"}`),
		body,
	)

	require.NotNil(t, failoverErr)
	assertSingleOpsEventUsesPolicy(t, c, ClassifyUpstreamHTTPFailure(resp.StatusCode, body, resp.Header))
}

func TestAppendResponsesUpstreamFailoverAppliesGatewayPolicyBeforeOpsAppend(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	account := &Account{ID: 9, Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
	body := []byte(`{"error":{"message":"upstream unavailable"}}`)
	resp := &http.Response{StatusCode: http.StatusBadGateway, Header: make(http.Header)}

	policy := appendResponsesUpstreamFailover(c, account, resp, body, "upstream unavailable", "", false)

	assertSingleOpsEventUsesPolicy(t, c, policy)
}

func assertSingleOpsEventUsesPolicy(t *testing.T, c *gin.Context, policy GatewayFailurePolicy) {
	t.Helper()
	value, ok := c.Get(OpsUpstreamErrorsKey)
	require.True(t, ok)
	events, ok := value.([]*OpsUpstreamErrorEvent)
	require.True(t, ok)
	require.Len(t, events, 1)
	require.Equal(t, string(policy.Class), events[0].FailureClass)
	require.Equal(t, string(policy.Retry), events[0].RetryDisposition)
	require.Equal(t, policy.StatusKnown, events[0].StatusKnown)
	require.NotEqual(t, string(GatewayFailureUnknown), events[0].FailureClass)
}
