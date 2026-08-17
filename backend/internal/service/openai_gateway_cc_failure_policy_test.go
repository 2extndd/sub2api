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
	policy := ClassifyUpstreamHTTPFailure(resp.StatusCode, body, resp.Header)
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
