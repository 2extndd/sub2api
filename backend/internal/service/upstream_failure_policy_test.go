package service

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassifyUpstreamTransportFailure(t *testing.T) {
	unknownAuthority := &x509.UnknownAuthorityError{}
	cases := []struct {
		name       string
		err        error
		class      GatewayFailureClass
		scope      GatewayFailureScope
		retry      GatewayRetryDisposition
		persistent bool
		network    string
	}{
		{name: "client canceled", err: context.Canceled, class: GatewayFailureClientCanceled, scope: GatewayFailureScopeRequest, retry: GatewayRetryNone, network: GatewayNetworkClientCanceled},
		{name: "deadline", err: context.DeadlineExceeded, class: GatewayFailureTimeout, scope: GatewayFailureScopeProvider, retry: GatewayRetrySameThenNext, network: GatewayNetworkTimeout},
		{name: "dns", err: &net.DNSError{Err: "no such host", Name: "upstream.invalid", IsNotFound: true}, class: GatewayFailureDNS, scope: GatewayFailureScopeAccount, retry: GatewayRetryNextAccount, persistent: true, network: GatewayNetworkDNS},
		{name: "tls", err: unknownAuthority, class: GatewayFailureTLS, scope: GatewayFailureScopeAccount, retry: GatewayRetryNextAccount, persistent: true, network: GatewayNetworkTLS},
		{name: "connection refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, class: GatewayFailureConnectionRefused, scope: GatewayFailureScopeAccount, retry: GatewayRetryNextAccount, persistent: true, network: GatewayNetworkConnectionRefused},
		{name: "connection reset", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, class: GatewayFailureConnectionReset, scope: GatewayFailureScopeProvider, retry: GatewayRetrySameThenNext, network: GatewayNetworkConnectionReset},
		{name: "unexpected eof", err: io.ErrUnexpectedEOF, class: GatewayFailureUnexpectedEOF, scope: GatewayFailureScopeProvider, retry: GatewayRetrySameThenNext, network: GatewayNetworkUnexpectedEOF},
		{name: "proxy credentials", err: errors.New("proxyconnect: username/password authentication failed"), class: GatewayFailureProxyAuth, scope: GatewayFailureScopeAccount, retry: GatewayRetryNextAccount, persistent: true, network: GatewayNetworkProxyAuth},
		{name: "unknown transport", err: errors.New("upstream disappeared without a response"), class: GatewayFailureUnknownTransport, scope: GatewayFailureScopeProvider, retry: GatewayRetrySameThenNext, network: GatewayNetworkUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy := ClassifyUpstreamTransportFailure(tc.err)
			require.Equal(t, tc.class, policy.Class)
			require.Equal(t, tc.scope, policy.Scope)
			require.Equal(t, tc.retry, policy.Retry)
			require.Equal(t, tc.persistent, policy.Persistent)
			require.Equal(t, tc.network, policy.NetworkType)
			require.False(t, policy.StatusKnown)
		})
	}
}

func TestClassifyUpstreamHTTPFailure(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		body     string
		class    GatewayFailureClass
		scope    GatewayFailureScope
		retry    GatewayRetryDisposition
		provider string
		code     string
	}{
		{name: "invalid request", status: http.StatusBadRequest, body: `{"error":{"type":"invalid_request_error","message":"bad input"}}`, class: GatewayFailureInvalidRequest, scope: GatewayFailureScopeRequest, retry: GatewayRetryNone, provider: "invalid_request_error"},
		{name: "model unavailable", status: http.StatusNotFound, body: `{"error":{"type":"model_not_found"}}`, class: GatewayFailureModelUnavailable, scope: GatewayFailureScopeRequest, retry: GatewayRetryNone, provider: "model_not_found"},
		{name: "account auth", status: http.StatusForbidden, body: `{"error":{"type":"invalid_api_key","code":"invalid_api_key"}}`, class: GatewayFailureAccountAuth, scope: GatewayFailureScopeAccount, retry: GatewayRetryNextAccount, provider: "invalid_api_key", code: "invalid_api_key"},
		{name: "rate limit", status: http.StatusTooManyRequests, body: `{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}`, class: GatewayFailureRateLimited, scope: GatewayFailureScopeAccount, retry: GatewayRetryNextAccount, provider: "rate_limit_error", code: "rate_limit_exceeded"},
		{name: "provider server", status: http.StatusBadGateway, body: `{"error":{"type":"upstream_error"}}`, class: GatewayFailureServer, scope: GatewayFailureScopeProvider, retry: GatewayRetrySameThenNext, provider: "upstream_error"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy := ClassifyUpstreamHTTPFailure(tc.status, []byte(tc.body), http.Header{})
			require.Equal(t, tc.class, policy.Class)
			require.Equal(t, tc.scope, policy.Scope)
			require.Equal(t, tc.retry, policy.Retry)
			require.True(t, policy.StatusKnown)
			require.Equal(t, tc.provider, policy.ProviderType)
			require.Equal(t, tc.code, policy.ProviderCode)
		})
	}
}

func TestClassifyUpstreamStreamFailureRespectsSemanticCommit(t *testing.T) {
	body := []byte(`{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`)
	preCommit := ClassifyUpstreamStreamFailure(body, false)
	require.Equal(t, GatewayFailureStreamError, preCommit.Class)
	require.Equal(t, GatewayRetrySameThenNext, preCommit.Retry)
	require.False(t, preCommit.ResponseCommitted)

	postCommit := ClassifyUpstreamStreamFailure(body, true)
	require.Equal(t, GatewayFailureStreamError, postCommit.Class)
	require.Equal(t, GatewayRetryNone, postCommit.Retry)
	require.True(t, postCommit.ResponseCommitted)
	postCommitErr := postCommit.NewFailoverError(body)
	require.True(t, postCommitErr.ResponseCommitted)
	require.False(t, postCommitErr.SafeToFailoverAfterWrite)
	require.False(t, postCommitErr.ShouldRetryNextAccount())
}

func TestClassifyProviderTextWithoutNumericStatus(t *testing.T) {
	cases := []struct {
		name     string
		message  string
		class    GatewayFailureClass
		category string
		retry    GatewayRetryDisposition
	}{
		{name: "overloaded envelope text", message: "provider overloaded_error, try again later", class: GatewayFailureProviderTransient, category: gatewayTextCategoryTransient, retry: GatewayRetrySameThenNext},
		{name: "plain auth text", message: "upstream authentication failed for api key", class: GatewayFailureAccountAuth, category: gatewayTextCategoryAuth, retry: GatewayRetryNextAccount},
		{name: "plain timeout text", message: "upstream request timed out after 30 seconds", class: GatewayFailureTimeout, category: gatewayTextCategoryTimeout, retry: GatewayRetrySameThenNext},
		{name: "plain model text", message: "model is not supported by this provider", class: GatewayFailureModelUnavailable, category: gatewayTextCategoryModel, retry: GatewayRetryNone},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy := ClassifyUpstreamTransportFailure(errors.New(tc.message))
			require.Equal(t, tc.class, policy.Class)
			require.Equal(t, tc.category, policy.TextCategory)
			require.Equal(t, tc.retry, policy.Retry)
			require.Len(t, policy.TextSignature, 16)
		})
	}
}

func TestGatewayFailurePolicyApplyPreservesClientStatusAndDiagnostics(t *testing.T) {
	policy := ClassifyUpstreamTransportFailure(context.DeadlineExceeded)
	err := policy.NewFailoverError(nil)
	require.Equal(t, 502, err.StatusCode)
	require.Equal(t, 502, err.ClientStatusCode)
	require.Equal(t, string(GatewayFailureTimeout), string(err.FailureClass))
	require.Equal(t, string(GatewayRetrySameThenNext), string(err.RetryDisposition))
	require.Equal(t, string(GatewayNetworkTimeout), err.NetworkErrorType)
	require.True(t, err.RetryableOnSameAccount)
	require.True(t, err.ShouldRetryNextAccount())
	require.True(t, err.ShouldReportAccountScheduleFailure())
}
