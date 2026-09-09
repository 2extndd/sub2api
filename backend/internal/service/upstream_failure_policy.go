package service

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
)

// GatewayFailureClass is a bounded, protocol-neutral failure category. It is
// intentionally independent of model names and HTTP status codes.
type GatewayFailureClass string

const (
	GatewayFailureClientCanceled     GatewayFailureClass = "client_canceled"
	GatewayFailureTimeout            GatewayFailureClass = "timeout"
	GatewayFailureDNS                GatewayFailureClass = "dns"
	GatewayFailureTLS                GatewayFailureClass = "tls"
	GatewayFailureProxyAuth          GatewayFailureClass = "proxy_auth"
	GatewayFailureConnectionRefused  GatewayFailureClass = "connection_refused"
	GatewayFailureConnectionReset    GatewayFailureClass = "connection_reset"
	GatewayFailureNetworkUnreachable GatewayFailureClass = "network_unreachable"
	GatewayFailureUnexpectedEOF      GatewayFailureClass = "unexpected_eof"
	GatewayFailureEmptyResponse      GatewayFailureClass = "empty_response"
	GatewayFailureAccountAuth        GatewayFailureClass = "account_auth"
	GatewayFailureRateLimited        GatewayFailureClass = "rate_limited"
	GatewayFailureModelUnavailable   GatewayFailureClass = "model_unavailable"
	GatewayFailureInvalidRequest     GatewayFailureClass = "invalid_request"
	GatewayFailureServer             GatewayFailureClass = "server_error"
	GatewayFailureProviderTransient  GatewayFailureClass = "provider_transient"
	GatewayFailureStreamError        GatewayFailureClass = "stream_error"
	GatewayFailureUnknownTransport   GatewayFailureClass = "unknown_transport"
	GatewayFailureUnknown            GatewayFailureClass = "unknown"
)

// GatewayRetryDisposition is the only retry decision a caller needs to apply.
type GatewayRetryDisposition string

const (
	GatewayRetryNone         GatewayRetryDisposition = "none"
	GatewayRetryNextAccount  GatewayRetryDisposition = "next_account"
	GatewayRetrySameThenNext GatewayRetryDisposition = "same_account_then_next"
)

const (
	GatewayNetworkClientCanceled    = "client_canceled"
	GatewayNetworkTimeout           = "timeout"
	GatewayNetworkDNS               = "dns"
	GatewayNetworkTLS               = "tls"
	GatewayNetworkProxyAuth         = "proxy_auth"
	GatewayNetworkConnectionRefused = "connection_refused"
	GatewayNetworkConnectionReset   = "connection_reset"
	GatewayNetworkUnreachable       = "network_unreachable"
	GatewayNetworkUnexpectedEOF     = "unexpected_eof"
	GatewayNetworkUnknown           = "unknown"
)

// GatewayFailurePolicy is a bounded decision plus aggregate-safe diagnostics.
type GatewayFailurePolicy struct {
	Class             GatewayFailureClass
	Scope             GatewayFailureScope
	Retry             GatewayRetryDisposition
	StatusKnown       bool
	Persistent        bool
	ResponseCommitted bool
	NetworkType       string
	ProviderType      string
	ProviderCode      string
	RetryAfterSeconds int
	TextCategory      string
	TextSignature     string
	ClientStatusCode  int
}

func (p GatewayFailurePolicy) CanRetrySameAccount() bool {
	return p.Retry == GatewayRetrySameThenNext && !p.ResponseCommitted
}

func (p GatewayFailurePolicy) CanRetryNextAccount() bool {
	return p.Retry != GatewayRetryNone && !p.ResponseCommitted
}

// NewFailoverError converts the policy into the legacy-compatible error used by
// all gateway handlers. StatusCode remains the client-facing fallback status;
// StatusKnown and the diagnostic fields preserve whether upstream supplied one.
func (p GatewayFailurePolicy) NewFailoverError(body []byte) *UpstreamFailoverError {
	status := p.ClientStatusCode
	if status <= 0 {
		status = http.StatusBadGateway
	}
	err := &UpstreamFailoverError{
		StatusCode:             status,
		ClientStatusCode:       status,
		ResponseBody:           body,
		FailureClass:           p.Class,
		RetryDisposition:       p.Retry,
		StatusKnown:            p.StatusKnown,
		Persistent:             p.Persistent,
		ResponseCommitted:      p.ResponseCommitted,
		NetworkErrorType:       p.NetworkType,
		ProviderErrorType:      p.ProviderType,
		ProviderErrorCode:      p.ProviderCode,
		RetryAfterSeconds:      p.RetryAfterSeconds,
		TextCategory:           p.TextCategory,
		TextSignature:          p.TextSignature,
		Scope:                  p.Scope,
		Reason:                 GatewayFailureReason(p.Class),
		NextAccountAction:      NextAccountStop,
		RetryableOnSameAccount: p.CanRetrySameAccount(),
	}
	if p.CanRetryNextAccount() {
		err.NextAccountAction = NextAccountRetry
	}
	return err
}

const (
	gatewayTextCategoryClientCanceled = "client_canceled"
	gatewayTextCategoryTimeout        = "timeout"
	gatewayTextCategoryDNS            = "dns"
	gatewayTextCategoryTLS            = "tls"
	gatewayTextCategoryConnection     = "connection"
	gatewayTextCategoryAuth           = "account_auth"
	gatewayTextCategoryRateLimit      = "rate_limited"
	gatewayTextCategoryTransient      = "provider_transient"
	gatewayTextCategoryModel          = "model_unavailable"
	gatewayTextCategoryStream         = "stream_error"
	gatewayTextCategoryUnknown        = "unknown_text"
)

func normalizeGatewayFailureText(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	fields := strings.Fields(value)
	for i, field := range fields {
		switch {
		case strings.HasPrefix(field, "http://"), strings.HasPrefix(field, "https://"):
			fields[i] = "<url>"
		case strings.HasPrefix(field, "bearer"):
			fields[i] = "bearer <token>"
		default:
			var b strings.Builder
			for _, r := range field {
				if r >= '0' && r <= '9' {
					b.WriteString("<n>")
				} else {
					b.WriteRune(r)
				}
			}
			fields[i] = b.String()
		}
	}
	value = strings.Join(fields, " ")
	if len(value) > 192 {
		value = value[:192]
	}
	return value
}

func classifyGatewayFailureText(value string) (string, string) {
	normalized := normalizeGatewayFailureText(value)
	if normalized == "" {
		return "", ""
	}
	category := gatewayTextCategoryUnknown
	switch {
	case strings.Contains(normalized, "context canceled"), strings.Contains(normalized, "client canceled"):
		category = gatewayTextCategoryClientCanceled
	case strings.Contains(normalized, "timeout"), strings.Contains(normalized, "timed out"), strings.Contains(normalized, "deadline exceeded"):
		category = gatewayTextCategoryTimeout
	case strings.Contains(normalized, "no such host"), strings.Contains(normalized, "dns"):
		category = gatewayTextCategoryDNS
	case strings.Contains(normalized, "tls"), strings.Contains(normalized, "certificate"):
		category = gatewayTextCategoryTLS
	case strings.Contains(normalized, "connection reset"), strings.Contains(normalized, "connection refused"), strings.Contains(normalized, "broken pipe"):
		category = gatewayTextCategoryConnection
	case strings.Contains(normalized, "invalid api key"), strings.Contains(normalized, "unauthorized"), strings.Contains(normalized, "authentication failed"), strings.Contains(normalized, "forbidden"):
		category = gatewayTextCategoryAuth
	case strings.Contains(normalized, "rate limit"), strings.Contains(normalized, "rate_limit"), strings.Contains(normalized, "too many requests"), strings.Contains(normalized, "quota exceeded"):
		category = gatewayTextCategoryRateLimit
	case strings.Contains(normalized, "overloaded"), strings.Contains(normalized, "capacity"), strings.Contains(normalized, "temporarily unavailable"), strings.Contains(normalized, "try again later"), strings.Contains(normalized, "overloaded_error"):
		category = gatewayTextCategoryTransient
	case strings.Contains(normalized, "model not found"), strings.Contains(normalized, "model unavailable"), strings.Contains(normalized, "not supported"):
		category = gatewayTextCategoryModel
	case strings.Contains(normalized, "stream error"), strings.Contains(normalized, "stream ended"), strings.Contains(normalized, "sse"), strings.Contains(normalized, "terminal event"), strings.Contains(normalized, "protocol error"):
		category = gatewayTextCategoryStream
	}
	sum := sha256.Sum256([]byte(normalized))
	return category, fmt.Sprintf("%x", sum[:8])
}

func normalizeGatewayTextCategory(value string) string {
	switch strings.TrimSpace(value) {
	case gatewayTextCategoryClientCanceled, gatewayTextCategoryTimeout, gatewayTextCategoryDNS,
		gatewayTextCategoryTLS, gatewayTextCategoryConnection, gatewayTextCategoryAuth,
		gatewayTextCategoryRateLimit, gatewayTextCategoryTransient, gatewayTextCategoryModel,
		gatewayTextCategoryStream, gatewayTextCategoryUnknown:
		return strings.TrimSpace(value)
	case "":
		return ""
	default:
		return gatewayTextCategoryUnknown
	}
}

func normalizeGatewayTextSignature(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 16 {
		return ""
	}
	for _, r := range value {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return ""
		}
	}
	return value
}

func applyGatewayTextPolicy(p *GatewayFailurePolicy) {
	if p == nil {
		return
	}
	switch p.TextCategory {
	case gatewayTextCategoryClientCanceled:
		p.Class, p.Scope, p.Retry = GatewayFailureClientCanceled, GatewayFailureScopeRequest, GatewayRetryNone
	case gatewayTextCategoryTimeout:
		p.Class, p.Retry = GatewayFailureTimeout, GatewayRetrySameThenNext
	case gatewayTextCategoryDNS:
		p.Class, p.Scope, p.Retry = GatewayFailureDNS, GatewayFailureScopeAccount, GatewayRetryNextAccount
	case gatewayTextCategoryTLS:
		p.Class, p.Scope, p.Retry, p.Persistent = GatewayFailureTLS, GatewayFailureScopeAccount, GatewayRetryNextAccount, true
	case gatewayTextCategoryConnection:
		p.Class, p.Retry = GatewayFailureConnectionReset, GatewayRetrySameThenNext
	case gatewayTextCategoryAuth:
		p.Class, p.Scope, p.Retry, p.Persistent = GatewayFailureAccountAuth, GatewayFailureScopeAccount, GatewayRetryNextAccount, true
	case gatewayTextCategoryRateLimit:
		p.Class, p.Scope, p.Retry = GatewayFailureRateLimited, GatewayFailureScopeAccount, GatewayRetryNextAccount
	case gatewayTextCategoryTransient:
		p.Class, p.Scope, p.Retry = GatewayFailureProviderTransient, GatewayFailureScopeProvider, GatewayRetrySameThenNext
	case gatewayTextCategoryModel:
		p.Class, p.Scope, p.Retry = GatewayFailureModelUnavailable, GatewayFailureScopeRequest, GatewayRetryNone
	case gatewayTextCategoryStream:
		p.Class, p.Scope, p.Retry = GatewayFailureStreamError, GatewayFailureScopeProvider, GatewayRetrySameThenNext
	}
}

func ClassifyUpstreamTransportFailure(err error) GatewayFailurePolicy {
	base := GatewayFailurePolicy{
		Class:            GatewayFailureUnknownTransport,
		Scope:            GatewayFailureScopeProvider,
		Retry:            GatewayRetrySameThenNext,
		NetworkType:      GatewayNetworkUnknown,
		ClientStatusCode: http.StatusBadGateway,
	}
	if err == nil {
		base.Class = GatewayFailureEmptyResponse
		base.NetworkType = GatewayNetworkUnknown
		return base
	}
	base.TextCategory, base.TextSignature = classifyGatewayFailureText(err.Error())
	applyGatewayTextPolicy(&base)
	if errors.Is(err, context.Canceled) {
		base.Class = GatewayFailureClientCanceled
		base.Scope = GatewayFailureScopeRequest
		base.Retry = GatewayRetryNone
		base.NetworkType = GatewayNetworkClientCanceled
		return base
	}
	if errors.Is(err, context.DeadlineExceeded) {
		base.Class = GatewayFailureTimeout
		base.NetworkType = GatewayNetworkTimeout
		return base
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsTimeout {
			base.Class = GatewayFailureTimeout
			base.NetworkType = GatewayNetworkTimeout
			return base
		}
		base.Class = GatewayFailureDNS
		base.Scope = GatewayFailureScopeAccount
		base.Retry = GatewayRetryNextAccount
		base.Persistent = dnsErr.IsNotFound
		base.NetworkType = GatewayNetworkDNS
		return base
	}
	var unknownAuthority *x509.UnknownAuthorityError
	if errors.As(err, &unknownAuthority) {
		base.Class = GatewayFailureTLS
		base.Scope = GatewayFailureScopeAccount
		base.Retry = GatewayRetryNextAccount
		base.Persistent = true
		base.NetworkType = GatewayNetworkTLS
		return base
	}
	var tlsHeaderErr *tls.RecordHeaderError
	if errors.As(err, &tlsHeaderErr) {
		base.Class = GatewayFailureTLS
		base.Scope = GatewayFailureScopeAccount
		base.Retry = GatewayRetryNextAccount
		base.Persistent = true
		base.NetworkType = GatewayNetworkTLS
		return base
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		base.Class = GatewayFailureConnectionRefused
		base.Scope = GatewayFailureScopeAccount
		base.Retry = GatewayRetryNextAccount
		base.Persistent = true
		base.NetworkType = GatewayNetworkConnectionRefused
		return base
	}
	if errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) {
		base.Class = GatewayFailureNetworkUnreachable
		base.Scope = GatewayFailureScopeAccount
		base.Retry = GatewayRetryNextAccount
		base.Persistent = true
		base.NetworkType = GatewayNetworkUnreachable
		return base
	}
	if errors.Is(err, syscall.ECONNRESET) {
		base.Class = GatewayFailureConnectionReset
		base.NetworkType = GatewayNetworkConnectionReset
		return base
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		base.Class = GatewayFailureUnexpectedEOF
		base.NetworkType = GatewayNetworkUnexpectedEOF
		return base
	}

	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "username/password authentication failed"),
		strings.Contains(lower, "proxy authentication required"):
		base.Class = GatewayFailureProxyAuth
		base.Scope = GatewayFailureScopeAccount
		base.Retry = GatewayRetryNextAccount
		base.Persistent = true
		base.NetworkType = GatewayNetworkProxyAuth
	case strings.Contains(lower, "connection refused"):
		base.Class = GatewayFailureConnectionRefused
		base.Scope = GatewayFailureScopeAccount
		base.Retry = GatewayRetryNextAccount
		base.Persistent = true
		base.NetworkType = GatewayNetworkConnectionRefused
	case strings.Contains(lower, "no route to host"), strings.Contains(lower, "network is unreachable"):
		base.Class = GatewayFailureNetworkUnreachable
		base.Scope = GatewayFailureScopeAccount
		base.Retry = GatewayRetryNextAccount
		base.Persistent = true
		base.NetworkType = GatewayNetworkUnreachable
	case strings.Contains(lower, "no such host"):
		base.Class = GatewayFailureDNS
		base.Scope = GatewayFailureScopeAccount
		base.Retry = GatewayRetryNextAccount
		base.Persistent = true
		base.NetworkType = GatewayNetworkDNS
	case strings.Contains(lower, "timeout"), strings.Contains(lower, "timed out"):
		base.Class = GatewayFailureTimeout
		base.NetworkType = GatewayNetworkTimeout
	case strings.Contains(lower, "connection reset"):
		base.Class = GatewayFailureConnectionReset
		base.NetworkType = GatewayNetworkConnectionReset
	case strings.Contains(lower, "unexpected eof"), strings.HasSuffix(lower, " eof"):
		base.Class = GatewayFailureUnexpectedEOF
		base.NetworkType = GatewayNetworkUnexpectedEOF
	}
	return base
}

func ClassifyUpstreamHTTPFailure(status int, body []byte, headers http.Header) GatewayFailurePolicy {
	providerType, providerCode := extractBoundedProviderFields(body)
	p := GatewayFailurePolicy{
		StatusKnown:      status > 0,
		ProviderType:     providerType,
		ProviderCode:     providerCode,
		ClientStatusCode: status,
		Scope:            GatewayFailureScopeProvider,
		Retry:            GatewayRetryNone,
		Class:            GatewayFailureUnknown,
	}
	p.TextCategory, p.TextSignature = classifyGatewayFailureText(string(body))
	applyGatewayTextPolicy(&p)
	if retryAfter := parseRetryAfterSeconds(headers); retryAfter > 0 {
		p.RetryAfterSeconds = retryAfter
	}

	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		p.Class, p.Scope, p.Retry, p.Persistent = GatewayFailureAccountAuth, GatewayFailureScopeAccount, GatewayRetryNextAccount, true
	case status == http.StatusTooManyRequests:
		p.Class, p.Scope, p.Retry = GatewayFailureRateLimited, GatewayFailureScopeAccount, GatewayRetryNextAccount
	case status == http.StatusRequestTimeout:
		p.Class, p.Retry = GatewayFailureTimeout, GatewayRetrySameThenNext
	case status == http.StatusNotFound:
		p.Class, p.Scope = GatewayFailureModelUnavailable, GatewayFailureScopeRequest
	case status == http.StatusBadRequest || status == http.StatusRequestEntityTooLarge || status == http.StatusUnprocessableEntity:
		p.Class, p.Scope = GatewayFailureInvalidRequest, GatewayFailureScopeRequest
	case status >= 500:
		p.Class, p.Retry = GatewayFailureServer, GatewayRetrySameThenNext
	default:
		p.Class = GatewayFailureUnknown
	}

	joined := strings.ToLower(providerType + " " + providerCode)
	if status >= 400 && status < 500 && (strings.Contains(joined, "overloaded") || strings.Contains(joined, "temporar") || strings.Contains(joined, "capacity")) {
		p.Class, p.Scope, p.Retry = GatewayFailureProviderTransient, GatewayFailureScopeProvider, GatewayRetrySameThenNext
	}
	return p
}

func ClassifyUpstreamStreamFailure(body []byte, responseCommitted bool) GatewayFailurePolicy {
	p := ClassifyUpstreamHTTPFailure(0, body, nil)
	p.StatusKnown = false
	p.Class = GatewayFailureStreamError
	p.Scope = GatewayFailureScopeProvider
	p.TextCategory, p.TextSignature = classifyGatewayFailureText(string(body))
	if p.TextCategory == "" || p.TextCategory == gatewayTextCategoryUnknown {
		p.TextCategory = gatewayTextCategoryStream
	}
	if p.Retry == GatewayRetryNone {
		p.Retry = GatewayRetrySameThenNext
	}
	p.ResponseCommitted = responseCommitted
	if responseCommitted {
		p.Retry = GatewayRetryNone
	}
	return p
}

func extractBoundedProviderFields(body []byte) (string, string) {
	if len(body) == 0 {
		return "", ""
	}
	var root map[string]json.RawMessage
	if json.Unmarshal(body, &root) != nil {
		return "", ""
	}
	readString := func(raw json.RawMessage, key string) string {
		var value string
		if json.Unmarshal(raw, &value) == nil {
			return value
		}
		return ""
	}
	providerType := readString(root["type"], "type")
	providerCode := readString(root["code"], "code")
	if raw, ok := root["error"]; ok {
		var nested map[string]json.RawMessage
		if json.Unmarshal(raw, &nested) == nil {
			if providerType == "" {
				providerType = readString(nested["type"], "type")
			}
			if providerCode == "" {
				providerCode = readString(nested["code"], "code")
			}
		}
	}
	return normalizeProviderField(providerType), normalizeProviderField(providerCode)
}

func normalizeProviderField(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.NewReplacer("-", "_", " ", "_").Replace(value)
	allowed := map[string]struct{}{
		"invalid_request_error": {}, "model_not_found": {}, "invalid_api_key": {},
		"rate_limit_error": {}, "rate_limit_exceeded": {}, "upstream_error": {},
		"overloaded_error": {}, "capacity_exhausted": {}, "context_length_exceeded": {},
		"content_policy_violation": {},
	}
	if _, ok := allowed[value]; ok {
		return value
	}
	if value == "" {
		return ""
	}
	return "other"
}

func normalizeGatewayFailureClass(value string) string {
	switch GatewayFailureClass(strings.TrimSpace(value)) {
	case GatewayFailureClientCanceled, GatewayFailureTimeout, GatewayFailureDNS,
		GatewayFailureTLS, GatewayFailureProxyAuth, GatewayFailureConnectionRefused,
		GatewayFailureConnectionReset, GatewayFailureNetworkUnreachable,
		GatewayFailureUnexpectedEOF, GatewayFailureEmptyResponse, GatewayFailureAccountAuth,
		GatewayFailureRateLimited, GatewayFailureModelUnavailable, GatewayFailureInvalidRequest,
		GatewayFailureServer, GatewayFailureProviderTransient, GatewayFailureStreamError,
		GatewayFailureUnknownTransport, GatewayFailureUnknown:
		return strings.TrimSpace(value)
	default:
		return string(GatewayFailureUnknown)
	}
}

func normalizeGatewayRetryDisposition(value string) string {
	switch GatewayRetryDisposition(strings.TrimSpace(value)) {
	case GatewayRetryNone, GatewayRetryNextAccount, GatewayRetrySameThenNext:
		return strings.TrimSpace(value)
	default:
		return string(GatewayRetryNone)
	}
}

func normalizeGatewayNetworkType(value string) string {
	switch strings.TrimSpace(value) {
	case GatewayNetworkClientCanceled, GatewayNetworkTimeout, GatewayNetworkDNS,
		GatewayNetworkTLS, GatewayNetworkProxyAuth, GatewayNetworkConnectionRefused,
		GatewayNetworkConnectionReset, GatewayNetworkUnreachable, GatewayNetworkUnexpectedEOF,
		GatewayNetworkUnknown:
		return strings.TrimSpace(value)
	case "":
		return ""
	default:
		return GatewayNetworkUnknown
	}
}

func parseRetryAfterSeconds(headers http.Header) int {
	if headers == nil {
		return 0
	}
	seconds, err := strconv.Atoi(strings.TrimSpace(headers.Get("Retry-After")))
	if err != nil || seconds < 0 || seconds > 3600 {
		return 0
	}
	return seconds
}
