package handler

import (
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// A 413 from an API-key relay usually describes that relay's request-size
// limit, not a credential-specific limit. Keep cross-relay failover while
// avoiding repeated network calls through sibling accounts on the same route.
func openAIBodyLimitFailureDomain(account *service.Account) string {
	if account == nil || !account.IsOpenAIApiKey() {
		return ""
	}
	baseURL := strings.TrimRight(strings.TrimSpace(account.GetOpenAIBaseURL()), "/")
	if baseURL == "" {
		return ""
	}
	proxyID := int64(0)
	if account.ProxyID != nil {
		proxyID = *account.ProxyID
	}
	return fmt.Sprintf("%s|%d", baseURL, proxyID)
}

func recordOpenAIBodyLimitFailureDomain(domains map[string]struct{}, account *service.Account, failoverErr *service.UpstreamFailoverError) {
	if domains == nil || failoverErr == nil || !failoverErr.IsOpenAIRequestBodyTooLarge() {
		return
	}
	if domain := openAIBodyLimitFailureDomain(account); domain != "" {
		domains[domain] = struct{}{}
	}
}

func openAIBodyLimitFailureDomainBlocked(domains map[string]struct{}, account *service.Account) bool {
	if len(domains) == 0 {
		return false
	}
	domain := openAIBodyLimitFailureDomain(account)
	if domain == "" {
		return false
	}
	_, blocked := domains[domain]
	return blocked
}
