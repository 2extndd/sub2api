//go:build unit

package handler

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai_compat"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type openAIResponsesHedgeCountingConcurrencyCache struct {
	fakeConcurrencyCache
	mu       sync.Mutex
	acquires map[int64]int
	releases map[int64]int
}

func newOpenAIResponsesHedgeCountingConcurrencyCache() *openAIResponsesHedgeCountingConcurrencyCache {
	return &openAIResponsesHedgeCountingConcurrencyCache{
		acquires: make(map[int64]int),
		releases: make(map[int64]int),
	}
}

func (cache *openAIResponsesHedgeCountingConcurrencyCache) AcquireAccountSlot(
	_ context.Context,
	accountID int64,
	_ int,
	_ string,
) (bool, error) {
	cache.mu.Lock()
	cache.acquires[accountID]++
	cache.mu.Unlock()
	return true, nil
}

func (cache *openAIResponsesHedgeCountingConcurrencyCache) ReleaseAccountSlot(
	_ context.Context,
	accountID int64,
	_ string,
) error {
	cache.mu.Lock()
	cache.releases[accountID]++
	cache.mu.Unlock()
	return nil
}

func (cache *openAIResponsesHedgeCountingConcurrencyCache) counts(accountID int64) (int, int) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return cache.acquires[accountID], cache.releases[accountID]
}

type openAIResponsesHedgeUsageLogRepo struct {
	service.UsageLogRepository
	mu   sync.Mutex
	logs []service.UsageLog
}

func (repo *openAIResponsesHedgeUsageLogRepo) Create(_ context.Context, log *service.UsageLog) (bool, error) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if log != nil {
		repo.logs = append(repo.logs, *log)
	}
	return true, nil
}

func (repo *openAIResponsesHedgeUsageLogRepo) snapshot() []service.UsageLog {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return append([]service.UsageLog(nil), repo.logs...)
}

type openAIResponsesHedgeUpstreamPlan struct {
	started  chan struct{}
	release  chan struct{}
	text     string
	header   string
	canceled chan struct{}
}

type openAIResponsesHedgeUpstream struct {
	service.HTTPUpstream
	mu    sync.Mutex
	plans map[int64]*openAIResponsesHedgeUpstreamPlan
	calls []int64
}

func (upstream *openAIResponsesHedgeUpstream) snapshotCalls() []int64 {
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	return append([]int64(nil), upstream.calls...)
}

func (upstream *openAIResponsesHedgeUpstream) Do(
	req *http.Request,
	_ string,
	accountID int64,
	_ int,
) (*http.Response, error) {
	upstream.mu.Lock()
	plan := upstream.plans[accountID]
	upstream.calls = append(upstream.calls, accountID)
	upstream.mu.Unlock()
	if plan == nil {
		return nil, io.EOF
	}
	select {
	case <-plan.started:
	default:
		close(plan.started)
	}
	select {
	case <-plan.release:
	case <-req.Context().Done():
		select {
		case <-plan.canceled:
		default:
			close(plan.canceled)
		}
		return nil, req.Context().Err()
	}
	body := strings.Join([]string{
		`data: {"id":"chatcmpl_hedge","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_hedge","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"content":"` + plan.text + `"},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_hedge","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		"",
		`data: {"id":"chatcmpl_hedge","object":"chat.completion.chunk","model":"gpt-5.4","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"text/event-stream"},
			"X-Request-Id": []string{plan.header},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}, nil
}

func newOpenAIResponsesHedgePlan(text, header string) *openAIResponsesHedgeUpstreamPlan {
	return &openAIResponsesHedgeUpstreamPlan{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		text:     text,
		header:   header,
		canceled: make(chan struct{}),
	}
}

func newOpenAIResponsesHedgeHandlerFixture(
	t *testing.T,
	enabled bool,
	upstream service.HTTPUpstream,
) (*OpenAIGatewayHandler, *openAIResponsesHedgeCountingConcurrencyCache, *openAIResponsesHedgeUsageLogRepo) {
	return newOpenAIResponsesHedgeHandlerFixtureWithPool(t, enabled, true, upstream)
}

func newOpenAIResponsesHedgeHandlerFixtureWithPool(
	t *testing.T,
	enabled bool,
	includeSecondary bool,
	upstream service.HTTPUpstream,
) (*OpenAIGatewayHandler, *openAIResponsesHedgeCountingConcurrencyCache, *openAIResponsesHedgeUsageLogRepo) {
	t.Helper()
	accounts := []service.Account{
		{
			ID: 1, Name: "responses-primary", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
			Concurrency: 2, Priority: 0,
			Credentials: map[string]any{"api_key": "primary", "base_url": "https://primary.test"},
			Extra: map[string]any{
				openai_compat.ExtraKeyResponsesMode: string(openai_compat.ResponsesSupportModeForceChatCompletions),
			},
		},
		{
			ID: 2, Name: "responses-secondary", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
			Concurrency: 2, Priority: 1,
			Credentials: map[string]any{"api_key": "secondary", "base_url": "https://secondary.test"},
			Extra: map[string]any{
				openai_compat.ExtraKeyResponsesMode: string(openai_compat.ResponsesSupportModeForceChatCompletions),
			},
		},
	}
	if !includeSecondary {
		accounts = accounts[:1]
	}
	accountRepo := openAIImagesFailoverAccountRepo{accounts: accounts}
	schedulerAccounts := make([]*service.Account, len(accounts))
	for index := range accounts {
		schedulerAccounts[index] = &accounts[index]
	}
	schedulerSnapshot := service.NewSchedulerSnapshotService(
		&fakeSchedulerCache{accounts: schedulerAccounts}, nil, nil, nil, nil,
	)
	concurrencyCache := newOpenAIResponsesHedgeCountingConcurrencyCache()
	concurrencyService := service.NewConcurrencyService(concurrencyCache)
	usageLogRepo := &openAIResponsesHedgeUsageLogRepo{}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Gateway.Scheduling.OpenAIHedge = config.OpenAIHedgeConfig{
		Enabled:                   enabled,
		StandardThresholdSeconds:  1,
		HighThresholdSeconds:      1,
		VeryHeavyThresholdSeconds: 1,
		EventChannelCapacity:      config.DefaultOpenAIHedgeEventChannelCapacity,
		MaxPrecommitEvents:        config.DefaultOpenAIHedgeMaxPrecommitEvents,
		MaxPrecommitBytes:         config.DefaultOpenAIHedgeMaxPrecommitBytes,
		CancelDrainTimeoutSeconds: 1,
		MaxDuplicateCostMicros:    config.MaximumOpenAIHedgeMaxDuplicateCostMicros,
		MaxAttempts:               2,
	}
	billingService := service.NewBillingService(cfg, nil)
	gatewayService := service.NewOpenAIGatewayService(
		accountRepo,
		usageLogRepo,
		nil,
		nil,
		nil,
		nil,
		nil,
		cfg,
		schedulerSnapshot,
		concurrencyService,
		billingService,
		nil,
		nil,
		upstream,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	handlerBillingService := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(handlerBillingService.Stop)
	handler := NewOpenAIGatewayHandler(
		gatewayService,
		concurrencyService,
		handlerBillingService,
		service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg),
		nil,
		nil,
		nil,
		nil,
		cfg,
	)
	handler.maxAccountSwitches = 0
	runtime, err := service.NewAdaptiveLatencyRuntime(cfg, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(runtime.Stop)
	handler.adaptiveLatencyRuntime = runtime
	return handler, concurrencyCache, usageLogRepo
}

func newOpenAIResponsesHedgeHandlerContext(
	t *testing.T,
	ctx context.Context,
) (*gin.Context, *httptest.ResponseRecorder) {
	return newOpenAIResponsesHedgeHandlerContextWithBody(t, ctx, []byte(`{"model":"gpt-5.4","stream":true,"store":false,"max_output_tokens":16,"input":"hello"}`))
}

func newOpenAIResponsesHedgeHandlerContextWithBody(
	t *testing.T,
	ctx context.Context,
	body []byte,
) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	groupID := int64(3131)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req
	c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
		ID: 41, UserID: 51, GroupID: &groupID, Status: service.StatusActive,
		Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive},
		User:  &service.User{ID: 51, Status: service.StatusActive},
	})
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 51, Concurrency: 2})
	return c, rec
}

func waitOpenAIResponsesHedgeSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(4 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func startOpenAIResponsesHedgeHandler(handler *OpenAIGatewayHandler, c *gin.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.Responses(c)
	}()
	return done
}

func waitOpenAIResponsesHedgeAttempts(
	t *testing.T,
	done <-chan struct{},
	rec *httptest.ResponseRecorder,
	upstream *openAIResponsesHedgeUpstream,
	concurrency *openAIResponsesHedgeCountingConcurrencyCache,
	primary, secondary *openAIResponsesHedgeUpstreamPlan,
) {
	t.Helper()
	select {
	case <-primary.started:
	case <-done:
		t.Fatalf("handler completed before primary upstream: status=%d body=%q calls=%v", rec.Code, rec.Body.String(), upstream.snapshotCalls())
	case <-time.After(4 * time.Second):
		t.Fatalf("timed out waiting for primary upstream: status=%d body=%q calls=%v", rec.Code, rec.Body.String(), upstream.snapshotCalls())
	}
	select {
	case <-secondary.started:
	case <-time.After(4 * time.Second):
		primaryAcquires, primaryReleases := concurrency.counts(1)
		secondaryAcquires, secondaryReleases := concurrency.counts(2)
		t.Fatalf(
			"timed out waiting for secondary upstream: calls=%v primary_leases=%d/%d secondary_leases=%d/%d",
			upstream.snapshotCalls(), primaryAcquires, primaryReleases, secondaryAcquires, secondaryReleases,
		)
	}
}

func requireOpenAIResponsesHedgeLeaseCounts(
	t *testing.T,
	concurrency *openAIResponsesHedgeCountingConcurrencyCache,
	primaryAcquires, primaryReleases, secondaryAcquires, secondaryReleases int,
) {
	t.Helper()
	gotPrimaryAcquires, gotPrimaryReleases := concurrency.counts(1)
	gotSecondaryAcquires, gotSecondaryReleases := concurrency.counts(2)
	require.Equal(t, primaryAcquires, gotPrimaryAcquires)
	require.Equal(t, primaryReleases, gotPrimaryReleases)
	require.Equal(t, secondaryAcquires, gotSecondaryAcquires)
	require.Equal(t, secondaryReleases, gotSecondaryReleases)
}

func TestOpenAIGatewayHandlerResponses_HedgePrimaryWinUsesOnlyPrimaryOutputAndBilling(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primary := newOpenAIResponsesHedgePlan("primary-winner", "primary")
	secondary := newOpenAIResponsesHedgePlan("secondary-loser", "secondary")
	upstream := &openAIResponsesHedgeUpstream{plans: map[int64]*openAIResponsesHedgeUpstreamPlan{1: primary, 2: secondary}}
	handler, concurrency, usageLogs := newOpenAIResponsesHedgeHandlerFixture(t, true, upstream)
	c, rec := newOpenAIResponsesHedgeHandlerContext(t, context.Background())

	done := startOpenAIResponsesHedgeHandler(handler, c)
	waitOpenAIResponsesHedgeAttempts(t, done, rec, upstream, concurrency, primary, secondary)
	close(primary.release)
	waitOpenAIResponsesHedgeSignal(t, secondary.canceled, "secondary cancellation")
	waitOpenAIResponsesHedgeSignal(t, done, "handler completion")

	require.Contains(t, rec.Body.String(), "primary-winner")
	require.NotContains(t, rec.Body.String(), "secondary-loser")
	require.Equal(t, 1, strings.Count(rec.Body.String(), "event: response.completed"), rec.Body.String())
	require.Equal(t, 1, strings.Count(rec.Body.String(), "data: [DONE]"), rec.Body.String())
	require.Equal(t, "primary", rec.Header().Get("X-Request-Id"))
	requireOpenAIResponsesHedgeLeaseCounts(t, concurrency, 1, 1, 2, 2)
	logs := usageLogs.snapshot()
	require.Len(t, logs, 1)
	require.Equal(t, int64(1), logs[0].AccountID)
	require.Equal(t, 3, logs[0].InputTokens)
	require.Equal(t, 2, logs[0].OutputTokens)
}

func TestOpenAIGatewayHandlerResponses_HedgeSecondaryWinUsesOnlySecondaryOutputAndBilling(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primary := newOpenAIResponsesHedgePlan("primary-loser", "primary")
	secondary := newOpenAIResponsesHedgePlan("secondary-winner", "secondary")
	upstream := &openAIResponsesHedgeUpstream{plans: map[int64]*openAIResponsesHedgeUpstreamPlan{1: primary, 2: secondary}}
	handler, concurrency, usageLogs := newOpenAIResponsesHedgeHandlerFixture(t, true, upstream)
	c, rec := newOpenAIResponsesHedgeHandlerContext(t, context.Background())

	done := startOpenAIResponsesHedgeHandler(handler, c)
	waitOpenAIResponsesHedgeAttempts(t, done, rec, upstream, concurrency, primary, secondary)
	close(secondary.release)
	waitOpenAIResponsesHedgeSignal(t, primary.canceled, "primary cancellation")
	waitOpenAIResponsesHedgeSignal(t, done, "handler completion")

	require.Contains(t, rec.Body.String(), "secondary-winner")
	require.NotContains(t, rec.Body.String(), "primary-loser")
	require.Equal(t, 1, strings.Count(rec.Body.String(), "event: response.completed"))
	require.Equal(t, 1, strings.Count(rec.Body.String(), "data: [DONE]"))
	require.Equal(t, "secondary", rec.Header().Get("X-Request-Id"))
	requireOpenAIResponsesHedgeLeaseCounts(t, concurrency, 1, 1, 2, 2)
	logs := usageLogs.snapshot()
	require.Len(t, logs, 1)
	require.Equal(t, int64(2), logs[0].AccountID)
	require.Equal(t, 3, logs[0].InputTokens)
	require.Equal(t, 2, logs[0].OutputTokens)
}

func TestOpenAIGatewayHandlerResponses_HedgeFeatureOffPreservesSinglePrimaryPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primary := newOpenAIResponsesHedgePlan("feature-off-primary", "primary")
	secondary := newOpenAIResponsesHedgePlan("must-not-start", "secondary")
	upstream := &openAIResponsesHedgeUpstream{plans: map[int64]*openAIResponsesHedgeUpstreamPlan{1: primary, 2: secondary}}
	handler, concurrency, usageLogs := newOpenAIResponsesHedgeHandlerFixture(t, false, upstream)
	c, rec := newOpenAIResponsesHedgeHandlerContext(t, context.Background())

	done := startOpenAIResponsesHedgeHandler(handler, c)
	waitOpenAIResponsesHedgeSignal(t, primary.started, "feature-off primary upstream")
	select {
	case <-secondary.started:
		t.Fatal("feature-off request launched a secondary upstream")
	case <-time.After(100 * time.Millisecond):
	}
	close(primary.release)
	waitOpenAIResponsesHedgeSignal(t, done, "feature-off handler completion")

	require.Contains(t, rec.Body.String(), "feature-off-primary")
	require.NotContains(t, rec.Body.String(), "must-not-start")
	require.Equal(t, "primary", rec.Header().Get("X-Request-Id"))
	requireOpenAIResponsesHedgeLeaseCounts(t, concurrency, 1, 1, 0, 0)
	logs := usageLogs.snapshot()
	require.Len(t, logs, 1)
	require.Equal(t, int64(1), logs[0].AccountID)
	require.Equal(t, 3, logs[0].InputTokens)
	require.Equal(t, 2, logs[0].OutputTokens)
}

func TestOpenAIGatewayHandlerResponses_HedgeIneligibleRequestPreservesSinglePrimaryPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primary := newOpenAIResponsesHedgePlan("ineligible-primary", "primary")
	secondary := newOpenAIResponsesHedgePlan("must-not-start", "secondary")
	upstream := &openAIResponsesHedgeUpstream{plans: map[int64]*openAIResponsesHedgeUpstreamPlan{1: primary, 2: secondary}}
	handler, concurrency, usageLogs := newOpenAIResponsesHedgeHandlerFixture(t, true, upstream)
	body := []byte(`{"model":"gpt-5.4","stream":true,"store":true,"max_output_tokens":16,"input":"hello"}`)
	c, rec := newOpenAIResponsesHedgeHandlerContextWithBody(t, context.Background(), body)

	done := startOpenAIResponsesHedgeHandler(handler, c)
	waitOpenAIResponsesHedgeSignal(t, primary.started, "ineligible primary upstream")
	select {
	case <-secondary.started:
		t.Fatal("ineligible request launched a secondary upstream")
	case <-time.After(100 * time.Millisecond):
	}
	close(primary.release)
	waitOpenAIResponsesHedgeSignal(t, done, "ineligible handler completion")

	require.Contains(t, rec.Body.String(), "ineligible-primary")
	require.NotContains(t, rec.Body.String(), "must-not-start")
	requireOpenAIResponsesHedgeLeaseCounts(t, concurrency, 1, 1, 0, 0)
	logs := usageLogs.snapshot()
	require.Len(t, logs, 1)
	require.Equal(t, int64(1), logs[0].AccountID)
	require.Equal(t, 3, logs[0].InputTokens)
	require.Equal(t, 2, logs[0].OutputTokens)
}

func TestOpenAIGatewayHandlerResponses_HedgeNoSecondaryCandidatePreservesPrimaryPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primary := newOpenAIResponsesHedgePlan("no-candidate-primary", "primary")
	upstream := &openAIResponsesHedgeUpstream{plans: map[int64]*openAIResponsesHedgeUpstreamPlan{1: primary}}
	handler, concurrency, usageLogs := newOpenAIResponsesHedgeHandlerFixtureWithPool(t, true, false, upstream)
	c, rec := newOpenAIResponsesHedgeHandlerContext(t, context.Background())

	done := startOpenAIResponsesHedgeHandler(handler, c)
	waitOpenAIResponsesHedgeSignal(t, primary.started, "no-candidate primary upstream")
	select {
	case <-done:
		t.Fatalf("handler completed before primary release: status=%d body=%q", rec.Code, rec.Body.String())
	case <-time.After(1200 * time.Millisecond):
	}
	close(primary.release)
	waitOpenAIResponsesHedgeSignal(t, done, "no-candidate handler completion")

	require.Equal(t, []int64{1}, upstream.snapshotCalls())
	require.Contains(t, rec.Body.String(), "no-candidate-primary")
	require.Equal(t, 1, strings.Count(rec.Body.String(), "event: response.completed"))
	require.Equal(t, 1, strings.Count(rec.Body.String(), "data: [DONE]"))
	requireOpenAIResponsesHedgeLeaseCounts(t, concurrency, 1, 1, 0, 0)
	logs := usageLogs.snapshot()
	require.Len(t, logs, 1)
	require.Equal(t, int64(1), logs[0].AccountID)
	require.Equal(t, 3, logs[0].InputTokens)
	require.Equal(t, 2, logs[0].OutputTokens)
}

func TestOpenAIGatewayHandlerResponses_HedgeInitializationErrorReleasesTransferredPrimaryLeaseOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &openAIResponsesHedgeUpstream{plans: map[int64]*openAIResponsesHedgeUpstreamPlan{}}
	handler, _, _ := newOpenAIResponsesHedgeHandlerFixture(t, true, upstream)
	groupID := int64(3131)
	primary := &service.Account{
		ID: 1, Name: "responses-primary", Platform: service.PlatformOpenAI,
		Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
		Concurrency: 2, Priority: 0,
		Credentials: map[string]any{"api_key": "primary", "base_url": "https://primary.test"},
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	var releases int
	result, handled := handler.tryForwardResponsesWithHedge(
		context.Background(),
		c,
		&service.APIKey{ID: 41, GroupID: &groupID},
		"gpt-5.4",
		service.PlatformOpenAI,
		[]byte(`{"model":"gpt-5.4","stream":true,"store":false,"input":"hello"}`),
		primary,
		func() { releases++ },
		service.CanonicalOpenAIResponsesCohort(nil, "gpt-5.4"),
		true,
		false,
		false,
	)

	require.True(t, handled)
	require.Error(t, result.err)
	require.Equal(t, 1, releases)
	require.Empty(t, upstream.snapshotCalls())
}

func TestOpenAIGatewayHandlerResponses_HedgeClientCancellationCancelsBothAttemptsAndReleasesLeases(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primary := newOpenAIResponsesHedgePlan("must-not-commit-primary", "primary")
	secondary := newOpenAIResponsesHedgePlan("must-not-commit-secondary", "secondary")
	upstream := &openAIResponsesHedgeUpstream{plans: map[int64]*openAIResponsesHedgeUpstreamPlan{1: primary, 2: secondary}}
	handler, concurrency, usageLogs := newOpenAIResponsesHedgeHandlerFixture(t, true, upstream)
	requestContext, cancelRequest := context.WithCancel(context.Background())
	c, rec := newOpenAIResponsesHedgeHandlerContext(t, requestContext)

	done := startOpenAIResponsesHedgeHandler(handler, c)
	waitOpenAIResponsesHedgeAttempts(t, done, rec, upstream, concurrency, primary, secondary)
	cancelRequest()
	waitOpenAIResponsesHedgeSignal(t, primary.canceled, "primary client cancellation")
	waitOpenAIResponsesHedgeSignal(t, secondary.canceled, "secondary client cancellation")
	waitOpenAIResponsesHedgeSignal(t, done, "canceled handler completion")

	require.NotContains(t, rec.Body.String(), "must-not-commit-primary")
	require.NotContains(t, rec.Body.String(), "must-not-commit-secondary")
	require.Empty(t, rec.Header().Get("X-Request-Id"))
	requireOpenAIResponsesHedgeLeaseCounts(t, concurrency, 1, 1, 2, 2)
	require.Empty(t, usageLogs.snapshot())
}
