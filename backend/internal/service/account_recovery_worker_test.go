package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

type recoveryRepoStub struct {
	mu              sync.Mutex
	accounts        map[int64]Account
	lastPlatform    string
	lastAccountType string
	lastStatus      string
}

func (s *recoveryRepoStub) ListAllWithFilters(_ context.Context, platform, accountType, status, _ string, _ int64, _ string) ([]Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPlatform = platform
	s.lastAccountType = accountType
	s.lastStatus = status
	out := make([]Account, 0, len(s.accounts))
	for _, account := range s.accounts {
		if account.Platform == platform && account.Type == accountType && account.Status == status {
			out = append(out, account)
		}
	}
	return out, nil
}

func (s *recoveryRepoStub) GetByID(_ context.Context, id int64) (*Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	account, ok := s.accounts[id]
	if !ok {
		return nil, errors.New("account not found")
	}
	copy := account
	return &copy, nil
}

func (s *recoveryRepoStub) recover(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	account := s.accounts[id]
	account.Status = StatusActive
	account.Schedulable = true
	account.ErrorMessage = ""
	s.accounts[id] = account
}

type recoveryProbeStub struct {
	mu      sync.Mutex
	calls   []int64
	results map[int64]*ScheduledTestResult
	errors  map[int64]error
}

func (s *recoveryProbeStub) RunTestBackground(_ context.Context, accountID int64, _ string) (*ScheduledTestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, accountID)
	if err := s.errors[accountID]; err != nil {
		return nil, err
	}
	if result := s.results[accountID]; result != nil {
		copy := *result
		return &copy, nil
	}
	return &ScheduledTestResult{Status: "failed", ErrorMessage: "probe failed"}, nil
}

func (s *recoveryProbeStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

type recoveryStateStub struct {
	repo  *recoveryRepoStub
	mu    sync.Mutex
	calls []int64
	err   error
}

func (s *recoveryStateStub) RecoverAccountAfterSuccessfulTest(_ context.Context, accountID int64) (*SuccessfulTestRecoveryResult, error) {
	s.mu.Lock()
	s.calls = append(s.calls, accountID)
	s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	s.repo.recover(accountID)
	return &SuccessfulTestRecoveryResult{ClearedError: true, ClearedRateLimit: true}, nil
}

func recoveryWorkerTestConfig() config.AccountRecoveryConfig {
	return config.AccountRecoveryConfig{
		Enabled:             true,
		IntervalSeconds:     60,
		BaseBackoffSeconds:  60,
		MaxBackoffSeconds:   120,
		ProbeTimeoutSeconds: 5,
		MaxWorkers:          2,
		ProbeModel:          "gpt-test",
	}
}

func TestAccountRecoveryWorkerProbesOnlyOpenAIAPIKeyErrors(t *testing.T) {
	repo := &recoveryRepoStub{accounts: map[int64]Account{
		1: {ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusError, Schedulable: false},
		2: {ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: false},
		3: {ID: 3, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusError, Schedulable: false},
		4: {ID: 4, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Status: StatusError, Schedulable: false},
	}}
	probe := &recoveryProbeStub{results: map[int64]*ScheduledTestResult{1: {Status: "success"}}, errors: map[int64]error{}}
	recoverer := &recoveryStateStub{repo: repo}
	worker := newAccountRecoveryWorker(repo, probe, recoverer, recoveryWorkerTestConfig())

	worker.runOnce(context.Background(), time.Unix(1_700_000_000, 0))

	if repo.lastPlatform != PlatformOpenAI || repo.lastAccountType != AccountTypeAPIKey || repo.lastStatus != StatusError {
		t.Fatalf("repository filters = platform=%q type=%q status=%q", repo.lastPlatform, repo.lastAccountType, repo.lastStatus)
	}
	if got := probe.callCount(); got != 1 {
		t.Fatalf("probe call count = %d, want 1", got)
	}
	if probe.calls[0] != 1 {
		t.Fatalf("probed account = %d, want 1", probe.calls[0])
	}
	if got, _ := repo.GetByID(context.Background(), 1); got.Status != StatusActive || !got.Schedulable {
		t.Fatalf("account 1 not recovered: status=%s schedulable=%t", got.Status, got.Schedulable)
	}
	if got, _ := repo.GetByID(context.Background(), 2); got.Schedulable {
		t.Fatal("manually unschedulable active account was changed")
	}
}

func TestAccountRecoveryWorkerUsesExponentialBackoff(t *testing.T) {
	repo := &recoveryRepoStub{accounts: map[int64]Account{
		7: {ID: 7, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusError, Schedulable: false},
	}}
	probe := &recoveryProbeStub{results: map[int64]*ScheduledTestResult{}, errors: map[int64]error{}}
	recoverer := &recoveryStateStub{repo: repo}
	worker := newAccountRecoveryWorker(repo, probe, recoverer, recoveryWorkerTestConfig())
	start := time.Unix(1_700_000_000, 0)

	worker.runOnce(context.Background(), start)
	worker.runOnce(context.Background(), start.Add(59*time.Second))
	if got := probe.callCount(); got != 1 {
		t.Fatalf("probe count before first backoff elapsed = %d, want 1", got)
	}

	worker.runOnce(context.Background(), start.Add(60*time.Second))
	worker.runOnce(context.Background(), start.Add(179*time.Second))
	if got := probe.callCount(); got != 2 {
		t.Fatalf("probe count before second backoff elapsed = %d, want 2", got)
	}

	worker.runOnce(context.Background(), start.Add(180*time.Second))
	if got := probe.callCount(); got != 3 {
		t.Fatalf("probe count after capped second backoff = %d, want 3", got)
	}
	attempt := worker.currentAttempt(7)
	if attempt.Failures != 3 {
		t.Fatalf("failure count = %d, want 3", attempt.Failures)
	}
	if got := attempt.NextAttempt.Sub(start.Add(180 * time.Second)); got != 120*time.Second {
		t.Fatalf("third backoff = %s, want 2m", got)
	}
}

func TestAccountRecoveryWorkerDoesNotRecoverFailedProbe(t *testing.T) {
	repo := &recoveryRepoStub{accounts: map[int64]Account{
		9: {ID: 9, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusError, Schedulable: false},
	}}
	probe := &recoveryProbeStub{results: map[int64]*ScheduledTestResult{}, errors: map[int64]error{9: errors.New("upstream unavailable")}}
	recoverer := &recoveryStateStub{repo: repo}
	worker := newAccountRecoveryWorker(repo, probe, recoverer, recoveryWorkerTestConfig())

	worker.runOnce(context.Background(), time.Unix(1_700_000_000, 0))

	if len(recoverer.calls) != 0 {
		t.Fatalf("recover called after failed probe: %v", recoverer.calls)
	}
	got, _ := repo.GetByID(context.Background(), 9)
	if got.Status != StatusError || got.Schedulable {
		t.Fatalf("failed account state changed: status=%s schedulable=%t", got.Status, got.Schedulable)
	}
}

func TestTruncateRecoveryErrorRedactsCredentials(t *testing.T) {
	got := truncateRecoveryError("Authorization: Bearer secret-token and api_key=sk-example_secret")
	if got != "Authorization: Bearer [redacted] and api_key=[redacted]" {
		t.Fatalf("redacted error = %q", got)
	}
}

func TestAccountRecoveryWorkerClearsBackoffAfterSuccess(t *testing.T) {
	repo := &recoveryRepoStub{accounts: map[int64]Account{
		11: {ID: 11, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusError, Schedulable: false},
	}}
	probe := &recoveryProbeStub{results: map[int64]*ScheduledTestResult{11: {Status: "success"}}, errors: map[int64]error{}}
	recoverer := &recoveryStateStub{repo: repo}
	worker := newAccountRecoveryWorker(repo, probe, recoverer, recoveryWorkerTestConfig())
	worker.attempts[11] = accountRecoveryAttempt{Failures: 2, NextAttempt: time.Unix(1_700_000_000, 0)}

	worker.runOnce(context.Background(), time.Unix(1_700_000_001, 0))

	if _, exists := worker.attempts[11]; exists {
		t.Fatal("backoff state was not cleared after successful recovery")
	}
	if len(recoverer.calls) != 1 || recoverer.calls[0] != 11 {
		t.Fatalf("recover calls = %v, want [11]", recoverer.calls)
	}
}
