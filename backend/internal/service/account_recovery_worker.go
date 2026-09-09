package service

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

var (
	recoveryBearerPattern = regexp.MustCompile(`(?i)(authorization\s*[:=]?\s*bearer\s+)[^\s,;]+`)
	recoveryAPIKeyPattern = regexp.MustCompile(`sk-[A-Za-z0-9_-]+`)
)

const (
	defaultAccountRecoveryIntervalSeconds     = 60
	defaultAccountRecoveryBaseBackoffSeconds  = 60
	defaultAccountRecoveryMaxBackoffSeconds   = 1800
	defaultAccountRecoveryProbeTimeoutSeconds = 90
	defaultAccountRecoveryMaxWorkers          = 2
)

type accountRecoveryRepository interface {
	ListAllWithFilters(ctx context.Context, platform, accountType, status, search string, groupID int64, privacyMode string) ([]Account, error)
	GetByID(ctx context.Context, id int64) (*Account, error)
}

type accountRecoveryProbe interface {
	RunTestBackground(ctx context.Context, accountID int64, modelID string) (*ScheduledTestResult, error)
}

type accountRecoveryStateRecoverer interface {
	RecoverAccountAfterSuccessfulTest(ctx context.Context, accountID int64) (*SuccessfulTestRecoveryResult, error)
}

type accountRecoveryAttempt struct {
	Failures    int
	NextAttempt time.Time
	LastError   string
}

// AccountRecoveryWorker probes only OpenAI API-key accounts in error state.
// Healthy accounts and manually unschedulable active accounts are never probed.
type AccountRecoveryWorker struct {
	repo      accountRecoveryRepository
	probe     accountRecoveryProbe
	recoverer accountRecoveryStateRecoverer
	cfg       config.AccountRecoveryConfig

	attemptMu sync.Mutex
	attempts  map[int64]accountRecoveryAttempt

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
	startOnce   sync.Once
	stopOnce    sync.Once
}

func NewAccountRecoveryWorker(
	repo AccountRepository,
	probe *AccountTestService,
	recoverer *RateLimitService,
	cfg *config.Config,
) *AccountRecoveryWorker {
	workerCfg := config.AccountRecoveryConfig{}
	if cfg != nil {
		workerCfg = cfg.AccountRecovery
	}
	return newAccountRecoveryWorker(repo, probe, recoverer, workerCfg)
}

func newAccountRecoveryWorker(
	repo accountRecoveryRepository,
	probe accountRecoveryProbe,
	recoverer accountRecoveryStateRecoverer,
	cfg config.AccountRecoveryConfig,
) *AccountRecoveryWorker {
	cfg = normalizeAccountRecoveryConfig(cfg)
	return &AccountRecoveryWorker{
		repo:      repo,
		probe:     probe,
		recoverer: recoverer,
		cfg:       cfg,
		attempts:  make(map[int64]accountRecoveryAttempt),
	}
}

func normalizeAccountRecoveryConfig(cfg config.AccountRecoveryConfig) config.AccountRecoveryConfig {
	if cfg.IntervalSeconds <= 0 {
		cfg.IntervalSeconds = defaultAccountRecoveryIntervalSeconds
	}
	if cfg.BaseBackoffSeconds <= 0 {
		cfg.BaseBackoffSeconds = defaultAccountRecoveryBaseBackoffSeconds
	}
	if cfg.MaxBackoffSeconds < cfg.BaseBackoffSeconds {
		cfg.MaxBackoffSeconds = defaultAccountRecoveryMaxBackoffSeconds
		if cfg.MaxBackoffSeconds < cfg.BaseBackoffSeconds {
			cfg.MaxBackoffSeconds = cfg.BaseBackoffSeconds
		}
	}
	if cfg.ProbeTimeoutSeconds <= 0 {
		cfg.ProbeTimeoutSeconds = defaultAccountRecoveryProbeTimeoutSeconds
	}
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = defaultAccountRecoveryMaxWorkers
	}
	cfg.ProbeModel = strings.TrimSpace(cfg.ProbeModel)
	return cfg
}

func (w *AccountRecoveryWorker) Start() {
	if w == nil {
		return
	}
	w.startOnce.Do(func() {
		if !w.cfg.Enabled {
			slog.Info("account_recovery_disabled")
			return
		}
		if w.repo == nil || w.probe == nil || w.recoverer == nil {
			slog.Error("account_recovery_not_started", "reason", "missing_dependency")
			return
		}
		if w.cfg.ProbeModel == "" {
			slog.Error("account_recovery_not_started", "reason", "probe_model_required")
			return
		}

		ctx, cancel := context.WithCancel(context.Background())
		w.lifecycleMu.Lock()
		w.cancel = cancel
		w.done = make(chan struct{})
		done := w.done
		w.lifecycleMu.Unlock()

		go w.loop(ctx, done)
		slog.Info("account_recovery_started",
			"interval_seconds", w.cfg.IntervalSeconds,
			"base_backoff_seconds", w.cfg.BaseBackoffSeconds,
			"max_backoff_seconds", w.cfg.MaxBackoffSeconds,
			"probe_timeout_seconds", w.cfg.ProbeTimeoutSeconds,
			"max_workers", w.cfg.MaxWorkers,
			"probe_model", w.cfg.ProbeModel,
		)
	})
}

func (w *AccountRecoveryWorker) Stop() {
	if w == nil {
		return
	}
	w.stopOnce.Do(func() {
		w.lifecycleMu.Lock()
		cancel := w.cancel
		done := w.done
		w.lifecycleMu.Unlock()
		if cancel == nil || done == nil {
			return
		}
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			slog.Warn("account_recovery_stop_timeout")
		}
	})
}

func (w *AccountRecoveryWorker) loop(ctx context.Context, done chan struct{}) {
	defer close(done)

	// A short startup delay avoids competing with migrations and cache warmup.
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}

	w.runOnce(ctx, time.Now())
	ticker := time.NewTicker(time.Duration(w.cfg.IntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			w.runOnce(ctx, now)
		}
	}
}

func (w *AccountRecoveryWorker) runOnce(ctx context.Context, now time.Time) {
	accounts, err := w.repo.ListAllWithFilters(ctx, PlatformOpenAI, AccountTypeAPIKey, StatusError, "", 0, "")
	if err != nil {
		slog.Error("account_recovery_scan_failed", "error", err)
		return
	}

	candidates := make([]Account, 0)
	candidateIDs := make(map[int64]struct{})
	for _, account := range accounts {
		if !account.IsOpenAIApiKey() || account.Status != StatusError {
			continue
		}
		candidateIDs[account.ID] = struct{}{}
		if w.isDue(account.ID, now) {
			candidates = append(candidates, account)
		}
	}
	w.dropInactiveAttempts(candidateIDs)
	if len(candidates) == 0 {
		return
	}

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })
	slog.Info("account_recovery_candidates", "count", len(candidates))

	sem := make(chan struct{}, w.cfg.MaxWorkers)
	var wg sync.WaitGroup
	for i := range candidates {
		account := candidates[i]
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			w.probeOne(ctx, account.ID, now)
		}()
	}
	wg.Wait()
}

func (w *AccountRecoveryWorker) probeOne(parent context.Context, accountID int64, now time.Time) {
	attempt := w.currentAttempt(accountID).Failures + 1
	slog.Info("account_recovery_probe_started", "account_id", accountID, "attempt", attempt, "probe_model", w.cfg.ProbeModel)

	probeCtx, cancelProbe := context.WithTimeout(parent, time.Duration(w.cfg.ProbeTimeoutSeconds)*time.Second)
	startedAt := time.Now()
	result, err := w.probe.RunTestBackground(probeCtx, accountID, w.cfg.ProbeModel)
	cancelProbe()
	latencyMs := time.Since(startedAt).Milliseconds()
	if err != nil {
		w.recordFailure(accountID, now, fmt.Sprintf("probe error: %v", err), latencyMs)
		return
	}
	if result == nil || result.Status != "success" {
		message := "probe returned no result"
		if result != nil {
			message = result.ErrorMessage
			if strings.TrimSpace(message) == "" {
				message = "probe status " + result.Status
			}
		}
		w.recordFailure(accountID, now, message, latencyMs)
		return
	}

	recoveryCtx, cancelRecovery := context.WithTimeout(parent, 15*time.Second)
	defer cancelRecovery()

	// Avoid reviving an account that an operator changed while the paid probe ran.
	latest, err := w.repo.GetByID(recoveryCtx, accountID)
	if err != nil {
		w.recordFailure(accountID, now, fmt.Sprintf("post-probe account read: %v", err), latencyMs)
		return
	}
	if latest.Status != StatusError {
		w.clearAttempt(accountID)
		slog.Info("account_recovery_skipped_after_probe", "account_id", accountID, "reason", "state_changed")
		return
	}

	recovery, err := w.recoverer.RecoverAccountAfterSuccessfulTest(recoveryCtx, accountID)
	if err != nil {
		w.recordFailure(accountID, now, fmt.Sprintf("state recovery: %v", err), latencyMs)
		return
	}
	verified, err := w.repo.GetByID(recoveryCtx, accountID)
	if err != nil {
		w.recordFailure(accountID, now, fmt.Sprintf("recovery verification read: %v", err), latencyMs)
		return
	}
	if verified.Status != StatusActive || !verified.Schedulable {
		w.recordFailure(accountID, now, fmt.Sprintf("recovery verification failed: status=%s schedulable=%t", verified.Status, verified.Schedulable), latencyMs)
		return
	}

	w.clearAttempt(accountID)
	slog.Info("account_recovery_succeeded",
		"account_id", accountID,
		"attempt", attempt,
		"latency_ms", latencyMs,
		"cleared_error", recovery != nil && recovery.ClearedError,
		"cleared_rate_limit", recovery != nil && recovery.ClearedRateLimit,
	)
}

func (w *AccountRecoveryWorker) isDue(accountID int64, now time.Time) bool {
	w.attemptMu.Lock()
	defer w.attemptMu.Unlock()
	attempt, exists := w.attempts[accountID]
	return !exists || !now.Before(attempt.NextAttempt)
}

func (w *AccountRecoveryWorker) currentAttempt(accountID int64) accountRecoveryAttempt {
	w.attemptMu.Lock()
	defer w.attemptMu.Unlock()
	return w.attempts[accountID]
}

func (w *AccountRecoveryWorker) recordFailure(accountID int64, now time.Time, message string, latencyMs int64) {
	message = truncateRecoveryError(message)
	w.attemptMu.Lock()
	attempt := w.attempts[accountID]
	attempt.Failures++
	attempt.LastError = message
	backoff := w.backoffForFailure(attempt.Failures)
	attempt.NextAttempt = now.Add(backoff)
	w.attempts[accountID] = attempt
	w.attemptMu.Unlock()

	slog.Warn("account_recovery_probe_failed",
		"account_id", accountID,
		"failure_count", attempt.Failures,
		"latency_ms", latencyMs,
		"next_attempt_at", attempt.NextAttempt,
		"backoff_seconds", int(backoff.Seconds()),
		"error", message,
	)
}

func (w *AccountRecoveryWorker) backoffForFailure(failures int) time.Duration {
	base := time.Duration(w.cfg.BaseBackoffSeconds) * time.Second
	maxBackoff := time.Duration(w.cfg.MaxBackoffSeconds) * time.Second
	backoff := base
	for i := 1; i < failures && backoff < maxBackoff; i++ {
		if backoff > maxBackoff/2 {
			return maxBackoff
		}
		backoff *= 2
	}
	if backoff > maxBackoff {
		return maxBackoff
	}
	return backoff
}

func (w *AccountRecoveryWorker) clearAttempt(accountID int64) {
	w.attemptMu.Lock()
	delete(w.attempts, accountID)
	w.attemptMu.Unlock()
}

func (w *AccountRecoveryWorker) dropInactiveAttempts(active map[int64]struct{}) {
	w.attemptMu.Lock()
	defer w.attemptMu.Unlock()
	for accountID := range w.attempts {
		if _, ok := active[accountID]; !ok {
			delete(w.attempts, accountID)
		}
	}
}

func truncateRecoveryError(message string) string {
	message = recoveryBearerPattern.ReplaceAllString(message, "${1}[redacted]")
	message = recoveryAPIKeyPattern.ReplaceAllString(message, "[redacted]")
	message = strings.Join(strings.Fields(message), " ")
	const maxLength = 240
	if len(message) > maxLength {
		return message[:maxLength] + "..."
	}
	return message
}
