package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

var safeCorrelationID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$`)

func validateOptions(opts runOptions) error {
	if opts.BatchSize < 1 || opts.BatchSize > maxBatchSize {
		return fmt.Errorf("--batch-size must be between 1 and %d", maxBatchSize)
	}
	if opts.Direction != "forward" && opts.Direction != "rollback" {
		return errors.New("--direction must be forward or rollback")
	}
	if opts.OnlyID > 0 && opts.AfterID > 0 {
		return errors.New("--only-id and --after-id cannot be combined")
	}
	if !opts.Execute {
		return nil
	}
	if len(strings.TrimSpace(opts.ExpectedChecksum)) != sha256.Size*2 {
		return errors.New("execute mode requires a full --manifest-sha256")
	}
	if !safeCorrelationID.MatchString(opts.CorrelationID) {
		return errors.New("execute mode requires a safe --correlation-id (1-96 allowlisted characters)")
	}
	if opts.BatchNumber < 1 {
		return errors.New("execute mode requires --batch-number >= 1")
	}
	return nil
}

func run(ctx context.Context, db *sql.DB, cache service.APIKeyCache, value *manifest, manifestChecksum string, exclusions map[int64]struct{}, opts runOptions) (runSummary, error) {
	if err := validateOptions(opts); err != nil {
		return runSummary{}, err
	}
	if value == nil {
		return runSummary{}, errors.New("manifest is nil")
	}
	if value.Schema == subsetManifestSchema {
		if len(strings.TrimSpace(opts.ExpectedChecksum)) != sha256.Size*2 || !strings.EqualFold(opts.ExpectedChecksum, manifestChecksum) {
			return runSummary{}, errors.New("exact subset dry-run and execute require the matching --manifest-sha256")
		}
		if len(exclusions) != 0 || opts.OnlyID > 0 || opts.AfterID > 0 || opts.Direction != "forward" || opts.AllowProtectedDrift || opts.RepairCacheOnly {
			return runSummary{}, errors.New("exact subset mode forbids exclusions, partial selection, rollback direction, protected drift, and cache-only repair")
		}
		if opts.BatchSize < requiredSubsetSize {
			return runSummary{}, fmt.Errorf("exact subset mode requires --batch-size >= %d", requiredSubsetSize)
		}
	} else if opts.Execute && !strings.EqualFold(opts.ExpectedChecksum, manifestChecksum) {
		return runSummary{}, errors.New("manifest checksum mismatch")
	}
	entries, summary, err := selectEntries(value, exclusions, opts)
	if err != nil {
		return summary, err
	}
	if len(entries) == 0 {
		return summary, errors.New("no transferable non-excluded rows selected")
	}
	if value.Schema == subsetManifestSchema && len(entries) != requiredSubsetSize {
		return summary, fmt.Errorf("exact subset mode requires all %d rows, selected %d", requiredSubsetSize, len(entries))
	}

	candidates := make([]validatedCandidate, 0, len(entries))
	for _, entry := range entries {
		var candidate validatedCandidate
		var err error
		if opts.RepairCacheOnly {
			candidate, err = validateCommittedCandidate(ctx, db, entry, opts.Direction, opts.AllowProtectedDrift)
		} else {
			candidate, err = validateCandidate(ctx, db, entry, opts.Direction, opts.AllowProtectedDrift)
		}
		if err != nil {
			return summary, err
		}
		candidates = append(candidates, candidate)
		summary.Validated++
		summary.HistoryRows += candidate.History.UsageRows
		summary.BillingRows += candidate.History.BillingRows
		if !candidate.ManifestProtectedMatch {
			summary.ProtectedDrift++
		}
	}
	if !opts.Execute {
		return summary, nil
	}
	if cache == nil {
		return summary, errors.New("execute mode requires an authentication cache client")
	}

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Row.ID < candidates[j].Row.ID })
	if opts.RepairCacheOnly {
		return repairCommittedCache(ctx, db, cache, candidates, summary, opts, manifestChecksum)
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return summary, fmt.Errorf("begin relocation batch: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	lockedRows := make([]keyRow, 0, len(candidates))
	for _, candidate := range candidates {
		locked, err := lockCandidate(ctx, tx, candidate, opts)
		if err != nil {
			return summary, err
		}
		lockedRows = append(lockedRows, locked)
	}
	updatedRows := make([]keyRow, 0, len(candidates))
	for i, candidate := range candidates {
		updated, err := relocateLocked(ctx, tx, candidate, lockedRows[i], opts, manifestChecksum)
		if err != nil {
			return summary, err
		}
		updatedRows = append(updatedRows, updated)
	}
	if err := tx.Commit(); err != nil {
		return summary, fmt.Errorf("commit relocation batch: %w", err)
	}
	summary.Moved = len(updatedRows)

	for i, row := range updatedRows {
		afterHistory, err := historyForKey(ctx, db, row.ID, candidates[i].HistoryCutoff)
		if err != nil {
			return summary, err
		}
		if err := compareHistory(candidates[i].History, afterHistory, false); err != nil {
			return summary, fmt.Errorf("key row %d history invariant: %w", row.ID, err)
		}
		cacheKey := authSnapshotCacheKey(row.Key)
		if err := cache.DeleteAuthCache(ctx, cacheKey); err != nil {
			return summary, fmt.Errorf("cache delete failed after committed key row %d: %w", row.ID, err)
		}
		if err := cache.PublishAuthCacheInvalidation(ctx, cacheKey); err != nil {
			return summary, fmt.Errorf("cache publish failed after committed key row %d: %w", row.ID, err)
		}
		summary.CacheCleared++
	}
	return summary, nil
}

func repairCommittedCache(ctx context.Context, db *sql.DB, cache service.APIKeyCache, candidates []validatedCandidate, summary runSummary, opts runOptions, manifestChecksum string) (runSummary, error) {
	for _, candidate := range candidates {
		proof, err := loadRelocationAuditProof(ctx, db, candidate.Row.ID, opts, manifestChecksum)
		if err != nil {
			return summary, fmt.Errorf("verify relocation provenance for key row %d: %w", candidate.Row.ID, err)
		}
		history, err := historyForKey(ctx, db, candidate.Row.ID, proof.HistoryCutoff)
		if err != nil {
			return summary, err
		}
		if historyDigest(history) != proof.HistoryDigest {
			return summary, fmt.Errorf("key row %d history proof mismatch during cache repair", candidate.Row.ID)
		}
		cacheKey := authSnapshotCacheKey(candidate.Row.Key)
		if err := cache.DeleteAuthCache(ctx, cacheKey); err != nil {
			return summary, fmt.Errorf("cache repair delete failed for key row %d: %w", candidate.Row.ID, err)
		}
		if err := cache.PublishAuthCacheInvalidation(ctx, cacheKey); err != nil {
			return summary, fmt.Errorf("cache repair publish failed for key row %d: %w", candidate.Row.ID, err)
		}
		if err := insertCacheRepairAudit(ctx, db, candidate.Row.ID, opts, manifestChecksum); err != nil {
			return summary, fmt.Errorf("cache repair audit failed for key row %d: %w", candidate.Row.ID, err)
		}
		summary.CacheCleared++
	}
	return summary, nil
}

// authSnapshotCacheKey mirrors APIKeyService.authCacheKey. The repository
// cache accepts this SHA-256 identifier, not the plaintext credential.
func authSnapshotCacheKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func safeChecksumRef(checksum string) string {
	if len(checksum) < 12 {
		return "invalid"
	}
	return checksum[:12]
}

func formatSummary(summary runSummary, execute bool, checksum string, direction string) string {
	mode := "dry-run"
	if execute {
		mode = "execute"
	}
	ids := make([]string, 0, len(summary.SelectedIDs))
	for _, id := range summary.SelectedIDs {
		ids = append(ids, fmt.Sprintf("%d", id))
	}
	return fmt.Sprintf(
		"mode=%s direction=%s manifest_total=%d correct=%d transfer=%d absent=%d excluded=%d selected=%d validated=%d protected_live_drift=%d moved=%d cache_invalidated=%d usage_rows_preserved=%d billing_rows_preserved=%d selected_ids=%s manifest_ref=%s",
		mode, direction, summary.ManifestTotal, summary.Correct, summary.Transfer, summary.Absent,
		summary.Excluded, summary.Selected, summary.Validated, summary.ProtectedDrift, summary.Moved, summary.CacheCleared,
		summary.HistoryRows, summary.BillingRows, strings.Join(ids, ","), safeChecksumRef(checksum),
	)
}

func waitForContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
