package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const keySelectColumns = `
	id, user_id, key, quota, quota_used, expires_at, group_id, status,
	COALESCE(ip_blacklist, 'null'::jsonb), COALESCE(ip_whitelist, 'null'::jsonb),
	rate_limit_5h, rate_limit_1d, rate_limit_7d,
	usage_5h, usage_1d, usage_7d,
	window_5h_start, window_1d_start, window_7d_start,
	created_at, updated_at, deleted_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanKey(scanner rowScanner) (keyRow, error) {
	var row keyRow
	if err := scanner.Scan(
		&row.ID, &row.UserID, &row.Key, &row.Quota, &row.QuotaUsed,
		&row.ExpiresAt, &row.GroupID, &row.Status,
		&row.IPBlacklist, &row.IPWhitelist,
		&row.RateLimit5h, &row.RateLimit1d, &row.RateLimit7d,
		&row.Usage5h, &row.Usage1d, &row.Usage7d,
		&row.Window5hStart, &row.Window1dStart, &row.Window7dStart,
		&row.CreatedAt, &row.UpdatedAt, &row.DeletedAt,
	); err != nil {
		return keyRow{}, err
	}
	return row, nil
}

func loadKey(ctx context.Context, db *sql.DB, id int64, lock bool, tx *sql.Tx) (keyRow, error) {
	query := "SELECT " + keySelectColumns + " FROM api_keys WHERE id = $1"
	if lock {
		query += " FOR UPDATE"
	}
	var scanner rowScanner
	if tx != nil {
		scanner = tx.QueryRowContext(ctx, query, id)
	} else {
		scanner = db.QueryRowContext(ctx, query, id)
	}
	row, err := scanKey(scanner)
	if errors.Is(err, sql.ErrNoRows) {
		return keyRow{}, fmt.Errorf("remote key row %d not found", id)
	}
	if err != nil {
		return keyRow{}, fmt.Errorf("load remote key row %d: %w", id, err)
	}
	return row, nil
}

func historyForKey(ctx context.Context, db *sql.DB, id int64, cutoff time.Time) (historySnapshot, error) {
	var value historySnapshot
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(total_cost), 0),
		       COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0),
		       MIN(created_at), MAX(created_at), COUNT(DISTINCT user_id)
		FROM usage_logs WHERE api_key_id = $1 AND created_at <= $2`, id, cutoff).Scan(
		&value.UsageRows, &value.TotalCost, &value.InputTokens, &value.OutputTokens,
		&value.FirstUsage, &value.LastUsage, &value.DistinctUserRows,
	)
	if err != nil {
		return historySnapshot{}, fmt.Errorf("load usage history for key row %d: %w", id, err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COUNT(*) FILTER (WHERE applied IS TRUE), COALESCE(SUM(delta_usd), 0)
		FROM billing_usage_entries WHERE api_key_id = $1 AND created_at <= $2`, id, cutoff).Scan(
		&value.BillingRows, &value.BillingApplied, &value.BillingDeltaUSD,
	); err != nil {
		return historySnapshot{}, fmt.Errorf("load billing history for key row %d: %w", id, err)
	}
	return value, nil
}

func validateCandidate(ctx context.Context, db *sql.DB, entry manifestEntry, direction string, allowProtectedDrift bool) (validatedCandidate, error) {
	if entry.RemoteKeyID == nil || entry.CurrentUserID == nil || entry.Fingerprint == nil || entry.ProtectedDigest == nil {
		return validatedCandidate{}, errors.New("manifest entry is missing required fields")
	}
	row, err := loadKey(ctx, db, *entry.RemoteKeyID, false, nil)
	if err != nil {
		return validatedCandidate{}, err
	}
	if row.DeletedAt.Valid {
		return validatedCandidate{}, fmt.Errorf("remote key row %d is soft deleted", row.ID)
	}
	if !row.GroupID.Valid || row.GroupID.Int64 != 8 {
		return validatedCandidate{}, fmt.Errorf("remote key row %d must remain in group 8", row.ID)
	}
	if err := validateUniqueKeyValue(ctx, db, row); err != nil {
		return validatedCandidate{}, err
	}
	expectedCurrent, expectedTarget := ownerDirection(entry, direction)
	if row.UserID != expectedCurrent {
		return validatedCandidate{}, fmt.Errorf("remote key row %d owner mismatch: got %d expected %d", row.ID, row.UserID, expectedCurrent)
	}
	if expectedCurrent == expectedTarget {
		return validatedCandidate{}, fmt.Errorf("remote key row %d has identical source and target owner", row.ID)
	}
	actualFingerprint := fingerprint(row.Key)
	if actualFingerprint != *entry.Fingerprint {
		return validatedCandidate{}, fmt.Errorf("remote key row %d fingerprint mismatch", row.ID)
	}
	actualProtected, err := protectedDigest(row)
	if err != nil {
		return validatedCandidate{}, fmt.Errorf("remote key row %d protected digest: %w", row.ID, err)
	}
	manifestProtectedMatch := actualProtected == *entry.ProtectedDigest
	if !manifestProtectedMatch && !allowProtectedDrift {
		return validatedCandidate{}, fmt.Errorf("remote key row %d protected-state mismatch", row.ID)
	}
	if err := validateTargetOwnerAndGroup(ctx, db, expectedTarget, row.GroupID); err != nil {
		return validatedCandidate{}, fmt.Errorf("remote key row %d target validation: %w", row.ID, err)
	}
	cutoff := time.Now().UTC()
	history, err := historyForKey(ctx, db, row.ID, cutoff)
	if err != nil {
		return validatedCandidate{}, err
	}
	return validatedCandidate{
		Entry:                  entry,
		Row:                    row,
		ProtectedDigest:        actualProtected,
		Fingerprint:            actualFingerprint,
		History:                history,
		HistoryCutoff:          cutoff,
		ManifestProtectedMatch: manifestProtectedMatch,
	}, nil
}

func validateCommittedCandidate(ctx context.Context, db *sql.DB, entry manifestEntry, direction string, allowProtectedDrift bool) (validatedCandidate, error) {
	if entry.RemoteKeyID == nil || entry.CurrentUserID == nil || entry.Fingerprint == nil || entry.ProtectedDigest == nil {
		return validatedCandidate{}, errors.New("manifest entry is missing required fields")
	}
	row, err := loadKey(ctx, db, *entry.RemoteKeyID, false, nil)
	if err != nil {
		return validatedCandidate{}, err
	}
	originalOwner, relocatedOwner := ownerDirection(entry, direction)
	_ = originalOwner
	if row.DeletedAt.Valid || row.UserID != relocatedOwner {
		return validatedCandidate{}, fmt.Errorf("remote key row %d is not in committed relocated ownership state", row.ID)
	}
	actualFingerprint := fingerprint(row.Key)
	if actualFingerprint != *entry.Fingerprint {
		return validatedCandidate{}, fmt.Errorf("remote key row %d fingerprint mismatch", row.ID)
	}
	actualProtected, err := protectedDigest(row)
	if err != nil {
		return validatedCandidate{}, err
	}
	manifestProtectedMatch := actualProtected == *entry.ProtectedDigest
	if !manifestProtectedMatch && !allowProtectedDrift {
		return validatedCandidate{}, fmt.Errorf("remote key row %d protected-state mismatch", row.ID)
	}
	cutoff := time.Now().UTC()
	history, err := historyForKey(ctx, db, row.ID, cutoff)
	if err != nil {
		return validatedCandidate{}, err
	}
	return validatedCandidate{Entry: entry, Row: row, ProtectedDigest: actualProtected, Fingerprint: actualFingerprint, History: history, HistoryCutoff: cutoff, ManifestProtectedMatch: manifestProtectedMatch}, nil
}

func ownerDirection(entry manifestEntry, direction string) (int64, int64) {
	if direction == "rollback" {
		return entry.ExpectedUserID, *entry.CurrentUserID
	}
	return *entry.CurrentUserID, entry.ExpectedUserID
}

func validateUniqueKeyValue(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, row keyRow) error {
	var duplicates int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_keys WHERE key = $1 AND id <> $2 AND deleted_at IS NULL`, row.Key, row.ID).Scan(&duplicates); err != nil {
		return fmt.Errorf("check duplicate key value for remote row %d: %w", row.ID, err)
	}
	if duplicates != 0 {
		return fmt.Errorf("remote key row %d has %d duplicate key values", row.ID, duplicates)
	}
	return nil
}

func validateTargetOwnerAndGroup(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, targetUserID int64, groupID sql.NullInt64) error {
	var userStatus string
	var userDeleted sql.NullTime
	if err := q.QueryRowContext(ctx, `SELECT status, deleted_at FROM users WHERE id = $1`, targetUserID).Scan(&userStatus, &userDeleted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("target user %d not found", targetUserID)
		}
		return err
	}
	if userDeleted.Valid || userStatus != "active" {
		return fmt.Errorf("target user %d is not live and active", targetUserID)
	}
	if !groupID.Valid {
		return nil
	}
	var groupStatus, subscriptionType string
	var exclusive bool
	if err := q.QueryRowContext(ctx, `SELECT status, is_exclusive, subscription_type FROM groups WHERE id = $1`, groupID.Int64).Scan(&groupStatus, &exclusive, &subscriptionType); err != nil {
		return fmt.Errorf("load group %d: %w", groupID.Int64, err)
	}
	if groupStatus != "active" {
		return fmt.Errorf("group %d is not active", groupID.Int64)
	}
	if subscriptionType != "standard" && strings.TrimSpace(subscriptionType) != "" {
		return fmt.Errorf("group %d uses subscription type %q; relocation requires an explicit subscription policy", groupID.Int64, subscriptionType)
	}
	if exclusive {
		var allowed bool
		if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM user_allowed_groups WHERE user_id = $1 AND group_id = $2)`, targetUserID, groupID.Int64).Scan(&allowed); err != nil {
			return err
		}
		if !allowed {
			return fmt.Errorf("target user %d lacks exclusive group %d entitlement", targetUserID, groupID.Int64)
		}
	}
	return nil
}

func lockCandidate(ctx context.Context, tx *sql.Tx, candidate validatedCandidate, opts runOptions) (keyRow, error) {
	expectedCurrent, expectedTarget := ownerDirection(candidate.Entry, opts.Direction)
	locked, err := loadKey(ctx, nil, candidate.Row.ID, true, tx)
	if err != nil {
		return keyRow{}, err
	}
	if locked.DeletedAt.Valid || locked.UserID != expectedCurrent {
		return keyRow{}, fmt.Errorf("remote key row %d changed before lock", locked.ID)
	}
	if !locked.GroupID.Valid || locked.GroupID.Int64 != 8 {
		return keyRow{}, fmt.Errorf("remote key row %d changed from group 8 before lock", locked.ID)
	}
	if err := validateUniqueKeyValue(ctx, tx, locked); err != nil {
		return keyRow{}, err
	}
	lockedFingerprint := fingerprint(locked.Key)
	lockedProtected, err := protectedDigest(locked)
	if err != nil {
		return keyRow{}, err
	}
	if lockedFingerprint != candidate.Fingerprint || lockedProtected != candidate.ProtectedDigest {
		return keyRow{}, fmt.Errorf("remote key row %d protected state changed before lock", locked.ID)
	}
	if err := validateTargetOwnerAndGroupTx(ctx, tx, expectedTarget, locked.GroupID); err != nil {
		return keyRow{}, err
	}
	return locked, nil
}

func relocateLocked(ctx context.Context, tx *sql.Tx, candidate validatedCandidate, locked keyRow, opts runOptions, manifestChecksum string) (keyRow, error) {
	expectedCurrent, expectedTarget := ownerDirection(candidate.Entry, opts.Direction)
	lockedFingerprint := fingerprint(locked.Key)
	lockedProtected, err := protectedDigest(locked)
	if err != nil {
		return keyRow{}, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE api_keys SET user_id = $1, updated_at = NOW()
		WHERE id = $2 AND user_id = $3 AND deleted_at IS NULL`, expectedTarget, locked.ID, expectedCurrent)
	if err != nil {
		return keyRow{}, fmt.Errorf("update remote key row %d owner: %w", locked.ID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return keyRow{}, err
	}
	if affected != 1 {
		return keyRow{}, fmt.Errorf("remote key row %d affected %d rows, expected 1", locked.ID, affected)
	}
	updated, err := loadKey(ctx, nil, locked.ID, false, tx)
	if err != nil {
		return keyRow{}, err
	}
	updatedProtected, err := protectedDigest(updated)
	if err != nil {
		return keyRow{}, err
	}
	if updated.UserID != expectedTarget || fingerprint(updated.Key) != lockedFingerprint || updatedProtected != lockedProtected || updated.ID != locked.ID || !updated.CreatedAt.Equal(locked.CreatedAt) || updated.DeletedAt.Valid != locked.DeletedAt.Valid {
		return keyRow{}, fmt.Errorf("remote key row %d post-update invariant failed", locked.ID)
	}
	if err := insertRelocationAudit(ctx, tx, locked.ID, expectedCurrent, expectedTarget, lockedFingerprint, lockedProtected, candidate.History, candidate.HistoryCutoff, candidate.ManifestProtectedMatch, opts, manifestChecksum); err != nil {
		return keyRow{}, err
	}
	return updated, nil
}

func validateTargetOwnerAndGroupTx(ctx context.Context, tx *sql.Tx, targetUserID int64, groupID sql.NullInt64) error {
	var userStatus string
	var userDeleted sql.NullTime
	if err := tx.QueryRowContext(ctx, `SELECT status, deleted_at FROM users WHERE id = $1 FOR UPDATE`, targetUserID).Scan(&userStatus, &userDeleted); err != nil {
		return fmt.Errorf("lock target user %d: %w", targetUserID, err)
	}
	if userDeleted.Valid || userStatus != "active" {
		return fmt.Errorf("target user %d is not live and active", targetUserID)
	}
	if !groupID.Valid {
		return nil
	}
	var groupStatus, subscriptionType string
	var exclusive bool
	if err := tx.QueryRowContext(ctx, `SELECT status, is_exclusive, subscription_type FROM groups WHERE id = $1 FOR SHARE`, groupID.Int64).Scan(&groupStatus, &exclusive, &subscriptionType); err != nil {
		return fmt.Errorf("lock group %d: %w", groupID.Int64, err)
	}
	if groupStatus != "active" {
		return fmt.Errorf("group %d is not active", groupID.Int64)
	}
	if subscriptionType != "standard" && strings.TrimSpace(subscriptionType) != "" {
		return fmt.Errorf("group %d uses unsupported subscription type %q", groupID.Int64, subscriptionType)
	}
	if exclusive {
		var allowed bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM user_allowed_groups WHERE user_id = $1 AND group_id = $2)`, targetUserID, groupID.Int64).Scan(&allowed); err != nil {
			return err
		}
		if !allowed {
			return fmt.Errorf("target user %d lacks exclusive group %d entitlement", targetUserID, groupID.Int64)
		}
	}
	return nil
}

func insertRelocationAudit(ctx context.Context, tx *sql.Tx, keyID, oldOwnerID, newOwnerID int64, keyFingerprint, protectedDigest string, history historySnapshot, historyCutoff time.Time, manifestProtectedMatch bool, opts runOptions, manifestChecksum string) error {
	extra, err := json.Marshal(map[string]any{
		"operation":                 "api_key_owner_relocation",
		"remote_key_id":             keyID,
		"old_user_id":               oldOwnerID,
		"new_user_id":               newOwnerID,
		"manifest_sha256":           manifestChecksum,
		"batch_number":              opts.BatchNumber,
		"correlation_id":            opts.CorrelationID,
		"direction":                 opts.Direction,
		"fingerprint":               keyFingerprint,
		"protected_digest_before":   protectedDigest,
		"protected_digest_after":    protectedDigest,
		"manifest_protected_match":  manifestProtectedMatch,
		"protected_state_unchanged": true,
		"usage_rows_preserved":      history.UsageRows,
		"billing_rows_preserved":    history.BillingRows,
		"history_cutoff":            historyCutoff.UTC().Format(time.RFC3339Nano),
		"history_digest":            historyDigest(history),
	})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO audit_logs (
			created_at, actor_user_id, actor_email, actor_role, auth_method,
			credential_masked, action, method, path, request_id, client_ip, user_agent,
			request_body, status_code, latency_ms, extra
		) VALUES (
			NOW(), NULL, '', 'automation', 'internal_cli',
			'', 'admin.api_key.owner_relocated', 'CLI', 'cmd/relocate-api-key-owners', $2, '', '',
			'', 200, 0, $1::jsonb
		)`, string(extra), opts.CorrelationID)
	if err != nil {
		return fmt.Errorf("insert relocation audit for key row %d: %w", keyID, err)
	}
	return nil
}

type relocationAuditProof struct {
	HistoryCutoff time.Time
	HistoryDigest string
}

func loadRelocationAuditProof(ctx context.Context, db *sql.DB, keyID int64, opts runOptions, manifestChecksum string) (relocationAuditProof, error) {
	var cutoffRaw, digest string
	err := db.QueryRowContext(ctx, `
		SELECT extra->>'history_cutoff', extra->>'history_digest'
		FROM audit_logs
		WHERE action = 'admin.api_key.owner_relocated'
		  AND extra->>'remote_key_id' = $1
		  AND extra->>'manifest_sha256' = $2
		  AND extra->>'direction' = $3
		ORDER BY id DESC LIMIT 1`, fmt.Sprintf("%d", keyID), manifestChecksum, opts.Direction).Scan(&cutoffRaw, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return relocationAuditProof{}, fmt.Errorf("no matching committed relocation audit")
	}
	if err != nil {
		return relocationAuditProof{}, err
	}
	cutoff, err := time.Parse(time.RFC3339Nano, cutoffRaw)
	if err != nil || len(digest) != sha256.Size*2 {
		return relocationAuditProof{}, fmt.Errorf("matching relocation audit has invalid history proof")
	}
	return relocationAuditProof{HistoryCutoff: cutoff, HistoryDigest: digest}, nil
}

func insertCacheRepairAudit(ctx context.Context, db *sql.DB, keyID int64, opts runOptions, manifestChecksum string) error {
	extra, err := json.Marshal(map[string]any{
		"operation":         "api_key_owner_relocation_cache_repair",
		"remote_key_id":     keyID,
		"manifest_sha256":   manifestChecksum,
		"batch_number":      opts.BatchNumber,
		"correlation_id":    opts.CorrelationID,
		"direction":         opts.Direction,
		"cache_invalidated": true,
	})
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO audit_logs (
			created_at, actor_user_id, actor_email, actor_role, auth_method,
			credential_masked, action, method, path, request_id, client_ip, user_agent,
			request_body, status_code, latency_ms, extra
		) VALUES (NOW(), NULL, '', 'automation', 'internal_cli', '',
			'admin.api_key.owner_relocation_cache_repaired', 'CLI', 'cmd/relocate-api-key-owners', $2, '', '', '', 200, 0, $1::jsonb)`, string(extra), opts.CorrelationID)
	return err
}

func compareHistory(before, after historySnapshot, allowGrowth bool) error {
	if allowGrowth {
		if after.UsageRows < before.UsageRows || after.BillingRows < before.BillingRows || after.BillingApplied < before.BillingApplied || after.TotalCost < before.TotalCost || after.BillingDeltaUSD < before.BillingDeltaUSD || after.InputTokens < before.InputTokens || after.OutputTokens < before.OutputTokens {
			return errors.New("historical usage or billing counters decreased")
		}
		return nil
	}
	if before.UsageRows != after.UsageRows || before.BillingRows != after.BillingRows || before.BillingApplied != after.BillingApplied || before.BillingDeltaUSD != after.BillingDeltaUSD || before.TotalCost != after.TotalCost || before.InputTokens != after.InputTokens || before.OutputTokens != after.OutputTokens || before.DistinctUserRows != after.DistinctUserRows || !sameNullTime(before.FirstUsage, after.FirstUsage) || !sameNullTime(before.LastUsage, after.LastUsage) {
		return errors.New("historical usage or billing changed during relocation")
	}
	return nil
}

func historyDigest(value historySnapshot) string {
	raw, err := json.Marshal(map[string]any{
		"usage_rows":         value.UsageRows,
		"billing_rows":       value.BillingRows,
		"billing_applied":    value.BillingApplied,
		"billing_delta_usd":  value.BillingDeltaUSD,
		"total_cost":         value.TotalCost,
		"input_tokens":       value.InputTokens,
		"output_tokens":      value.OutputTokens,
		"first_usage":        timestampOrNil(value.FirstUsage),
		"last_usage":         timestampOrNil(value.LastUsage),
		"distinct_user_rows": value.DistinctUserRows,
	})
	if err != nil {
		panic("history snapshot is not JSON serializable")
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func sameNullTime(a, b sql.NullTime) bool {
	if a.Valid != b.Valid {
		return false
	}
	return !a.Valid || a.Time.Equal(b.Time)
}

func timestampOrNil(value sql.NullTime) any {
	if !value.Valid {
		return nil
	}
	return value.Time.UTC().Format(time.RFC3339Nano)
}
