package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

func fingerprint(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:24]
}

func protectedDigest(row keyRow) (string, error) {
	blacklist, err := decodeJSONValue(row.IPBlacklist)
	if err != nil {
		return "", fmt.Errorf("decode ip_blacklist: %w", err)
	}
	whitelist, err := decodeJSONValue(row.IPWhitelist)
	if err != nil {
		return "", fmt.Errorf("decode ip_whitelist: %w", err)
	}
	protected := map[string]any{
		"expires_at":      nullableTimeJSON(row.ExpiresAt),
		"group_id":        nullableInt64JSON(row.GroupID),
		"ip_blacklist":    blacklist,
		"ip_whitelist":    whitelist,
		"key":             row.Key,
		"quota":           row.Quota,
		"quota_used":      row.QuotaUsed,
		"rate_limit_1d":   row.RateLimit1d,
		"rate_limit_5h":   row.RateLimit5h,
		"rate_limit_7d":   row.RateLimit7d,
		"status":          row.Status,
		"usage_1d":        row.Usage1d,
		"usage_5h":        row.Usage5h,
		"usage_7d":        row.Usage7d,
		"window_1d_start": nullableTimeJSON(row.Window1dStart),
		"window_5h_start": nullableTimeJSON(row.Window5hStart),
		"window_7d_start": nullableTimeJSON(row.Window7dStart),
	}
	raw, err := json.Marshal(protected)
	if err != nil {
		return "", fmt.Errorf("marshal protected state: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func decodeJSONValue(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func nullableTimeJSON(value sql.NullTime) any {
	if value.Valid {
		return value.Time.Format(time.RFC3339Nano)
	}
	return nil
}

func nullableInt64JSON(value sql.NullInt64) any {
	if value.Valid {
		return value.Int64
	}
	return nil
}
