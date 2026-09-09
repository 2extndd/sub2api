package service

import (
	"context"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
)

// mergeAccountDenialsFromContext combines transient failover exclusions with
// the authenticated user's persistent account deny-list. It returns a copy so
// retries can extend their own exclusion set without mutating caller state.
func mergeAccountDenialsFromContext(ctx context.Context, excludedIDs map[int64]struct{}) map[int64]struct{} {
	deniedIDs, _ := ctx.Value(ctxkey.DeniedAccountIDs).([]int64)
	if len(deniedIDs) == 0 {
		return excludedIDs
	}

	merged := make(map[int64]struct{}, len(excludedIDs)+len(deniedIDs))
	for id := range excludedIDs {
		merged[id] = struct{}{}
	}
	for _, id := range deniedIDs {
		if id > 0 {
			merged[id] = struct{}{}
		}
	}
	return merged
}
