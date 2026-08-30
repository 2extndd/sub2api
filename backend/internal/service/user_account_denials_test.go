package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func TestMergeAccountDenialsFromContext_DefaultAllowsAll(t *testing.T) {
	existing := map[int64]struct{}{9: {}}

	merged := mergeAccountDenialsFromContext(context.Background(), existing)

	require.Equal(t, existing, merged)
}

func TestMergeAccountDenialsFromContext_AddsStaticDenialsWithoutDroppingFailures(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxkey.DeniedAccountIDs, []int64{85, 86, 85})
	existing := map[int64]struct{}{9: {}}

	merged := mergeAccountDenialsFromContext(ctx, existing)

	require.ElementsMatch(t, []int64{9, 85, 86}, accountIDSetKeys(merged))
}

func TestMergeAccountDenialsFromContext_DoesNotMutateCallerMap(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxkey.DeniedAccountIDs, []int64{85})
	existing := map[int64]struct{}{9: {}}

	_ = mergeAccountDenialsFromContext(ctx, existing)

	require.Equal(t, map[int64]struct{}{9: {}}, existing)
}

func TestReplaceUserAccountDenials_NormalizesAndInvalidatesAuthCache(t *testing.T) {
	repo := &userAccountDenialRepoStub{}
	invalidator := &userAccountDenialInvalidatorStub{}
	svc := &adminServiceImpl{userRepo: repo, authCacheInvalidator: invalidator}

	policy, err := svc.ReplaceUserAccountDenials(context.Background(), 7, 3, []int64{95, -1, 85, 95, 0})

	require.NoError(t, err)
	require.Equal(t, int64(4), policy.Revision)
	require.Equal(t, []int64{85, 95}, policy.AccountIDs)
	require.Equal(t, int64(3), repo.expectedRevision)
	require.Equal(t, []int64{85, 95}, repo.replacedIDs)
	require.Equal(t, []int64{7}, invalidator.userIDs)
}

func TestReplaceUserAccountDenials_RejectsStaleRevisionWithoutInvalidation(t *testing.T) {
	repo := &userAccountDenialRepoStub{replaceErr: ErrUserAccountDenialRevisionConflict}
	invalidator := &userAccountDenialInvalidatorStub{}
	svc := &adminServiceImpl{userRepo: repo, authCacheInvalidator: invalidator}

	_, err := svc.ReplaceUserAccountDenials(context.Background(), 7, 2, []int64{85})

	require.ErrorIs(t, err, ErrUserAccountDenialRevisionConflict)
	require.Empty(t, invalidator.userIDs)
}

func TestAPIKeyAuthSnapshotPreservesDeniedAccountIDs(t *testing.T) {
	svc := &APIKeyService{}
	apiKey := &APIKey{
		ID:               10,
		UserID:           20,
		Key:              "test-key",
		Status:           StatusAPIKeyActive,
		User:             &User{ID: 20, Status: StatusActive},
		DeniedAccountIDs: []int64{85, 95},
	}

	snapshot := svc.snapshotFromAPIKey(context.Background(), apiKey)
	restored := svc.snapshotToAPIKey(apiKey.Key, snapshot)

	require.Equal(t, []int64{85, 95}, restored.DeniedAccountIDs)
}

type userAccountDenialRepoStub struct {
	UserRepository
	expectedRevision int64
	replacedIDs      []int64
	replaceErr       error
}

func (r *userAccountDenialRepoStub) GetByID(_ context.Context, id int64) (*User, error) {
	return &User{ID: id, Status: StatusActive}, nil
}

func (r *userAccountDenialRepoStub) GetDeniedAccountPolicy(_ context.Context, _ int64) ([]int64, int64, error) {
	return append([]int64(nil), r.replacedIDs...), 4, nil
}

func (r *userAccountDenialRepoStub) ReplaceDeniedAccountPolicy(_ context.Context, _ int64, expectedRevision int64, accountIDs []int64) (int64, error) {
	r.expectedRevision = expectedRevision
	r.replacedIDs = append([]int64(nil), accountIDs...)
	if r.replaceErr != nil {
		return 0, r.replaceErr
	}
	return expectedRevision + 1, nil
}

func (r *userAccountDenialRepoStub) GetUsersDenyingAccount(context.Context, int64) ([]UserAccountDenialUser, error) {
	return nil, nil
}

type userAccountDenialInvalidatorStub struct {
	userIDs []int64
}

func (s *userAccountDenialInvalidatorStub) InvalidateAuthCacheByKey(context.Context, string)    {}
func (s *userAccountDenialInvalidatorStub) InvalidateAuthCacheByGroupID(context.Context, int64) {}
func (s *userAccountDenialInvalidatorStub) InvalidateAuthCacheByUserID(_ context.Context, userID int64) {
	s.userIDs = append(s.userIDs, userID)
}

func accountIDSetKeys(values map[int64]struct{}) []int64 {
	keys := make([]int64, 0, len(values))
	for id := range values {
		keys = append(keys, id)
	}
	return keys
}
