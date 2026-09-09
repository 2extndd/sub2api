//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func TestSelectAccountForModelWithExclusions_EnforcesUserDenyList(t *testing.T) {
	groupID := int64(10)
	accountRepo := &mockAccountRepoForPlatform{
		accounts: []Account{
			{ID: 1, Platform: PlatformAnthropic, Priority: 1, Status: StatusActive, Schedulable: true},
			{ID: 2, Platform: PlatformAnthropic, Priority: 2, Status: StatusActive, Schedulable: true},
		},
		accountsByID: map[int64]*Account{},
	}
	for i := range accountRepo.accounts {
		accountRepo.accountsByID[accountRepo.accounts[i].ID] = &accountRepo.accounts[i]
	}
	groupRepo := &mockGroupRepoForGateway{groups: map[int64]*Group{
		groupID: {ID: groupID, Platform: PlatformAnthropic, Status: StatusActive, Hydrated: true},
	}}
	svc := &GatewayService{accountRepo: accountRepo, groupRepo: groupRepo, cfg: testConfig()}
	ctx := context.WithValue(context.Background(), ctxkey.Group, groupRepo.groups[groupID])
	ctx = context.WithValue(ctx, ctxkey.DeniedAccountIDs, []int64{1})

	account, err := svc.SelectAccountForModelWithExclusions(ctx, &groupID, "", "", nil)

	require.NoError(t, err)
	require.NotNil(t, account)
	require.Equal(t, int64(2), account.ID)
}
