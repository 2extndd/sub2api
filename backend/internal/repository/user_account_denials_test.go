package repository

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUserAccountDenialPolicyRepository(t *testing.T) {
	t.Run("reads revision and active account ids", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		repo := newUserRepositoryWithSQL(nil, db)
		mock.ExpectQuery(`SELECT COALESCE\(p\.revision, 0\), a\.id`).
			WithArgs(int64(7)).
			WillReturnRows(sqlmock.NewRows([]string{"revision", "account_id"}).
				AddRow(int64(3), int64(85)).
				AddRow(int64(3), int64(95)))

		ids, revision, err := repo.GetDeniedAccountPolicy(context.Background(), 7)

		require.NoError(t, err)
		require.Equal(t, int64(3), revision)
		require.Equal(t, []int64{85, 95}, ids)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns revision zero for an absent policy", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		repo := newUserRepositoryWithSQL(nil, db)
		mock.ExpectQuery(`SELECT COALESCE\(p\.revision, 0\), a\.id`).
			WithArgs(int64(7)).
			WillReturnRows(sqlmock.NewRows([]string{"revision", "account_id"}).AddRow(int64(0), nil))

		ids, revision, err := repo.GetDeniedAccountPolicy(context.Background(), 7)

		require.NoError(t, err)
		require.Zero(t, revision)
		require.Empty(t, ids)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("lists the account-side user projection", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		repo := newUserRepositoryWithSQL(nil, db)
		mock.ExpectQuery(`SELECT u\.id, u\.email`).
			WithArgs(int64(85)).
			WillReturnRows(sqlmock.NewRows([]string{"user_id", "email"}).
				AddRow(int64(7), "admin-visible@example.test"))

		users, err := repo.GetUsersDenyingAccount(context.Background(), 85)

		require.NoError(t, err)
		require.Equal(t, []service.UserAccountDenialUser{{UserID: 7, Email: "admin-visible@example.test"}}, users)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("increments a matching revision", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		repo := newUserRepositoryWithSQL(nil, db)
		mock.ExpectQuery(`WITH bumped AS`).
			WithArgs(int64(7), sqlmock.AnyArg(), int64(3)).
			WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(4)))

		revision, err := repo.ReplaceDeniedAccountPolicy(context.Background(), 7, 3, []int64{85, 95})

		require.NoError(t, err)
		require.Equal(t, int64(4), revision)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("rejects a stale revision without changing rules", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		repo := newUserRepositoryWithSQL(nil, db)
		mock.ExpectQuery(`WITH bumped AS`).
			WithArgs(int64(7), sqlmock.AnyArg(), int64(2)).
			WillReturnRows(sqlmock.NewRows([]string{"revision"}))

		_, err = repo.ReplaceDeniedAccountPolicy(context.Background(), 7, 2, []int64{85})

		require.ErrorIs(t, err, service.ErrUserAccountDenialRevisionConflict)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
