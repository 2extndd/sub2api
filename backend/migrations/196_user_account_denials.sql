-- Per-user upstream account deny-list. Absence of rows preserves the legacy
-- behavior: every otherwise eligible account, including newly created ones,
-- is available to the user.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '55s';

CREATE TABLE IF NOT EXISTS user_account_denial_policies (
    user_id BIGINT PRIMARY KEY,
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT user_account_denial_policies_user_fk
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS user_account_denials (
    user_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, account_id),
    CONSTRAINT user_account_denials_user_fk
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT user_account_denials_account_fk
        FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS user_account_denials_account_id_idx
    ON user_account_denials (account_id);
