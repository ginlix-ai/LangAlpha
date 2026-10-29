"""Migration 092: a subscription connection that needs signing in again.

A connected ChatGPT or Claude account is kept alive by refreshing its access
token, and the refresh can fail for good: the provider revoked the refresh
token, it expired, or (Codex refresh tokens are single use) it was spent.
Until now that failure was only a log line, so the account read as connected
while every turn on it was refused.

``user_oauth_tokens.needs_reauth_at`` is when a refresh first failed for
good, and ``last_error_code`` the provider's code for it (``invalid_grant``,
``refresh_token_reused``, ...). The write is conditioned on the refresh token
the failing attempt used still being the stored one, so a request that lost
a race with a successful refresh, or with a reconnect, never flags the row.
A connect or a successful refresh clears both. A transient failure (network,
5xx, 429, lock timeout) writes nothing.

Both columns are nullable with no default, so adding them rewrites no rows.
The previous build lists its columns by name and never reads these.

Rollback: ``alembic downgrade 091``. It drops the columns, and a connection
that needed signing in reads as connected again until its next refresh fails.
"""

from alembic import op

revision = "092"
down_revision = "091"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE user_oauth_tokens "
        "ADD COLUMN IF NOT EXISTS needs_reauth_at TIMESTAMPTZ NULL, "
        "ADD COLUMN IF NOT EXISTS last_error_code TEXT NULL"
    )
    op.execute("RESET lock_timeout")


def downgrade() -> None:
    op.execute("SET lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE user_oauth_tokens "
        "DROP COLUMN IF EXISTS needs_reauth_at, "
        "DROP COLUMN IF EXISTS last_error_code"
    )
    op.execute("RESET lock_timeout")
