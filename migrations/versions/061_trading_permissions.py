"""Migration 061: each user's trading permission, and the record of every change.

``trading_permissions`` holds one row per user who has ever chosen a level; a
user without one is at the default, ``approve_each``, where every live order and
staged instruction asks. ``agreement_version`` is the agreement the user accepted
when the level needs one: a level that skips approval holds only while that
version is the current one, so the readers, not this schema, decide what a stale
acceptance means.

``trading_permission_events`` is append-only and has no foreign key to
``users``, like ``order_attempts``: it is the evidence of what a user agreed to
and when, and an order placed without approval is read against it. Its clock is
the insert's, not the transaction's: two writes for one user queue on the
level's row, and the later one has to sort later.

It also clears every live or staged approval switch stored off. Under the
level an unset switch and one stored off answer the same, so nothing changes
for this build, but the previous build reads a stored ``false`` as never
asking, and it is still finishing turns while a deploy swaps over. Writes keep
the two switches only while they ask.

Rollback: ``alembic stamp 060`` and redeploy the previous build; nothing it runs
reads these tables, and with no live or staged switch stored off, every live
order and staged instruction asks again. ``downgrade`` drops both tables, and
with them the record of every acceptance; the switches stay cleared.
"""

from alembic import op

revision = "061"
down_revision = "060"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_permissions (
            user_id VARCHAR(255) PRIMARY KEY
                REFERENCES users(user_id) ON DELETE CASCADE,
            level TEXT NOT NULL CHECK (
                level IN ('no_trading', 'approve_each', 'plan_first', 'autonomous')
            ),
            agreement_version INTEGER,
            agreed_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_permission_events (
            event_id BIGSERIAL PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            level TEXT NOT NULL,
            agreement_version INTEGER,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_trading_permission_events_user
        ON trading_permission_events (user_id, created_at DESC)
        """
    )
    op.execute(
        """
        UPDATE user_mcp_servers
        SET order_approval = order_approval - 'live'
        WHERE order_approval -> 'live' = 'false'::jsonb
        """
    )
    op.execute(
        """
        UPDATE user_mcp_servers
        SET order_approval = order_approval - 'staged'
        WHERE order_approval -> 'staged' = 'false'::jsonb
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS trading_permission_events")
    op.execute("DROP TABLE IF EXISTS trading_permissions")
