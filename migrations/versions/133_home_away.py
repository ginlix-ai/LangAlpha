"""Migration 133: when a user's Home ran on the cloud, and how far that is handed back.

A Home turn admitted while the machine Home lives on (the user's own) can't be
reached runs in Home's stand-in, a hidden workspace on one of the user's cloud
computers, and marks the user away here. What it wrote there is owed to Home
while ``last_away_at`` is newer than ``handed_back_through``; one worker at a
time hands it back under an advisory lock. ``last_handback`` is the last
hand-back, for the user's notice until ``handback_seen_at``; which files each
hand-back carried is in ``home_handbacks`` (134). A failed hand-back waits
until ``next_handback_at`` and counts ``handback_attempts`` until it gives up.
``home_id`` is Home's workspace, whose computer the sweep asks after. One row
per user, written only by an upsert or a guarded update.

A new table, so nothing existing is locked. Rollback: the previous build never
reads it, so ``alembic downgrade 132`` drops it once no process of this build
is left.
"""

from alembic import op

revision = "133"
down_revision = "132"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS home_away (
            user_id TEXT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
            home_id UUID NOT NULL,
            away_since TIMESTAMPTZ,
            last_away_at TIMESTAMPTZ,
            handed_back_through TIMESTAMPTZ,
            last_handback JSONB,
            handback_seen_at TIMESTAMPTZ,
            handback_attempts INTEGER NOT NULL DEFAULT 0,
            next_handback_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    # The sweep's scan: users whose cloud work is not handed back yet.
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_home_away_owed ON home_away(last_away_at) "
        "WHERE last_away_at IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS home_away")
