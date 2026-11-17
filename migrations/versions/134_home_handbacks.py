"""Migration 134: which files each hand-back of Home's stand-in work carried.

A Home turn in Home's stand-in names its files by their path in the stand-in's
folder. A hand-back copies them into a dated folder of their own in Home, so
the path a file card holds no longer leads to them there. One row per
hand-back keeps the folder and the files it carried, by path with the
``[size, mtime]`` each was listed with, and the paths it found as an earlier
one carried them (``kept``), each with the folder the latest of those filled,
so a row may copy nothing and fill no folder. A card of such a turn reads
them to open the copy that came back with it (the first hand-back after the
turn that carried or kept it), and the next hand-back carries only what
differs from every earlier one. ``user_id`` references ``users`` with
``ON UPDATE CASCADE``, so the rows follow ``migrate_user_id`` to the user's
new id.

A new table, so nothing existing is locked. Rollback: the previous build never
reads it, so ``alembic downgrade 133`` drops it once no process of this build
is left.
"""

from alembic import op

revision = "134"
down_revision = "133"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS home_handbacks (
            handback_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id TEXT NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
            through TIMESTAMPTZ NOT NULL,
            folder TEXT,
            files JSONB NOT NULL,
            kept JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_home_handbacks_user "
        "ON home_handbacks(user_id, through)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS home_handbacks")
