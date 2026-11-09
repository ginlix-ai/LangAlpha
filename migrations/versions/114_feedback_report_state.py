"""Migration 114: a thumbs-down remembers the report it sent to the team.

A thumbs-down is also sent to the platform as a report, and reopening it shows
what the team has: when it arrived and the notes the user added since. The
words live here, not on the platform, which keeps no user text.

- ``report_id``: the report a thumbs-down sent, new each time the rating
  becomes a thumbs-down, so a new thumbs-down is a new report. Null on a
  thumbs-up.
- ``reported_at``: when the platform accepted it; null until then, which is
  what a reopened rating offers to send again.
- ``report_follow_ups``: ``[{note, at}]``, each note the platform posted under
  the report.

New nullable columns and a constant default, so no table rewrite.

Rollback: ``alembic downgrade 113`` drops the three columns.
"""

from alembic import op

revision = "114"
down_revision = "113"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE conversation_feedback "
        "ADD COLUMN IF NOT EXISTS report_id UUID, "
        "ADD COLUMN IF NOT EXISTS reported_at TIMESTAMPTZ, "
        "ADD COLUMN IF NOT EXISTS report_follow_ups JSONB NOT NULL DEFAULT '[]'::jsonb"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE conversation_feedback "
        "DROP COLUMN IF EXISTS report_follow_ups, "
        "DROP COLUMN IF EXISTS reported_at, "
        "DROP COLUMN IF EXISTS report_id"
    )
