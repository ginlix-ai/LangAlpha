"""Migration 053: let the user dismiss a failed automation run.

``automation_executions.dismissed_at`` is when the user took a failed run out
of Needs attention. An automation there that the user means to leave as it
is, switched off or ended on a failure, otherwise stays there until it is
deleted. The mark is on the run rather than the automation, so the next
failure is a new row without one and asks for attention again. NULL is a run
nobody dismissed.

Catalog work only: a nullable column with no default takes ACCESS EXCLUSIVE
for the catalog update and rewrites nothing. Rollback leaves the column in
place, since the previous build never reads it: ``alembic stamp 052`` first,
because that build's ``upgrade head`` cannot start from a revision it does
not ship, then redeploy it; a later upgrade finds the column already there.
``downgrade`` is only safe once no process of this build is left, since every
automation read selects the column, and it drops the marks, which returns
every dismissed run to Needs attention.
"""

from alembic import op

revision = "053"
down_revision = "052"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE automation_executions "
        "ADD COLUMN IF NOT EXISTS dismissed_at TIMESTAMPTZ"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE automation_executions DROP COLUMN IF EXISTS dismissed_at"
    )
