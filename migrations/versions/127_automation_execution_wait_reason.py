"""Migration 127: say what a waiting automation run waits for.

``automation_executions.wait_reason`` is what a firing held as ``waiting``
waits for: ``thread_busy`` (the turn running in its thread) or
``computer_unreachable`` (the user's own machine its turn runs on, asleep or
offline). The status alone cannot tell them apart, and the two waits end
differently: a server stopping skips a wait for the thread, while a wait for
the computer is taken over by the next process's sweep. A firing that waited
keeps the reason of its last wait once it runs or is skipped. NULL is a run
that never waited, or one written before this column.

``checked_computer_id`` is the computer a run started from a computer's token
was checked to run on, written when the firing is made, so whichever process
starts its turn (after a hand-off, or a wait for the computer) holds it to that
computer. NULL for any other run.

Catalog work only: nullable columns with no default take ACCESS EXCLUSIVE
for the catalog update and rewrite nothing. Rollback leaves the columns in
place, since the previous build never reads them: ``alembic stamp 126`` first,
then redeploy it. ``downgrade`` is only safe once no process of this build is
left, since every automation read selects ``wait_reason``.
"""

from alembic import op

revision = "127"
down_revision = "126"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE automation_executions "
        "ADD COLUMN IF NOT EXISTS wait_reason TEXT, "
        "ADD COLUMN IF NOT EXISTS checked_computer_id UUID"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE automation_executions "
        "DROP COLUMN IF EXISTS wait_reason, "
        "DROP COLUMN IF EXISTS checked_computer_id"
    )
