"""Migration 060: each thread says whether its agent may use subagents.

``conversation_threads.subagents_allowed`` is the user's switch for the
thread. A column rather than a ``metadata`` key because it is mutable and read
during a turn, on every model call and before every subagent launch, while
``metadata`` holds creation-time provenance.

NULL follows the owner's ``other_preference.subagents_default``, resolved
where the gate reads the switch, so changing the default moves every thread
with no value of its own while a thread switched off the default keeps its
value. Existing
rows start NULL and, with no default stored yet, run as every thread ran
before this, with subagents on, so they need no backfill.

A nullable column with no default rewrites no rows, so the statement holds its
lock only for the catalog change. ``updated_at`` is left alone: the thread
lists sort on it.

Rollback: ``alembic stamp 059`` first, because the previous build's
``upgrade head`` cannot start from a revision it does not ship, then redeploy
it; it never reads the column. ``downgrade`` drops the column, and with it
every thread's switch, so a rollback does not need it.
"""

from alembic import op

revision = "060"
down_revision = "059"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Own transaction, so the ACCESS EXCLUSIVE lock is released at once.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(
            "ALTER TABLE conversation_threads "
            "ADD COLUMN IF NOT EXISTS subagents_allowed BOOLEAN"
        )
        op.execute("RESET lock_timeout")


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE conversation_threads DROP COLUMN IF EXISTS subagents_allowed"
    )
