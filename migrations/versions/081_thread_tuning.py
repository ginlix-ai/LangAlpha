"""Migration 081: a thread may keep its own reasoning effort and fast mode.

``conversation_threads.reasoning_effort`` and ``fast_mode`` are what the user
picked in the thread's composer. Before this the composer wrote the pick into
the account's per-model profile, so raising effort for one question raised it
for every thread on that model. The defaults now change only in Settings, and
a pick in a thread is that thread's own.

NULL follows Settings: the model's profile, then the account default, then
what the model declares, resolved per turn, so a change in Settings moves
every thread without a value of its own. ``reasoning_effort`` is a level from
the full vocabulary, not the model's ladder: the thread's model can change
under it, and the turn clamps the level to whatever the model offers. Existing
rows start NULL and run what they ran before, so they need no backfill.

Nullable columns with no default rewrite no rows, so the statement holds its
lock only for the catalog change. ``updated_at`` is left alone: the thread
lists sort on it.

Rollback: ``alembic stamp 080`` first, because the previous build's
``upgrade head`` cannot start from a revision it does not ship, then redeploy
it; it never reads the columns. ``downgrade`` drops them, and with them every
thread's pick, so a rollback does not need it.
"""

from alembic import op

revision = "081"
down_revision = "080"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Own transaction, so the ACCESS EXCLUSIVE lock is released at once.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(
            "ALTER TABLE conversation_threads "
            "ADD COLUMN IF NOT EXISTS reasoning_effort TEXT, "
            "ADD COLUMN IF NOT EXISTS fast_mode BOOLEAN"
        )
        op.execute("RESET lock_timeout")


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE conversation_threads "
        "DROP COLUMN IF EXISTS reasoning_effort, "
        "DROP COLUMN IF EXISTS fast_mode"
    )
