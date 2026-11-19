"""Migration 146: whether a shared workspace's thread is shared with it.

A thread in a shared workspace is its starter's alone until they share it with
the workspace. ``workspace_shared_at`` records when it was shared and
``workspace_shared_by`` who did; null ``workspace_shared_at`` means private.
Null is what every insert writes unless it says otherwise, so a door that makes
threads without knowing of sharing (a channel, an automation, a hand-off) makes
a private one, and the mistake that costs nothing is the one the default makes.
A personal workspace's threads keep null and nothing reads it there.

Threads already in a shared workspace were seen by every member until now, so
they are recorded as shared, at their creation, by their starter. A starter
whose account has since been deleted (``created_by`` carries no foreign key,
141) is left out of ``workspace_shared_by``, which does: such a thread reads as
shared with the sharer unknown, exactly as one does once its sharer's account
is deleted (``ON DELETE SET NULL``). Whether a thread is shared is
``workspace_shared_at`` alone, so it stays visible to the workspace, which is
what its members saw before; read as private it would be no one's, its starter
being gone.

Locks, since the previous build serves while this runs. The columns and the
foreign key, added ``NOT VALID``, are catalog changes under a short ACCESS
EXCLUSIVE lock on ``conversation_threads`` (and SHARE ROW EXCLUSIVE on
``users``), held with the backfill, which touches only the shared workspaces'
threads, until that transaction commits. The key is then validated in an
autocommit block, where VALIDATE scans ``conversation_threads`` under SHARE
UPDATE EXCLUSIVE, which blocks neither reads nor writes; ``lock_timeout``
bounds only the wait for a lock, never how long it is held, so the scan must
not run under the first one. Every value written after the key was added was
checked as it was written, so the validation cannot fail. Re-runnable after an
abort between the two: the columns are added ``IF NOT EXISTS``, the key is
dropped ``IF EXISTS`` before it is added again, and the backfill skips threads
it already recorded.

Rollback: roll the code back first, then ``alembic downgrade 145``, which drops
the key and both columns, and every shared workspace's threads are seen by all
its members again. So the downgrade refuses while any of them is private,
naming how many, unless ``ALLOW_ENV`` is set to ``1``: have their starters
share them first, or accept that their workspaces will see them.
"""

import os

from alembic import op
from sqlalchemy import text

revision = "146"
down_revision = "145"
branch_labels = None
depends_on = None


_FOREIGN_KEY = "conversation_threads_workspace_shared_by_fkey"


def upgrade() -> None:
    # Every turn writes conversation_threads: give up rather than queue them
    # behind the ACCESS EXCLUSIVE request, and let the deploy retry.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE conversation_threads "
        "ADD COLUMN IF NOT EXISTS workspace_shared_at TIMESTAMPTZ, "
        "ADD COLUMN IF NOT EXISTS workspace_shared_by VARCHAR(255)"
    )
    # ADD CONSTRAINT has no IF NOT EXISTS; the DROP clears a key an abort
    # left unvalidated.
    op.execute(
        f"""
        ALTER TABLE conversation_threads
            DROP CONSTRAINT IF EXISTS {_FOREIGN_KEY},
            ADD CONSTRAINT {_FOREIGN_KEY} FOREIGN KEY (workspace_shared_by)
                REFERENCES users(user_id) ON DELETE SET NULL NOT VALID
        """
    )
    # The key checks what this writes: a starter with no account left is
    # recorded as no one.
    op.execute(
        """
        UPDATE conversation_threads t
           SET workspace_shared_at = t.created_at,
               workspace_shared_by = (
                   SELECT u.user_id FROM users u WHERE u.user_id = t.created_by)
          FROM shared_workspaces s
         WHERE s.workspace_id = t.workspace_id
           AND t.workspace_shared_at IS NULL
        """
    )
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"ALTER TABLE conversation_threads VALIDATE CONSTRAINT {_FOREIGN_KEY}")


#: Set to ``1`` to downgrade while threads are private, showing them to
#: everyone in their workspaces.
ALLOW_ENV = "MIGRATION_146_DOWNGRADE_SHOWS_PRIVATE_THREADS"

_PRIVATE_THREADS_SQL = """
    SELECT count(*), count(DISTINCT t.workspace_id)
      FROM conversation_threads t
      JOIN shared_workspaces s ON s.workspace_id = t.workspace_id
     WHERE t.workspace_shared_at IS NULL
"""


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(text("SET LOCAL lock_timeout = '5s'"))
    # Held through the drop, so no thread is made private after the count.
    bind.execute(text("LOCK TABLE conversation_threads IN ACCESS EXCLUSIVE MODE"))
    threads, workspaces = bind.execute(text(_PRIVATE_THREADS_SQL)).one()
    if threads and os.environ.get(ALLOW_ENV) != "1":
        raise RuntimeError(
            f"{threads} thread(s) in {workspaces} shared workspace(s) are private to "
            "the person who started them. Downgrading below 146 drops the columns "
            "that keep them so, and every member of those workspaces would see "
            "them. Have their starters share or delete them first, or set "
            f"{ALLOW_ENV}=1 to downgrade anyway."
        )
    # A catalog change under the lock already held: no scan.
    op.execute(
        "ALTER TABLE conversation_threads "
        f"DROP CONSTRAINT IF EXISTS {_FOREIGN_KEY}, "
        "DROP COLUMN IF EXISTS workspace_shared_by, "
        "DROP COLUMN IF EXISTS workspace_shared_at"
    )
