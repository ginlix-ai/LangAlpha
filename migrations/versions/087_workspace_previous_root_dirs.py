"""Migration 087: the computer roots a workspace has left.

The agent spells a file by its absolute path under the computer it runs on,
and a computer its owner brought keeps its folders under a root of its own.
A move takes the folder to another root, so a turn written before it names
the file under the old one. Readers fold a path under a root they know, and
the live computer list stops knowing a root once that computer is removed.

``workspaces.previous_root_dirs`` keeps the roots of the computers a
workspace moved off, newest first, appended in the transaction that commits
the move and capped there. Nothing is backfilled: a workspace moved before
this revision keeps an empty list, and its readers fall back to the roots of
the computers that are still there.

Adding a column with a constant default rewrites no rows, so the lock is
held only to change the catalog. The previous build lists its columns by
name and never reads this one.

Rollback: ``alembic downgrade 086``. It drops the column, and a path under a
root only it remembered reads as outside the workspace again.
"""

from alembic import op

revision = "087"
down_revision = "086"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE workspaces "
        "ADD COLUMN IF NOT EXISTS previous_root_dirs TEXT[] NOT NULL DEFAULT '{}'"
    )
    op.execute("RESET lock_timeout")


def downgrade() -> None:
    op.execute("SET lock_timeout = '5s'")
    op.execute("ALTER TABLE workspaces DROP COLUMN IF EXISTS previous_root_dirs")
    op.execute("RESET lock_timeout")
