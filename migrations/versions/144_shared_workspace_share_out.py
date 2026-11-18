"""Migration 144: who may share a shared workspace's work outside it.

A restricted shared workspace holds work its managers chose to keep among its
listed people. ``share_out`` is their choice of who may take it further:
``managers`` (only a manager turns a thread's or file's organization link on
or off, or publishes to the organization library), ``members`` (any member
may), or ``none`` (nobody publishes, and every link from the workspace opens
only for the people in it). It is read when a link is opened, so a change
holds for links made before it. While the workspace is open to everyone in
its organization the value is kept and not applied.

Online-safe: one column with a constant default on a small table, which
Postgres adds without rewriting it; every existing row reads ``managers``.
Rollback: roll the code back first, then ``alembic downgrade 143``, which
drops the column and with it each workspace's choice.
"""

from alembic import op

revision = "144"
down_revision = "143"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE shared_workspaces "
        "ADD COLUMN IF NOT EXISTS share_out TEXT NOT NULL DEFAULT 'managers' "
        "CONSTRAINT shared_workspaces_share_out "
        "CHECK (share_out IN ('managers', 'members', 'none'))"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE shared_workspaces DROP COLUMN IF EXISTS share_out")
