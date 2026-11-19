"""Migration 143: when a shared workspace opened to everyone.

A member with no read row on a shared thread has read none of it, so someone
who joins a busy workspace would find every finished turn unread. A turn that
ended before the member could reach the workspace is not theirs to read: for a
listed member that moment is their row's ``added_at``; for someone who reaches
it through "everyone in the organization" it is the later of when the
workspace opened to everyone (``everyone_since``, set on each opening and
cleared on closing) and when their account was made, since an organization
account is made in its organization.

Online-safe: one nullable column on a small table, filled for the rows open
to everyone now from their creation time. Rollback: roll the code back first,
then ``alembic downgrade 142``, which drops the column; read state stays.
"""

from alembic import op

revision = "143"
down_revision = "142"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE shared_workspaces "
        "ADD COLUMN IF NOT EXISTS everyone_since TIMESTAMPTZ"
    )
    op.execute(
        "UPDATE shared_workspaces SET everyone_since = created_at "
        "WHERE everyone AND everyone_since IS NULL"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE shared_workspaces DROP COLUMN IF EXISTS everyone_since")
