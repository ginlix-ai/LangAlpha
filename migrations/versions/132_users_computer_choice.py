"""Migration 132: where a user chose to work, their own machine or the cloud.

``users.computer_choice`` is ``own_machine`` or ``cloud``, set by the desktop
app's first run, and read only while the user has no primary computer and
none of theirs can become one: ``own_machine`` keeps a cloud computer from
being built for them, so a turn or a new workspace is refused with
``needs_computer`` until they connect one. NULL is a user never asked, who
gets a cloud computer as before when none of theirs can be primary.

A nullable column with no default and its CHECK in one statement: no
rewrite, and the check reads only NULLs. Migrations run in one transaction,
so the ACCESS EXCLUSIVE lock on ``users`` is held until the whole upgrade
commits; every statement after it is catalog work too. Rollback leaves the
column in place, since the previous build never reads it: ``alembic stamp
131`` first, then redeploy it.
"""

from alembic import op

revision = "132"
down_revision = "131"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS computer_choice TEXT"
        " CONSTRAINT users_computer_choice_check"
        " CHECK (computer_choice IN ('own_machine', 'cloud'))"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS users_computer_choice_check")
    op.execute("ALTER TABLE users DROP COLUMN IF EXISTS computer_choice")
