"""Migration 112: when a Claude Code credential's plan was last checked on demand.

Settings and the model picker offer Check now, which sends one minimal
request on the credential's Claude plan so the CLI reports its windows. A
check spends a request on the user's plan, so one may go per credential every
5 minutes, counted on every worker alike: ``checked_at`` on the credential's
``claude_code_plan`` row is when the last one went, taken and moved in one
statement. It is cleared when the credential's token is replaced or removed,
since the new token may be another Claude account's.

Rollback: roll the code back first, then ``alembic downgrade 111`` drops the
column; nothing else reads it.
"""

from alembic import op

revision = "112"
down_revision = "111"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE claude_code_plan ADD COLUMN IF NOT EXISTS checked_at TIMESTAMPTZ")


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE claude_code_plan DROP COLUMN IF EXISTS checked_at")
