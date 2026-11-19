"""Migration 145: which member wrote each message on a shared workspace's comment.

A file's comments are its record owner's, so in a shared workspace every
message reads as written by the workspace's holder and a member's words show
as nobody's in particular. ``written_by`` names the member who wrote a
person's message there; it stays null for a personal workspace's messages,
which are the owner's, and for the agent's.

Online-safe: one nullable column, no backfill (messages written before it
name no member). Rollback: roll the code back first, then ``alembic downgrade
144``, which drops the column and with it who wrote each message.
"""

from alembic import op

revision = "145"
down_revision = "144"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE artifact_comment_messages "
        "ADD COLUMN IF NOT EXISTS written_by VARCHAR(255) "
        "REFERENCES users(user_id) ON DELETE SET NULL"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE artifact_comment_messages DROP COLUMN IF EXISTS written_by")
