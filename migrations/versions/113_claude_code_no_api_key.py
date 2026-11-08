"""Migration 113: Claude Code on cloud computers no longer runs on an API key.

A user's Claude Code turns on a cloud computer ran on one of three choices in
``claude_code_custody``: ``local`` (none: only on their own computers),
``api_key`` (their Anthropic API key) or ``stored`` (a setup token the server
keeps). The API key choice is gone, so each user who chose it now reads as
``local``, the default, and the check allows only the two that remain. An
operator's block on a user stays on their row.

Rollback: roll the code back first, then ``alembic downgrade 112`` allows
``api_key`` again. Users moved to ``local`` stay there until they choose again.
"""

from alembic import op

revision = "113"
down_revision = "112"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE claude_code_custody SET mode = 'local', chosen_at = NOW() WHERE mode = 'api_key'")
    op.execute("ALTER TABLE claude_code_custody DROP CONSTRAINT IF EXISTS claude_code_custody_mode_check")
    op.execute(
        "ALTER TABLE claude_code_custody ADD CONSTRAINT claude_code_custody_mode_check CHECK (mode IN ('local', 'stored'))"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE claude_code_custody DROP CONSTRAINT IF EXISTS claude_code_custody_mode_check")
    op.execute(
        "ALTER TABLE claude_code_custody ADD CONSTRAINT claude_code_custody_mode_check "
        "CHECK (mode IN ('local', 'api_key', 'stored'))"
    )
