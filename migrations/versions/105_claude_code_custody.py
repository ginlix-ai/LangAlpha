"""Migration 105: how Claude Code runs on a user's cloud computers.

A Claude Code turn on a cloud computer runs its CLI in the backend worker
that owns the turn, on a credential of the user's. ``claude_code_custody``
holds which one each user chose: ``local`` (none: Claude Code runs only on
their own computers, each with its own setup token), ``api_key`` (their own
Anthropic API key, the one saved under Model access) or ``stored`` (a setup
token the server keeps). No row reads as ``local``. ``stored`` is named
from the start so a later build that offers it needs no second migration;
until then the service refuses it. ``disclosure`` is the version of the
consent text a user accepted for ``stored``, and ``blocked_at`` and
``blocked_reason`` an operator's block on it for one user, which the user's
own writes never clear: the reason this is a table and not a preference.

``user_id`` references ``users`` with ``ON UPDATE CASCADE``, so a row follows
``migrate_user_id`` to the user's new id and goes when the user does. No turn reads
the table unless an operator switch for cloud computers is on; turning
Claude Code off reads it on the platform, so a stored token goes with it.
The previous build never reads it.

Rollback: ``alembic downgrade 104`` drops the table. Every user then reads
as ``local`` once upgraded again, and the previous build refuses every
Claude Code turn on a cloud computer. Setup tokens stored on the platform
stay in the token store and operator blocks are lost, so purge them first
(``scripts/ops/claude_code_custody.py purge --all``).
"""

from alembic import op

revision = "105"
down_revision = "104"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS claude_code_custody (
            user_id        TEXT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
            mode           TEXT NOT NULL CHECK (mode IN ('local', 'api_key', 'stored')),
            disclosure     TEXT NULL,
            chosen_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            blocked_at     TIMESTAMPTZ NULL,
            blocked_reason TEXT NULL
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS claude_code_custody")
