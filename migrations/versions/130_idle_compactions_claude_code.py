"""Migration 130: idle compactions of Claude Code sessions.

``idle_compactions.kind`` takes ``CC`` besides ``I1`` and ``I2``: a firing that
has the Claude Code CLI compact a thread's session while its prompt cache is
warm, armed from the end of a Claude Code turn. The table holds at most one
row per thread and only for the hour a cache entry lives, so the CHECK is
replaced in place, validated as it is added.

Rollback: ``alembic downgrade 129`` drops the ``CC`` rows, whose threads
compact at their next turn start as before, and narrows the CHECK again. The
previous build skips a ``CC`` row it claims meanwhile on a thread naming a
Claude Code model; on one following an account default of Claude Code, its
summary call fails without a write and is logged ``reason=error``.

Revision ID: 130
Revises: 129
"""

from alembic import op

revision = "130"
down_revision = "129"
branch_labels = None
depends_on = None

_KIND_CHECK = "idle_compactions_kind_check"


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f"""
        ALTER TABLE idle_compactions
            DROP CONSTRAINT IF EXISTS {_KIND_CHECK},
            ADD CONSTRAINT {_KIND_CHECK} CHECK (kind IN ('I1', 'I2', 'CC'))
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DELETE FROM idle_compactions WHERE kind = 'CC'")
    op.execute(f"""
        ALTER TABLE idle_compactions
            DROP CONSTRAINT IF EXISTS {_KIND_CHECK},
            ADD CONSTRAINT {_KIND_CHECK} CHECK (kind IN ('I1', 'I2'))
    """)
