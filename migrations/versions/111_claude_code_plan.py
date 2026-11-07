"""Migration 111: the Claude plan behind each Claude Code credential.

The Claude Code CLI reports the plan's windows on every response that moves
one (``rate_limit_event``), and nothing kept them, so a turn past the plan's
limit on an account with usage credits on was billed to them silently, and
admission could not refuse a turn the plan would refuse. ``claude_code_plan``
keeps one row per user and credential: ``saved`` for the setup token the
user saved or stored, wherever it runs, and ``computer:<id>`` for a
computer's own token (an Anthropic API key has no plan windows).

``state`` holds what the reports said, each part with when it was seen:
the windows' use, every limit still known to hold, and whether usage credits
pay past them. The server merges each report into it under the row's lock,
so a report carrying some windows keeps the others, a late write never
undoes a newer one, and a limit on one window never replaces another's.
``generation`` moves each time the credential's token is replaced or
removed, and a run's writes go only to the generation it started on, so
reports from a replaced token are dropped. ``token_digest`` (the token's
sha256) names the token last saved on it, so saving the same token again
keeps its state. ``user_id`` references ``users`` with ``ON UPDATE
CASCADE``, so a row follows ``migrate_user_id`` to the user's new id and
goes when the user does.

Rollback: roll the code back first, then ``alembic downgrade 110`` drops the
table; with this code still running, a token save or removal fails without
it. The next Claude Code request on each credential reports the windows again.
"""

from alembic import op

revision = "111"
down_revision = "110"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS claude_code_plan (
            user_id      TEXT        NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
            credential   TEXT        NOT NULL,
            generation   INTEGER     NOT NULL DEFAULT 0,
            token_digest TEXT,
            state        JSONB       NOT NULL DEFAULT '{}',
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (user_id, credential)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS claude_code_plan")
