"""Migration 100: what a computer's agent did through the ``la`` CLI.

One row per request a computer's token makes to a route that takes tokens,
those a scope or reach check refuses included; one turned away before that
(by a session-only route, a bad token or the rate) leaves none. Each names
the thread whose command made it: the command's call id, sent as
``X-Langalpha-Call``, finds the workspace and thread its tool filed. The
thread is the command's own report, not proof, since any process on the
computer can read a sibling's environment; with no call id (a person at the
machine, a cron job) it is NULL. ``thread_id`` has no foreign key so the
record outlives a deleted thread; it goes with its computer, and the CLI
token sweep prunes rows past their retention.
"""

from alembic import op

revision = "100"
down_revision = "099"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS computer_cli_calls (
            id BIGSERIAL PRIMARY KEY,
            computer_id UUID NOT NULL
                REFERENCES computers(computer_id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            thread_id UUID,
            call_id VARCHAR(64),
            method VARCHAR(10) NOT NULL,
            route TEXT NOT NULL,
            targets JSONB NOT NULL DEFAULT '{}'::jsonb,
            status SMALLINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computer_cli_calls_thread
        ON computer_cli_calls (thread_id, created_at) WHERE thread_id IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computer_cli_calls_computer
        ON computer_cli_calls (computer_id, created_at)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computer_cli_calls_created
        ON computer_cli_calls (created_at)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS computer_cli_calls")
