"""Migration 115: what each turn said, kept for search.

``thread_search_turns`` holds one row per turn of a thread: the user's message
and the main agent's answer, each capped at 32K characters, so the command
palette finds a conversation by what was said in it. It is keyed by the turn,
not the response, so a regenerate or an edit replaces the turn's row and
search only ever sees the current branch. ``conversation_response_id`` is the
response the answer was read from, which is how a later attempt at the turn is
noticed; its cascade takes the row out the moment an edit or a regenerate
deletes the response, before the new one settles. There is no ``user_id``:
ownership joins through ``workspaces``, since an account merge moves threads
between workspaces. Archived threads and deleted workspaces are filtered when
read.

A search matches its terms over the user's own rows alone (turns, thread
titles, backed-up file paths), so none of them has a text index: one over
every user's rows only ever drove a plan from all of their matches. The file
search matches paths from ``unique_file_per_workspace`` (001) alone, so it
needs no index of its own.

A row also records which attempt it was read from (``attempt_no``), when the
read began (``read_at``, the database's clock), and whether the answer was
read at all (``answer_read``): a write lands only over a row read from an
older attempt or earlier, so two writers racing never put the older text
back. An answer whose read failed is retried at ``retry_at``, backing off
with each of its ``read_failures``, until a cap leaves it unread with no
retry; ``idx_thread_search_turns_retry`` holds only the unread rows, which
is all the sweep's retries read.

``thread_search_walk`` is the backfill's one row: where the walk over every
thread stands, when its next slice may run and when it last finished, so
the deployment's workers walk once between them rather than once each.

``conversation_responses.usage_settled_at``, the instant a turn settled, gets
a btree, partial to the settled rows, so the index's sweep reads only the
turns settled in its window. It is built CONCURRENTLY in an autocommit
block, under no lock timeout, since the table takes continuous writes (033's
and 039's reasoning): the build holds SHARE UPDATE EXCLUSIVE, which lets
every insert, update and delete through, and waits out the transactions open at
each of its phases rather than failing. A statement timeout bounds that
wait, so a build stuck behind a transaction left open fails the deploy
loudly instead of hanging it; run it again once that transaction is gone.
The block resets both settings at its end, since they are the session's and
would otherwise reach every later migration. Every step is idempotent, so
this file can be renumbered and run again over a database that already has
it.

Rollback: ``alembic downgrade 114`` drops both tables and the index. The
previous build never reads any of them.

Revision ID: 115
Revises: 114
"""

from alembic import op

revision = "115"
down_revision = "114"
branch_labels = None
depends_on = None

_SETTLED_INDEX = "idx_responses_usage_settled_at"
# How long a CONCURRENTLY step may wait out the transactions open at its
# phases before the deploy fails rather than hangs.
_BUILD_TIMEOUT = "15min"


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS thread_search_turns (
            conversation_thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id) ON DELETE CASCADE,
            turn_index INTEGER NOT NULL,
            conversation_response_id UUID NOT NULL
                REFERENCES conversation_responses(conversation_response_id) ON DELETE CASCADE,
            attempt_no INTEGER NOT NULL,
            user_text TEXT NOT NULL DEFAULT '',
            answer_text TEXT NOT NULL DEFAULT '',
            answer_read BOOLEAN NOT NULL DEFAULT TRUE,
            read_failures SMALLINT NOT NULL DEFAULT 0,
            retry_at TIMESTAMPTZ,
            turn_at TIMESTAMPTZ NOT NULL,
            read_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (conversation_thread_id, turn_index)
        )
    """)
    # The response cascade looks rows up by response.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_thread_search_turns_response
        ON thread_search_turns (conversation_response_id)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_thread_search_turns_retry
        ON thread_search_turns (retry_at)
        WHERE NOT answer_read
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS thread_search_walk (
            one BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (one),
            after_created_at TIMESTAMPTZ,
            after_thread_id UUID,
            next_slice_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMPTZ
        )
    """)
    op.execute("INSERT INTO thread_search_walk DEFAULT VALUES ON CONFLICT DO NOTHING")

    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"SET statement_timeout = '{_BUILD_TIMEOUT}'")
        try:
            # Drop before create: IF NOT EXISTS matches on the name alone,
            # and would adopt the INVALID index a failed CONCURRENTLY build
            # leaves.
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_SETTLED_INDEX}")
            op.execute(f"""
                CREATE INDEX CONCURRENTLY {_SETTLED_INDEX}
                ON conversation_responses (usage_settled_at)
                WHERE usage_settled_at IS NOT NULL
            """)
        finally:
            op.execute("RESET statement_timeout")
            op.execute("RESET lock_timeout")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_SETTLED_INDEX}")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS thread_search_walk")
    op.execute("DROP TABLE IF EXISTS thread_search_turns")
