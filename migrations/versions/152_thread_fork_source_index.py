"""Migration 152: find the forks that show a thread's turns.

A fork made from a snapshot keeps none of its sources' earlier turns and
shows them from the threads that hold them, so deleting one takes that part
of every such fork's view with it. The delete dialog says so, which needs
every fork that shows a thread's turns: its children, and the forks made from
them, since a fork of a fork lists each thread it shows turns from in its own
``metadata->'fork'->'borrow'->'ancestors'``. A GIN ``jsonb_path_ops`` index on
that array answers the containment query (``@>`` of ``[{"thread_id": X}]``).
It is partial to snapshot forks: a fork that copied its history (lineage
without ``borrow``) shows nothing of its source and is not counted.

Built CONCURRENTLY in an autocommit block under no lock timeout, as 115 built
its index on a table taking continuous writes: the build lets every write
through and waits out the transactions open at each of its phases. Rollback:
``alembic downgrade 151`` drops the index; nothing else reads it.
"""

from alembic import op

revision = "152"
down_revision = "151"
branch_labels = None
depends_on = None

_INDEX = "idx_threads_fork_borrowed_from"
# How long a CONCURRENTLY step may wait out the transactions open at its
# phases before the deploy fails rather than hangs.
_BUILD_TIMEOUT = "15min"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"SET statement_timeout = '{_BUILD_TIMEOUT}'")
        try:
            # Drop before create: IF NOT EXISTS matches on the name alone,
            # and would adopt the INVALID index a failed CONCURRENTLY build
            # leaves.
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}")
            op.execute(f"""
                CREATE INDEX CONCURRENTLY {_INDEX}
                ON conversation_threads
                USING gin ((metadata->'fork'->'borrow'->'ancestors') jsonb_path_ops)
                WHERE metadata->'fork'->'borrow' IS NOT NULL
            """)
        finally:
            op.execute("RESET statement_timeout")
            op.execute("RESET lock_timeout")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}")
