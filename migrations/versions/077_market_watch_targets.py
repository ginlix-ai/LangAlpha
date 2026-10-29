"""Migration 077: price targets on a thread's market watch.

A target is a level or a % move the agent sets on a watched symbol. When it
hits, the alert reaches the thread: inline if a turn is running, as a new turn
if not. It is a promise that outlives the turn that made it, so it lives here
rather than beside the watch list in Redis.

Delivery rides ``hook_outbox``, whose jobs until now were all born from a
run's finalize. A target's alert is born from a price tick, so ``run_id``
becomes nullable. The wake turn's query is typed ``market_target`` so it is
told apart from a turn the user typed.

Lock discipline follows 052, except that no lock on a hot table is held while
waiting for another. The new table's foreign key holds SHARE ROW EXCLUSIVE on
``conversation_threads`` until its transaction commits, so the table and its
indexes commit on their own first. Each statement on ``hook_outbox`` and
``conversation_queries`` then runs as its own transaction in an autocommit
block. Held in one transaction, the lock on ``hook_outbox`` would stall every
finalize and steer read while the migration waited on
``conversation_queries``, and a turn's start, which writes
``conversation_queries`` before it reads ``hook_outbox``, could deadlock with
it. Those ACCESS EXCLUSIVE statements are catalog work only, a dropped NOT
NULL and a CHECK added NOT VALID, each capped at 5 s; the CHECK is validated
last with no cap, under a lock that blocks no writer. ``downgrade`` sets NOT
NULL again off a CHECK validated first, as SET NOT NULL alone scans
``hook_outbox`` under ACCESS EXCLUSIVE. An upgrade or downgrade that dies
partway may have committed some of its steps with the revision it started
from still recorded: run it again, which is safe.

Roll back with ``downgrade`` while this build is stopped: it writes jobs with
no run and wake turns typed ``market_target``, which the narrowed schema
refuses. Redeploying the previous build over schema 077 instead loses
alerts: that build has no monitor, so armed targets go unwatched, and its
drainer knows no ``market_target`` job, so it dead-letters every pending
notice, a claimed hit included.
"""

from alembic import op

revision = "077"
down_revision = "076"
branch_labels = None
depends_on = None

_TYPE_CHECK = "conversation_queries_type_check"
_QUERY_TYPES = (
    "'initial', 'follow_up', 'resume_feedback', 'regenerate', 'steering', 'system'"
)
# Not ``hook_outbox_run_id_not_null``: PostgreSQL 18 gives that name to the
# column's own NOT NULL constraint.
_RUN_ID_CHECK = "hook_outbox_run_id_present"


def _replace_type_check(values: str) -> None:
    # ADD CONSTRAINT has no IF NOT EXISTS; the DROP makes this re-runnable
    # after an abort in the autocommit block.
    op.execute(
        f"""
        ALTER TABLE conversation_queries
            DROP CONSTRAINT IF EXISTS {_TYPE_CHECK},
            ADD CONSTRAINT {_TYPE_CHECK} CHECK (type IN ({values})) NOT VALID
        """
    )


def _validate_type_check() -> None:
    op.execute(f"ALTER TABLE conversation_queries VALIDATE CONSTRAINT {_TYPE_CHECK}")


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS market_watch_targets (
            target_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            conversation_thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            market VARCHAR(10) NOT NULL DEFAULT 'stock'
                CHECK (market IN ('stock', 'index')),
            kind VARCHAR(20) NOT NULL
                CHECK (kind IN ('above', 'below', 'pct_up', 'pct_down')),
            value NUMERIC NOT NULL CHECK (value > 0),
            pct_from VARCHAR(20)
                CHECK (pct_from IN ('previous_close', 'armed')),
            armed_price NUMERIC,
            status VARCHAR(20) NOT NULL DEFAULT 'armed'
                CHECK (status IN ('armed', 'fired', 'expired', 'cancelled')),
            expires_at TIMESTAMPTZ NOT NULL,
            warn_at TIMESTAMPTZ NOT NULL,
            warned_at TIMESTAMPTZ,
            fired_at TIMESTAMPTZ,
            fired_price NUMERIC,
            hit_delivered_at TIMESTAMPTZ,
            hit_delivered_run_id UUID,
            warn_delivered_at TIMESTAMPTZ,
            warn_delivered_run_id UUID,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT market_watch_targets_pct_from_iff_pct_kind
                CHECK ((kind IN ('pct_up', 'pct_down')) = (pct_from IS NOT NULL))
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_market_watch_targets_armed
        ON market_watch_targets (warn_at)
        WHERE status = 'armed'
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_market_watch_targets_thread
        ON market_watch_targets (conversation_thread_id, created_at DESC)
    """)
    # One armed target per (thread, listing, kind): setting the same kind
    # again replaces it, so "above 205" then "above 210" never leaves both
    # armed. The listing is the symbol in its market, since the US ticker HSI
    # and the Hang Seng are both stored as HSI.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_market_watch_targets_armed_kind
        ON market_watch_targets (conversation_thread_id, symbol, market, kind)
        WHERE status = 'armed'
    """)

    # Entering the block commits the table, releasing conversation_threads.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute("ALTER TABLE hook_outbox ALTER COLUMN run_id DROP NOT NULL")
        _replace_type_check(_QUERY_TYPES + ", 'market_target'")
        op.execute("SET lock_timeout = 0")
        _validate_type_check()
        op.execute("RESET lock_timeout")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute("""
            UPDATE conversation_queries SET type = 'system'
            WHERE type = 'market_target'
        """)
        _replace_type_check(_QUERY_TYPES)
        op.execute("DELETE FROM hook_outbox WHERE run_id IS NULL")
        op.execute(f"""
            ALTER TABLE hook_outbox
                DROP CONSTRAINT IF EXISTS {_RUN_ID_CHECK},
                ADD CONSTRAINT {_RUN_ID_CHECK} CHECK (run_id IS NOT NULL) NOT VALID
        """)
        op.execute("SET lock_timeout = 0")
        _validate_type_check()
        op.execute(f"ALTER TABLE hook_outbox VALIDATE CONSTRAINT {_RUN_ID_CHECK}")
        op.execute("SET lock_timeout = '5s'")
        # The validated CHECK proves no row is NULL, so this skips the scan,
        # and once the column says so itself the CHECK has no work left.
        op.execute("ALTER TABLE hook_outbox ALTER COLUMN run_id SET NOT NULL")
        op.execute(f"ALTER TABLE hook_outbox DROP CONSTRAINT IF EXISTS {_RUN_ID_CHECK}")
        op.execute("RESET lock_timeout")

    # Dropping the foreign key takes ACCESS EXCLUSIVE on conversation_threads,
    # so the table goes in a transaction that holds no other hot lock.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS market_watch_targets")
