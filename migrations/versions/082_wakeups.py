"""Migration 082: wakeups, the agent's own follow-ups on a thread.

A wakeup brings a thread back to its agent at a time, on a cron series, on a
price move, or when an order it placed fills or ends. Every kind lives in one
table with one firing shape, a price included: ``market_watch_targets`` is
not widened into it, because during a blue/green cutover the old color still
reads and writes that table as 077 left it. Its rows live about a day and a
half, so it drains on its own and is dropped a release after it is empty.

A cron series stores its whole fire schedule, computed when it is set
(``schedule``, at most 24 slots), so a fire is the slot the set confirmation
promised: no cron evaluation at fire time, and no daylight-saving drift
between the two. ``fires_done`` counts slots consumed, fired or skipped;
``last_fired_seq`` and ``delivered_seq`` are the latest slot that sent a
notice and the latest one the agent has read, so a slot is undelivered while
the first is ahead of the second, and the slots a notice waited past are
counted from the three rather than stored. Notices ride ``hook_outbox`` as
``wakeup`` jobs, and a wake turn's query is typed ``wakeup``.

Lock discipline follows 077. The foreign key holds SHARE ROW EXCLUSIVE on
``conversation_threads`` until its transaction commits, so the table and its
indexes commit on their own first. The query-type CHECK is then replaced in
an autocommit block, added NOT VALID under a 5 s lock timeout, which is
catalog work only, and validated last with no cap under a lock that blocks
no writer. ``hook_outbox`` needs nothing: it has no hook-type CHECK, and 077
already made ``run_id`` nullable. An upgrade or downgrade that dies partway
may have committed some of its steps with the revision it started from still
recorded: run it again, which is safe.

Roll back with ``downgrade`` while this build is stopped: it retypes wake
turns as ``system`` and drops their ``wakeup`` metadata, which names any
order a wake fired on and which the previous build's share view does not
redact, deletes the ``wakeup`` jobs, which a build without their executor
would dead-letter, and narrows the CHECK back to 077's set. To run the
previous build without downgrading, ``alembic stamp 081`` first, since its
``upgrade head`` cannot start from a revision it does not ship. Armed
wakeups then never fire, its drainer dead-letters each ``wakeup`` job that
comes due (a parked or held one never does), and it draws each wake turn's
notice as a message from the user, a shared thread's included.
"""

from alembic import op

revision = "082"
down_revision = "081"
branch_labels = None
depends_on = None

_TYPE_CHECK = "conversation_queries_type_check"
_QUERY_TYPES_077 = (
    "'initial', 'follow_up', 'resume_feedback', 'regenerate', 'steering', "
    "'system', 'market_target'"
)


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
        CREATE TABLE IF NOT EXISTS wakeups (
            wakeup_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            short_id VARCHAR(12) NOT NULL,
            conversation_thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            label VARCHAR(40),
            kind VARCHAR(10) NOT NULL
                CHECK (kind IN ('at', 'cron', 'price', 'order')),
            -- at / cron
            timezone VARCHAR(64),
            cron VARCHAR(120),
            until_at TIMESTAMPTZ,
            schedule TIMESTAMPTZ[] NOT NULL DEFAULT '{}',
            stop_reason VARCHAR(8) CHECK (stop_reason IN ('24', '7day', 'until')),
            next_fire_at TIMESTAMPTZ,
            fires_done INTEGER NOT NULL DEFAULT 0,
            last_fired_seq INTEGER NOT NULL DEFAULT 0,
            delivered_seq INTEGER NOT NULL DEFAULT 0,
            last_fired_at TIMESTAMPTZ,
            delivered_at TIMESTAMPTZ,
            delivered_run_id UUID,
            -- price
            symbol VARCHAR(20),
            market VARCHAR(10) CHECK (market IN ('stock', 'index')),
            price_kind VARCHAR(10)
                CHECK (price_kind IN ('above', 'below', 'pct_up', 'pct_down')),
            price_value NUMERIC CHECK (price_value > 0),
            pct_from VARCHAR(20) CHECK (pct_from IN ('previous_close', 'armed')),
            armed_price NUMERIC,
            fired_price NUMERIC,
            warn_at TIMESTAMPTZ,
            warned_at TIMESTAMPTZ,
            lapse_delivered_at TIMESTAMPTZ,
            lapse_delivered_run_id UUID,
            -- price + order
            expires_at TIMESTAMPTZ,
            -- order
            attempt_id UUID,
            vendor_order_id VARCHAR(255),
            baseline_filled_qty NUMERIC,
            order_outcome JSONB,
            -- lifecycle
            status VARCHAR(12) NOT NULL DEFAULT 'armed'
                CHECK (status IN ('armed', 'fired', 'finished', 'expired', 'cancelled')),
            cancelled_by VARCHAR(12)
                CHECK (cancelled_by IN (
                    'agent', 'user', 'setting_off', 'disabled', 'archive', 'workspace'
                )),
            cancelled_at TIMESTAMPTZ,
            -- clock_timestamp(): rows armed by one call keep the order they
            -- were set in, which NOW() (the transaction's start) would tie.
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT wakeups_short_id_unique UNIQUE (conversation_thread_id, short_id),
            CONSTRAINT wakeups_cancelled
                CHECK ((status = 'cancelled') = (cancelled_by IS NOT NULL)),
            CONSTRAINT wakeups_timed CHECK (kind NOT IN ('at', 'cron')
                OR (timezone IS NOT NULL AND cardinality(schedule) BETWEEN 1 AND 24)),
            CONSTRAINT wakeups_cron CHECK ((kind = 'cron') = (cron IS NOT NULL)),
            CONSTRAINT wakeups_price CHECK (kind <> 'price' OR (
                symbol IS NOT NULL AND price_kind IS NOT NULL
                AND price_value IS NOT NULL AND expires_at IS NOT NULL
                AND warn_at IS NOT NULL)),
            CONSTRAINT wakeups_pct_from_iff_pct_kind CHECK (kind <> 'price'
                OR (price_kind IN ('pct_up', 'pct_down')) = (pct_from IS NOT NULL)),
            CONSTRAINT wakeups_order CHECK (kind <> 'order' OR (
                attempt_id IS NOT NULL AND vendor_order_id IS NOT NULL
                AND expires_at IS NOT NULL))
        )
    """)
    # What the wakeup sweep claims on every worker: slots due, lapse
    # reminders due, windows passed, and armed orders to check for a fill.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_wakeups_due ON wakeups (next_fire_at)
        WHERE status = 'armed' AND next_fire_at IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_wakeups_lapse ON wakeups (warn_at)
        WHERE status = 'armed' AND kind = 'price' AND warned_at IS NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_wakeups_expiry ON wakeups (expires_at)
        WHERE status = 'armed' AND kind IN ('price', 'order')
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_wakeups_armed_order ON wakeups (attempt_id)
        WHERE status = 'armed' AND kind = 'order'
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_wakeups_thread
        ON wakeups (conversation_thread_id, created_at DESC)
    """)
    # Read on every main-agent model call, and empty for nearly every
    # thread, since few users cancel by hand.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_wakeups_user_cancel
        ON wakeups (conversation_thread_id)
        WHERE cancelled_by IN ('user', 'setting_off')
    """)
    # Turning the setting off cancels everything the user still has open.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_wakeups_user_open ON wakeups (user_id)
        WHERE status = 'armed' OR last_fired_seq > delivered_seq
    """)

    # Entering the block commits the table, releasing conversation_threads.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        _replace_type_check(_QUERY_TYPES_077 + ", 'wakeup'")
        op.execute("SET lock_timeout = 0")
        _validate_type_check()
        op.execute("RESET lock_timeout")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute("""
            UPDATE conversation_queries
            SET type = 'system', metadata = metadata - 'wakeup'
            WHERE type = 'wakeup'
        """)
        op.execute("DELETE FROM hook_outbox WHERE hook_type = 'wakeup'")
        _replace_type_check(_QUERY_TYPES_077)
        op.execute("SET lock_timeout = 0")
        _validate_type_check()
        op.execute("RESET lock_timeout")

    # Dropping the foreign key takes ACCESS EXCLUSIVE on conversation_threads,
    # so the table goes in a transaction that holds no other hot lock.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS wakeups")
