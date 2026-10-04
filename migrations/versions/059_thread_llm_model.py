"""Migration 059: each thread owns its model.

``conversation_threads.llm_model`` is the model a client last named for the
thread; NULL means the thread follows the account default. A column rather
than a ``metadata`` key because it is mutable, read on every turn and
bulk-updated when the default moves, while ``metadata`` holds creation-time
provenance.

Threads from the web surfaces (``web``, and ``market_view`` with or without
its symbol suffix) are seeded with the model their latest turn from a person
was sent on (``conversation_queries.metadata.llm_model``), which is what
the composer reopened a thread on before it seeded from the account default.
Only such turns count: an automation's turn (an ``origin`` in its metadata)
ran the automation's own model, and a report-back (type ``system``) ran
whatever the thread's default was, and neither is ever kept going forward.
Channel and system threads never named a model, so they stay NULL.

A thread whose latest such turn ran again stays NULL as well. A retry or a
regenerate reuses the turn's query row without rewriting its model, though
the client sends the one its picker holds, so the row may not name the model
the turn last ran; no durable record of what did run is reliable enough to
seed from instead. NULL follows the account default, as every thread did
before this column, so a thread left NULL needlessly is no worse off. A rerun
from before the v4 run lifecycle left no mark to tell it by, so it seeds from
its row.

The seed leaves ``updated_at`` alone: the thread lists sort on it, and no
trigger rewrites it (022 dropped the table's generic one).

Rollback: ``alembic stamp 058`` first, because the previous build's
``upgrade head`` cannot start from a revision it does not ship, then redeploy
it; it never reads the column. Upgrading again keeps every stored model and
seeds only the threads still NULL. ``downgrade`` drops the column, and with it
every model chosen per thread, so a rollback does not need it.
"""

from alembic import op

revision = "059"
down_revision = "058"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Own transaction: ALTER TABLE's ACCESS EXCLUSIVE would otherwise hold
    # every read of conversation_threads through the backfill.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(
            "ALTER TABLE conversation_threads ADD COLUMN IF NOT EXISTS llm_model TEXT"
        )
        op.execute("RESET lock_timeout")

    op.execute("SET LOCAL lock_timeout = '5s'")
    # The latest turn from a person that recorded a model, per thread. A turn
    # from before models were recorded has none, so "latest" skips rows
    # without one rather than seeding NULL. A turn that ran again is tested
    # after DISTINCT ON, so its thread stays NULL rather than taking an older
    # turn's model.
    op.execute("""
        UPDATE conversation_threads ct
        SET llm_model = latest.llm_model
        FROM (
            SELECT DISTINCT ON (q.conversation_thread_id)
                   q.conversation_thread_id,
                   q.turn_index,
                   q.created_at,
                   q.metadata->>'llm_model' AS llm_model
            FROM conversation_queries q
            JOIN conversation_threads t
              ON t.conversation_thread_id = q.conversation_thread_id
            WHERE (t.platform IN ('web', 'market_view')
                   OR starts_with(t.platform, 'market_view:'))
              AND COALESCE(q.metadata->>'llm_model', '') <> ''
              AND q.metadata->'origin' IS NULL
              AND q.type IS DISTINCT FROM 'system'
            ORDER BY q.conversation_thread_id, q.turn_index DESC
        ) latest
        WHERE ct.conversation_thread_id = latest.conversation_thread_id
          AND ct.llm_model IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM conversation_responses r
              WHERE r.conversation_thread_id = latest.conversation_thread_id
                AND r.turn_index = latest.turn_index
                -- A retry is a later attempt. START stamps a turn's query and
                -- first attempt with one created_at, so a first attempt
                -- stamped otherwise is a regenerate's; burst_slot_id, which
                -- START always writes, keeps out older rows stamped when
                -- their turn ended.
                AND (r.attempt_no > 1
                     OR (r.metadata ? 'burst_slot_id'
                         AND r.created_at <> latest.created_at))
          )
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE conversation_threads DROP COLUMN IF EXISTS llm_model")
