"""Repair what the first cut of the bots layer could leave behind.

Revision 114 was amended before release: a Home turn must run as the derived
Chief of Staff, not as a backfilled analyst, and a bot thread created by a run
must carry the bot from its first turn. A database that already ran the first
cut of 114 will not re-run the amended revision (the revision id did not
change), so this migration repairs that residue, idempotently:

- a backfilled analyst bot in Home is unlinked from its threads and
  automations and soft-deleted (the resolver now derives the CoS there);
- a private bot-thread mapping that points at a shared or archived thread is
  dropped, and a bot's ``primary_thread_id`` pointing there is cleared, so the
  next delivery mints a dedicated thread instead of adopting a user's chat;
- the live-name uniqueness becomes partial (``deleted_at IS NULL``), so a
  soft-deleted bot frees its name for the workspace's default bot;
- the sweeper scans get the indexes they read by.

Rollback drops the indexes and restores the all-rows name constraint where it
still fits; the unlinked/soft-deleted rows stay unlinked because reusing a
Home analyst would be the bug this repairs.

Revision ID: 115
Revises: 114
Rollback: ``alembic downgrade 114``
"""

import uuid

import sqlalchemy as sa
from alembic import op

#: The namespace Home ids derive from (``database.home_workspace``); spelled
#: here so the migration does not import application code that may change.
_HOME_NAMESPACE = uuid.UUID("f1a50000-0000-5000-e000-f1a500000000")


revision = "115"
down_revision = "114"
branch_labels = None
depends_on = None


def _home_ids(bind) -> list[str]:
    rows = bind.execute(
        sa.text("SELECT DISTINCT user_id FROM workspaces WHERE status <> 'deleted'")
    ).fetchall()
    return [str(uuid.uuid5(_HOME_NAMESPACE, str(row[0]))) for row in rows]


def upgrade() -> None:
    bind = op.get_bind()
    home_ids = _home_ids(bind)
    stale_ids = []
    for home_id in home_ids:
        # One execute per Home: a list of parameter sets is an executemany,
        # whose SELECT result is closed and returns no rows.
        rows = bind.execute(
            sa.text(
                "SELECT bot_id FROM bots WHERE role = 'analyst' "
                "AND workspace_id = CAST(:home AS uuid)"
            ),
            {"home": home_id},
        ).fetchall()
        stale_ids.extend(row[0] for row in rows)
    for bot_id in stale_ids:
        bind.execute(
            sa.text(
                "UPDATE conversation_threads SET bot_id = NULL "
                "WHERE bot_id = :bot"
            ),
            {"bot": bot_id},
        )
        bind.execute(
            sa.text("UPDATE automations SET bot_id = NULL WHERE bot_id = :bot"),
            {"bot": bot_id},
        )
        bind.execute(
            sa.text("DELETE FROM bot_threads WHERE bot_id = :bot"),
            {"bot": bot_id},
        )
    if home_ids:
        bind.execute(
            sa.text(
                "UPDATE bots SET deleted_at = NOW(), status = 'paused' "
                "WHERE role = 'analyst' AND workspace_id = CAST(:home AS uuid) "
                "AND deleted_at IS NULL"
            ),
            [{"home": home_id} for home_id in home_ids],
        )

    # A private mapping is a bot's own conversation, never a user's shared or
    # archived chat the first cut may have adopted.
    op.execute(
        """
        DELETE FROM bot_threads bt
        USING conversation_threads t
        WHERE bt.thread_id = t.conversation_thread_id
          AND bt.kind = 'private'
          AND (t.is_shared OR t.archived_at IS NOT NULL)
        """
    )
    # A mapping must name the bot the thread actually belongs to; a row the
    # first cut wrote against another bot's thread is not private to anyone.
    op.execute(
        """
        DELETE FROM bot_threads bt
        USING conversation_threads t
        WHERE bt.kind = 'private'
          AND t.conversation_thread_id = bt.thread_id
          AND t.bot_id IS DISTINCT FROM bt.bot_id
        """
    )
    op.execute(
        """
        UPDATE bots b
        SET primary_thread_id = NULL, updated_at = NOW()
        FROM conversation_threads t
        WHERE b.primary_thread_id = t.conversation_thread_id
          AND (
              t.is_shared
              OR t.archived_at IS NOT NULL
              OR t.bot_id IS DISTINCT FROM b.bot_id
          )
        """
    )

    # A delivery claims its row before posting (the claim is a lease, so a
    # crashed attempt is retried after it lapses, and two sweepers never post
    # the same message). The first cut had no such column.
    op.execute(
        "ALTER TABLE bot_messages "
        "ADD COLUMN IF NOT EXISTS delivering_until TIMESTAMPTZ"
    )

    # A retried tool call must not leave a second assignment (which would keep
    # blocking the target as "waiting on you").
    op.execute(
        "ALTER TABLE assignments "
        "ADD COLUMN IF NOT EXISTS idem VARCHAR(128)"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_assignments_idem "
        "ON assignments (user_id, idem) WHERE idem IS NOT NULL"
    )

    # A deleted bot must not keep its name hostage: only live rows are unique.
    op.execute(
        "ALTER TABLE bots DROP CONSTRAINT IF EXISTS bots_workspace_id_name_key"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_bots_live_name "
        "ON bots (workspace_id, name) WHERE deleted_at IS NULL"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_bots_user "
        "ON bots (user_id) WHERE deleted_at IS NULL"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_room_rounds_due "
        "ON room_rounds (status, lease_until) "
        "WHERE status IN ('queued', 'running')"
    )
    # A slot's reply is appended once, even when a resumed round re-adopts the
    # same run; a user send's request key makes a client retry one message.
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_room_log_round_slot "
        "ON room_log (round_id, ((metadata->>'slot')::int)) "
        "WHERE metadata ? 'slot'"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_room_log_request_key "
        "ON room_log (room_id, (metadata->>'request_key')) "
        "WHERE metadata ? 'request_key'"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_assignments_idem")
    op.execute("ALTER TABLE assignments DROP COLUMN IF EXISTS idem")
    op.execute(
        "ALTER TABLE bot_messages DROP COLUMN IF EXISTS delivering_until"
    )
    op.execute("DROP INDEX IF EXISTS ix_room_log_request_key")
    op.execute("DROP INDEX IF EXISTS ix_room_log_round_slot")
    op.execute("DROP INDEX IF EXISTS ix_room_rounds_due")
    op.execute("DROP INDEX IF EXISTS ix_bots_user")
    op.execute("DROP INDEX IF EXISTS ix_bots_live_name")
    # The old constraint covers deleted rows too; if a freed name was reused
    # the restore cannot hold, and the notice says why the index stays partial.
    op.execute(
        """
        DO $$
        BEGIN
            BEGIN
                ALTER TABLE bots
                    ADD CONSTRAINT bots_workspace_id_name_key
                    UNIQUE (workspace_id, name);
            EXCEPTION
                WHEN unique_violation THEN
                    RAISE NOTICE 'bot name duplicate over deleted rows; '
                                 'the live-name index stays partial';
                WHEN duplicate_table THEN NULL;
            END;
        END $$;
        """
    )
