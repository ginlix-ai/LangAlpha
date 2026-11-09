"""Bots and rooms: the identity layer over threads, and the shared room log.

A workspace is unchanged — it is still the folder on a computer, the unit of
sandbox, files and workspace memory. This migration adds the two layers above
it: a bot (identity: name, persona, model, tools, approval, memory scope) that
threads belong to, and a room (a shared ordered log whose members are bots).

Enough of the room and message tables exist for the whole design to land
before the code that writes them does; the P0 slice only reads ``bots``,
``threads.bot_id`` and the backfilled ``bot_threads``.

The backfill keeps behavior identical: one analyst bot per live workspace,
threads pointed at their workspace's bot, automations pointed at it too. Home
is the one place left out: it holds the derived Chief of Staff, made lazily by
the resolver (its row may not exist yet), so a Home row gets no analyst and
its threads/automations keep no bot until that resolver stamps them.

Revision ID: 114
Revises: 113
Rollback: ``alembic downgrade 113``
"""

import uuid

import sqlalchemy as sa
from alembic import op

#: The namespace Home ids derive from (``database.home_workspace``); spelled
#: here so the migration does not import application code that may change.
_HOME_NAMESPACE = uuid.UUID("f1a50000-0000-5000-e000-f1a500000000")


revision = "114"
down_revision = "113"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bots (
            bot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            workspace_id UUID NOT NULL
                REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            name VARCHAR(64) NOT NULL,
            role VARCHAR(32) NOT NULL DEFAULT 'analyst'
                CHECK (role IN ('analyst', 'chief_of_staff')),
            persona TEXT NOT NULL DEFAULT '',
            avatar JSONB NOT NULL DEFAULT '{}'::jsonb,
            provider VARCHAR(64),
            model VARCHAR(128),
            skills JSONB NOT NULL DEFAULT '[]'::jsonb,
            tools_allow JSONB NOT NULL DEFAULT '[]'::jsonb,
            tools_deny JSONB NOT NULL DEFAULT '[]'::jsonb,
            approval VARCHAR(16) NOT NULL DEFAULT 'ask'
                CHECK (approval IN ('ask', 'edits', 'auto', 'full')),
            memory_scopes JSONB NOT NULL DEFAULT '["workspace"]'::jsonb,
            status VARCHAR(16) NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'paused')),
            primary_thread_id UUID,
            created_by VARCHAR(255) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            deleted_at TIMESTAMPTZ,
            UNIQUE (workspace_id, name)
        )
        """
    )
    # One Home bot per user, among the live ones. A deleted one may be
    # restored beside a new one only after the restore path clears the flag.
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ix_bots_one_home
        ON bots (user_id)
        WHERE role = 'chief_of_staff' AND deleted_at IS NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_bots_workspace_live
        ON bots (workspace_id, role)
        WHERE deleted_at IS NULL
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_versions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            bot_id UUID NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
            field VARCHAR(32) NOT NULL,
            value JSONB NOT NULL,
            actor VARCHAR(255) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_bot_versions_bot "
        "ON bot_versions (bot_id, created_at)"
    )

    # The identity layer's one change to the conversation table. A chat-wide
    # table must not be locked for the rest of the migration: the ALTER runs
    # in its own autocommit block (lock_timeout bounds only acquiring it), and
    # the index is built CONCURRENTLY, so turns keep reading and writing while
    # the backfill below runs.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(
            """
            ALTER TABLE conversation_threads
            ADD COLUMN IF NOT EXISTS bot_id UUID
                REFERENCES bots(bot_id) ON DELETE SET NULL
            """
        )
        op.execute("RESET lock_timeout")
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_conversation_threads_bot "
            "ON conversation_threads (bot_id)"
        )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_threads (
            bot_id UUID NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
            thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE CASCADE,
            kind VARCHAR(16) NOT NULL
                CHECK (kind IN ('private', 'room', 'routine')),
            room_id UUID,
            seen_seq BIGINT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (bot_id, thread_id)
        )
        """
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_bot_threads_room "
        "ON bot_threads (bot_id, room_id) WHERE room_id IS NOT NULL"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS rooms (
            room_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            workspace_id UUID NOT NULL
                REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            name VARCHAR(64) NOT NULL,
            host_bot_id UUID REFERENCES bots(bot_id) ON DELETE SET NULL,
            responder VARCHAR(64) NOT NULL DEFAULT 'everyone',
            bulletin TEXT NOT NULL DEFAULT '',
            status VARCHAR(16) NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'archived')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (workspace_id, name)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS room_members (
            room_id UUID NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
            member_type VARCHAR(16) NOT NULL
                CHECK (member_type IN ('bot', 'user')),
            member_id VARCHAR(255) NOT NULL,
            role VARCHAR(16) NOT NULL DEFAULT 'member'
                CHECK (role IN ('host', 'member')),
            joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (room_id, member_type, member_id)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS room_log (
            room_id UUID NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
            seq BIGSERIAL,
            round_id UUID,
            sender_type VARCHAR(16) NOT NULL
                CHECK (sender_type IN ('bot', 'user', 'system')),
            sender_id VARCHAR(255) NOT NULL,
            kind VARCHAR(16) NOT NULL DEFAULT 'message'
                CHECK (kind IN ('message', 'pass', 'status', 'system')),
            content TEXT NOT NULL,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (room_id, seq)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS room_rounds (
            round_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            room_id UUID NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
            started_by VARCHAR(255) NOT NULL,
            start_seq BIGINT NOT NULL,
            status VARCHAR(16) NOT NULL DEFAULT 'queued'
                CHECK (status IN ('queued', 'running', 'finished', 'failed')),
            slots JSONB NOT NULL DEFAULT '[]'::jsonb,
            slot_cursor INTEGER NOT NULL DEFAULT 0,
            lease_owner VARCHAR(64),
            lease_until TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ix_room_one_running_round
        ON room_rounds (room_id)
        WHERE status IN ('queued', 'running')
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_messages (
            message_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            from_type VARCHAR(16) NOT NULL
                CHECK (from_type IN ('bot', 'user', 'system')),
            from_id VARCHAR(255) NOT NULL,
            to_bot_id UUID NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
            kind VARCHAR(16) NOT NULL DEFAULT 'message'
                CHECK (kind IN ('message', 'request', 'reply', 'report',
                                'question', 'notice')),
            content TEXT NOT NULL,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            idem VARCHAR(128),
            status VARCHAR(16) NOT NULL DEFAULT 'queued'
                CHECK (status IN ('queued', 'delivered', 'consumed', 'failed')),
            thread_id UUID
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE SET NULL,
            in_reply_to UUID REFERENCES bot_messages(message_id)
                ON DELETE SET NULL,
            root_id UUID,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_attempt_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            delivered_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_bot_messages_idem "
        "ON bot_messages (user_id, idem) WHERE idem IS NOT NULL"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_bot_messages_inbox "
        "ON bot_messages (to_bot_id, status, created_at)"
    )
    # The delivery sweeper's scan: queued rows due for a retry.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_bot_messages_due "
        "ON bot_messages (last_attempt_at, created_at) WHERE status = 'queued'"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS assignments (
            assignment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            from_bot_id UUID REFERENCES bots(bot_id) ON DELETE SET NULL,
            to_bot_id UUID NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
            thread_id UUID
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE SET NULL,
            status VARCHAR(16) NOT NULL DEFAULT 'requested'
                CHECK (status IN ('requested', 'working', 'needs_input',
                                  'done', 'failed', 'cancelled')),
            outcome VARCHAR(32),
            evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
            depth INTEGER NOT NULL DEFAULT 0,
            root_id UUID,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            closed_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_assignments_to_bot "
        "ON assignments (to_bot_id, status)"
    )

    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(
            """
            ALTER TABLE automations
            ADD COLUMN IF NOT EXISTS bot_id UUID
                REFERENCES bots(bot_id) ON DELETE SET NULL
            """
        )
        op.execute("RESET lock_timeout")
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_automations_bot "
            "ON automations (bot_id)"
        )

    # --- Backfill: one analyst bot per live workspace, then point the rows
    # that already have a workspace at it. Idempotent by the unique key.
    op.execute(
        """
        INSERT INTO bots (user_id, workspace_id, name, role, model, created_by)
        SELECT w.user_id,
               w.workspace_id,
               COALESCE(NULLIF(left(btrim(w.name), 64), ''), 'Workspace'),
               'analyst',
               NULL,
               w.user_id
        FROM workspaces w
        WHERE w.status <> 'deleted'
        ON CONFLICT (workspace_id, name) DO NOTHING
        """
    )
    # Home holds the derived Chief of Staff, never a backfilled analyst:
    # answering Home turns as an analyst is a role regression, and an analyst
    # beside the CoS in Home contradicts the Bots rules. Home ids are
    # uuid5(user_id) under a fixed namespace, computed here because Postgres
    # has no uuid5; the delete runs before threads/automations are pointed at
    # bots, so nothing else references these rows yet.
    bind = op.get_bind()
    user_rows = bind.execute(
        sa.text("SELECT DISTINCT user_id FROM workspaces WHERE status <> 'deleted'")
    ).fetchall()
    home_ids = [
        str(uuid.uuid5(_HOME_NAMESPACE, str(row[0]))) for row in user_rows
    ]
    if home_ids:
        bind.execute(
            sa.text(
                "DELETE FROM bots WHERE role = 'analyst' "
                "AND workspace_id = CAST(:home AS uuid)"
            ),
            [{"home": home_id} for home_id in home_ids],
        )

    op.execute(
        """
        UPDATE conversation_threads t
        SET bot_id = b.bot_id
        FROM bots b
        WHERE b.workspace_id = t.workspace_id
          AND b.role = 'analyst'
          AND b.deleted_at IS NULL
          AND t.bot_id IS NULL
        """
    )
    op.execute(
        """
        INSERT INTO bot_threads (bot_id, thread_id, kind)
        SELECT t.bot_id, t.conversation_thread_id, 'private'
        FROM conversation_threads t
        WHERE t.bot_id IS NOT NULL
        ON CONFLICT (bot_id, thread_id) DO NOTHING
        """
    )
    op.execute(
        """
        UPDATE automations a
        SET bot_id = b.bot_id
        FROM bots b
        WHERE b.workspace_id = a.workspace_id
          AND b.role = 'analyst'
          AND b.deleted_at IS NULL
          AND a.bot_id IS NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_automations_bot")
    op.execute("ALTER TABLE automations DROP COLUMN IF EXISTS bot_id")
    op.execute("DROP TABLE IF EXISTS assignments")
    op.execute("DROP TABLE IF EXISTS bot_messages")
    op.execute("DROP INDEX IF EXISTS ix_room_one_running_round")
    op.execute("DROP TABLE IF EXISTS room_rounds")
    op.execute("DROP TABLE IF EXISTS room_log")
    op.execute("DROP TABLE IF EXISTS room_members")
    op.execute("DROP TABLE IF EXISTS rooms")
    op.execute("DROP TABLE IF EXISTS bot_threads")
    op.execute("DROP INDEX IF EXISTS ix_conversation_threads_bot")
    op.execute("ALTER TABLE conversation_threads DROP COLUMN IF EXISTS bot_id")
    op.execute("DROP TABLE IF EXISTS bot_versions")
    op.execute("DROP INDEX IF EXISTS ix_bots_workspace_live")
    op.execute("DROP INDEX IF EXISTS ix_bots_one_home")
    op.execute("DROP TABLE IF EXISTS bots")
