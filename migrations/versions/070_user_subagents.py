"""Migration 070: each user's own subagents and their changes to the others.

A row is either a subagent the user wrote (``kind = 'custom'``: its
description in ``fields``, its instructions, the roles it serves) or the
user's changes to one they did not write (``kind = 'override'``), keyed to
that base by ``base_key`` (``builtin:<name>``, ``deployment:<name>``,
``plugin:<plugin name>/<agent>``). An override stores only the keys the user
set, so a later improvement to the base still reaches someone who changed
only its model.

``workspace_id`` is reserved for per-workspace settings and NULL in this
version, so uniqueness is per user on the NULL scope. One name has at most one
row, which is what lets a custom subagent and an override never collide.
``user_id`` is a bare VARCHAR(255) with no foreign key, the convention every
user-scoped table follows, and ``updated_thread_id`` has none either, so a
deleted thread keeps the provenance line rather than the row.

Rollback: ``alembic stamp 069`` first, because the previous build's
``upgrade head`` cannot start from a revision it does not ship, then redeploy
it; it never reads the table. ``downgrade`` drops it, and with it every
user's subagent settings.
"""

from alembic import op

revision = "070"
down_revision = "069"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_subagents (
            user_subagent_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL,
            -- Unbounded: an override carries a deployment subagent's name,
            -- which the config does not limit.
            name TEXT NOT NULL,
            workspace_id UUID NULL,
            kind VARCHAR(16) NOT NULL,
            base_key TEXT NULL,
            fields JSONB NOT NULL DEFAULT '{}',
            instructions TEXT NULL,
            roles TEXT[] NULL,
            updated_by VARCHAR(16) NOT NULL,
            updated_thread_id UUID NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT user_subagents_kind CHECK (kind IN ('custom', 'override')),
            -- An override always names the base it was written against, and
            -- only a custom subagent has instructions of its own.
            CONSTRAINT user_subagents_base_key CHECK (
                (kind = 'override') = (base_key IS NOT NULL)
            ),
            CONSTRAINT user_subagents_instructions CHECK (
                kind = 'custom' OR instructions IS NULL
            ),
            CONSTRAINT user_subagents_updated_by CHECK (
                updated_by IN ('user', 'agent')
            )
        )
    """)
    op.execute(
        "DROP TRIGGER IF EXISTS update_user_subagents_updated_at ON user_subagents"
    )
    op.execute("""
        CREATE TRIGGER update_user_subagents_updated_at
        BEFORE UPDATE ON user_subagents
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """)
    # A partial index rather than UNIQUE(user_id, workspace_id, name): NULL
    # workspace rows would never collide under a composite unique, and the
    # per-turn read and ON CONFLICT both infer it by columns and predicate.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_user_subagents_user_name
        ON user_subagents(user_id, name)
        WHERE workspace_id IS NULL
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS user_subagents CASCADE")
