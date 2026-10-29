"""One table for both account-wide off switches, told apart by kind.

A bundle needs exactly what 027 already gives a builtin server: a row whose
presence is the whole state, with no flag anywhere else to carry. ``kind`` is
what lets the two share that table instead of duplicating it, and it is part
of the key because bundle names and server names are separate namespaces, so
a bundle named after a builtin would otherwise collide into one row.

Revision ID: 031
Revises: 030
"""

from alembic import op


revision = "031"
down_revision = "030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The DEFAULT backfills the existing rows, all server disables by
    # construction, and is dropped below so a writer that forgets ``kind``
    # fails rather than landing in whichever bucket came first.
    op.execute("""
        ALTER TABLE user_mcp_builtin_disables
            ADD COLUMN IF NOT EXISTS kind VARCHAR(16) NOT NULL DEFAULT 'server'
    """)
    op.execute("""
        ALTER TABLE user_mcp_builtin_disables
            ADD CONSTRAINT user_mcp_builtin_disables_kind_check
            CHECK (kind IN ('server', 'bundle'))
    """)
    op.execute("""
        ALTER TABLE user_mcp_builtin_disables
            DROP CONSTRAINT user_mcp_builtin_disables_pkey
    """)
    op.execute("""
        ALTER TABLE user_mcp_builtin_disables
            ADD PRIMARY KEY (user_id, kind, name)
    """)
    op.execute("""
        ALTER TABLE user_mcp_builtin_disables ALTER COLUMN kind DROP DEFAULT
    """)
    op.execute("""
        COMMENT ON TABLE user_mcp_builtin_disables IS
        'Account-wide off switches for shipped things. A row is a disable; '
        'absence is enabled. kind tells a builtin MCP server apart from the '
        'plugins/ bundle that ships one. Named for servers alone in 027.'
    """)


def downgrade() -> None:
    op.execute("DELETE FROM user_mcp_builtin_disables WHERE kind <> 'server'")
    op.execute("""
        ALTER TABLE user_mcp_builtin_disables
            DROP CONSTRAINT user_mcp_builtin_disables_pkey
    """)
    # Dropping the column takes the CHECK with it.
    op.execute("ALTER TABLE user_mcp_builtin_disables DROP COLUMN kind")
    op.execute("""
        ALTER TABLE user_mcp_builtin_disables ADD PRIMARY KEY (user_id, name)
    """)
    op.execute("COMMENT ON TABLE user_mcp_builtin_disables IS NULL")
