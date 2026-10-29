"""Migration 049: one stable short link per file or app, and the grant key.

A link is private by default and sharing flips ``shared_at``; the code never
changes, so a URL survives being stopped and shared again. A ``file`` row that
is shared carries the exact list a visitor may fetch, frozen at share time, so
the set never grows on its own when the report gains a new asset. ``revision``
moves on every share and stop, which is what a share compares against: a stop
clears ``shared_at`` back to a value a slow share may already have read.

``server_keys`` holds the HMAC key behind the owner's path grants. It is
minted here by Postgres rather than read from an env var so every worker and
both deploy colours sign with one key without any configuration.

Every statement is IF NOT EXISTS / ON CONFLICT DO NOTHING so a blocked branch
can be re-applied by hand.
"""

from alembic import op

revision = "049"
down_revision = "048"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS share_links (
            code TEXT PRIMARY KEY,
            workspace_id UUID NOT NULL
                REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            kind TEXT NOT NULL CHECK (kind IN ('file', 'app')),
            path TEXT,
            port INTEGER,
            title TEXT,
            files TEXT[],
            shared_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            revision INTEGER NOT NULL DEFAULT 0,
            CONSTRAINT share_links_file_shape CHECK (
                kind <> 'file' OR (path IS NOT NULL AND port IS NULL)
            ),
            CONSTRAINT share_links_app_shape CHECK (
                kind <> 'app'
                OR (port BETWEEN 3000 AND 9999 AND shared_at IS NULL AND files IS NULL)
            ),
            CONSTRAINT share_links_shared_files CHECK (
                shared_at IS NULL
                OR (files IS NOT NULL AND cardinality(files) > 0 AND path = ANY(files))
            )
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_share_links_file
        ON share_links (workspace_id, path) WHERE kind = 'file'
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_share_links_app
        ON share_links (workspace_id, port) WHERE kind = 'app'
    """)
    # Both unique indexes are partial, so neither serves a lookup by workspace
    # alone: this one does, for the shared list and the workspace delete.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_share_links_workspace
        ON share_links (workspace_id)
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS server_keys (
            name TEXT PRIMARY KEY,
            secret BYTEA NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    # pgcrypto has been present since 001.
    op.execute("""
        INSERT INTO server_keys (name, secret)
        VALUES ('file_grant', gen_random_bytes(32))
        ON CONFLICT (name) DO NOTHING
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS share_links")
    op.execute("DROP TABLE IF EXISTS server_keys")
