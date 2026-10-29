"""Migration 057: the credential a computer's file mount presents, and what
the mount serves.

A running computer mounts the user's memory, profile, workflows and
transcripts as files, served by the server on each read and write. The mount
authenticates with a random token written into the sandbox; only its SHA-256
is kept here. The previous token stays valid until its own expiry so a
rewrite never races a request already in flight. Stopping, rebuilding or
deleting the computer deletes the row, which ends every token it held.

``held_by`` names the sandbox that took the current token, since a mint can
land without its publish; a worker adopts a token without an exec only when
its own sandbox took it. The ``served_*`` columns are what the last start
answered: the sandbox whose daemon serves (``served_by``, NULL once one
answered down or the server learned it restarted), the code it runs and the
server it dials, and until when the token it holds is good. Every server
worker reads them at a turn's start, so one that has seen nothing of the
computer trusts another's start instead of asking the sandbox again. A mint
leaves them, since the daemon serves on with the token it holds, unless that
token was the one the mint pushes out.

``livefs_links`` holds the folders whose links into the mount went in, per
sandbox: a workspace's own (``workspace_id``) or the computer's (NULL). A link
writes the computer's whole set, and a workspace that leaves the computer
takes its row with it in the same transaction, so no worker serves it as
linked once it is gone. ``layout`` names the link layout the row was laid
under; a release that declares another reads no row of the old one, so
every running sandbox is linked again.
"""

from alembic import op

revision = "057"
down_revision = "056"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS livefs_tokens (
            computer_id UUID PRIMARY KEY
                REFERENCES computers(computer_id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            token_sha256 BYTEA NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            prev_token_sha256 BYTEA,
            prev_expires_at TIMESTAMPTZ,
            held_by VARCHAR(255),
            served_by VARCHAR(255),
            served_code VARCHAR(64),
            served_url TEXT,
            served_until TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    # NULLS NOT DISTINCT (PostgreSQL 15+, as 025 requires): the computer's own
    # links are one row per sandbox, like each workspace's. The constraint's
    # index also serves the per-turn read by (computer, sandbox).
    op.execute("""
        CREATE TABLE IF NOT EXISTS livefs_links (
            computer_id UUID NOT NULL
                REFERENCES computers(computer_id) ON DELETE CASCADE,
            sandbox_id VARCHAR(255) NOT NULL,
            workspace_id UUID
                REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            layout VARCHAR(64) NOT NULL,
            linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT livefs_links_folder_key
                UNIQUE NULLS NOT DISTINCT (computer_id, sandbox_id, workspace_id)
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_livefs_links_workspace "
        "ON livefs_links (workspace_id)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS livefs_links")
    op.execute("DROP TABLE IF EXISTS livefs_tokens")
