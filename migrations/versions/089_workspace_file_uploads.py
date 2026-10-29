"""Migration 089: uploads a browser sends to object storage for a sandbox to pull.

A file too large to carry through the server goes from the browser to the
store in parts, on URLs the server signs one batch at a time, and the
sandbox then pulls the assembled object into the workspace folder. Each step
of that is a separate request that may reach any worker, and the pull
outlives the request that starts it, so ``workspace_file_uploads`` is the one
record of an upload: what it may write (``path``, ``size``, ``part_size``,
``part_count``, fixed at its start), where its bytes are (``object_key``,
``store_upload_id``), and how far it got (``state``).

``expires_at`` is when an upload nobody is sending parts for is given up;
signing a batch moves it. ``lease_until`` is a worker's claim while it
assembles the parts or runs the pull, renewed as it works, so a claim whose
worker died lapses and another worker takes it up; ``attempts`` counts the
pulls claimed and is the fencing token each writes its result under.
``touched_at`` paces how often signing counts as use of the computer.
``placing_inode`` is set by an attempt about to move the fetched file into
place: the fetched copy's inode, which the file keeps through the rename. An
attempt after one whose end was never recorded settles the pull by what the
folder holds instead of placing the file over it again, and only a file with
that inode at the path is the upload's; another there is what it would have
replaced.
``cleaned_at`` is when the upload's object, or its unfinished parts, were
removed from the store; a row without it still holds bytes there.

No foreign key to ``workspaces``: an upload into a workspace deleted midway
still holds parts in the store, and the sweep that removes them needs the
row to find them.

Rollback: ``alembic downgrade 088``. It drops the table. Uploads in flight
are forgotten, and their objects and parts stay in the store until the
bucket's lifecycle rules remove them.
"""

from alembic import op

revision = "089"
down_revision = "088"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS workspace_file_uploads (
            upload_id UUID PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            workspace_id UUID NOT NULL,
            path TEXT NOT NULL,
            size BIGINT NOT NULL CHECK (size > 0),
            part_size BIGINT NOT NULL CHECK (part_size > 0),
            part_count INTEGER NOT NULL CHECK (part_count > 0),
            sha256 TEXT,
            object_key TEXT NOT NULL,
            store_upload_id TEXT NOT NULL,
            sandbox_id TEXT,
            state TEXT NOT NULL DEFAULT 'uploading' CHECK (state IN (
                'uploading', 'completing', 'pulling', 'done', 'failed', 'aborted'
            )),
            error TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            lease_until TIMESTAMPTZ,
            expires_at TIMESTAMPTZ NOT NULL,
            touched_at TIMESTAMPTZ,
            placing_inode BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            cleaned_at TIMESTAMPTZ
        )
    """)
    # Serves the checks made per user: the open-upload cap and the room left
    # on the user's computers at each start, and the one live pull per user;
    # and the sweep, which only ever reads rows still holding bytes in the store.
    op.execute("""
        CREATE INDEX IF NOT EXISTS workspace_file_uploads_open
        ON workspace_file_uploads (user_id)
        WHERE cleaned_at IS NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS workspace_file_uploads_cleaned
        ON workspace_file_uploads (cleaned_at)
        WHERE cleaned_at IS NOT NULL
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS workspace_file_uploads")
