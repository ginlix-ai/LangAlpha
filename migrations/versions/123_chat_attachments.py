"""Migration 123: files attached in the chat composer, uploaded before the message is sent.

The composer uploads a file the moment it is added, to a place tied to the
message rather than to a workspace: the workspace a message goes to may not
be bound to a computer yet, its computer may be stopped, and on the Dashboard
the target can change after the file is added. The turn that sends the
message places the file in its workspace. Each step is a separate request
that may reach any worker, so ``chat_attachments`` is the one record of an
attachment: what was declared at its start (``name``, ``mime_type``,
``size``), how its bytes travel (``mode``: ``relay`` through the server in
one request, ``direct`` from the browser to the store in parts of
``part_size``, ``part_count`` of them) and where they are.

The bytes are in one of two places. ``object_key`` names the store object
when the deployment has object storage, with ``store_upload_id`` the open
multipart upload a ``direct`` one is sent into; that object outlives the
attachment, as the copy the thread's history shows. Without storage the bytes
are in ``content``, only until the turns that use them are past retrying.

``state`` is ``uploading`` until every byte is in, then ``ready``, or
``aborted`` for one cancelled or given up. ``expires_at`` is when an upload
nobody is sending to is given up; signing a batch of parts moves it.
``ready_at`` starts the clock on one that is never sent, and ``sent_at``,
stamped by the first turn that uses it, keeps it: a retry of that turn
resolves the same attachment again. ``lease_until`` is a worker's claim while
it removes an aborted attachment's bytes from the store, and ``cleaned_at``
says they are gone.

No foreign key to ``users`` or ``threads``: an attachment belongs to no thread
until a message is sent, and the sweep that removes its bytes needs the row
whatever happened to its user.

Rollback: ``alembic downgrade 122``. It drops the table. Attachments not yet
sent are forgotten, and their objects and parts stay in the store until the
bucket's lifecycle rules remove them; sent ones keep their objects, which the
history links to directly.
"""

from alembic import op

revision = "123"
down_revision = "122"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS chat_attachments (
            attachment_id UUID PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            name TEXT NOT NULL,
            mime_type TEXT NOT NULL,
            size BIGINT NOT NULL CHECK (size >= 0),
            mode TEXT NOT NULL CHECK (mode IN ('relay', 'direct')),
            object_key TEXT,
            store_upload_id TEXT,
            part_size BIGINT,
            part_count INTEGER,
            content BYTEA,
            state TEXT NOT NULL DEFAULT 'uploading' CHECK (
                state IN ('uploading', 'ready', 'aborted')
            ),
            error TEXT,
            expires_at TIMESTAMPTZ NOT NULL,
            lease_until TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ready_at TIMESTAMPTZ,
            sent_at TIMESTAMPTZ,
            cleaned_at TIMESTAMPTZ,
            CHECK (mode = 'relay' OR (
                object_key IS NOT NULL AND store_upload_id IS NOT NULL
                AND part_size > 0 AND part_count > 0
            ))
        )
    """)
    # The per-user cap on uploads in progress, and the sweep's idle check.
    op.execute("""
        CREATE INDEX IF NOT EXISTS chat_attachments_uploading
        ON chat_attachments (user_id, expires_at)
        WHERE state = 'uploading'
    """)
    # Ready but never sent: removed a day after they were ready.
    op.execute("""
        CREATE INDEX IF NOT EXISTS chat_attachments_unsent
        ON chat_attachments (ready_at)
        WHERE state = 'ready' AND sent_at IS NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS chat_attachments_sent
        ON chat_attachments (sent_at)
        WHERE sent_at IS NOT NULL
    """)
    # The per-user cap on bytes Postgres holds, on a deployment without storage.
    op.execute("""
        CREATE INDEX IF NOT EXISTS chat_attachments_held
        ON chat_attachments (user_id)
        WHERE object_key IS NULL
    """)
    # The per-user cap on bytes waiting in the store for a message.
    op.execute("""
        CREATE INDEX IF NOT EXISTS chat_attachments_staged
        ON chat_attachments (user_id)
        WHERE sent_at IS NULL AND state <> 'aborted'
    """)
    # Aborted ones until their bytes leave the store, then until the row goes.
    op.execute("""
        CREATE INDEX IF NOT EXISTS chat_attachments_aborted
        ON chat_attachments (updated_at)
        WHERE state = 'aborted' AND cleaned_at IS NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS chat_attachments_cleaned
        ON chat_attachments (cleaned_at)
        WHERE cleaned_at IS NOT NULL
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS chat_attachments")
