"""Migration 101: the file is the artifact.

A workspace file gets one record, the ``share_links`` row its ``/a/`` link
already uses, extended here rather than moved: a worker of the previous build
keeps reading and writing that table through a blue/green cutover, and every
column added has a default such a worker can leave out. A record now exists
before any share, made by whichever comes first of the agent writing the file,
a comment on it, a binding or a publish, so a private file row is ordinary.
``artifact_id`` names the record everywhere else; ``code`` stays the link's.

``created_thread_id`` is the first thread whose agent wrote the file, and
``bound_thread_id`` the thread the owner chose to send its comments to. Either
is cleared when its thread is deleted, and comments then go to the next thread
in line, which the app decides.

Publishing shares the file and cuts a version: a copy of the file and the
files it reads, each under its workspace-relative path, so
``../../data/x.json`` from ``task-a/report/index.html`` resolves inside the
copy. ``latest_seq`` is 0 until the first version. A shared row keeps
``files`` as its latest version's paths, so 049's check holds and a previous
build's worker serves the same list. Unsharing keeps the versions, and sharing
again resumes at the same link. ``origin`` says who cut a version: the owner,
the end of a turn for a link kept updated, or the first view of a link shared
before versions existed. Links already shared followed the live file, so they
start kept updated.

``artifact_version_files`` keeps each file the way ``thread_transcript_files``
does: its bytes in ``content`` when they are small or there is no object
storage, else the content-addressed blob ``sha256`` names. ``user_id`` is the
blob's namespace, on the row so the blob sweep's reference check needs no join.

A comment sits on the live file, not a version. It keeps the file's path and
sha256 when it was made, so a reader can tell the file has changed under it,
and the document's title then. ``number`` is its pin's number, taken from the
record's counter so it never shifts. ``delivery`` is ``note`` (never sent),
``queued`` (waiting for a thread's next send) or ``sent``; ``thread_id`` is
the thread it waits for or last went to. A sent owner message records its
thread, the only one whose agent may answer it, and the send's key, so a
retried send re-collects what it marked. Every message takes the next ``seq``
under the comment's row lock, so seq order is commit order, which
``created_at`` (the transaction's start) is not. ``artifact_comment_reads`` is
the owner's read cursor per comment, apart from the comment because reading is
not an edit.

``server_keys`` gains ``artifact_grant``, behind the owner's grant to a
published version, and ``artifact_share_grant``, behind a visitor's. Each is
apart from the other and from ``file_grant``, so no grant can be read as
another kind.

Rollback: ``alembic downgrade 100``. The previous build never reads the new
columns or tables, so it can also run beside them; downgrading drops every
version, comment and binding.

Revision ID: 101
Revises: 100
"""

from alembic import op

revision = "101"
down_revision = "100"
branch_labels = None
depends_on = None

# A workspace-relative path: no leading slash, no empty, ``.`` or ``..``
# segment, so a served URL can name it and nothing above the workspace.
_PATH_CHECK = r"""
    char_length({col}) BETWEEN 1 AND 1024
    AND {col} !~ '(^/|(^|/)\.\.?(/|$)|//)'
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE share_links
            ADD COLUMN IF NOT EXISTS artifact_id UUID NOT NULL
                DEFAULT gen_random_uuid(),
            ADD COLUMN IF NOT EXISTS created_thread_id UUID
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE SET NULL,
            ADD COLUMN IF NOT EXISTS bound_thread_id UUID
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE SET NULL,
            ADD COLUMN IF NOT EXISTS keep_updated BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS latest_seq INT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS last_comment_number INT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS legacy_refused VARCHAR(16)
    """)
    # An app row gets an artifact_id from the default and never uses it; the
    # rest stays at its defaults, which a previous build's insert relies on.
    op.execute("""
        ALTER TABLE share_links
            DROP CONSTRAINT IF EXISTS share_links_artifact_counters,
            ADD CONSTRAINT share_links_artifact_counters CHECK (
                latest_seq >= 0 AND last_comment_number >= 0
            ),
            DROP CONSTRAINT IF EXISTS share_links_artifact_kind,
            ADD CONSTRAINT share_links_artifact_kind CHECK (
                kind = 'file' OR (
                    created_thread_id IS NULL AND bound_thread_id IS NULL
                    AND NOT keep_updated AND latest_seq = 0
                    AND last_comment_number = 0
                )
            ),
            -- A link shared before versions whose files are past the caps:
            -- its first version is not tried again until the owner publishes.
            DROP CONSTRAINT IF EXISTS share_links_legacy_refused,
            ADD CONSTRAINT share_links_legacy_refused CHECK (
                legacy_refused IS NULL OR (
                    kind = 'file' AND latest_seq = 0
                    AND legacy_refused IN ('too_many_files', 'too_many_bytes')
                )
            )
    """)
    # The key the artifact tables reference.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_share_links_artifact
        ON share_links (artifact_id)
    """)
    # A thread's delete clears these, and finds the rows through them.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_share_links_created_thread
        ON share_links (created_thread_id) WHERE created_thread_id IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_share_links_bound_thread
        ON share_links (bound_thread_id) WHERE bound_thread_id IS NOT NULL
    """)
    op.execute("""
        UPDATE share_links SET keep_updated = TRUE
         WHERE kind = 'file' AND shared_at IS NOT NULL AND latest_seq = 0
    """)
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS artifact_versions (
            artifact_id UUID NOT NULL
                REFERENCES share_links(artifact_id) ON DELETE CASCADE,
            seq INT NOT NULL,
            entry_path TEXT NOT NULL,
            origin VARCHAR(16) NOT NULL,
            title TEXT,
            label TEXT,
            byte_len BIGINT NOT NULL,
            published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (artifact_id, seq),
            CONSTRAINT artifact_versions_seq CHECK (seq >= 1),
            CONSTRAINT artifact_versions_origin CHECK (
                origin IN ('owner', 'keep_updated', 'legacy')
            ),
            CONSTRAINT artifact_versions_title_length CHECK (
                title IS NULL OR char_length(title) BETWEEN 1 AND 300
            ),
            CONSTRAINT artifact_versions_label_length CHECK (
                label IS NULL OR char_length(label) BETWEEN 1 AND 200
            ),
            CONSTRAINT artifact_versions_byte_len CHECK (byte_len >= 0),
            CONSTRAINT artifact_versions_entry_path CHECK (
                {_PATH_CHECK.format(col="entry_path")}
            )
        )
    """)
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS artifact_version_files (
            artifact_id UUID NOT NULL,
            seq INT NOT NULL,
            path TEXT NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            sha256 VARCHAR(64) NOT NULL,
            byte_len BIGINT NOT NULL,
            content_type TEXT NOT NULL,
            content BYTEA,
            PRIMARY KEY (artifact_id, seq, path),
            FOREIGN KEY (artifact_id, seq)
                REFERENCES artifact_versions(artifact_id, seq) ON DELETE CASCADE,
            CONSTRAINT artifact_version_files_sha256 CHECK (
                sha256 ~ '^[0-9a-f]{{64}}$'
            ),
            CONSTRAINT artifact_version_files_byte_len CHECK (byte_len >= 0),
            CONSTRAINT artifact_version_files_path CHECK (
                {_PATH_CHECK.format(col="path")}
            )
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_artifact_version_files_sha256
        ON artifact_version_files (sha256)
    """)
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS artifact_comments (
            comment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            artifact_id UUID NOT NULL
                REFERENCES share_links(artifact_id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            number INT NOT NULL,
            path TEXT NOT NULL,
            file_sha256 VARCHAR(64),
            title TEXT,
            anchor JSONB NOT NULL,
            status VARCHAR(16) NOT NULL DEFAULT 'open',
            delivery VARCHAR(8) NOT NULL DEFAULT 'note',
            sent_from VARCHAR(8),
            thread_id UUID
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE SET NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            resolved_at TIMESTAMPTZ,
            CONSTRAINT artifact_comments_number UNIQUE (artifact_id, number),
            CONSTRAINT artifact_comments_number_positive CHECK (number >= 1),
            CONSTRAINT artifact_comments_path CHECK (
                {_PATH_CHECK.format(col="path")}
            ),
            CONSTRAINT artifact_comments_file_sha256 CHECK (
                file_sha256 IS NULL OR file_sha256 ~ '^[0-9a-f]{{64}}$'
            ),
            CONSTRAINT artifact_comments_title_length CHECK (
                title IS NULL OR char_length(title) BETWEEN 1 AND 300
            ),
            CONSTRAINT artifact_comments_status CHECK (
                status IN ('open', 'resolved')
            ),
            CONSTRAINT artifact_comments_delivery CHECK (
                delivery IN ('note', 'queued', 'sent')
            ),
            CONSTRAINT artifact_comments_sent_from CHECK (
                (delivery = 'sent') = (sent_from IS NOT NULL)
                AND (sent_from IS NULL OR sent_from IN ('note', 'queued'))
            ),
            CONSTRAINT artifact_comments_queued_open CHECK (
                delivery <> 'queued' OR status = 'open'
            ),
            -- Not the converse: a thread's delete clears thread_id under a
            -- queued or sent comment, which then goes to the next in line.
            CONSTRAINT artifact_comments_note_thread CHECK (
                delivery <> 'note' OR thread_id IS NULL
            ),
            CONSTRAINT artifact_comments_resolved_at CHECK (
                (status = 'resolved') = (resolved_at IS NOT NULL)
            )
        )
    """)
    # What a thread's next send carries, and what its delete clears.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_artifact_comments_thread
        ON artifact_comments (thread_id) WHERE thread_id IS NOT NULL
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS artifact_comment_messages (
            message_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            comment_id UUID NOT NULL
                REFERENCES artifact_comments(comment_id) ON DELETE CASCADE,
            seq INT NOT NULL,
            author VARCHAR(8) NOT NULL,
            body TEXT NOT NULL,
            sent_at TIMESTAMPTZ,
            request_key UUID,
            thread_id UUID
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE SET NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT artifact_comment_messages_comment_seq UNIQUE (comment_id, seq),
            CONSTRAINT artifact_comment_messages_seq CHECK (seq >= 1),
            CONSTRAINT artifact_comment_messages_author CHECK (
                author IN ('user', 'agent')
            ),
            CONSTRAINT artifact_comment_messages_sent CHECK (
                (sent_at IS NULL) = (request_key IS NULL)
                AND (author = 'user' OR sent_at IS NULL)
            ),
            -- An owner's message names a thread once a send carried it; an
            -- agent's names the thread it answered from.
            CONSTRAINT artifact_comment_messages_thread CHECK (
                author = 'agent' OR sent_at IS NOT NULL OR thread_id IS NULL
            ),
            CONSTRAINT artifact_comment_messages_body_length CHECK (
                char_length(body) BETWEEN 1 AND 8000
            )
        )
    """)
    # A send the server refuses gives back what its key marked.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_artifact_comment_messages_request_key
        ON artifact_comment_messages (request_key) WHERE request_key IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_artifact_comment_messages_sent_at
        ON artifact_comment_messages (sent_at) WHERE sent_at IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_artifact_comment_messages_thread
        ON artifact_comment_messages (thread_id) WHERE thread_id IS NOT NULL
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS artifact_comment_reads (
            comment_id UUID NOT NULL
                REFERENCES artifact_comments(comment_id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            read_through INT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (comment_id, user_id),
            CONSTRAINT artifact_comment_reads_read_through CHECK (read_through >= 0)
        )
    """)
    op.execute("""
        INSERT INTO server_keys (name, secret)
        VALUES ('artifact_grant', gen_random_bytes(32)),
               ('artifact_share_grant', gen_random_bytes(32))
        ON CONFLICT (name) DO NOTHING
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "DELETE FROM server_keys WHERE name IN ('artifact_grant', 'artifact_share_grant')"
    )
    op.execute("DROP TABLE IF EXISTS artifact_comment_reads")
    op.execute("DROP TABLE IF EXISTS artifact_comment_messages")
    op.execute("DROP TABLE IF EXISTS artifact_comments")
    op.execute("DROP TABLE IF EXISTS artifact_version_files")
    op.execute("DROP TABLE IF EXISTS artifact_versions")
    op.execute("DROP INDEX IF EXISTS idx_share_links_bound_thread")
    op.execute("DROP INDEX IF EXISTS idx_share_links_created_thread")
    op.execute("DROP INDEX IF EXISTS idx_share_links_artifact")
    op.execute("""
        ALTER TABLE share_links
            DROP CONSTRAINT IF EXISTS share_links_artifact_kind,
            DROP CONSTRAINT IF EXISTS share_links_artifact_counters,
            DROP CONSTRAINT IF EXISTS share_links_legacy_refused,
            DROP COLUMN IF EXISTS last_comment_number,
            DROP COLUMN IF EXISTS legacy_refused,
            DROP COLUMN IF EXISTS latest_seq,
            DROP COLUMN IF EXISTS keep_updated,
            DROP COLUMN IF EXISTS bound_thread_id,
            DROP COLUMN IF EXISTS created_thread_id,
            DROP COLUMN IF EXISTS artifact_id
    """)
