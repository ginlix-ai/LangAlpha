"""Migration 150: the Drive, a server-held file tree per space.

A space (``drive_spaces``) holds entries (``drive_entries``): files and
folders whose ids survive a rename or a move. A file's every saved content is a
version (``drive_versions``) that says who wrote it and why; the bytes are kept
once per space by digest (``drive_contents``), in the row itself or in object
storage under the blob registry. Only a personal space, one per user, is made
for now; the ``kind`` leaves room for spaces several people share.

``change_seq`` on the space orders every change in it: each changed entry
takes the space's new value, so a client asks for what changed since the one
it last saw. A trashed entry stays as the notice of its delete until the trash
is purged, which raises ``purged_seq`` so a client behind it starts over.

Live paths are unique by ``path_key``, the path case-folded as the app
computes it (NFC, full case folding, NFC), so a copy to a case-insensitive
disk never finds two files on one name; ``lower()`` would let ``straße`` and
``STRASSE`` both in. The key compares bytewise (``COLLATE "C"``), so the
entries under a folder are one range of the live path index. A deleted entry says how it ``ended``: in the trash
(``trash_id`` set, restorable) or as a tombstone, the source of a rename over
a file, kept only as the notice of its end and purged after a day.
``trash_sitting`` lets a burst of deletes by one actor in one sitting share
one trash id, so ``rm -r`` arriving a file at a time is one item to restore.

An entry keeps only its head's ``head_seq``: what the head holds (digest,
size, type, whether it is text) is read from that version, so a save writes
it once.

What a space keeps is counted as it changes (``COUNTERS``, below), so no
save and no usage read scans the space.

The blob registry gains one trigger (``KEEP_DRIVE_BLOBS``, below): a row a
Drive file's bytes are in is never condemned or deleted, whatever code runs
the sweep, so a release that predates the Drive cannot reclaim its bytes.

Online-safe: new empty tables and their triggers, and a trigger on
``workspace_file_blobs`` (a brief lock to create, under the lock timeout);
no backfill.
Rollback: roll the code back first, then ``alembic downgrade 149``, which
drops the tables and with them every Drive file, version and trash; the bytes
they kept in object storage lose their reference and the blob sweep collects
them.
"""

from alembic import op

revision = "150"
down_revision = "149"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS drive_spaces (
            space_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            kind VARCHAR(16) NOT NULL
                CHECK (kind IN ('personal', 'team', 'org')),
            user_id VARCHAR(255) REFERENCES users(user_id) ON DELETE CASCADE,
            change_seq BIGINT NOT NULL DEFAULT 0,
            purged_seq BIGINT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            entry_count BIGINT NOT NULL DEFAULT 0,
            held_bytes BIGINT NOT NULL DEFAULT 0,
            released_bytes BIGINT NOT NULL DEFAULT 0,
            gone_bytes BIGINT NOT NULL DEFAULT 0,
            history_checked_at TIMESTAMPTZ,
            binary_versions BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT drive_spaces_personal_owner
                CHECK (kind <> 'personal' OR user_id IS NOT NULL)
        )
    """)
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_drive_spaces_personal "
        "ON drive_spaces (user_id) WHERE kind = 'personal'"
    )
    # The history sweep's batch: the spaces holding versions not kept as
    # text, looked at longest ago first, read a batch at a time.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_spaces_history "
        "ON drive_spaces (history_checked_at NULLS FIRST, space_id) WHERE binary_versions > 0"
    )
    # An account's delete finds its spaces through this, whatever their kind.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_spaces_user ON drive_spaces (user_id)"
    )
    op.execute("""
        CREATE TABLE IF NOT EXISTS drive_entries (
            entry_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            space_id UUID NOT NULL
                REFERENCES drive_spaces(space_id) ON DELETE CASCADE,
            path TEXT NOT NULL,
            path_key TEXT COLLATE "C" NOT NULL,
            parent TEXT NOT NULL,
            name TEXT NOT NULL,
            kind VARCHAR(8) NOT NULL CHECK (kind IN ('file', 'folder')),
            head_seq INT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            changed_seq BIGINT NOT NULL,
            deleted_at TIMESTAMPTZ,
            ended VARCHAR(16),
            trash_id UUID,
            deleted_by VARCHAR(255),
            trash_sitting TEXT,
            CONSTRAINT drive_entries_path_shape
                CHECK (path <> '' AND left(path, 1) <> '/' AND right(path, 1) <> '/'),
            CONSTRAINT drive_entries_folder_has_no_head
                CHECK (kind = 'file' OR head_seq IS NULL),
            CONSTRAINT drive_entries_ended CHECK (CASE ended
                WHEN 'trash' THEN deleted_at IS NOT NULL AND trash_id IS NOT NULL
                WHEN 'tombstone' THEN deleted_at IS NOT NULL AND trash_id IS NULL
                ELSE ended IS NULL AND deleted_at IS NULL AND trash_id IS NULL END)
        )
    """)
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_drive_entries_live_path "
        "ON drive_entries (space_id, path_key) WHERE deleted_at IS NULL"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_entries_parent "
        "ON drive_entries (space_id, parent) WHERE deleted_at IS NULL"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_entries_changed "
        "ON drive_entries (space_id, changed_seq)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_entries_trashed "
        "ON drive_entries (deleted_at) WHERE deleted_at IS NOT NULL"
    )
    # The trash's bound looks for the oldest deleted entry still holding
    # bytes nothing live does.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_entries_gone "
        "ON drive_entries (space_id) WHERE deleted_at IS NOT NULL"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_entries_trash_id "
        "ON drive_entries (space_id, trash_id) WHERE trash_id IS NOT NULL"
    )
    # A delete looks up the same sitting's last one to join its trash item:
    # one descent, not a walk of everything deleted in the last minute.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_entries_burst "
        "ON drive_entries (space_id, trash_sitting, deleted_at) "
        "WHERE ended = 'trash' AND trash_sitting IS NOT NULL"
    )
    # The mount's text pages walk a space's live files by entry id.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_entries_files "
        "ON drive_entries (space_id, entry_id) WHERE deleted_at IS NULL AND kind = 'file'"
    )
    op.execute("""
        CREATE TABLE IF NOT EXISTS drive_contents (
            space_id UUID NOT NULL
                REFERENCES drive_spaces(space_id) ON DELETE CASCADE,
            sha256 TEXT NOT NULL,
            byte_len BIGINT NOT NULL,
            is_text BOOLEAN NOT NULL,
            content BYTEA,
            blob_user_id VARCHAR(255),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            released_at TIMESTAMPTZ,
            live_refs INT NOT NULL DEFAULT 0,
            gone_refs INT NOT NULL DEFAULT 0,
            PRIMARY KEY (space_id, sha256),
            CONSTRAINT drive_contents_one_home
                CHECK ((content IS NULL) <> (blob_user_id IS NULL))
        )
    """)
    # A replaced copy no version keeps waits here (``released_at``) until the
    # sweep frees it, readable by digest meanwhile; every byte count leaves
    # these rows out, so they are found by this index alone.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_contents_released "
        "ON drive_contents (space_id, released_at) WHERE released_at IS NOT NULL"
    )
    # The blob sweep asks whether any row keeps a user's digest.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_contents_blob "
        "ON drive_contents (blob_user_id, sha256) WHERE blob_user_id IS NOT NULL"
    )
    # byte_len and is_text copy drive_contents on purpose (the writer and the
    # sitting fold set them together), so listings and the tree read a head
    # without a second join. Measured at 96,000 files: the tree's floor 149 ms
    # with the copy against 172 ms joining drive_contents; a text page the
    # same either way.
    op.execute("""
        CREATE TABLE IF NOT EXISTS drive_versions (
            entry_id UUID NOT NULL
                REFERENCES drive_entries(entry_id) ON DELETE CASCADE,
            space_id UUID NOT NULL,
            seq INT NOT NULL,
            sha256 TEXT NOT NULL,
            byte_len BIGINT NOT NULL,
            content_type TEXT NOT NULL,
            is_text BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            actor_kind VARCHAR(8) NOT NULL
                CHECK (actor_kind IN ('person', 'agent', 'system')),
            actor_user_id VARCHAR(255) NOT NULL,
            agent TEXT,
            via VARCHAR(8) NOT NULL
                CHECK (via IN ('app', 'cli', 'tool', 'mount', 'api', 'import')),
            thread_id TEXT,
            run_id TEXT,
            workspace_id TEXT,
            computer_id TEXT,
            sitting TEXT,
            note TEXT CHECK (note IS NULL OR length(note) <= 500),
            restored_from INT,
            PRIMARY KEY (entry_id, seq)
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_versions_content "
        "ON drive_versions (space_id, sha256)"
    )
    # The history sweep looks only at versions not held as text, which are
    # the ones that age out, a space at a time; text, most of a Drive, is
    # never read for it.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_versions_binary "
        "ON drive_versions (space_id, entry_id) WHERE NOT is_text"
    )
    for statement in COUNTERS:
        op.execute(statement)
    for statement in KEEP_DRIVE_BLOBS:
        op.execute(statement)
    # A file sent in parts: the row is the upload's only state, since each
    # step may reach any worker. Its parts sit under ``object_key`` until the
    # server has read the assembled object back and filed it by its digest.
    op.execute("""
        CREATE TABLE IF NOT EXISTS drive_uploads (
            upload_id UUID PRIMARY KEY,
            space_id UUID NOT NULL
                REFERENCES drive_spaces(space_id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            path TEXT NOT NULL,
            path_key TEXT COLLATE "C" NOT NULL,
            size BIGINT NOT NULL CHECK (size >= 0),
            part_size BIGINT NOT NULL,
            part_count INT NOT NULL,
            sha256 TEXT,
            content_type TEXT,
            note TEXT CHECK (note IS NULL OR length(note) <= 500),
            if_match TEXT,
            if_none_match BOOLEAN NOT NULL DEFAULT false,
            actor JSONB NOT NULL,
            mode VARCHAR(8) NOT NULL CHECK (mode IN ('direct', 'relay')),
            object_key TEXT NOT NULL,
            store_upload_id TEXT NOT NULL,
            state VARCHAR(10) NOT NULL DEFAULT 'uploading'
                CHECK (state IN ('uploading', 'verifying', 'done', 'failed', 'aborted')),
            error JSONB,
            entry_id UUID,
            lease_until TIMESTAMPTZ,
            attempts INT NOT NULL DEFAULT 0,
            report JSONB,
            assembled BOOLEAN NOT NULL DEFAULT false,
            cleaned BOOLEAN NOT NULL DEFAULT false,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            expires_at TIMESTAMPTZ NOT NULL,
            lease_token UUID
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_uploads_resume "
        "ON drive_uploads (space_id, path_key, size) WHERE state = 'uploading'"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_uploads_open "
        "ON drive_uploads (user_id) WHERE state IN ('uploading', 'verifying')"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_uploads_sweep "
        "ON drive_uploads (updated_at) WHERE NOT cleaned"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TRIGGER IF EXISTS workspace_file_blobs_keep_drive ON workspace_file_blobs")
    op.execute("DROP TRIGGER IF EXISTS workspace_file_blobs_keep_drive_rows ON workspace_file_blobs")
    op.execute("DROP FUNCTION IF EXISTS workspace_file_blobs_keep_drive()")
    op.execute("DROP TABLE IF EXISTS drive_uploads")
    op.execute("DROP TABLE IF EXISTS drive_versions")
    op.execute("DROP TABLE IF EXISTS drive_contents")
    op.execute("DROP TABLE IF EXISTS drive_entries")
    op.execute("DROP TABLE IF EXISTS drive_spaces")
    for name in ("drive_versions_counted", "drive_entries_counted", "drive_contents_counted"):
        op.execute(f"DROP FUNCTION IF EXISTS {name}()")


# What a space keeps, counted as it changes so that no save or usage read
# scans the space: every content row knows how many versions of live
# entries (``live_refs``) and of deleted ones (``gone_refs``) name it, and
# the space row sums its content rows by class. A row is ``released`` once
# it has a release time, ``gone`` while only deleted entries (the trash, or
# a replaced file's notice) hold it, and live otherwise; ``live_bytes`` is
# ``held_bytes - released_bytes - gone_bytes``. ``binary_versions`` counts
# the versions not kept as text, the only ones the history rule ages out, so
# the sweep finds the spaces it has work in without reading the others. Statement triggers with
# transition tables, so a change costs one pass over the rows it touched.
# Every writer holds the space's row lock already, so the counters add no
# lock of their own.
COUNTERS = [
    """
    CREATE OR REPLACE FUNCTION drive_versions_counted() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        -- A version whose entry is not there was removed with its entry,
        -- which only a purge of a deleted one does.
        IF TG_OP IN ('UPDATE', 'DELETE') THEN
            UPDATE drive_contents c
               SET live_refs = c.live_refs - d.live, gone_refs = c.gone_refs - d.gone
              FROM (SELECT o.space_id, o.sha256,
                           count(*) FILTER (WHERE e.deleted_at IS NULL AND e.entry_id IS NOT NULL) AS live,
                           count(*) FILTER (WHERE e.deleted_at IS NOT NULL OR e.entry_id IS NULL) AS gone
                      FROM old_rows o LEFT JOIN drive_entries e ON e.entry_id = o.entry_id
                     GROUP BY o.space_id, o.sha256) d
             WHERE c.space_id = d.space_id AND c.sha256 = d.sha256;
            UPDATE drive_spaces s SET binary_versions = s.binary_versions - d.n
              FROM (SELECT space_id, count(*) AS n FROM old_rows WHERE NOT is_text
                     GROUP BY space_id) d
             WHERE s.space_id = d.space_id;
        END IF;
        IF TG_OP IN ('UPDATE', 'INSERT') THEN
            UPDATE drive_contents c
               SET live_refs = c.live_refs + d.live, gone_refs = c.gone_refs + d.gone
              FROM (SELECT n.space_id, n.sha256,
                           count(*) FILTER (WHERE e.deleted_at IS NULL) AS live,
                           count(*) FILTER (WHERE e.deleted_at IS NOT NULL) AS gone
                      FROM new_rows n JOIN drive_entries e ON e.entry_id = n.entry_id
                     GROUP BY n.space_id, n.sha256) d
             WHERE c.space_id = d.space_id AND c.sha256 = d.sha256;
            UPDATE drive_spaces s SET binary_versions = s.binary_versions + d.n
              FROM (SELECT space_id, count(*) AS n FROM new_rows WHERE NOT is_text
                     GROUP BY space_id) d
             WHERE s.space_id = d.space_id;
        END IF;
        RETURN NULL;
    END $$
    """,
    "CREATE TRIGGER drive_versions_added AFTER INSERT ON drive_versions"
    " REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION drive_versions_counted()",
    "CREATE TRIGGER drive_versions_changed AFTER UPDATE ON drive_versions"
    " REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows"
    " FOR EACH STATEMENT EXECUTE FUNCTION drive_versions_counted()",
    "CREATE TRIGGER drive_versions_removed AFTER DELETE ON drive_versions"
    " REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION drive_versions_counted()",
    """
    CREATE OR REPLACE FUNCTION drive_entries_counted() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        IF TG_OP = 'UPDATE' THEN
            -- A delete or a restore moves its versions between the counts.
            UPDATE drive_contents c
               SET live_refs = c.live_refs + d.moved, gone_refs = c.gone_refs - d.moved
              FROM (SELECT v.space_id, v.sha256,
                           sum(CASE WHEN n.deleted_at IS NULL THEN 1 ELSE -1 END) AS moved
                      FROM old_rows o JOIN new_rows n ON n.entry_id = o.entry_id
                      JOIN drive_versions v ON v.entry_id = n.entry_id
                     WHERE (o.deleted_at IS NULL) <> (n.deleted_at IS NULL)
                     GROUP BY v.space_id, v.sha256) d
             WHERE c.space_id = d.space_id AND c.sha256 = d.sha256;
        ELSIF TG_OP = 'INSERT' THEN
            UPDATE drive_spaces s SET entry_count = s.entry_count + d.n
              FROM (SELECT space_id, count(*) AS n FROM new_rows GROUP BY space_id) d
             WHERE s.space_id = d.space_id;
        ELSE
            UPDATE drive_spaces s SET entry_count = s.entry_count - d.n
              FROM (SELECT space_id, count(*) AS n FROM old_rows GROUP BY space_id) d
             WHERE s.space_id = d.space_id;
        END IF;
        RETURN NULL;
    END $$
    """,
    "CREATE TRIGGER drive_entries_added AFTER INSERT ON drive_entries"
    " REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION drive_entries_counted()",
    "CREATE TRIGGER drive_entries_changed AFTER UPDATE ON drive_entries"
    " REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows"
    " FOR EACH STATEMENT EXECUTE FUNCTION drive_entries_counted()",
    "CREATE TRIGGER drive_entries_removed AFTER DELETE ON drive_entries"
    " REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION drive_entries_counted()",
    """
    CREATE OR REPLACE FUNCTION drive_contents_counted() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        -- Each event names only the transition tables it has.
        IF TG_OP IN ('UPDATE', 'DELETE') THEN
            UPDATE drive_spaces s
               SET held_bytes = s.held_bytes - d.held,
                   released_bytes = s.released_bytes - d.released,
                   gone_bytes = s.gone_bytes - d.gone
              FROM (SELECT space_id, sum(byte_len) AS held,
                           COALESCE(sum(byte_len) FILTER (WHERE released_at IS NOT NULL), 0) AS released,
                           COALESCE(sum(byte_len) FILTER (WHERE released_at IS NULL
                                    AND live_refs = 0 AND gone_refs > 0), 0) AS gone
                      FROM old_rows GROUP BY space_id) d
             WHERE s.space_id = d.space_id;
        END IF;
        IF TG_OP IN ('UPDATE', 'INSERT') THEN
            UPDATE drive_spaces s
               SET held_bytes = s.held_bytes + d.held,
                   released_bytes = s.released_bytes + d.released,
                   gone_bytes = s.gone_bytes + d.gone
              FROM (SELECT space_id, sum(byte_len) AS held,
                           COALESCE(sum(byte_len) FILTER (WHERE released_at IS NOT NULL), 0) AS released,
                           COALESCE(sum(byte_len) FILTER (WHERE released_at IS NULL
                                    AND live_refs = 0 AND gone_refs > 0), 0) AS gone
                      FROM new_rows GROUP BY space_id) d
             WHERE s.space_id = d.space_id;
        END IF;
        RETURN NULL;
    END $$
    """,
    "CREATE TRIGGER drive_contents_added AFTER INSERT ON drive_contents"
    " REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION drive_contents_counted()",
    "CREATE TRIGGER drive_contents_changed AFTER UPDATE ON drive_contents"
    " REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows"
    " FOR EACH STATEMENT EXECUTE FUNCTION drive_contents_counted()",
    "CREATE TRIGGER drive_contents_removed AFTER DELETE ON drive_contents"
    " REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION drive_contents_counted()",
]


# The blob sweep of a release that predates the Drive does not know
# ``drive_contents`` references a blob, so it would condemn and then delete
# the bytes of a Drive file nobody saved for a week. The database refuses
# instead, whatever code runs: condemning or deleting a registry row a
# Drive content row names leaves the row as it was. Skipped, not raised: the
# condemn is one statement over every candidate, and an error would abort it
# for the others, so nothing would ever be reclaimed. One index lookup per
# row (``ix_drive_contents_blob``), and only for rows being condemned or
# deleted; rows the Drive does not name go as before. The sweep that knows
# the Drive never asks for these, so for it nothing changes.
KEEP_DRIVE_BLOBS = [
    """
    CREATE OR REPLACE FUNCTION workspace_file_blobs_keep_drive() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        IF EXISTS (SELECT 1 FROM drive_contents d
                    WHERE d.blob_user_id = OLD.user_id AND d.sha256 = OLD.sha256) THEN
            RETURN NULL;
        END IF;
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END $$
    """,
    # Only the condemning update: a writer's claim restamps a row already
    # condemned, and every other update leaves condemned_at alone.
    "CREATE TRIGGER workspace_file_blobs_keep_drive"
    " BEFORE UPDATE OF condemned_at ON workspace_file_blobs FOR EACH ROW"
    " WHEN (OLD.condemned_at IS NULL AND NEW.condemned_at IS NOT NULL)"
    " EXECUTE FUNCTION workspace_file_blobs_keep_drive()",
    "CREATE TRIGGER workspace_file_blobs_keep_drive_rows"
    " BEFORE DELETE ON workspace_file_blobs FOR EACH ROW"
    " EXECUTE FUNCTION workspace_file_blobs_keep_drive()",
]
