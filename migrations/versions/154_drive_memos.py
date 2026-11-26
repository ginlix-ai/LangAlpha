"""Migration 154: memos, the one kind of Drive file that keeps its versions.

A memo is a file the person or their agent marked as one (``drive_memos``),
with an optional status and tickers; marking, un-marking and every change to
the mark is an event in its record (``drive_memo_events``) beside its
versions. Every other file keeps only the copy from before its last save,
for a while (``drive_entries.undo_until``), as a one-step undo.

The sweep finds due undo copies through ``ix_drive_entries_undo`` alone. The
bookkeeping the history sweep used before (``drive_spaces.binary_versions``,
``history_checked_at``, their indexes) is left as it is, still counted and
no longer read, so the code from before this migration runs against this
schema; a later migration drops it.

``drive_extracts`` (text read out of a Word or PDF file, by digest),
``drive_line_counts`` (lines added and removed, by the two digests) and
``drive_attributions`` (who wrote each line, by version, valid under the
file's ``drive_version_epochs`` row) are caches the record and its
comparisons fill on demand; none counts against the allowance.

An agent changes a memo inside a check-out (``drive_memo_changes``): one
chat's change of one memo, whose reason each version made under it carries
as its note and points at (``drive_versions.change_id``).

A memo's record is kept by the database as well as by the code: a trigger
(``drive_version_kept``) lets a version in it go only with its file or by a
clear, which says so for its transaction. The code from before this
migration trims versions by age and count and knows no memos, and it still
runs while a deploy drains and after a code-only rollback; its deletes of a
memo's record delete nothing. That code also folds a save into the head in
place, and the same trigger refuses that where the head is a version the
record holds as written (a pinned final, the version a mark was put on or
taken off at, one made under a check-out that has ended): the save fails,
and the next one from a new sitting is a new version.

Nothing is deleted here. A file holding earlier versions, in the trash or
not, gets an undo window of 7 days, after which the sweep drops them; a save
trims at once; a file marked within the window keeps them all.

Online-safe: additive only. New tables, two nullable columns, new indexes,
two new triggers, one UPDATE over files with history. It first takes
``drive_spaces`` in EXCLUSIVE mode, which every Drive write locks a row of
before anything else, so no write is midway when the later statements lock
``drive_entries`` and ``drive_versions``. Reads of those two wait from
their new column to the schema's commit, which is catalog changes and the
builds of two indexes that start empty, one pass over each table. The
UPDATE runs after that commit in batches of 1000 files, each under its own
row locks.
Rollback: roll the code back first, then ``alembic downgrade 153``: the memo
marks, their events and the caches go. Versions the new rule dropped
meanwhile stay dropped. A batch of the UPDATE that fails leaves the schema
committed and the revision at 153, where a downgrade has nothing to run: run
the upgrade again first, which makes nothing twice and stamps only the files
still waiting.
"""

from alembic import op
from sqlalchemy import text

revision = "154"
down_revision = "153"
branch_labels = None
depends_on = None

_ACTOR_COLUMNS = """
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
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("LOCK TABLE drive_spaces IN EXCLUSIVE MODE")
    op.execute("""
        CREATE TABLE IF NOT EXISTS drive_memos (
            entry_id UUID PRIMARY KEY
                REFERENCES drive_entries(entry_id) ON DELETE CASCADE,
            space_id UUID NOT NULL,
            is_memo BOOLEAN NOT NULL,
            status VARCHAR(8) CHECK (status IN ('draft', 'final')),
            tickers TEXT[] NOT NULL DEFAULT '{}',
            final_seq INT,
            final_at TIMESTAMPTZ,
            kept_through_seq INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            aside_key TEXT,
            aside_command TEXT,
            aside_at TIMESTAMPTZ
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_memos_space ON drive_memos (space_id) WHERE is_memo"
    )
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS drive_memo_events (
            event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            entry_id UUID NOT NULL
                REFERENCES drive_entries(entry_id) ON DELETE CASCADE,
            space_id UUID NOT NULL,
            kind VARCHAR(8) NOT NULL CHECK (kind IN ('mark', 'unmark', 'update', 'clear')),
            head_seq INT,
            status_before VARCHAR(8),
            status_after VARCHAR(8),
            tickers_before TEXT[],
            tickers_after TEXT[],
            final_seq INT,
            cleared INT,
            {_ACTOR_COLUMNS}
            note TEXT CHECK (note IS NULL OR length(note) <= 500),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_memo_events_entry "
        "ON drive_memo_events (entry_id, created_at DESC, event_id DESC)"
    )
    op.execute("""
        CREATE TABLE IF NOT EXISTS drive_extracts (
            space_id UUID NOT NULL,
            sha256 TEXT NOT NULL,
            kind VARCHAR(8) NOT NULL CHECK (kind IN ('pdf', 'docx')),
            status VARCHAR(10) NOT NULL
                CHECK (status IN ('ok', 'empty', 'failed', 'too_large')),
            text TEXT,
            truncated BOOLEAN NOT NULL DEFAULT false,
            extracted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (space_id, sha256),
            FOREIGN KEY (space_id, sha256)
                REFERENCES drive_contents (space_id, sha256) ON DELETE CASCADE
        )
    """)
    # Lines a version adds and removes against the one before, by the two
    # contents (``base_sha256`` '' for a first version): bytes never change,
    # so a row never goes stale.
    op.execute("""
        CREATE TABLE IF NOT EXISTS drive_line_counts (
            space_id UUID NOT NULL,
            base_sha256 TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            added INT NOT NULL,
            removed INT NOT NULL,
            -- ``sha256`` before its base, so the cascade from a freed
            -- content row finds its counts by this index.
            PRIMARY KEY (space_id, sha256, base_sha256),
            FOREIGN KEY (space_id, sha256)
                REFERENCES drive_contents (space_id, sha256) ON DELETE CASCADE
        )
    """)
    # Which version wrote each line, cached per version under the file's
    # version epoch: a row holds only while its epoch is the file's. It
    # hangs off the epoch row, not the version: a cache write locks the
    # epoch row alone (``database/drive/texts``), and a foreign key to the
    # version would lock that row too, the order that deadlocks with a
    # delete of several versions.
    op.execute("""
        CREATE TABLE IF NOT EXISTS drive_version_epochs (
            entry_id UUID PRIMARY KEY
                REFERENCES drive_entries (entry_id) ON DELETE CASCADE,
            epoch BIGINT NOT NULL DEFAULT 0
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS drive_attributions (
            entry_id UUID NOT NULL,
            seq INT NOT NULL,
            epoch BIGINT NOT NULL,
            blame INT[] NOT NULL,
            PRIMARY KEY (entry_id, seq),
            FOREIGN KEY (entry_id)
                REFERENCES drive_version_epochs (entry_id) ON DELETE CASCADE
        )
    """)
    # The open rule reads a chat's newest change to a memo; with no chat, the
    # computer's.
    op.execute("""
        CREATE TABLE IF NOT EXISTS drive_memo_changes (
            change_id UUID PRIMARY KEY,
            entry_id UUID NOT NULL
                REFERENCES drive_entries(entry_id) ON DELETE CASCADE,
            space_id UUID NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            thread_id TEXT,
            computer_id TEXT,
            reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 500),
            opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            touched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            ended_at TIMESTAMPTZ
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_memo_changes_chat "
        "ON drive_memo_changes (entry_id, thread_id, opened_at DESC)"
    )
    # No foreign key: a change goes only with its entry, and its versions with
    # it, so nothing would set one null, and a cascade's update would run the
    # version triggers for nothing.
    op.execute("ALTER TABLE drive_versions ADD COLUMN IF NOT EXISTS change_id UUID")
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_versions_change "
        "ON drive_versions (change_id) WHERE change_id IS NOT NULL"
    )
    # A memo a command on a computer renamed away from its name, until a file
    # written there by the same command takes it back (``drive/aside.py``):
    # the ``aside_*`` columns.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_memos_aside "
        "ON drive_memos (space_id, aside_key) WHERE aside_key IS NOT NULL"
    )
    op.execute(VERSION_EPOCH_154)
    op.execute("DROP TRIGGER IF EXISTS drive_versions_epoch ON drive_versions")
    op.execute("""
        CREATE TRIGGER drive_versions_epoch
        BEFORE UPDATE OR DELETE ON drive_versions
        FOR EACH ROW EXECUTE FUNCTION drive_versions_epoch()
    """)
    op.execute(VERSION_KEPT_154)
    # Named to fire before ``drive_versions_epoch`` (triggers fire in name
    # order), so a delete it stops moves no epoch.
    op.execute("DROP TRIGGER IF EXISTS drive_version_kept ON drive_versions")
    op.execute("""
        CREATE TRIGGER drive_version_kept
        BEFORE UPDATE OR DELETE ON drive_versions
        FOR EACH ROW EXECUTE FUNCTION drive_version_kept()
    """)
    op.execute("ALTER TABLE drive_entries ADD COLUMN IF NOT EXISTS undo_until TIMESTAMPTZ")
    # The sweep's batch: live files whose undo copy is due. A file in the
    # trash keeps its window and comes due if it is put back.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_drive_entries_undo "
        "ON drive_entries (undo_until) WHERE undo_until IS NOT NULL AND deleted_at IS NULL"
    )
    # The rule's default days, since a migration runs without the
    # application's config: a deployment that sets ``drive.history.undo_days``
    # to more restamps these rows after it.
    # Outside the schema's transaction: its time grows with the files that
    # hold history, and the locks above are gone by then. In batches, each
    # its own statement, so a write to a file being stamped waits for one
    # batch. Run again after a failure, it leaves the windows already set.
    with op.get_context().autocommit_block():
        bind = op.get_bind()
        # The schema's lock timeout ended with its transaction: a batch that
        # meets a row held open gives up as that did, and the run is retried.
        bind.execute(text("SET lock_timeout = '5s'"))
        try:
            # Until a batch finds no file to stamp: one whose files were all
            # deleted under it stamped none while others may still wait.
            while bind.execute(text(_WINDOWS_154)).scalar():
                pass
        finally:
            bind.execute(text("RESET lock_timeout"))


_WINDOWS_154 = """
    WITH due AS (
        SELECT e.entry_id FROM drive_entries e
         WHERE e.kind = 'file' AND e.undo_until IS NULL
           AND EXISTS (SELECT 1 FROM drive_versions v
                        WHERE v.entry_id = e.entry_id AND v.seq <> e.head_seq)
         LIMIT 1000),
    stamped AS (
        UPDATE drive_entries SET undo_until = now() + interval '7 days'
         WHERE entry_id IN (SELECT entry_id FROM due)
           AND undo_until IS NULL)
    SELECT count(*) FROM due
"""


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("LOCK TABLE drive_spaces IN EXCLUSIVE MODE")
    op.execute("DROP INDEX IF EXISTS ix_drive_versions_change")
    op.execute("ALTER TABLE drive_versions DROP COLUMN IF EXISTS change_id")
    op.execute("DROP TABLE IF EXISTS drive_memo_changes")
    op.execute("DROP INDEX IF EXISTS ix_drive_entries_undo")
    op.execute("ALTER TABLE drive_entries DROP COLUMN IF EXISTS undo_until")
    op.execute("DROP TRIGGER IF EXISTS drive_version_kept ON drive_versions")
    op.execute("DROP FUNCTION IF EXISTS drive_version_kept()")
    op.execute("DROP TRIGGER IF EXISTS drive_versions_epoch ON drive_versions")
    op.execute("DROP FUNCTION IF EXISTS drive_versions_epoch()")
    op.execute("DROP TABLE IF EXISTS drive_attributions")
    op.execute("DROP TABLE IF EXISTS drive_version_epochs")
    op.execute("DROP TABLE IF EXISTS drive_line_counts")
    op.execute("DROP TABLE IF EXISTS drive_extracts")
    op.execute("DROP TABLE IF EXISTS drive_memo_events")
    op.execute("DROP TABLE IF EXISTS drive_memos")


# Moves a file's version epoch when a version's content changes (a fold)
# or a version goes, in the same transaction, and deletes the file's
# cached attributions, every one of which stopped holding then: the epoch
# row first, then the attribution rows. A cache write locks only the epoch
# row (FOR SHARE, under the epoch it read), so it waits for a change that
# moved the epoch and then finds it moved, and a change that comes after
# waits for it. A delete that comes from purging the entry itself moves
# nothing: the epoch row and its attributions go with it.
VERSION_EPOCH_154 = """
    CREATE OR REPLACE FUNCTION drive_versions_epoch() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        IF TG_OP = 'UPDATE' AND NEW.sha256 IS NOT DISTINCT FROM OLD.sha256
           AND NEW.is_text IS NOT DISTINCT FROM OLD.is_text
           AND NEW.content_type IS NOT DISTINCT FROM OLD.content_type THEN
            RETURN NEW;
        END IF;
        IF EXISTS (SELECT 1 FROM drive_entries WHERE entry_id = OLD.entry_id) THEN
            INSERT INTO drive_version_epochs (entry_id, epoch) VALUES (OLD.entry_id, 1)
            ON CONFLICT (entry_id) DO UPDATE SET epoch = drive_version_epochs.epoch + 1;
            DELETE FROM drive_attributions WHERE entry_id = OLD.entry_id;
        END IF;
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END;
    $$
"""


# Keeps a memo's record: every version of a live memo, and of a former one
# its pinned final and the versions through ``kept_through_seq``. A delete of
# one is skipped unless the transaction says it is a clear
# (``drive.clearing``, set by ``retention._drop``) or the file itself is
# going, which a purge's cascade is: the entry row is gone by then. New
# bytes for a version the record holds as written (``policy.folds`` never
# folds into one) are refused, not skipped: a skipped save would read as
# made. A version made under a check-out that has ended is one of those: a
# fold needs the save's own change, which is open (a ``--continue`` reopens
# it first), so only code that knows no changes would give such a version
# another change's bytes under this one's reason.
VERSION_KEPT_154 = """
    CREATE OR REPLACE FUNCTION drive_version_kept() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        IF TG_OP = 'UPDATE' THEN
            IF NEW.sha256 IS DISTINCT FROM OLD.sha256
               AND (EXISTS (SELECT 1 FROM drive_memos m
                             WHERE m.entry_id = OLD.entry_id
                               AND (OLD.seq <= m.kept_through_seq
                                    OR OLD.seq = m.final_seq))
                    OR EXISTS (SELECT 1 FROM drive_memo_changes c
                                WHERE c.change_id = OLD.change_id
                                  AND c.ended_at IS NOT NULL)) THEN
                RAISE EXCEPTION 'version % of this memo is kept as written', OLD.seq
                    USING ERRCODE = 'check_violation';
            END IF;
            RETURN NEW;
        END IF;
        IF current_setting('drive.clearing', true) = 'on' THEN
            RETURN OLD;
        END IF;
        IF EXISTS (SELECT 1 FROM drive_memos m
                     JOIN drive_entries e ON e.entry_id = m.entry_id
                    WHERE m.entry_id = OLD.entry_id
                      AND (m.is_memo OR OLD.seq <= m.kept_through_seq
                           OR OLD.seq = m.final_seq)) THEN
            RETURN NULL;
        END IF;
        RETURN OLD;
    END;
    $$
"""
