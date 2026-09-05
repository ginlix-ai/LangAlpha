"""Move workspace file bytes out of Postgres into content-addressed object storage.

``workspace_files`` stored each file's bytes inline (``content_text`` /
``content_binary``, up to 1GB per workspace), which bloats the database,
amplifies every backup and replica stream, and pays TOAST overhead on rows that
are otherwise pure metadata. A blob-backed row instead keeps its bytes under
``blobs/{user_id}/{sha256}`` in object storage and leaves both content columns
NULL; ``workspace_file_blobs`` is the registry that records which digests each
user has written. Content addressing is what lets a forked workspace share
bytes instead of copying TOAST.

The registry is keyed by ``(user_id, sha256)`` and the manifest points at it by
digest alone, so there is no foreign key from ``workspace_files`` to the
registry: a manifest row has no ``user_id`` of its own, and a single-column
constraint cannot reference a composite key. Reference integrity is the sweep's
job instead. A registry row is deleted only under its row lock after a
re-check that no manifest row of the same user names its digest, and a writer
registers before it publishes a pointer, so no row ever points at an object the
sweep can take.

Four things ride along, because they are the same manifest:

* ``kind`` and ``symlink_target``. Backup walked the sandbox with ``find -type
  f``, so an empty directory, a symlink, or a file's mode never reached the
  manifest and never came back on restore. The ``permissions`` column from 001
  now holds the octal mode string it was declared for.
* ``workspaces.files_restore_incomplete_at``. ``sync_to_db`` prunes manifest
  rows whose files are absent from the sandbox, reading that absence as a user
  deletion; a restore that failed produces the identical signature. The sandbox's
  own ``.file_sync_marker`` cannot carry the distinction, because every way of
  failing to read it degrades to "no restore ever failed", which is the answer
  that permits pruning. Postgres is where this project keeps cross-worker truth,
  and putting the flag beside the manifest means the flag and the rows it
  protects fail together instead of independently. NULL means no restore is
  known to have failed, the correct reading for every pre-existing row.
* ``last_referenced_at`` and ``condemned_at`` on the registry. Every save of a
  changed file writes a new object and orphans its predecessor, and
  the sweep cannot be a plain delete-unreferenced: the upload happens in the
  sandbox and the manifest row that references it lands seconds later in its own
  transaction. Writers touch ``last_referenced_at`` before they upload; the
  sweep condemns a row only after a grace period with no touch and no reference,
  then deletes it a further grace period later under a row lock. A condemned row
  is invisible to writers, which re-upload and revive.
* ``pack_sha256`` and ``pack_offset``. Backup and restore are bound by the
  sandbox's CPU, and that cost is per object, not per byte. A pack is the
  concatenation, in sorted path order, of every regular file at or below a size
  cutoff, split at file boundaries into chunks and stored as ordinary
  content-addressed blobs. A member row points at its chunk and its byte offset
  in it; ``file_size`` is its length and ``content_hash`` stays the file's own
  digest, so change detection and byte verification are untouched.
  ``blob_sha256`` and ``pack_sha256`` are mutually exclusive on a row.

Lock discipline. ``workspace_files`` is hot, so every catalog change it needs is
one ALTER TABLE: five nullable-or-constant-default columns (metadata-only on
Postgres 11+, no rewrite) and both CHECKs, NOT VALID. ALTER TABLE runs its
subcommands in phases, adding columns before constraints, so a constraint in
the list may name a column in the same list. One ACCESS EXCLUSIVE acquisition
rather than seven, and it either lands whole or hits the lock timeout and lands
not at all.

Everything that does not need that lock runs afterwards in an autocommit block,
under no timeout: VALIDATE takes SHARE UPDATE EXCLUSIVE and blocks neither
readers nor writers, and CREATE INDEX CONCURRENTLY spends most of its life
waiting on transactions that can still see the table. Those waits are lock
waits, so a 5s cap fails the build on any table taking continuous writes and
leaves an INVALID index behind. Every existing row is NULL on both pointers and
carries the default ``kind``, so no validation here can fail.

Revision ID: 039
Revises: 038
"""

from alembic import op
from sqlalchemy import text


revision = "039"
down_revision = "038"
branch_labels = None
depends_on = None

_BLOB_INDEX = "idx_workspace_files_blob_sha256"
_PACK_INDEX = "idx_workspace_files_pack_sha256"
_KIND_CHECK = "chk_workspace_files_kind"
_POINTER_CHECK = "chk_workspace_files_one_pointer"
_CONDEMNED_INDEX = "idx_workspace_file_blobs_condemned"
_LIVE_INDEX = "idx_workspace_file_blobs_live_referenced"
_MOVED_ROWS_SQL = """
    SELECT count(*) FROM workspace_files
    WHERE blob_sha256 IS NOT NULL OR pack_sha256 IS NOT NULL
"""
_FLAGGED_WORKSPACES_SQL = """
    SELECT count(*) FROM workspaces WHERE files_restore_incomplete_at IS NOT NULL
"""


def upgrade() -> None:
    op.execute("SET lock_timeout = '5s'")

    # The PK doubles as an object key (blobs/{user_id}/{sha256}), so the CHECK is
    # the last line of defense against a non-digest reaching the bucket. All
    # CHECKs are inline and therefore valid from birth, which an empty table
    # gets for free. ``user_id`` matches ``workspaces.user_id``.
    op.execute("""
        CREATE TABLE IF NOT EXISTS workspace_file_blobs (
            user_id VARCHAR(255) NOT NULL,
            sha256 VARCHAR(64) NOT NULL
                CHECK (sha256 ~ '^[0-9a-f]{64}$'),
            byte_len BIGINT NOT NULL CHECK (byte_len >= 0),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_referenced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            condemned_at TIMESTAMPTZ,
            PRIMARY KEY (user_id, sha256)
        )
    """)

    # Sweep pass 2 walks condemned rows oldest-first; pass 1 walks live rows by
    # last touch. Both partial, since each pass only ever wants one side. Built
    # inline: the registry is created empty right above, and it stays a fraction
    # of ``workspace_files`` with a handful of inserts per sync.
    op.execute(
        f"CREATE INDEX IF NOT EXISTS {_CONDEMNED_INDEX} "
        "ON workspace_file_blobs (condemned_at) WHERE condemned_at IS NOT NULL"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS {_LIVE_INDEX} "
        "ON workspace_file_blobs (last_referenced_at) WHERE condemned_at IS NULL"
    )

    op.execute(
        "ALTER TABLE workspaces "
        "ADD COLUMN IF NOT EXISTS files_restore_incomplete_at TIMESTAMPTZ"
    )

    # The one ACCESS EXCLUSIVE window on the hot table. The DROPs make the whole
    # statement re-runnable: ADD CONSTRAINT has no IF NOT EXISTS, and a
    # migration that aborted in the autocommit block below is re-run from here.
    op.execute(f"""
        ALTER TABLE workspace_files
            ADD COLUMN IF NOT EXISTS blob_sha256 VARCHAR(64),
            ADD COLUMN IF NOT EXISTS kind VARCHAR(8) NOT NULL DEFAULT 'file',
            ADD COLUMN IF NOT EXISTS symlink_target TEXT,
            ADD COLUMN IF NOT EXISTS pack_sha256 VARCHAR(64),
            ADD COLUMN IF NOT EXISTS pack_offset BIGINT,
            DROP CONSTRAINT IF EXISTS {_KIND_CHECK},
            DROP CONSTRAINT IF EXISTS {_POINTER_CHECK},
            ADD CONSTRAINT {_KIND_CHECK}
                CHECK (kind IN ('file', 'dir', 'symlink')) NOT VALID,
            ADD CONSTRAINT {_POINTER_CHECK}
                CHECK (blob_sha256 IS NULL OR pack_sha256 IS NULL) NOT VALID
    """)

    with op.get_context().autocommit_block():
        # No cap here, for the reason the docstring gives. Nothing below takes a
        # lock worth bounding, and a cap on CREATE INDEX CONCURRENTLY is a way
        # to leave an INVALID index behind rather than a way to protect anyone.
        op.execute("SET lock_timeout = 0")
        op.execute(f"ALTER TABLE workspace_files VALIDATE CONSTRAINT {_KIND_CHECK}")
        op.execute(f"ALTER TABLE workspace_files VALIDATE CONSTRAINT {_POINTER_CHECK}")

        # Drop before create, always. IF NOT EXISTS matches on NAME alone, so on
        # its own it adopts whatever index already wears the name, and a failed
        # CONCURRENTLY build leaves an INVALID one that Postgres keeps
        # maintaining on every write while no planner will use it. That skips
        # silently and never shows up in alembic_version. Dropping first makes
        # the definitions below the only ones that can survive this migration.
        #
        # Partial: only blob-backed and packed rows matter. These back the
        # garbage collector's reference check and the orphan report's anti-join.
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_BLOB_INDEX}")
        op.execute(f"""
            CREATE INDEX CONCURRENTLY {_BLOB_INDEX}
                ON workspace_files (blob_sha256)
                WHERE blob_sha256 IS NOT NULL
        """)
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_PACK_INDEX}")
        op.execute(f"""
            CREATE INDEX CONCURRENTLY {_PACK_INDEX}
                ON workspace_files (pack_sha256)
                WHERE pack_sha256 IS NOT NULL
        """)


def downgrade() -> None:
    """Drop the blob schema.

    DESTRUCTIVE AFTER A BACKFILL. ``blob_sha256`` and ``pack_sha256`` are a
    moved row's only reference to its bytes, and the registry holds the digests
    those bytes are keyed by: the objects survive in the bucket but nothing can
    say which file each one belongs to. Run
    ``scripts/ops/backfill_workspace_file_blobs.py --reverse --apply`` first to
    pull the bytes back inline, blob-backed and packed rows alike; the
    downgrade refuses to run while any row still points into storage.

    Dropping ``files_restore_incomplete_at`` re-arms the deletion it exists to
    prevent: any workspace currently flagged loses the only record that its
    sandbox is missing files, and the next sync prunes its manifest rows. The
    downgrade refuses while any workspace is flagged; starting the workspace
    retries the restore and clears the flag when it completes.

    Both checks run under an exclusive lock on the tables they read, held
    through the DDL in the same transaction: a worker that is still running
    can publish a blob-backed row the instant after an unlocked count, and
    the column drop that follows would strand that file's bytes. The lock
    waits at most ``lock_timeout`` for in-flight syncs, then fails; a
    downgrade should run with the application stopped in any case.
    """
    bind = op.get_bind()
    bind.execute(text("SET LOCAL lock_timeout = '5s'"))
    bind.execute(
        text("LOCK TABLE workspace_files, workspaces IN ACCESS EXCLUSIVE MODE")
    )
    moved = bind.execute(text(_MOVED_ROWS_SQL)).scalar()
    if moved:
        raise RuntimeError(
            f"{moved} workspace_files rows keep their bytes in object storage; "
            "run scripts/ops/backfill_workspace_file_blobs.py --reverse --apply "
            "before downgrading"
        )
    flagged = bind.execute(text(_FLAGGED_WORKSPACES_SQL)).scalar()
    if flagged:
        raise RuntimeError(
            f"{flagged} workspace(s) have an incomplete file restore recorded in "
            "files_restore_incomplete_at; start each one so the restore completes "
            "before downgrading"
        )

    # The partial indexes go with their columns below. Dropping them
    # CONCURRENTLY first would need an autocommit block, which commits the
    # transaction and releases the lock the checks were made under.

    # Rows that describe a directory or a symlink have no meaning to the
    # previous release's readers, which would surface them as empty files. This
    # has to run while ``kind`` still exists.
    op.execute("DELETE FROM workspace_files WHERE kind <> 'file'")

    op.execute(f"""
        ALTER TABLE workspace_files
            DROP CONSTRAINT IF EXISTS {_POINTER_CHECK},
            DROP CONSTRAINT IF EXISTS {_KIND_CHECK},
            DROP COLUMN IF EXISTS pack_offset,
            DROP COLUMN IF EXISTS pack_sha256,
            DROP COLUMN IF EXISTS symlink_target,
            DROP COLUMN IF EXISTS kind,
            DROP COLUMN IF EXISTS blob_sha256
    """)
    op.execute("DROP TABLE IF EXISTS workspace_file_blobs")
    op.execute(
        "ALTER TABLE workspaces DROP COLUMN IF EXISTS files_restore_incomplete_at"
    )
