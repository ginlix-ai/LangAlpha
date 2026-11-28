"""Migration 155: a Drive file holds comments, through a record of its own.

A Drive file's record is a ``share_links`` row of ``kind = 'drive'`` in its
owner's Home, whose ``path`` is the entry id, as a chart's is its chart id
(migration 151). ``drive_entry_id`` names the same entry with a foreign key,
so the record follows the file through a rename, a move, the trash and a
restore, and a purge of the entry deletes it and the comments on it. Like a
chart it has no workspace file: it is never shared, published, kept or
versioned, and names no creator thread, which ``share_links_drive_shape``
holds in the row.

Online-safe: the column is added with no default and each check NOT VALID
under a short lock, then validated in an autocommit block. The index on
``drive_entry_id``, which a purge's cascade looks a record up by, is built
concurrently before the foreign key exists: a purge by the build still
serving would otherwise run each cascade as a scan of ``share_links``, and
its open transaction would hold up the build. The key is then added NOT
VALID under a short lock and validated. No drive row exists before this
build writes one, so nothing can fail validation.

``_worked_candidates`` asks for an entry's newest version an agent saved
from a thread on every comment box it opens; a partial index on
``drive_versions`` keeps that one descent, where the primary key alone walks
every version a person saved. Built concurrently, as the other indexes are.

Rollback needs the downgrade, not only the code: the previous build reads
some queries by record without a kind, so once drive rows exist it would
show them as files. Roll the code back and run ``alembic downgrade 154``
together; the downgrade deletes every drive record and the comments on it.
"""

from alembic import op

revision = "155"
down_revision = "154"
branch_labels = None
depends_on = None

_KIND = "share_links_kind_check"
_ARTIFACT_KIND = "share_links_artifact_kind"
_DRIVE_SHAPE = "share_links_drive_shape"
_DRIVE_ENTRY = "share_links_drive_entry"
_DRIVE_FK = "share_links_drive_entry_id_fkey"
_INDEX = "idx_share_links_drive"
_ENTRY_INDEX = "idx_share_links_drive_entry"
_AGENT_SAVES = "ix_drive_versions_agent_thread"
# How long a CONCURRENTLY step may wait out the transactions open at its
# phases before the deploy fails rather than hangs.
_BUILD_TIMEOUT = "15min"
# How long the foreign key's short lock on ``share_links`` and
# ``drive_entries`` may wait before the deploy fails rather than queues every
# write behind it.
_LOCK_TIMEOUT = "5s"


def _rebuild(name: str, definition: str) -> None:
    # Drop before create: IF NOT EXISTS matches on the name alone, and would
    # adopt the INVALID index a failed CONCURRENTLY build leaves.
    op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
    op.execute(f"CREATE {definition.format(name=name)}")


def upgrade() -> None:
    op.execute(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'")
    op.execute("ALTER TABLE share_links ADD COLUMN IF NOT EXISTS drive_entry_id UUID")
    op.execute(f"""
        ALTER TABLE share_links
            DROP CONSTRAINT IF EXISTS {_KIND},
            ADD CONSTRAINT {_KIND}
                CHECK (kind IN ('file', 'app', 'chart', 'drive')) NOT VALID,
            DROP CONSTRAINT IF EXISTS {_ARTIFACT_KIND},
            ADD CONSTRAINT {_ARTIFACT_KIND} CHECK (
                kind IN ('file', 'chart', 'drive') OR (
                    created_thread_id IS NULL AND bound_thread_id IS NULL
                    AND NOT keep_updated AND latest_seq = 0
                    AND last_comment_number = 0
                )
            ) NOT VALID,
            DROP CONSTRAINT IF EXISTS {_DRIVE_SHAPE},
            ADD CONSTRAINT {_DRIVE_SHAPE} CHECK (
                kind <> 'drive' OR (
                    path IS NOT NULL AND drive_entry_id::text = path
                    AND port IS NULL AND title IS NULL
                    AND files IS NULL AND shared_at IS NULL
                    AND created_thread_id IS NULL AND NOT keep_updated
                    AND latest_seq = 0 AND shared_seq = 0 AND kept_at IS NULL
                    AND audience IS NULL AND legacy_refused IS NULL
                    AND cardinality(loaded_files) = 0
                    AND cardinality(left_out_files) = 0
                )
            ) NOT VALID,
            DROP CONSTRAINT IF EXISTS {_DRIVE_ENTRY},
            ADD CONSTRAINT {_DRIVE_ENTRY}
                CHECK ((kind = 'drive') = (drive_entry_id IS NOT NULL)) NOT VALID
    """)
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"SET statement_timeout = '{_BUILD_TIMEOUT}'")
        try:
            for name in (_KIND, _ARTIFACT_KIND, _DRIVE_SHAPE, _DRIVE_ENTRY):
                op.execute(f"ALTER TABLE share_links VALIDATE CONSTRAINT {name}")
            _rebuild(_INDEX, """
                UNIQUE INDEX CONCURRENTLY {name}
                ON share_links (workspace_id, path) WHERE kind = 'drive'
            """)
            _rebuild(_ENTRY_INDEX, """
                INDEX CONCURRENTLY {name}
                ON share_links (drive_entry_id) WHERE drive_entry_id IS NOT NULL
            """)
            _rebuild(_AGENT_SAVES, """
                INDEX CONCURRENTLY {name}
                ON drive_versions (entry_id, seq DESC)
                WHERE actor_kind = 'agent' AND thread_id IS NOT NULL
            """)
            # Its own statement, so the short lock it takes on both tables is
            # held only for the catalog change, with the index it cascades
            # through already there.
            op.execute(f"SET lock_timeout = '{_LOCK_TIMEOUT}'")
            op.execute(f"""
                ALTER TABLE share_links
                    DROP CONSTRAINT IF EXISTS {_DRIVE_FK},
                    ADD CONSTRAINT {_DRIVE_FK} FOREIGN KEY (drive_entry_id)
                        REFERENCES drive_entries(entry_id) ON DELETE CASCADE NOT VALID
            """)
            op.execute("SET lock_timeout = 0")
            op.execute(f"ALTER TABLE share_links VALIDATE CONSTRAINT {_DRIVE_FK}")
        finally:
            op.execute("RESET statement_timeout")
            op.execute("RESET lock_timeout")


def downgrade() -> None:
    op.execute(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'")
    # The lock first, held to the commit: a drive row inserted between the
    # delete and the ALTER would survive it and fail the validation below.
    op.execute("LOCK TABLE share_links IN ACCESS EXCLUSIVE MODE")
    # Its comments go with it (``artifact_comments`` cascades).
    op.execute("DELETE FROM share_links WHERE kind = 'drive'")
    op.execute(f"""
        ALTER TABLE share_links
            DROP CONSTRAINT IF EXISTS {_DRIVE_FK},
            DROP CONSTRAINT IF EXISTS {_DRIVE_ENTRY},
            DROP CONSTRAINT IF EXISTS {_DRIVE_SHAPE},
            DROP CONSTRAINT IF EXISTS {_ARTIFACT_KIND},
            ADD CONSTRAINT {_ARTIFACT_KIND} CHECK (
                kind IN ('file', 'chart') OR (
                    created_thread_id IS NULL AND bound_thread_id IS NULL
                    AND NOT keep_updated AND latest_seq = 0
                    AND last_comment_number = 0
                )
            ) NOT VALID,
            DROP CONSTRAINT IF EXISTS {_KIND},
            ADD CONSTRAINT {_KIND}
                CHECK (kind IN ('file', 'app', 'chart')) NOT VALID
    """)
    # Under the same lock; its own index goes with it.
    op.execute("ALTER TABLE share_links DROP COLUMN IF EXISTS drive_entry_id")
    # The NOT VALID checks already refuse a new drive row, so nothing can
    # land between the delete committing and the validation.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"SET statement_timeout = '{_BUILD_TIMEOUT}'")
        try:
            for name in (_ARTIFACT_KIND, _KIND):
                op.execute(f"ALTER TABLE share_links VALIDATE CONSTRAINT {name}")
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}")
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_AGENT_SAVES}")
        finally:
            op.execute("RESET statement_timeout")
            op.execute("RESET lock_timeout")
