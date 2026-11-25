"""Migration 151: a chart holds comments, through a record of its own.

A chart is a ``share_links`` row of ``kind = 'chart'`` whose ``path`` is the
chart id, ``SYMBOL:timeframe`` as chart layers spell it, one per workspace
and chart. It exists only to hold comments and where they go: it has no file,
so it is never shared, published, kept or versioned, and no agent writes it,
so it names no creator thread. ``share_links_chart_shape`` holds all of that
in the row, so no path that shares or versions a record can take a chart by
mistake. A binding (``bound_thread_id``) is the owner's, as on a file.

Online-safe: each check is added NOT VALID under a short lock and validated
in an autocommit block under SHARE UPDATE EXCLUSIVE, and the unique index is
built concurrently. No chart row exists before this build writes one, so
nothing can fail validation, and the previous build, still serving while the
migration runs, meets no chart row until this build makes one.

Rollback needs the downgrade, not only the code: the previous build reads some
queries by record without a kind (a thread's queued comments, a send's claim,
a link by its code), so once chart rows exist it would show them as files. Roll
the code back and run ``alembic downgrade 150`` together; the downgrade
deletes every chart record and the comments on it.
"""

from alembic import op

revision = "151"
down_revision = "150"
branch_labels = None
depends_on = None

_KIND = "share_links_kind_check"
_ARTIFACT_KIND = "share_links_artifact_kind"
_CHART_SHAPE = "share_links_chart_shape"
_INDEX = "idx_share_links_chart"
# How long a CONCURRENTLY step may wait out the transactions open at its
# phases before the deploy fails rather than hangs.
_BUILD_TIMEOUT = "15min"


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f"""
        ALTER TABLE share_links
            DROP CONSTRAINT IF EXISTS {_KIND},
            ADD CONSTRAINT {_KIND}
                CHECK (kind IN ('file', 'app', 'chart')) NOT VALID,
            DROP CONSTRAINT IF EXISTS {_ARTIFACT_KIND},
            ADD CONSTRAINT {_ARTIFACT_KIND} CHECK (
                kind IN ('file', 'chart') OR (
                    created_thread_id IS NULL AND bound_thread_id IS NULL
                    AND NOT keep_updated AND latest_seq = 0
                    AND last_comment_number = 0
                )
            ) NOT VALID,
            DROP CONSTRAINT IF EXISTS {_CHART_SHAPE},
            ADD CONSTRAINT {_CHART_SHAPE} CHECK (
                kind <> 'chart' OR (
                    path IS NOT NULL AND port IS NULL AND title IS NULL
                    AND files IS NULL AND shared_at IS NULL
                    AND created_thread_id IS NULL AND NOT keep_updated
                    AND latest_seq = 0 AND shared_seq = 0 AND kept_at IS NULL
                    AND audience IS NULL AND legacy_refused IS NULL
                    AND cardinality(loaded_files) = 0
                    AND cardinality(left_out_files) = 0
                )
            ) NOT VALID
    """)
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"SET statement_timeout = '{_BUILD_TIMEOUT}'")
        try:
            for name in (_KIND, _ARTIFACT_KIND, _CHART_SHAPE):
                op.execute(f"ALTER TABLE share_links VALIDATE CONSTRAINT {name}")
            # Drop before create: IF NOT EXISTS matches on the name alone, and
            # would adopt the INVALID index a failed CONCURRENTLY build leaves,
            # which no ON CONFLICT ... WHERE kind = 'chart' can use.
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}")
            op.execute(f"""
                CREATE UNIQUE INDEX CONCURRENTLY {_INDEX}
                ON share_links (workspace_id, path) WHERE kind = 'chart'
            """)
        finally:
            op.execute("RESET statement_timeout")
            op.execute("RESET lock_timeout")


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # Its comments go with it (``artifact_comments`` cascades).
    op.execute("DELETE FROM share_links WHERE kind = 'chart'")
    op.execute(f"""
        ALTER TABLE share_links
            DROP CONSTRAINT IF EXISTS {_CHART_SHAPE},
            DROP CONSTRAINT IF EXISTS {_ARTIFACT_KIND},
            ADD CONSTRAINT {_ARTIFACT_KIND} CHECK (
                kind = 'file' OR (
                    created_thread_id IS NULL AND bound_thread_id IS NULL
                    AND NOT keep_updated AND latest_seq = 0
                    AND last_comment_number = 0
                )
            ) NOT VALID,
            DROP CONSTRAINT IF EXISTS {_KIND},
            ADD CONSTRAINT {_KIND} CHECK (kind IN ('file', 'app')) NOT VALID
    """)
    # The NOT VALID checks already refuse a new chart row, so nothing can
    # land between the delete committing and the validation.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"SET statement_timeout = '{_BUILD_TIMEOUT}'")
        try:
            for name in (_ARTIFACT_KIND, _KIND):
                op.execute(f"ALTER TABLE share_links VALIDATE CONSTRAINT {name}")
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}")
        finally:
            op.execute("RESET statement_timeout")
            op.execute("RESET lock_timeout")
