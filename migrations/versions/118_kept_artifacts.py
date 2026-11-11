"""Migration 118: a file kept for its owner, and a shared version of its own.

A page the owner sets as their dashboard is framed from a version the server
holds, so it opens while the computer that wrote it is stopped. ``kept_at``
marks a record whose versions the owner keeps for themselves: keeping cuts one
without sharing, and each turn's end in its workspace cuts another when its
files changed. Such a record, shared, needs two pointers: ``latest_seq`` stays
the newest, which the dashboard follows, and ``shared_seq`` is the version
visitors see, which ``files`` describes. 0 means none: not shared, or shared
before versions and not snapshotted yet, which is now what ``legacy_refused``
requires, since a keep can cut versions for the owner before that first view.

Both columns are added without a rewrite (nullable, or a constant default);
the backfill touches shared rows only. The check does not tie ``shared_seq``
to ``shared_at``: the build before this one unshares without knowing the
column and runs against this schema until a deploy moves traffic, so it would
be refused. A row it unshares keeps a ``shared_seq`` nothing reads, since
every reader asks whether the row is shared first, and sharing again sets it.

Rollback: ``alembic downgrade 117``. The previous build shows visitors the
latest version, so a shared record whose latest is not its shared one is
unshared first (its visitors' grants end; the owner publishes again). The
versions a keep cut stay and read as the owner's.
"""

from alembic import op

revision = "118"
down_revision = "117"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE share_links
            ADD COLUMN kept_at TIMESTAMPTZ,
            ADD COLUMN shared_seq INTEGER NOT NULL DEFAULT 0
    """)
    op.execute("""
        UPDATE share_links SET shared_seq = latest_seq
         WHERE shared_at IS NOT NULL AND latest_seq > 0
    """)
    op.execute("""
        ALTER TABLE share_links
            ADD CONSTRAINT share_links_shared_seq CHECK (
                shared_seq >= 0 AND shared_seq <= latest_seq
            ),
            DROP CONSTRAINT IF EXISTS share_links_legacy_refused,
            ADD CONSTRAINT share_links_legacy_refused CHECK (
                legacy_refused IS NULL OR (
                    kind = 'file' AND shared_seq = 0
                    AND legacy_refused IN ('too_many_files', 'too_many_bytes')
                )
            )
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        UPDATE share_links
           SET shared_at = NULL, files = NULL, revision = revision + 1
         WHERE shared_at IS NOT NULL AND shared_seq <> latest_seq
    """)
    op.execute("""
        UPDATE share_links SET legacy_refused = NULL
         WHERE legacy_refused IS NOT NULL AND latest_seq > 0
    """)
    op.execute("""
        ALTER TABLE share_links
            DROP CONSTRAINT IF EXISTS share_links_shared_seq,
            DROP CONSTRAINT IF EXISTS share_links_legacy_refused,
            ADD CONSTRAINT share_links_legacy_refused CHECK (
                legacy_refused IS NULL OR (
                    kind = 'file' AND latest_seq = 0
                    AND legacy_refused IN ('too_many_files', 'too_many_bytes')
                )
            ),
            DROP COLUMN shared_seq,
            DROP COLUMN kept_at
    """)
