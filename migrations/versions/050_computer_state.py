"""Migration 050: what a computer reports about itself between requests.

Three nullable columns, none with a default, so the old deployment keeps
reading and writing both tables unchanged. NULL always means "no record yet",
never an empty or clean state.

``computers.disk_*``: the last disk reading. Every workspace on a computer
shares one disk, so the list, the warning and the agent's turn context read the
stored figure rather than a running sandbox.

``computers.spec_change``: the outcome of the last spec change. The recreate
takes minutes, longer than an edge proxy keeps one request open, so it runs
behind a 202 and the client polls the row; the worker that ran it is rarely the
one answering.

``workspaces.files_scan_mark``: ``{ns, boot_id, sandbox_id}``, the sandbox clock
at the start of the project's last complete backup scan less a margin. The
post-turn sweep compares change times against it to skip projects nothing
touched. A hint only, never a reason to prune: a mark from another boot or
sandbox reads as changed.
"""

from alembic import op

revision = "050"
down_revision = "049"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE computers
            ADD COLUMN IF NOT EXISTS disk_total_bytes BIGINT,
            ADD COLUMN IF NOT EXISTS disk_used_bytes BIGINT,
            ADD COLUMN IF NOT EXISTS disk_free_bytes BIGINT,
            ADD COLUMN IF NOT EXISTS disk_measured_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS disk_sandbox_ref TEXT,
            ADD COLUMN IF NOT EXISTS spec_change JSONB
    """)
    op.execute("ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS files_scan_mark JSONB")


def downgrade() -> None:
    op.execute("ALTER TABLE workspaces DROP COLUMN IF EXISTS files_scan_mark")
    op.execute("""
        ALTER TABLE computers
            DROP COLUMN IF EXISTS spec_change,
            DROP COLUMN IF EXISTS disk_sandbox_ref,
            DROP COLUMN IF EXISTS disk_measured_at,
            DROP COLUMN IF EXISTS disk_free_bytes,
            DROP COLUMN IF EXISTS disk_used_bytes,
            DROP COLUMN IF EXISTS disk_total_bytes
    """)
