"""Migration 079: moving a workspace to another of the user's computers.

A move keeps the workspace's id, threads and settings and changes only where
its folder lives. Each one is a ``workspace_moves`` row, which is also what
fences the workspace while it runs: a turn may not start until the row is
done. One move per user at a time, held by the partial unique index.

``workspaces.files_epoch`` names the move that last put the workspace's files
on its computer. A worker that already attached the workspace to a sandbox
keys that on the epoch too, so a workspace moved away and back is restored
again rather than taken as already there.

Rollback: ``downgrade`` drops both. A move still running is lost with them;
its workspace is on whichever computer the move had reached.
"""

from alembic import op

revision = "079"
down_revision = "078"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS files_epoch UUID")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS workspace_moves (
            move_id UUID PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            workspace_id UUID NOT NULL
                REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            source_computer_id UUID NOT NULL
                REFERENCES computers(computer_id) ON DELETE CASCADE,
            target_computer_id UUID NOT NULL
                REFERENCES computers(computer_id) ON DELETE CASCADE,
            make_primary BOOLEAN NOT NULL DEFAULT FALSE,
            phase TEXT NOT NULL CHECK (phase IN (
                'preparing', 'backing_up', 'staged', 'committed',
                'done', 'failed', 'cancelled'
            )),
            runner_id UUID NOT NULL,
            source_folder TEXT,
            leftovers_path TEXT,
            target_folder TEXT,
            error JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            staged_at TIMESTAMPTZ,
            committed_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            restored_at TIMESTAMPTZ,
            leftovers_removed_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS workspace_moves_one_active_per_user
        ON workspace_moves (user_id)
        WHERE phase IN ('preparing', 'backing_up', 'staged', 'committed')
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS workspace_moves_by_workspace
        ON workspace_moves (workspace_id, created_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS workspace_moves_leftovers
        ON workspace_moves (source_computer_id)
        WHERE leftovers_path IS NOT NULL AND leftovers_removed_at IS NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS workspace_moves")
    op.execute("ALTER TABLE workspaces DROP COLUMN IF EXISTS files_epoch")
