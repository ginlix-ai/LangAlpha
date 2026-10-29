"""Migration 078: a workspace's updated_at moves only when a writer stamps it.

The gallery orders workspaces by ``updated_at``, and the row trigger set it on
every UPDATE. Since workspaces share a computer, a machine write (a stop, a
tier, a sandbox stamp, a backup mark) reaches every folder on it, so each idle
stop gave all of a computer's workspaces the same timestamp and the gallery
fell back to their ids. The writers that mean to move a workspace (a rename,
a pin, a reorder, its own activity, a delete) already stamp the column
themselves, so the trigger goes.

Rows a machine write stamped after their last use are set back to that use,
or to their creation when never used. A workspace renamed or pinned after its
last use moves back with them: the trigger stamped single-row machine writes
(a scan mark, a restore mark, an MCP config bump, a folder settle) just as it
stamped a rename, so nothing in the row tells the two apart.

The drop commits before the repair runs, so an upgrade that dies between them
leaves the trigger gone with 077 still recorded: run it again, which is safe.

Rollback: ``downgrade`` restores the trigger; repaired timestamps stay.
"""

from alembic import op

revision = "078"
down_revision = "077"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Each statement commits on its own. DROP TRIGGER takes ACCESS EXCLUSIVE on
    # workspaces; held through the repair and the rest of the run, it would
    # stall every read of the table.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute("DROP TRIGGER IF EXISTS trg_workspaces_updated_at ON workspaces")
        # Deleted rows are left alone: their updated_at orders the folder
        # cleanup queue, not the gallery.
        op.execute(
            """
            UPDATE workspaces
            SET updated_at = GREATEST(created_at, COALESCE(last_activity_at, created_at))
            WHERE status <> 'deleted'
              AND computer_id IS NOT NULL
              AND updated_at > GREATEST(created_at, COALESCE(last_activity_at, created_at))
            """
        )
        op.execute("RESET lock_timeout")


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TRIGGER IF EXISTS trg_workspaces_updated_at ON workspaces")
    op.execute(
        """
        CREATE TRIGGER trg_workspaces_updated_at
            BEFORE UPDATE ON workspaces
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column()
        """
    )
