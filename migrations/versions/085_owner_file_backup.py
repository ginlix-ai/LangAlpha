"""Migration 085: file backup an SSH computer's owner chooses, and restores they ask for.

The server mirrors every workspace's files so it can lay them again on a
machine it rebuilds. A machine its owner brought is never rebuilt, so on one
the mirror is the owner's choice: ``computers.files_backup`` says whether
routine backups (after a turn, at a stop) run there. Off, the last backup is
kept and stops updating. A move still copies the folder through it, since
that is how the files reach the other computer.

Nor does the server lay files on such a machine unasked. A folder found gone
from it while a backup exists is recorded in ``workspaces.files_restore_offered_at``
and left for the owner to restore or keep empty; until then no backup of that
workspace runs, so the files it could restore are the ones it held when the
folder went. ``workspaces.files_backed_up_at`` is when the last complete
backup pass ran, which is what the owner is shown before choosing.

``workspace_moves.from_backup`` marks a move that lays the workspace on its
target from the backup alone, without the computer it was on, for a machine
that is gone for good. Its folder stays there under its name, which the move
keeps holding until the sweep sets the folder aside on the machine's return,
so nothing on it is ever bound to that stale copy; the partial index serves
that hold, read on every folder allocation there.
``workspace_moves.left_folder_tried_at`` is when a worker last claimed that
folder for the sweep, so each is tried by one worker at a time, and one that
never clears waits behind those not yet tried instead of ahead of them.

Each statement commits on its own, so an upgrade cut short leaves some of
them applied with 084 still recorded: run it again, which is safe.

Rollback: ``alembic downgrade 084``. It drops the five columns: every computer
backs up again, a pending offer is forgotten and its folder is restored at
the next open, a move from the backup still in flight is run as an
ordinary move, which fails without its source, and a folder such a move left
holds its name no more.
"""

from alembic import op

revision = "085"
down_revision = "084"
branch_labels = None
depends_on = None


_STATEMENTS = (
    "ALTER TABLE computers "
    "ADD COLUMN IF NOT EXISTS files_backup BOOLEAN NOT NULL DEFAULT TRUE",
    "ALTER TABLE workspaces "
    "ADD COLUMN IF NOT EXISTS files_restore_offered_at TIMESTAMPTZ, "
    "ADD COLUMN IF NOT EXISTS files_backed_up_at TIMESTAMPTZ",
    "ALTER TABLE workspace_moves "
    "ADD COLUMN IF NOT EXISTS from_backup BOOLEAN NOT NULL DEFAULT FALSE, "
    "ADD COLUMN IF NOT EXISTS left_folder_tried_at TIMESTAMPTZ",
    """
    CREATE INDEX IF NOT EXISTS workspace_moves_left_in_place
    ON workspace_moves (source_computer_id)
    WHERE from_backup AND leftovers_removed_at IS NULL
    """,
)

_ROLLBACK = (
    "DROP INDEX IF EXISTS workspace_moves_left_in_place",
    "ALTER TABLE workspace_moves "
    "DROP COLUMN IF EXISTS left_folder_tried_at, "
    "DROP COLUMN IF EXISTS from_backup",
    "ALTER TABLE workspaces "
    "DROP COLUMN IF EXISTS files_backed_up_at, "
    "DROP COLUMN IF EXISTS files_restore_offered_at",
    "ALTER TABLE computers DROP COLUMN IF EXISTS files_backup",
)


def _each_on_its_own(statements: tuple[str, ...]) -> None:
    # Own transaction each: a table's ACCESS EXCLUSIVE lock, held while the
    # next statement waits on another table, would stall every read of it.
    for statement in statements:
        with op.get_context().autocommit_block():
            op.execute("SET lock_timeout = '5s'")
            op.execute(statement)
            op.execute("RESET lock_timeout")


def upgrade() -> None:
    _each_on_its_own(_STATEMENTS)


def downgrade() -> None:
    _each_on_its_own(_ROLLBACK)
