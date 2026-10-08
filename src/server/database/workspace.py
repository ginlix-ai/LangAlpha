"""Workspace persistence with migration 046 write-through computer shadows.

Rolling-deploy readers and platform capacity accounting still read sandbox_id,
resource_tier, is_always_on, platform_secret_version, and artifacts here.
Follow database/computer.py's atomic-write and lock-order rules; NULL computer_id
rows (flash and shared-sandbox backfill losers) retain single-table behavior.
"""

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from psycopg.rows import dict_row

from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.server.database.computer import (
    COMPUTER_STATUSES,
    get_computer,
)
from src.server.database.home_fold import fold_former_homes
from src.server.database.livefs_links import drop_workspace_links
from src.server.database.mcp_servers import start_new_workspace_selection
from src.server.database.pool import get_db_connection
from src.server.database.home_workspace import get_flash_workspace_id
from src.server.database.runs.lifecycle import workspace_has_live_runs
from src.server.database.sql_fences import (
    FENCE_LIVE_WORKSPACE,
    FENCE_NOT_DELETED,
    shadowed_write,
)
from src.server.database.user_lock import lock_user_writes
from src.server.database.workspace_folders import (
    MOVING_DIR,
    WorkspaceFolderMoving,
    folder_allocation,
    moving_path,
    recorded_landings,
    release_former_folder,
    takes_name_folders,
)
from src.server.database.workspace_names import (
    WorkspaceNameInvalid,
    WorkspaceNameTaken,
    candidate_dir_name,
    candidate_dir_names,
    checked_workspace_name,
    workspace_folder_name,
    workspace_name_key,
)
from src.server.services.workspace_status_pubsub import publish_status_change
from src.server.utils.pg_sanitize import normalize_uuid

logger = logging.getLogger(__name__)

# A shared literal column list keeps dict_row shapes consistent and interpolation safe.
_WS_COLUMNS: tuple[str, ...] = (
    "workspace_id",
    "user_id",
    "name",
    "description",
    "sandbox_id",
    "computer_id",
    "dir_name",
    "previous_dir_names",
    "layout_origin",
    "status",
    "created_at",
    "updated_at",
    "last_activity_at",
    "stopped_at",
    "config",
    "artifacts",
    "is_pinned",
    "sort_order",
    "resource_tier",
    "is_always_on",
    "platform_secret_version",
    "mcp_config_version",
)

# The restore flag is read as a boolean, never as its column: the timestamp is
# bookkeeping for the prune gate, and a reader only needs to know that files
# are missing.
_WS_RESTORE_FLAG = (
    "({prefix}files_restore_incomplete_at IS NOT NULL) AS files_restore_incomplete"
)


def _ws_cols(alias: str = "") -> str:
    """The workspace projection, qualified when the statement needs it.

    Six of these names also belong to the machine CTE the bind joins against,
    and an unqualified one in that RETURNING is ambiguous to Postgres rather
    than defaulting to the target table.
    """
    prefix = f"{alias}." if alias else ""
    return ", ".join(
        [f"{prefix}{column}" for column in _WS_COLUMNS]
        + [_WS_RESTORE_FLAG.format(prefix=prefix)]
    )


_WS_COLS = _ws_cols()

def _mirrors_to_computer(status: str) -> bool:
    """Deleting a project must not delete a shared machine; ComputerManager owns machine teardown."""
    return status in COMPUTER_STATUSES and status != "deleted"


@asynccontextmanager
async def _ws_cursor(conn=None):
    async with get_db_connection(conn) as owned:
        async with owned.cursor(row_factory=dict_row) as cur:
            yield cur


@asynccontextmanager
async def _ws_transaction(conn=None):
    """A cursor in a transaction of its own, or in a savepoint of the caller's.

    The pool's connections are autocommit, and a workspace INSERT has to
    commit with the MCP selection it starts with.
    """
    async with get_db_connection(conn) as owned, owned.transaction():
        async with owned.cursor(row_factory=dict_row) as cur:
            yield cur


class FlashWorkspaceTaken(RuntimeError):
    """The user's flash id names a row another account owns.

    An account merge moves an account's rows onto the account it joins, flash
    row included, so a merged id that signs in again finds its flash id held by
    the account it joined.
    """

    def __init__(self, workspace_id: str, user_id: str) -> None:
        super().__init__(f"flash workspace {workspace_id} for user {user_id} is owned by another user")


async def get_or_create_flash_workspace(user_id: str, conn=None) -> Dict[str, Any]:
    """Deterministic identity and ON CONFLICT make concurrent creation idempotent.

    Every Flash turn lands here, so a row that exists is touched in one
    statement with no lock. Only a miss opens a transaction and takes the
    lock, so the workspace commits with the MCP selection it starts with.
    Both statements match the owner as well as the id: the row is the one a
    turn runs in and Home binds, so another account's must never come back.
    An account merge can leave a former flash row beside it, which every
    resolve tries to fold in; a row with a run in progress, or a fold that
    fails, waits for a later resolve.
    """
    from psycopg.types.json import Json

    workspace_id = get_flash_workspace_id(user_id)
    config_json = Json({"flash_mode": True})

    try:
        async with _ws_cursor(conn) as cur:
            await cur.execute(
                f"""
                UPDATE workspaces SET updated_at = NOW(), is_pinned = TRUE
                WHERE workspace_id = %s AND user_id = %s
                RETURNING {_WS_COLS}
                """,
                (workspace_id, user_id),
            )
            result = await cur.fetchone()
        if result is None:
            async with _ws_transaction(conn) as cur:
                await lock_user_writes(cur, user_id)
                await cur.execute(
                    f"""
                    INSERT INTO workspaces (workspace_id, user_id, name, description, config, status, is_pinned)
                    VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                    ON CONFLICT (workspace_id) DO UPDATE SET updated_at = NOW(), is_pinned = TRUE
                    WHERE workspaces.user_id = EXCLUDED.user_id
                    RETURNING {_WS_COLS}, (xmax = 0) AS inserted
                    """,
                    (
                        workspace_id,
                        user_id,
                        "Flash",
                        "Flash mode conversations",
                        config_json,
                        "flash",
                    ),
                )
                row = await cur.fetchone()
                if row is None:
                    raise FlashWorkspaceTaken(workspace_id, user_id)
                result = dict(row)
                # False when a concurrent first turn inserted it while this
                # one waited on the lock; that one started the selection.
                if result.pop("inserted"):
                    await start_new_workspace_selection(cur, user_id, workspace_id)

        logger.info(f"Upserted flash workspace: {workspace_id} for user: {user_id}")

    except Exception as e:
        logger.error(f"Error upserting flash workspace for user {user_id}: {e}")
        raise

    try:
        await fold_former_homes(user_id, workspace_id, conn)
    except Exception:
        # Home itself resolved; the fold retries on the next resolve.
        logger.exception(f"[home_fold] fold into {workspace_id} failed")
    return dict(result)


async def create_workspace(
    user_id: str,
    name: str,
    description: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    conn=None,
    workspace_id: Optional[str] = None,
    status: str = "creating",
) -> Dict[str, Any]:
    from psycopg.types.json import Json

    # Flash rows sit outside the name index: they have no folder to collide.
    name_key = None
    if status != "flash":
        name = checked_workspace_name(name)
        name_key = workspace_name_key(name)
    try:
        config_json = Json(config) if config else Json({})

        async with _ws_transaction(conn) as cur:
            await lock_user_writes(cur, user_id)
            if workspace_id:
                # Flash mode may supply thread_id as workspace_id.
                await cur.execute(
                    f"""
                    INSERT INTO workspaces (workspace_id, user_id, name, name_key, description, config, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING {_WS_COLS}
                    """,
                    (workspace_id, user_id, name, name_key, description, config_json, status),
                )
            else:
                await cur.execute(
                    f"""
                    INSERT INTO workspaces (user_id, name, name_key, description, config, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING {_WS_COLS}
                    """,
                    (user_id, name, name_key, description, config_json, status),
                )
            result = await cur.fetchone()
            await start_new_workspace_selection(
                cur, user_id, str(result["workspace_id"])
            )

        logger.info(f"Created workspace: {result['workspace_id']} for user: {user_id}")
        return dict(result)

    except Exception as e:
        logger.error(f"Error creating workspace for user {user_id}: {e}")
        raise


async def get_workspace(
    workspace_id: str,
    conn=None,
) -> Optional[Dict[str, Any]]:
    # Normalize urn:uuid forms PostgreSQL rejects with 22P02; non-UUID SPA keys
    # cannot match the primary key and should return not-found, not 500.
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None

    try:
        async with _ws_cursor(conn) as cur:
            await cur.execute(
                f"""
                SELECT {_ws_cols("w")}, c.root_dir AS computer_root_dir
                FROM workspaces w
                LEFT JOIN computers c ON c.computer_id = w.computer_id
                WHERE w.workspace_id = %s AND w.status != 'deleted'
                """,
                (workspace_id,),
            )
            result = await cur.fetchone()

        if result:
            return dict(result)
        return None

    except Exception as e:
        logger.error(f"Error getting workspace {workspace_id}: {e}")
        raise


async def get_workspace_placement(workspace_id: str) -> Optional[Dict[str, Any]]:
    """A live workspace's owner, computer and folder: what the file mount
    checks on every request under a workspace, without the full row."""
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None

    async with _ws_cursor() as cur:
        await cur.execute(
            """
            SELECT workspace_id, user_id, computer_id, dir_name
            FROM workspaces
            WHERE workspace_id = %s AND status != 'deleted'
            """,
            (workspace_id,),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def get_workspace_identity(workspace_id: str) -> Optional[Dict[str, Any]]:
    """Avoid large JSONB reads on every cached-session validation.

    Include tombstones so deleted differs from missing. Join computer identity to
    detect split bindings where only the computer half committed and the losing
    provisioner deleted the sandbox still named by the workspace, and the
    machine's root owner, which the layout migration needs on this same path.
    """
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None

    try:
        async with _ws_cursor() as cur:
            await cur.execute(
                """
                SELECT w.status, w.sandbox_id, w.computer_id,
                       c.provider_ref, c.status AS computer_status,
                       c.origin_workspace_id
                FROM workspaces w
                LEFT JOIN computers c ON c.computer_id = w.computer_id
                WHERE w.workspace_id = %s
                """,
                (workspace_id,),
            )
            result = await cur.fetchone()
        return dict(result) if result else None

    except Exception as e:
        logger.error(f"Error reading identity for workspace {workspace_id}: {e}")
        raise


# Match migration 046's constraint_name to distinguish folder collisions.
_COMPUTER_DIR_INDEX = "idx_workspaces_computer_dir"
# Migration 054's per-user name index.
_USER_NAME_INDEX = "idx_workspaces_user_name_key"


def _violated_index(error: Exception) -> str | None:
    return getattr(getattr(error, "diag", None), "constraint_name", None)


async def find_workspace_by_name_key(user_id: str, key: str) -> Optional[Dict[str, Any]]:
    """The live workspace holding a name, read on a fresh connection.

    Callers reach this after a UniqueViolation, when their own transaction is
    already aborted.
    """
    async with _ws_cursor() as cur:
        await cur.execute(
            """
            SELECT workspace_id, name FROM workspaces
            WHERE user_id = %s AND name_key = %s
              AND status NOT IN ('deleted', 'flash')
            """,
            (user_id, key),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def _name_taken(user_id: str, name: str) -> WorkspaceNameTaken:
    holder = await find_workspace_by_name_key(user_id, workspace_name_key(name))
    if holder is None:
        return WorkspaceNameTaken(name)
    return WorkspaceNameTaken(holder["name"], str(holder["workspace_id"]))


async def get_workspace_name_keys(user_id: str) -> set[str]:
    """Every name key this user's live workspaces hold, for picking a free name."""
    async with _ws_cursor() as cur:
        await cur.execute(
            """
            SELECT name_key FROM workspaces
            WHERE user_id = %s AND name_key IS NOT NULL
              AND status NOT IN ('deleted', 'flash')
            """,
            (user_id,),
        )
        return {row["name_key"] for row in await cur.fetchall()}


class WorkspaceDirNameTaken(Exception):
    """Only the caller can choose a replacement dir_name because it knows why that name was picked."""

    def __init__(self, workspace_id: str, computer_id: str, dir_name: str | None):
        self.workspace_id = workspace_id
        self.computer_id = computer_id
        self.dir_name = dir_name
        super().__init__(
            f"Folder {dir_name!r} is already taken on computer {computer_id} "
            f"(rebinding workspace {workspace_id})"
        )


async def bind_workspace_to_computer(
    workspace_id: str,
    computer_id: str,
    *,
    expected_computer_id: str | None,
    dir_name: str | None = None,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Move a project onto a machine and take that machine's lifecycle with it.

    None is the expected_computer_id of a row migration 046 left unbound, which
    is what the edge resolve passes; the CAS is what stops two concurrent binds
    from each creating a machine for it. The shadow columns come from the
    machine, as create_workspace_on_computer's insert does: a row left naming
    the old machine's lifecycle is what wedges the idle reaper. A flash row
    passes the fence because this is how it becomes the user's Home; the
    callers decide which rows may be bound.
    """
    from psycopg.errors import UniqueViolation

    workspace_id = normalize_uuid(workspace_id)
    computer_id = normalize_uuid(computer_id)
    if workspace_id is None or computer_id is None:
        return None
    expected = None
    if expected_computer_id is not None:
        # Invalid expectations must not normalize to None and match never-bound rows.
        expected = normalize_uuid(expected_computer_id)
        if expected is None:
            return None

    try:
        # The pool autocommits, so without this the bind is visible while a
        # sibling still lists the folder as its former one.
        async with get_db_connection(conn) as owned, owned.transaction(), _ws_cursor(owned) as cur:
            await cur.execute(
                f"""
                WITH comp AS (
                    SELECT computer_id, status, resource_tier, is_always_on,
                           provider_ref, platform_secret_version
                    FROM computers
                    WHERE computer_id = %(computer_id)s AND {FENCE_NOT_DELETED}
                      AND user_id = (
                          SELECT user_id FROM workspaces
                          WHERE workspace_id = %(workspace_id)s
                      )
                    FOR SHARE
                )
                UPDATE workspaces w
                SET computer_id = comp.computer_id,
                    dir_name = COALESCE(%(dir_name)s, w.dir_name),
                    -- A folder given here ends any move a settle staged.
                    config = CASE WHEN %(dir_name)s::text IS NULL THEN w.config
                        ELSE w.config #- '{{folder_landings}}' END,
                    status = comp.status,
                    sandbox_id = comp.provider_ref,
                    resource_tier = comp.resource_tier,
                    is_always_on = comp.is_always_on,
                    platform_secret_version = comp.platform_secret_version,
                    updated_at = NOW()
                FROM comp
                WHERE w.workspace_id = %(workspace_id)s
                  AND w.computer_id IS NOT DISTINCT FROM %(expected)s
                  AND w.{FENCE_NOT_DELETED}
                RETURNING {_ws_cols("w")}
                """,
                {
                    "computer_id": computer_id,
                    "dir_name": dir_name,
                    "workspace_id": workspace_id,
                    "expected": expected,
                },
            )
            row = await cur.fetchone()
            if row is not None:
                # A move takes the mount's links on the computer left with
                # it: a folder made there again, should the workspace come
                # back, holds none until a link lays them, and every worker's
                # next turn there reads it so.
                await drop_workspace_links(
                    cur, workspace_id, keep_computer_id=computer_id
                )
            if row is not None and row.get("dir_name"):
                await release_former_folder(
                    cur, computer_id=computer_id, workspace_id=workspace_id, folder=row["dir_name"]
                )
    except UniqueViolation as e:
        constraint = getattr(getattr(e, "diag", None), "constraint_name", None)
        # An unnamed violation still qualifies: these columns have no other unique index.
        if constraint in (_COMPUTER_DIR_INDEX, None):
            raise WorkspaceDirNameTaken(workspace_id, computer_id, dir_name) from e
        raise

    if row is None:
        logger.info(
            f"Workspace {workspace_id} was not bindable to computer "
            f"{computer_id} (expected {expected}, deleted, or the target is gone)"
        )
        return None
    logger.info(
        f"Bound workspace {workspace_id} to computer {computer_id} as "
        f"{row.get('dir_name')} (status {row['status']}, was on {expected})"
    )
    return dict(row)


async def get_live_workspace_ids_for_computer(
    computer_id: str,
    *,
    conn=None,
) -> List[str]:
    computer_id = normalize_uuid(computer_id)
    if computer_id is None:
        return []

    async with _ws_cursor(conn) as cur:
        await cur.execute(
            """
            SELECT workspace_id
            FROM workspaces
            WHERE computer_id = %s AND status <> 'deleted'
            ORDER BY created_at
            """,
            (computer_id,),
        )
        return [str(r["workspace_id"]) for r in await cur.fetchall()]


async def get_restore_owed_workspace_ids_for_computer(
    computer_id: str,
    *,
    conn=None,
) -> List[str]:
    """Live workspaces on a computer whose files a recreate has not restored yet."""
    computer_id = normalize_uuid(computer_id)
    if computer_id is None:
        return []

    async with _ws_cursor(conn) as cur:
        await cur.execute(
            """
            SELECT workspace_id
            FROM workspaces
            WHERE computer_id = %s AND status <> 'deleted'
              AND files_restore_incomplete_at IS NOT NULL
            ORDER BY created_at
            """,
            (computer_id,),
        )
        return [str(r["workspace_id"]) for r in await cur.fetchall()]


async def get_live_workspace_folders_for_computer(
    computer_id: str,
) -> List[Dict[str, Any]]:
    """Each live project's name and folder, for attributing disk use to it."""
    computer_id = normalize_uuid(computer_id)
    if computer_id is None:
        return []

    async with _ws_cursor() as cur:
        await cur.execute(
            f"""
            SELECT workspace_id, name, dir_name
            FROM workspaces
            WHERE computer_id = %s AND {FENCE_LIVE_WORKSPACE}
            ORDER BY created_at
            """,
            (computer_id,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def get_workspace_dir_names_for_computer(
    computer_id: str, *, conn=None
) -> tuple[str, ...]:
    """Every folder a row on this computer holds, tombstones included.

    A stopped machine may still hold a deleted workspace's folder. A row staged
    mid-move also holds the folder it left, the one its name lands on and every
    one a pass planned for it: until the move is recorded its content may be in
    any, and a deleted one's clearing takes whatever it finds there.
    """
    async with _ws_cursor(conn) as cur:
        await cur.execute(
            """
            SELECT dir_name AS folder, NULL AS landing, NULL::jsonb AS landings
            FROM workspaces
            WHERE computer_id = %(computer)s AND dir_name IS NOT NULL
            UNION ALL
            SELECT previous_dir_names[1], name, config->'folder_landings' FROM workspaces
            WHERE computer_id = %(computer)s
              AND dir_name = %(moving)s || workspace_id::text
            """,
            {"computer": computer_id, "moving": f"{MOVING_DIR}/"},
        )
        rows = await cur.fetchall()
    held = [row["folder"] for row in rows if row["folder"]]
    for row in rows:
        held.extend(recorded_landings(row.get("landings")))
        if row["landing"] is not None:
            try:
                held.append(workspace_folder_name(row["landing"]))
            except WorkspaceNameInvalid:
                pass
    return tuple(held)


async def count_live_workspaces_by_computer(
    computer_ids: List[str],
    *,
    conn=None,
) -> Dict[str, int]:
    """How many live projects sit on each of these machines, in one query.

    A machine with none is absent from the result, so callers default to zero.
    """
    ids = [i for i in (normalize_uuid(c) for c in computer_ids) if i is not None]
    if not ids:
        return {}

    async with _ws_cursor(conn) as cur:
        await cur.execute(
            """
            SELECT computer_id, count(*) AS live
            FROM workspaces
            WHERE computer_id = ANY(%s::uuid[]) AND status <> 'deleted'
            GROUP BY computer_id
            """,
            (ids,),
        )
        return {str(r["computer_id"]): int(r["live"]) for r in await cur.fetchall()}


async def get_workspace_dir_name(workspace_id: str, *, conn=None) -> Optional[str]:
    """Avoid fetching large JSONB columns for a folder lookup on every acquisition."""
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None

    async with _ws_cursor(conn) as cur:
        await cur.execute(
            "SELECT dir_name FROM workspaces WHERE workspace_id = %s",
            (workspace_id,),
        )
        row = await cur.fetchone()
    return row["dir_name"] if row else None


async def create_workspace_on_computer(
    user_id: str,
    name: str,
    computer_id: str,
    *,
    description: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    workspace_id: Optional[str] = None,
    selection_from: Optional[str] = None,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Create a complete shadow atomically so failed binding cannot leave an unbound project.

    FOR SHARE holds status changes until the new row can receive their shadow UPDATE;
    otherwise it may retain a stale status that later transitions skip. Include
    sandbox_id or attachment sees a split binding and rebuilds every sibling's machine.
    None means the computer is gone: resolve another instead of retrying this one.

    The folder is the name. A row still holding it on this computer under any
    case (a deleted workspace awaiting cleanup, or a sibling renamed away that
    has not moved yet) gets this one a placeholder, which the next folder
    settle replaces.
    The row commits with the MCP selection it starts with; ``selection_from``
    is a duplicate's source, whose switched-off servers the copy keeps.
    """
    from psycopg.errors import UniqueViolation
    from psycopg.types.json import Json

    computer_id = normalize_uuid(computer_id)
    if computer_id is None:
        return None
    workspace_id = normalize_uuid(workspace_id) or str(uuid.uuid4())
    name = checked_workspace_name(name)
    name_key = workspace_name_key(name)

    try:
        # The pool autocommits, and the row must not be visible before its
        # folder's release and its MCP selection commit with it.
        async with folder_allocation(computer_id, conn=conn) as owned, _ws_cursor(owned) as cur:
            await lock_user_writes(cur, user_id)
            held = await get_workspace_dir_names_for_computer(computer_id, conn=owned)
            computer = await get_computer(computer_id, conn=owned)
            own_folder = computer is None or takes_name_folders(computer)
            dir_name = candidate_dir_name(name, workspace_id, 0)
            for dir_name in candidate_dir_names(name, workspace_id, held, own_folder=own_folder):
                try:
                    # A savepoint, which a failed insert rolls back so the next
                    # attempt can run.
                    async with owned.transaction():
                        await cur.execute(
                            f"""
                            WITH comp AS (
                                SELECT computer_id, status, resource_tier, is_always_on,
                                       provider_ref, platform_secret_version
                                FROM computers
                                WHERE computer_id = %(computer_id)s AND {FENCE_NOT_DELETED}
                                FOR SHARE
                            )
                            INSERT INTO workspaces (
                                workspace_id, user_id, name, name_key, description, config,
                                computer_id, dir_name, status, resource_tier,
                                is_always_on, sandbox_id, platform_secret_version
                            )
                            SELECT %(workspace_id)s::uuid, %(user_id)s, %(name)s,
                                   %(name_key)s, %(description)s, %(config)s,
                                   comp.computer_id, %(dir_name)s,
                                   comp.status, comp.resource_tier, comp.is_always_on,
                                   comp.provider_ref, comp.platform_secret_version
                            FROM comp
                            RETURNING {_WS_COLS}
                            """,
                            {
                                "computer_id": computer_id,
                                "workspace_id": workspace_id,
                                "user_id": user_id,
                                "name": name,
                                "name_key": name_key,
                                "description": description,
                                "config": Json(config or {}),
                                "dir_name": dir_name,
                            },
                        )
                        row = await cur.fetchone()
                        if row is not None:
                            await release_former_folder(
                                cur, computer_id=computer_id, workspace_id=workspace_id, folder=dir_name
                            )
                            await start_new_workspace_selection(
                                cur, user_id, workspace_id, like_workspace_id=selection_from
                            )
                    break
                except UniqueViolation as e:
                    if _violated_index(e) not in (_COMPUTER_DIR_INDEX, None):
                        raise
                    logger.info(
                        f"Folder {dir_name!r} held on computer {computer_id}; "
                        f"placing workspace {workspace_id} until it frees"
                    )
            else:
                raise WorkspaceDirNameTaken(workspace_id, computer_id, dir_name)
    except UniqueViolation as e:
        if _violated_index(e) == _USER_NAME_INDEX:
            raise await _name_taken(user_id, name) from e
        raise

    if row is None:
        logger.warning(
            f"Computer {computer_id} is gone or deleted; workspace {name!r} "
            f"for user {user_id} was not created on it"
        )
        return None
    logger.info(
        f"Created workspace {row['workspace_id']} for user {user_id} on computer "
        f"{computer_id} as {row['dir_name']} (status {row['status']})"
    )
    return dict(row)


async def duplicate_workspace_on_computer(
    source_id: str,
    user_id: str,
    name: str,
    computer_id: str,
    *,
    description: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Publish the project and its file manifest in the same transaction.

    A visible empty project can attach before the copy and record an empty
    restore as complete, permanently skipping the files that arrive later.
    """
    from src.server.database.workspace_file import copy_workspace_files

    async with get_db_connection() as conn:
        async with conn.transaction():
            workspace = await create_workspace_on_computer(
                user_id, name, computer_id,
                description=description, config=config,
                selection_from=source_id, conn=conn,
            )
            if workspace is not None:
                await copy_workspace_files(
                    source_id, str(workspace["workspace_id"]), conn=conn,
                )
            return workspace


async def adopt_computer_sandbox_into_workspaces(
    computer_id: str,
) -> list[str]:
    """Repair shadows that the bind's previous-ref fence cannot reach.

    Two arms, both keyed on identity rather than on a guess: a NULL sandbox_id
    has nothing to contradict, and a row already naming this machine's
    provider_ref is the same machine, so its lifecycle columns may be realigned.
    A different non-NULL sandbox_id is a split binding and is left alone.
    """
    computer_id = normalize_uuid(computer_id)
    if computer_id is None:
        return []
    async with _ws_cursor() as cur:
        await cur.execute(
            """
            WITH comp AS (
                SELECT computer_id, provider_ref, status, platform_secret_version
                FROM computers
                WHERE computer_id = %(computer_id)s
                  AND provider_ref IS NOT NULL
                  AND status = 'running'
                FOR SHARE
            )
            UPDATE workspaces w
            SET sandbox_id = comp.provider_ref,
                status = comp.status,
                platform_secret_version = comp.platform_secret_version,
                updated_at = NOW()
            FROM comp
            WHERE w.computer_id = comp.computer_id
              AND w.status NOT IN ('deleted', 'flash')
              AND (w.sandbox_id IS NULL OR w.sandbox_id = comp.provider_ref)
              AND (
                  w.sandbox_id IS DISTINCT FROM comp.provider_ref
                  OR w.status <> comp.status
                  OR w.platform_secret_version
                      IS DISTINCT FROM comp.platform_secret_version
              )
            RETURNING w.workspace_id
            """,
            {"computer_id": computer_id},
        )
        repaired = [str(r["workspace_id"]) for r in await cur.fetchall()]
    if repaired:
        logger.info(
            f"Realigned {len(repaired)} workspace shadow(s) with computer "
            f"{computer_id}: {', '.join(repaired)}"
        )
    return repaired


async def get_workspace_name_and_description(
    workspace_id: str,
) -> Optional[Dict[str, Any]]:
    """Avoid fetching config/artifacts JSONB for two prompt strings on every agent turn."""
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None

    try:
        async with _ws_cursor() as cur:
            await cur.execute(
                """
                SELECT name, description
                FROM workspaces
                WHERE workspace_id = %s AND status != 'deleted'
                """,
                (workspace_id,),
            )
            result = await cur.fetchone()
        return dict(result) if result else None

    except Exception as e:
        logger.error(f"Error reading the name of workspace {workspace_id}: {e}")
        raise


async def get_workspaces_for_user(
    user_id: str,
    limit: int | None = 20,
    offset: int = 0,
    include_deleted: bool = False,
    sort_by: str = "custom",
    include_flash: bool = False,
    conn=None,
) -> Tuple[List[Dict[str, Any]], int]:
    try:
        status_filter = "" if include_deleted else "AND status != 'deleted'"
        # The flash row by its id too: bound as Home it reads its computer's status.
        flash_filter = (
            ""
            if include_flash
            else "AND status != 'flash' AND workspace_id <> %(flash_id)s"
        )
        params = {"user_id": user_id, "flash_id": get_flash_workspace_id(user_id)}

        # Every order ends on workspace_id, appended below: rows that tie would
        # otherwise come back in any order, which shifts an offset page and
        # rewrites the Chief of Staff's activity block with nothing changed.
        if sort_by == "activity":
            order_clause = "is_pinned DESC, COALESCE(last_activity_at, updated_at) DESC"
        elif sort_by == "recent":
            order_clause = "COALESCE(last_activity_at, updated_at) DESC"
        elif sort_by == "name":
            order_clause = "is_pinned DESC, name ASC"
        else:
            order_clause = "is_pinned DESC, sort_order ASC, updated_at DESC"

        async with _ws_cursor(conn) as cur:
            await cur.execute(
                f"""
                SELECT COUNT(*) as total
                FROM workspaces
                WHERE user_id = %(user_id)s {status_filter} {flash_filter}
                """,
                params,
            )
            count_result = await cur.fetchone()
            total = count_result["total"] if count_result else 0

            await cur.execute(
                f"""
                SELECT {_WS_COLS}
                FROM workspaces
                WHERE user_id = %(user_id)s {status_filter} {flash_filter}
                ORDER BY {order_clause}, workspace_id DESC
                LIMIT %(limit)s OFFSET %(offset)s
                """,
                # LIMIT NULL is no limit, so None returns every row.
                {**params, "limit": limit, "offset": offset},
            )
            results = await cur.fetchall()
        return [dict(r) for r in results], total

    except Exception as e:
        logger.error(f"Error getting workspaces for user {user_id}: {e}")
        raise


async def update_workspace(
    workspace_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    is_pinned: Optional[bool] = None,
    conn=None,
) -> Optional[Dict[str, Any]]:
    from psycopg.errors import UniqueViolation
    from psycopg.types.json import Json

    try:
        updates = []
        params = []

        if name is not None:
            # The folder follows on the next settle; dir_name stays where it is.
            name = checked_workspace_name(name)
            updates.extend(("name = %s", "name_key = %s"))
            params.extend((name, workspace_name_key(name)))

        if description is not None:
            updates.append("description = %s")
            params.append(description)

        if config is not None:
            # A staged row's planned landings are the folder settle's record,
            # so a replaced config keeps the row's and never takes the caller's.
            updates.append(
                "config = (%s::jsonb - 'folder_landings') || CASE"
                " WHEN config ? 'folder_landings'"
                " THEN jsonb_build_object('folder_landings', config->'folder_landings')"
                " ELSE '{}'::jsonb END"
            )
            params.append(Json(config))

        if is_pinned is not None:
            updates.append("is_pinned = %s")
            params.append(is_pinned)

        if not updates:
            return await get_workspace(workspace_id, conn=conn)

        updates.append("updated_at = %s")
        params.append(datetime.now(timezone.utc))
        params.append(workspace_id)
        where = ""
        if name is not None:
            # A row staged mid-move keeps its name until the move is recorded:
            # its last landing went to that name's folder, and only the name
            # keeps another workspace from being given that folder meanwhile.
            where = " AND (name = %s OR dir_name IS DISTINCT FROM %s)"
            params.extend((name, moving_path(workspace_id)))

        update_clause = ", ".join(updates)

        try:
            async with _ws_cursor(conn) as cur:
                await cur.execute(
                    f"""
                    UPDATE workspaces
                    SET {update_clause}
                    WHERE workspace_id = %s AND status != 'deleted'{where}
                    RETURNING {_WS_COLS}
                    """,
                    params,
                )
                result = await cur.fetchone()
        except UniqueViolation as e:
            if name is None or _violated_index(e) != _USER_NAME_INDEX:
                raise
            async with _ws_cursor() as owner_cur:
                await owner_cur.execute(
                    "SELECT user_id FROM workspaces WHERE workspace_id = %s",
                    (workspace_id,),
                )
                owner = await owner_cur.fetchone()
            if owner is None:
                # The row went while the rename failed: not found, like any missing row.
                return None
            raise await _name_taken(owner["user_id"], name) from e

        if result:
            logger.debug(f"Updated workspace: {workspace_id}")
            return dict(result)
        if name is not None:
            current = await get_workspace(workspace_id, conn=conn)
            if current and current.get("dir_name") == moving_path(workspace_id):
                raise WorkspaceFolderMoving(workspace_id)
        return None

    except (WorkspaceNameInvalid, WorkspaceNameTaken, WorkspaceFolderMoving):
        raise
    except Exception as e:
        logger.error(f"Error updating workspace {workspace_id}: {e}")
        raise


async def update_workspace_status(
    workspace_id: str,
    status: str,
    *,
    expected_computer_always_on: Optional[bool] = None,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Only the provider-ref CAS may change the durable sandbox binding.

    Deleted rows retain sandbox_id; the tombstone guard prevents a racing reaper
    from reviving and handing out that sandbox.
    """
    try:
        now = datetime.now(timezone.utc)

        stopped_at_clause = ", stopped_at = %(now)s" if status == "stopped" else ""
        if _mirrors_to_computer(status):
            # As in update_computer_status: running ends any host outage.
            running_clause = (
                ", host_unavailable_since = NULL" if status == "running" else ""
            )
            always_on_guard = (
                "\n                  AND c.is_always_on = %(expected_always_on)s"
                if expected_computer_always_on is not None
                else ""
            )
            shadow_guard = (
                "\n                  AND (SELECT count(*) FROM comp) > 0"
                if expected_computer_always_on is not None
                else ""
            )
            query = shadowed_write(
                authority="workspace",
                computer_set=f"status = %(status)s{stopped_at_clause}{running_clause}",
                workspace_set=f"status = %(status)s{stopped_at_clause}",
                workspace_fence=FENCE_NOT_DELETED,
                computer_guard=always_on_guard,
                workspace_guard=shadow_guard,
                computer_returning="c.computer_id AS mirrored_computer_id",
                workspace_returning=_WS_COLS,
                select="SELECT * FROM shadow WHERE workspace_id = %(workspace_id)s",
                fan_out=True,
                now="%(now)s",
            )
        else:
            query = f"""
                UPDATE workspaces
                SET status = %(status)s, updated_at = %(now)s{stopped_at_clause}
                WHERE workspace_id = %(workspace_id)s AND status != 'deleted'
                RETURNING {_WS_COLS}
            """
        params = {
            "status": status,
            "now": now,
            "workspace_id": workspace_id,
            "expected_always_on": expected_computer_always_on,
        }

        async with _ws_cursor(conn) as cur:
            await cur.execute(query, params)
            result = await cur.fetchone()

        if result:
            logger.debug(f"Updated workspace {workspace_id} status to: {status}")
            # TODO(layering): database calls service pub/sub to wake cross-worker start
            # waiters and /events subscribers; failure falls back to polling.
            await publish_status_change(
                workspace_id, status, computer_id=result.get("computer_id")
            )
            return dict(result)
        return None

    except Exception as e:
        logger.error(f"Error updating workspace {workspace_id} status: {e}")
        raise


class SandboxIdentityLostError(SandboxTransientError):
    """The losing provisioner must delete its sandbox and attach to the winner, never retry the write.

    Inherit SandboxTransientError so the chat funnel treats the race as recoverable;
    a bare RuntimeError becomes an unrecognized 500.
    """

    def __init__(self, workspace_id: str, sandbox_id: str):
        self.workspace_id = workspace_id
        self.sandbox_id = sandbox_id
        super().__init__(
            f"Workspace {workspace_id} was bound to a different sandbox while "
            f"{sandbox_id} was being provisioned"
        )


ANY_SANDBOX: Any = object()
"""Skip the identity guard on the completeness flag, for a runtime with no id."""


async def set_files_restore_incomplete(
    workspace_id: str,
    incomplete: bool,
    *,
    conn=None,
    sandbox_id: Any = ANY_SANDBOX,
) -> bool:
    """Fence restore bookkeeping by sandbox identity, including None, across provisional binds.

    A raise names the CAS replacement target; a clear names the certified sandbox,
    so neither changes a row another provisioner bound. It leaves updated_at
    alone: a bump would reshuffle the gallery after a failed restore.
    """
    guarded = sandbox_id is not ANY_SANDBOX
    guard = "AND sandbox_id IS NOT DISTINCT FROM %s" if guarded else ""
    params: tuple = (datetime.now(timezone.utc) if incomplete else None, workspace_id)
    if guarded:
        params += (sandbox_id,)
    async with _ws_cursor(conn) as cur:
        await cur.execute(
            f"""
            UPDATE workspaces
            SET files_restore_incomplete_at = %s
            WHERE workspace_id = %s {guard}
            """,
            params,
        )
        return cur.rowcount > 0


async def flag_sibling_restores_pending(
    computer_id: str,
    *,
    except_workspace_id: str,
    expected_provider_ref: Optional[str],
) -> int:
    """Raise the completeness flag on every other project of a machine being recreated.

    A recreate restores only the project that asked for it; the rest rejoin
    lazily, and until then an empty or stray folder of theirs on the new
    sandbox reads to a sync as the user having deleted everything. Fenced by
    the machine's ref, which is the identity the recreate replaces, so a
    provisioner that already lost the race flags nothing.
    """
    async with _ws_cursor() as cur:
        await cur.execute(
            """
            UPDATE workspaces w
            SET files_restore_incomplete_at = %s
            WHERE w.computer_id = %s
              AND w.workspace_id <> %s
              AND w.status <> 'deleted'
              AND w.files_restore_incomplete_at IS NULL
              AND EXISTS (
                  SELECT 1 FROM computers c
                  WHERE c.computer_id = w.computer_id
                    AND c.provider_ref IS NOT DISTINCT FROM %s
              )
            """,
            (
                datetime.now(timezone.utc),
                normalize_uuid(computer_id),
                normalize_uuid(except_workspace_id),
                expected_provider_ref,
            ),
        )
        return cur.rowcount


async def files_restore_incomplete(workspace_id: str, *, conn=None) -> bool:
    """Propagate read failures: defaulting to complete could authorize destructive work."""
    async with _ws_cursor(conn) as cur:
        await cur.execute(
            "SELECT files_restore_incomplete_at FROM workspaces "
            "WHERE workspace_id = %s",
            (workspace_id,),
        )
        row = await cur.fetchone()
    return bool(row and row["files_restore_incomplete_at"] is not None)


async def workspace_owner(workspace_id: str, conn=None) -> str:
    """A missing owner must raise or storage keys could place bytes in an unowned namespace."""
    async with _ws_cursor(conn) as cur:
        await cur.execute(
            "SELECT user_id FROM workspaces WHERE workspace_id = %s",
            (workspace_id,),
        )
        row = await cur.fetchone()
    if not row:
        raise LookupError(f"Workspace {workspace_id} does not exist")
    return row["user_id"]


async def update_workspace_activity(
    workspace_id: str,
    conn=None,
) -> bool:
    """Independent SQL cooldowns avoid per-message writes while remaining worker-safe.

    The computer needs its own 60-second predicate so another project can keep the
    machine alive during this workspace's cooldown.
    """
    try:
        now = datetime.now(timezone.utc)

        async with _ws_cursor(conn) as cur:
            await cur.execute(
                shadowed_write(
                    authority="workspace",
                    computer_set="last_activity_at = %(now)s",
                    workspace_set="last_activity_at = %(now)s",
                    workspace_fence=FENCE_NOT_DELETED,
                    computer_guard=(
                        "\n                  AND (c.last_activity_at IS NULL"
                        "\n                       OR c.last_activity_at"
                        "\n                          < %(now)s - INTERVAL '60 seconds')"
                    ),
                    workspace_guard=(
                        "\n                  AND (w.last_activity_at IS NULL"
                        "\n                       OR w.last_activity_at"
                        "\n                          < %(now)s - INTERVAL '60 seconds')"
                    ),
                    computer_returning="c.computer_id",
                    workspace_returning="w.workspace_id",
                    select="SELECT count(*) AS stamped FROM shadow",
                    now="%(now)s",
                ),
                {"now": now, "workspace_id": workspace_id},
            )
            row = await cur.fetchone()
            return bool((row or {}).get("stamped"))

    except Exception as e:
        logger.error(f"Error updating workspace {workspace_id} activity: {e}")
        raise


class WorkspaceBusyError(RuntimeError):
    """Deletion must wait for the workspace's root and background runs."""


async def lock_run_workspace(conn, thread_id: str) -> None:
    """Serialize admission with deletion across workers until START commits."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT w.status FROM workspaces w
            JOIN conversation_threads t ON t.workspace_id = w.workspace_id
            WHERE t.conversation_thread_id = %s
            FOR SHARE OF w
            """,
            (thread_id,),
        )
        workspace = await cur.fetchone()
        if workspace and workspace["status"] == "deleted":
            raise ValueError("Cannot start work in a deleted workspace")


async def delete_workspace(
    workspace_id: str,
    conn=None,
) -> bool:
    """Project deletion must leave the shared computer alone.

    Sandbox teardown owns update_computer_status(computer_id, 'deleted'); until
    then the binding remains, and FENCE_NOT_DELETED prevents reuse after tombstoning.
    The tombstone is the only deletion: a row removed outright takes the layout
    owner's identity with it, and ON DELETE SET NULL leaves the folder unowned.
    """
    from src.server.database.egress_grants import (
        lock_user_egress_state,
        retire_workspace_grants,
    )

    try:
        async with get_db_connection(conn) as owned, owned.transaction():
            async with owned.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT user_id FROM workspaces WHERE workspace_id = %s",
                    (workspace_id,),
                )
                owner = await cur.fetchone()
                if owner is None:
                    return False
                # Same lock order as grant sync: owner first, workspace second.
                await lock_user_egress_state(cur, owner["user_id"])
                await cur.execute(
                    "SELECT workspace_id FROM workspaces WHERE workspace_id = %s FOR UPDATE",
                    (workspace_id,),
                )
                if await workspace_has_live_runs(cur, workspace_id):
                    raise WorkspaceBusyError(
                        "This workspace has active work. Stop it or wait for it to finish before deleting."
                    )
                await cur.execute(
                    """
                    UPDATE workspaces
                    SET status = 'deleted',
                        config = CASE
                            WHEN computer_id IS NOT NULL AND dir_name IS NOT NULL
                            THEN jsonb_set(
                                COALESCE(config, '{}'::jsonb),
                                '{folder_cleanup_pending}',
                                'true'::jsonb
                            )
                            ELSE config
                        END,
                        updated_at = %s
                    WHERE workspace_id = %s AND status <> 'deleted'
                    RETURNING workspace_id, computer_id
                    """,
                    (datetime.now(timezone.utc), workspace_id),
                )
                result = await cur.fetchone()
                if result:
                    await retire_workspace_grants(
                        cur, workspace_id, result.get("computer_id")
                    )
                    # No worker may serve the folder as linked once the
                    # tombstone is visible: a settle clears it.
                    await drop_workspace_links(cur, workspace_id)

        if result:
            logger.info(f"Deleted workspace: {workspace_id}")
            # Invalidate sibling workers' cached handles and notify /events of deletion.
            await publish_status_change(
                workspace_id, "deleted", computer_id=result.get("computer_id")
            )
            return True
        return False

    except Exception as e:
        logger.error(f"Error deleting workspace {workspace_id}: {e}")
        raise


async def get_pending_workspace_folder_cleanups(
    *,
    computer_id: str | None = None,
    limit: int = 1000,
    conn=None,
) -> List[Dict[str, Any]]:
    """Return deleted projects whose sandbox folder still needs removal."""
    computer_filter = "AND w.computer_id = %s" if computer_id is not None else ""
    params: list[Any] = []
    if computer_id is not None:
        params.append(computer_id)
    params.append(limit)
    async with _ws_cursor(conn) as cur:
        await cur.execute(
            f"""
            SELECT {_ws_cols("w")}
            FROM workspaces w
            JOIN computers c ON c.computer_id = w.computer_id
            WHERE w.status = 'deleted'
              AND w.dir_name IS NOT NULL
              AND c.status = 'running'
              AND c.provider_ref IS NOT NULL
              AND COALESCE(w.config, '{{}}'::jsonb)
                    @> '{{"folder_cleanup_pending": true}}'::jsonb
              {computer_filter}
            ORDER BY w.updated_at ASC, w.workspace_id
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in await cur.fetchall()]


async def complete_workspace_folder_cleanup(
    workspace_id: str,
    *,
    computer_id: str,
    dir_name: str,
    conn=None,
) -> bool:
    """Clear a cleanup claim only for the deleted workspace it describes.

    Clearing ``dir_name`` hands the folder name back: until now the tombstone
    held it, because the folder was still on disk.
    """
    async with _ws_cursor(conn) as cur:
        await cur.execute(
            """
            UPDATE workspaces
            SET config = COALESCE(config, '{}'::jsonb)
                    #- '{folder_cleanup_pending}',
                dir_name = NULL,
                updated_at = NOW()
            WHERE workspace_id = %s
              AND status = 'deleted'
              AND computer_id = %s
              AND dir_name = %s
              AND COALESCE(config, '{}'::jsonb)
                    @> '{"folder_cleanup_pending": true}'::jsonb
            """,
            (workspace_id, computer_id, dir_name),
        )
        return cur.rowcount > 0


async def defer_workspace_folder_cleanup(
    workspace_id: str,
    *,
    computer_id: str,
    dir_name: str,
    conn=None,
) -> bool:
    """Move a failed cleanup behind newer work without losing its claim."""
    async with _ws_cursor(conn) as cur:
        await cur.execute(
            """
            UPDATE workspaces
            SET updated_at = NOW()
            WHERE workspace_id = %s
              AND status = 'deleted'
              AND computer_id = %s
              AND dir_name = %s
              AND COALESCE(config, '{}'::jsonb)
                    @> '{"folder_cleanup_pending": true}'::jsonb
            """,
            (workspace_id, computer_id, dir_name),
        )
        return cur.rowcount > 0


async def batch_update_sort_order(
    user_id: str,
    items: List[Tuple[str, int]],
    conn=None,
) -> None:
    if not items:
        return

    try:
        values_parts = []
        params: list = []
        for ws_id, order in items:
            values_parts.append("(%s, %s)")
            params.extend([ws_id, order])
        values_sql = ", ".join(values_parts)
        params.append(user_id)

        async with _ws_cursor(conn) as cur:
            await cur.execute(
                f"""
                UPDATE workspaces w
                SET sort_order = v.new_order, updated_at = NOW()
                FROM (VALUES {values_sql}) AS v(wid, new_order)
                WHERE w.workspace_id = v.wid::uuid AND w.user_id = %s
                """,
                params,
            )
            updated = cur.rowcount

        if updated == 0:
            logger.warning(
                f"batch_update_sort_order: 0/{len(items)} rows updated for user {user_id}"
            )
        else:
            logger.info(
                f"Batch-updated sort_order for {updated}/{len(items)} workspaces (user {user_id})"
            )

    except Exception as e:
        logger.error(f"Error batch-updating sort_order for user {user_id}: {e}")
        raise


async def get_running_workspace_ids_for_user(user_id: str) -> List[str]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT workspace_id FROM workspaces "
                "WHERE user_id = %s AND status = 'running'",
                (user_id,),
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def get_workspaces_by_status(
    status: str,
    limit: int = 100,
    conn=None,
) -> List[Dict[str, Any]]:
    try:
        async with _ws_cursor(conn) as cur:
            await cur.execute(
                f"""
                SELECT {_WS_COLS}
                FROM workspaces
                WHERE status = %s
                ORDER BY last_activity_at ASC NULLS FIRST
                LIMIT %s
                """,
                (status, limit),
            )
            results = await cur.fetchall()
        return [dict(r) for r in results]

    except Exception as e:
        logger.error(f"Error getting workspaces by status {status}: {e}")
        raise


_PREVIEW_JSONB = """artifacts = jsonb_set(
                        COALESCE({alias}.artifacts, '{{}}'::jsonb),
                        '{{preview_servers}}',
                        COALESCE({alias}.artifacts->'preview_servers', '{{}}'::jsonb)
                            || jsonb_build_object(%(port)s::text, %(cmd)s::text),
                        true
                    )"""


async def save_preview_command(workspace_id: str, port: int, command: str) -> None:
    """Ports belong to the computer; retain workspace copies for old readers."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    shadowed_write(
                        authority="workspace",
                        computer_set=_PREVIEW_JSONB.format(alias="c"),
                        workspace_set=_PREVIEW_JSONB.format(alias="w"),
                        computer_fence="",
                        workspace_fence="",
                        computer_returning="c.computer_id",
                        workspace_returning="w.workspace_id",
                        select="SELECT count(*) AS stamped FROM shadow",
                    ),
                    {
                        "port": str(port),
                        "cmd": command,
                        "workspace_id": workspace_id,
                    },
                )
    except Exception:
        logger.debug("Failed to persist preview command", exc_info=True)


async def get_preview_command(workspace_id: str, port: int) -> Optional[str]:
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT artifacts->'preview_servers'->>%s FROM workspaces WHERE workspace_id = %s",
                    (str(port), workspace_id),
                )
                row = await cur.fetchone()
                return row[0] if row else None
    except Exception:
        logger.debug("Failed to read preview command", exc_info=True)
        return None
