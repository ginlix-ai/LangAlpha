"""
Database utility functions for workspace management.

Provides functions for creating, retrieving, and managing workspaces in PostgreSQL.
Each workspace has a 1:1 mapping with a Daytona sandbox.
"""

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from psycopg.rows import dict_row

from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.server.database.pool import get_db_connection
from src.server.services.workspace_status_pubsub import publish_status_change
from src.server.utils.pg_sanitize import normalize_uuid

logger = logging.getLogger(__name__)

# Deterministic namespace for flash workspace UUIDs
FLASH_WORKSPACE_NAMESPACE = uuid.UUID("f1a50000-0000-5000-e000-f1a500000000")

# Canonical column list returned by EVERY workspace SELECT/RETURNING query, so
# callers never have to ask which shape a given helper hands back. Hardcoded
# literals only (no user data) — safe to interpolate via f-string. Rows are
# consumed by name via dict_row.
_WS_COLS = (
    "workspace_id, user_id, name, description, sandbox_id, status, created_at, "
    "updated_at, last_activity_at, stopped_at, config, artifacts, is_pinned, "
    "sort_order, resource_tier, is_always_on, platform_secret_version, "
    "mcp_config_version"
)

# Scalar workspace columns that may be set via `_set_workspace_scalar`. The
# column name is interpolated as a SQL literal (never a bound param), so it must
# be whitelisted to keep the surface injection-free.
_SETTABLE_SCALAR_COLUMNS = frozenset({"resource_tier", "is_always_on"})


@asynccontextmanager
async def _ws_cursor(conn=None):
    """A ``dict_row`` cursor on *conn*, or on a freshly checked-out pooled one.

    Every query here takes an optional connection so callers can compose several
    writes into one transaction. ``get_db_connection`` owns that passthrough;
    this only spares each query from writing out the cursor nesting and hoisting
    its body into a closure to share it.
    """
    async with get_db_connection(conn) as owned:
        async with owned.cursor(row_factory=dict_row) as cur:
            yield cur


def get_flash_workspace_id(user_id: str) -> str:
    """Deterministic UUID v5 — same user always gets the same flash workspace ID."""
    return str(uuid.uuid5(FLASH_WORKSPACE_NAMESPACE, user_id))


async def get_or_create_flash_workspace(
    user_id: str, conn=None
) -> Dict[str, Any]:
    """
    Upsert the user's shared flash workspace. No lookup needed — ID is computed.

    Uses deterministic UUID v5 so the same user always maps to the same workspace.
    INSERT ... ON CONFLICT DO UPDATE makes this idempotent and race-condition-free.
    """
    from psycopg.types.json import Json

    workspace_id = get_flash_workspace_id(user_id)
    config_json = Json({"flash_mode": True})

    try:
        async with _ws_cursor(conn) as cur:
            await cur.execute(
                f"""
                INSERT INTO workspaces (workspace_id, user_id, name, description, config, status, is_pinned)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT (workspace_id) DO UPDATE SET updated_at = NOW(), is_pinned = TRUE
                RETURNING {_WS_COLS}
                """,
                (workspace_id, user_id, "Flash", "Flash mode conversations", config_json, "flash"),
            )
            result = await cur.fetchone()

        logger.info(f"Upserted flash workspace: {workspace_id} for user: {user_id}")
        return dict(result)

    except Exception as e:
        logger.error(f"Error upserting flash workspace for user {user_id}: {e}")
        raise


# =============================================================================
# Workspace CRUD Operations
# =============================================================================


async def create_workspace(
    user_id: str,
    name: str,
    description: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    conn=None,
    workspace_id: Optional[str] = None,
    status: str = "creating",
) -> Dict[str, Any]:
    """
    Create a new workspace entry.

    Args:
        user_id: User ID who owns the workspace
        name: Workspace name
        description: Optional workspace description
        config: Optional configuration as JSON
        conn: Optional database connection to reuse
        workspace_id: Optional specific workspace ID (UUID). If None, auto-generated.
        status: Initial status (default: "creating", use "flash" for flash workspaces)

    Returns:
        Created workspace record as dict
    """
    from psycopg.types.json import Json

    try:
        config_json = Json(config) if config else Json({})

        async with _ws_cursor(conn) as cur:
            if workspace_id:
                # Use specific workspace_id (for flash mode: workspace_id = thread_id)
                await cur.execute(
                    f"""
                    INSERT INTO workspaces (workspace_id, user_id, name, description, config, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING {_WS_COLS}
                    """,
                    (workspace_id, user_id, name, description, config_json, status),
                )
            else:
                # Auto-generate workspace_id
                await cur.execute(
                    f"""
                    INSERT INTO workspaces (user_id, name, description, config, status)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING {_WS_COLS}
                    """,
                    (user_id, name, description, config_json, status),
                )
            result = await cur.fetchone()

        logger.info(f"Created workspace: {result['workspace_id']} for user: {user_id}")
        return dict(result)

    except Exception as e:
        logger.error(f"Error creating workspace for user {user_id}: {e}")
        raise


async def get_workspace(
    workspace_id: str,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """
    Get a workspace by ID.

    Args:
        workspace_id: Workspace UUID
        conn: Optional database connection to reuse

    Returns:
        Workspace record as dict, or None if not found
    """
    # Normalize before querying: postgres' uuid type rejects some forms that
    # uuid.UUID() accepts (e.g. urn:uuid:...), so binding the raw value risks
    # InvalidTextRepresentation (22P02) → 500. A non-UUID id (e.g. a memory-file
    # key from the SPA tree) can never match the pk, so treat it as not-found.
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None

    try:
        async with _ws_cursor(conn) as cur:
            await cur.execute(
                f"""
                SELECT {_WS_COLS}
                FROM workspaces
                WHERE workspace_id = %s AND status != 'deleted'
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


async def get_workspace_identity(workspace_id: str) -> Optional[Dict[str, Any]]:
    """Read only ``status`` + ``sandbox_id`` — the durable identity of a workspace.

    Deliberately narrow so the session-acquisition warm path can validate the
    handle it is about to hand out on every request without pulling the JSONB
    ``config``/``artifacts`` columns that make ``get_workspace`` expensive.
    Unlike ``get_workspace`` this does NOT hide ``status='deleted'`` rows — a
    caller validating a cached session needs to see the tombstone rather than an
    ambiguous "not found".
    """
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None

    try:
        async with _ws_cursor() as cur:
            await cur.execute(
                """
                SELECT status, sandbox_id
                FROM workspaces
                WHERE workspace_id = %s
                """,
                (workspace_id,),
            )
            result = await cur.fetchone()
        return dict(result) if result else None

    except Exception as e:
        logger.error(f"Error reading identity for workspace {workspace_id}: {e}")
        raise


async def get_workspace_name_and_description(workspace_id: str) -> Optional[Dict[str, Any]]:
    """Read only ``name`` + ``description`` — what the agent's prompt calls it.

    Narrow for the same reason as ``get_workspace_identity``: this runs once per
    agent turn, and ``get_workspace`` would pull the JSONB ``config``/``artifacts``
    columns to hand back two short strings.
    """
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
    limit: int = 20,
    offset: int = 0,
    include_deleted: bool = False,
    sort_by: str = "custom",
    include_flash: bool = False,
    conn=None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get all workspaces for a user with pagination.

    Args:
        user_id: User ID
        limit: Maximum number of results
        offset: Number of results to skip
        include_deleted: Whether to include deleted workspaces
        sort_by: Sort mode
        include_flash: Whether to include flash workspaces in results
        conn: Optional database connection to reuse

    Returns:
        Tuple of (list of workspace dicts, total count)
    """
    try:
        status_filter = "" if include_deleted else "AND status != 'deleted'"
        # Exclude flash workspaces from gallery listings unless explicitly requested
        flash_filter = "" if include_flash else "AND status != 'flash'"

        # Build ORDER BY based on sort mode
        if sort_by == "activity":
            order_clause = "is_pinned DESC, COALESCE(last_activity_at, updated_at) DESC"
        elif sort_by == "name":
            order_clause = "is_pinned DESC, name ASC"
        else:
            # 'custom' — manual sort order, then recency
            order_clause = "is_pinned DESC, sort_order ASC, updated_at DESC"

        async with _ws_cursor(conn) as cur:
            # Get total count
            await cur.execute(
                f"""
                SELECT COUNT(*) as total
                FROM workspaces
                WHERE user_id = %s {status_filter} {flash_filter}
                """,
                (user_id,),
            )
            count_result = await cur.fetchone()
            total = count_result["total"] if count_result else 0

            # Get paginated results
            await cur.execute(
                f"""
                SELECT {_WS_COLS}
                FROM workspaces
                WHERE user_id = %s {status_filter} {flash_filter}
                ORDER BY {order_clause}
                LIMIT %s OFFSET %s
                """,
                (user_id, limit, offset),
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
    """
    Update workspace metadata.

    Args:
        workspace_id: Workspace UUID
        name: Optional new name
        description: Optional new description
        config: Optional new config (replaces existing)
        conn: Optional database connection to reuse

    Returns:
        Updated workspace record, or None if not found
    """
    from psycopg.types.json import Json

    try:
        # Build dynamic update query
        updates = []
        params = []

        if name is not None:
            updates.append("name = %s")
            params.append(name)

        if description is not None:
            updates.append("description = %s")
            params.append(description)

        if config is not None:
            updates.append("config = %s")
            params.append(Json(config))

        if is_pinned is not None:
            updates.append("is_pinned = %s")
            params.append(is_pinned)

        if not updates:
            # Nothing to update, just return current state
            return await get_workspace(workspace_id, conn=conn)

        updates.append("updated_at = %s")
        params.append(datetime.now(timezone.utc))
        params.append(workspace_id)

        update_clause = ", ".join(updates)

        async with _ws_cursor(conn) as cur:
            await cur.execute(
                f"""
                UPDATE workspaces
                SET {update_clause}
                WHERE workspace_id = %s AND status != 'deleted'
                RETURNING {_WS_COLS}
                """,
                params,
            )
            result = await cur.fetchone()

        if result:
            logger.debug(f"Updated workspace: {workspace_id}")
            return dict(result)
        return None

    except Exception as e:
        logger.error(f"Error updating workspace {workspace_id}: {e}")
        raise


async def update_workspace_status(
    workspace_id: str,
    status: str,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """
    Move a workspace's status.

    Never touches ``sandbox_id``: the durable workspace↔sandbox binding moves
    only through ``try_bind_workspace_sandbox``'s compare-and-set, so a plain
    status change can never silently rebind a workspace to a stale sandbox.

    The ``status != 'deleted'`` guard is what stops a soft-deleted row from
    being revived. Nothing in the codebase ever clears ``sandbox_id``, so a
    deleted workspace still names a real sandbox; without the guard a racing
    reaper flipping it back to 'running' would hand that sandbox out again.

    Args:
        workspace_id: Workspace UUID
        status: New status (creating, running, stopping, stopped, error, deleted)
        conn: Optional database connection to reuse

    Returns:
        Updated workspace record, or None if not found or already deleted
    """
    try:
        now = datetime.now(timezone.utc)

        # 'stopped' additionally stamps stopped_at; every other status is a
        # plain status move.
        stopped_at_clause = ", stopped_at = %s" if status == "stopped" else ""
        query = f"""
            UPDATE workspaces
            SET status = %s, updated_at = %s{stopped_at_clause}
            WHERE workspace_id = %s AND status != 'deleted'
            RETURNING {_WS_COLS}
        """
        params = (
            (status, now, now, workspace_id)
            if status == "stopped"
            else (status, now, workspace_id)
        )

        async with _ws_cursor(conn) as cur:
            await cur.execute(query, params)
            result = await cur.fetchone()

        if result:
            logger.debug(f"Updated workspace {workspace_id} status to: {status}")
            # TODO(layering): services-tier pub/sub called from the database
            # tier. Best-effort cross-worker notification — wakes any
            # _wait_for_start_completion loop and any /events SSE
            # subscribers in milliseconds. Swallows on failure.
            await publish_status_change(workspace_id, status)
            return dict(result)
        return None

    except Exception as e:
        logger.error(f"Error updating workspace {workspace_id} status: {e}")
        raise


class SandboxIdentityLostError(SandboxTransientError):
    """Another provisioner won the race to bind this workspace's sandbox.

    The loser owns a real, running sandbox that nothing points at — it must
    delete its own and attach to the winner's, never retry the write.

    Transient by inheritance, and deliberately so: losing this race is a
    recoverable condition the caller should retry into, but no caller catches it
    by name. As a bare ``RuntimeError`` it reached the client as a 500 and the
    chat funnel did not recognise it as a sandbox condition at all.
    """

    def __init__(self, workspace_id: str, sandbox_id: str):
        self.workspace_id = workspace_id
        self.sandbox_id = sandbox_id
        super().__init__(
            f"Workspace {workspace_id} was bound to a different sandbox while "
            f"{sandbox_id} was being provisioned"
        )


async def _try_claim_starting(
    workspace_id: str,
    *,
    from_status: str,
    expected_sandbox_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Cross-worker mutex for a ``<from_status>`` → ``starting`` transition.

    Only the worker whose UPDATE returns a row owns the transition; losers wait
    for it to reach 'running' (or 'error'). Owns its own connection so the
    UPDATE commits before the publish — a caller-supplied transaction would let
    ``publish_status_change`` wake SSE subscribers that then read the pre-claim
    row.

    ``expected_sandbox_id`` adds a compare-and-set so only the caller holding
    the identity it intends to replace can claim it.
    """
    try:
        now = datetime.now(timezone.utc)
        sandbox_guard = " AND sandbox_id = %s" if expected_sandbox_id else ""
        query = f"""
            UPDATE workspaces
            SET status = 'starting', updated_at = %s
            WHERE workspace_id = %s AND status = %s{sandbox_guard}
            RETURNING {_WS_COLS}
        """
        params: Tuple[Any, ...] = (now, workspace_id, from_status)
        if expected_sandbox_id:
            params += (expected_sandbox_id,)

        async with _ws_cursor() as cur:
            await cur.execute(query, params)
            result = await cur.fetchone()

        if result is None:
            return None

        logger.debug(
            f"Claimed workspace {workspace_id} for start (was {from_status}"
            + (f" on {expected_sandbox_id}" if expected_sandbox_id else "")
            + ")"
        )
        # Cross-worker /events subscribers learn about the flip without
        # polling. The WorkspaceManager wait loop doesn't need it (the loser
        # path triggers only after the claim returns None), but the FE does.
        await publish_status_change(workspace_id, "starting")
        return dict(result)

    except Exception as e:
        logger.error(f"Error claiming workspace {workspace_id} for start: {e}")
        raise


async def try_claim_workspace_for_start(
    workspace_id: str,
) -> Optional[Dict[str, Any]]:
    """Claim a stopped workspace for a start transition.

    Returns the claimed row (status='starting'), or None when the workspace was
    not 'stopped' — already starting, running, creating, error, deleted or
    stopping.
    """
    return await _try_claim_starting(workspace_id, from_status="stopped")


async def try_claim_workspace_for_replacement(
    workspace_id: str,
    expected_sandbox_id: str,
) -> Optional[Dict[str, Any]]:
    """Claim a running workspace whose sandbox is about to be replaced.

    Replacement (a tier change, a working-dir migration) deletes the sandbox and
    builds a new one. Without a durable claim the row keeps saying
    ``running``/<old id> across that window, so DB and cache *agree* on an
    identity that is being destroyed — the one state no identity check can catch.
    Claiming ``starting`` closes it with machinery that already exists: acquirers
    seeing ``starting`` wait for the owner instead of racing to provision a
    duplicate, and the stuck-start reaper is the existing backstop.

    Returns the claimed row, or None if another worker already moved the
    workspace off ``running``/*expected_sandbox_id*.
    """
    return await _try_claim_starting(
        workspace_id, from_status="running", expected_sandbox_id=expected_sandbox_id
    )


async def try_bind_workspace_sandbox(
    workspace_id: str,
    *,
    sandbox_id: str,
    expected_previous_sandbox_id: Optional[str],
    platform_secret_version: int,
) -> Optional[Dict[str, Any]]:
    """Atomically bind a freshly provisioned sandbox to a workspace and mark it running.

    The only writer of ``workspaces.sandbox_id``. The compare-and-set is what
    makes concurrent provisioning safe: without it two workers both "succeed",
    one sandbox is left running with nothing pointing at it, and workers briefly
    disagree about which identity is current. Returns None when another writer
    got there first — the loser owns a real sandbox it must now delete.

    ``platform_secret_version`` is always written, never preserved: it records
    what THIS sandbox was certified against, and 0 means "never certified — may
    hold plaintext env" (migration 021). Carrying a previous sandbox's
    generation forward would leave a plaintext sandbox looking certified, and
    the sweeper only visits rows behind the fleet generation.

    The predicate fences lifecycle as well as identity. Matching on the sandbox
    id alone is not enough: a stop and a recovery can race on the SAME id, and
    since this statement unconditionally writes ``running`` the bind would
    resurrect a workspace another worker had just stopped — leaving the row
    ``stopped`` while its sandbox runs on and bills, or the reverse. Stopping
    and stopped are named rather than a whitelist of bindable states because a
    losing bind destroys its own sandbox: a whitelist that omitted some future
    transitional state would fail legitimate provisions and churn sandboxes,
    where this only ever refuses the two states a bind must never overwrite.
    """
    query = f"""
        UPDATE workspaces
        SET status = 'running',
            sandbox_id = %s,
            platform_secret_version = %s,
            updated_at = NOW()
        WHERE workspace_id = %s
          AND status NOT IN ('deleted', 'stopping', 'stopped')
          AND sandbox_id IS NOT DISTINCT FROM %s
        RETURNING {_WS_COLS}
    """

    async with _ws_cursor() as cur:
        await cur.execute(
            query,
            (
                sandbox_id,
                platform_secret_version,
                workspace_id,
                expected_previous_sandbox_id,
            ),
        )
        result = await cur.fetchone()

    if result is None:
        return None
    # Same broadcast obligation as update_workspace_status: this is what wakes
    # cross-worker start waiters and /events subscribers.
    await publish_status_change(workspace_id, "running")
    return dict(result)


async def _set_workspace_scalar(
    workspace_id: str,
    column: str,
    value: Any,
    *,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Set one whitelisted scalar column; returns updated row, or None if not found.

    `column` is interpolated as a SQL literal, so it must be in
    `_SETTABLE_SCALAR_COLUMNS`; `value` is always bound as a parameter.
    """
    if column not in _SETTABLE_SCALAR_COLUMNS:
        raise ValueError(f"Column not settable via _set_workspace_scalar: {column!r}")

    try:
        now = datetime.now(timezone.utc)

        async with _ws_cursor(conn) as cur:
            await cur.execute(
                f"""
                UPDATE workspaces
                SET {column} = %s, updated_at = %s
                WHERE workspace_id = %s AND status != 'deleted'
                RETURNING {_WS_COLS}
                """,
                (value, now, workspace_id),
            )
            result = await cur.fetchone()

        if result:
            logger.info(f"Set workspace {workspace_id} {column} to: {value}")
            return dict(result)
        return None

    except Exception as e:
        logger.error(f"Error setting workspace {workspace_id} {column}: {e}")
        raise


async def set_workspace_resource_tier(
    workspace_id: str,
    tier: str,
    *,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Set the workspace resource tier; returns updated row, or None if not found."""
    return await _set_workspace_scalar(workspace_id, "resource_tier", tier, conn=conn)


async def set_workspace_always_on(
    workspace_id: str,
    enabled: bool,
    *,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Set the workspace always-on flag; returns updated row, or None if not found."""
    return await _set_workspace_scalar(workspace_id, "is_always_on", enabled, conn=conn)


ANY_SANDBOX: Any = object()
"""Skip the identity guard on the completeness flag, for a runtime with no id."""


async def set_files_restore_incomplete(
    workspace_id: str,
    incomplete: bool,
    *,
    conn=None,
    sandbox_id: Any = ANY_SANDBOX,
) -> bool:
    """Record whether the sandbox is missing files the manifest still lists.

    ``sandbox_id`` is the sandbox the row is expected to name, ``None``
    included, and the write lands only while it does. A restore runs on a
    provisional sandbox before the identity CAS picks a winner: a raise names
    the sandbox that CAS expects to replace and a clear names the sandbox it
    vouches for, so neither can land on a row another provisioner has since
    bound. Returns whether the write landed.

    Deliberately not routed through ``_set_workspace_scalar``: that helper's
    allowlist is for workspace settings a user chooses, this is persistence
    bookkeeping, and it bumps ``updated_at`` — which orders the workspace
    gallery, so a failed restore would silently reshuffle the user's list.
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


async def files_restore_incomplete(workspace_id: str, *, conn=None) -> bool:
    """Whether a restore is known to have left files unrecovered.

    Raises on a read failure rather than defaulting — the caller gates a
    destructive operation on this answer, so it must not be able to mistake
    "could not tell" for "everything is fine".
    """
    async with _ws_cursor(conn) as cur:
        await cur.execute(
            "SELECT files_restore_incomplete_at FROM workspaces "
            "WHERE workspace_id = %s",
            (workspace_id,),
        )
        row = await cur.fetchone()
    return bool(row and row["files_restore_incomplete_at"] is not None)


async def workspace_owner(workspace_id: str, conn=None) -> str:
    """The user whose object-storage namespace holds this workspace's bytes.

    Raises when the workspace does not exist: every caller is about to build
    a storage key from the answer, and a default would put bytes under a
    namespace nobody owns.
    """
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
    """
    Update workspace last_activity_at timestamp (conditional).

    Only writes if the last update was > 60 seconds ago, avoiding a full
    UPDATE on every message.  Process-safe via SQL WHERE clause.

    Args:
        workspace_id: Workspace UUID
        conn: Optional database connection to reuse

    Returns:
        True if the row was updated, False if skipped (within cooldown)
    """
    try:
        now = datetime.now(timezone.utc)

        async with _ws_cursor(conn) as cur:
            await cur.execute(
                """
                UPDATE workspaces
                SET last_activity_at = %s, updated_at = %s
                WHERE workspace_id = %s
                  AND status != 'deleted'
                  AND (last_activity_at IS NULL
                       OR last_activity_at < %s - INTERVAL '60 seconds')
                """,
                (now, now, workspace_id, now),
            )
            return cur.rowcount > 0

    except Exception as e:
        logger.error(f"Error updating workspace {workspace_id} activity: {e}")
        raise


async def delete_workspace(
    workspace_id: str,
    hard_delete: bool = False,
    conn=None,
) -> bool:
    """
    Delete a workspace (soft delete by default).

    Args:
        workspace_id: Workspace UUID
        hard_delete: If True, permanently delete the record
        conn: Optional database connection to reuse

    Returns:
        True if deleted, False if not found
    """
    try:
        async with _ws_cursor(conn) as cur:
            if hard_delete:
                await cur.execute(
                    """
                    DELETE FROM workspaces
                    WHERE workspace_id = %s
                    RETURNING workspace_id
                    """,
                    (workspace_id,),
                )
            else:
                await cur.execute(
                    """
                    UPDATE workspaces
                    SET status = 'deleted', updated_at = %s
                    WHERE workspace_id = %s AND status != 'deleted'
                    RETURNING workspace_id
                    """,
                    (datetime.now(timezone.utc), workspace_id),
                )
            result = await cur.fetchone()

        if result:
            logger.info(
                f"{'Hard' if hard_delete else 'Soft'} deleted workspace: {workspace_id}"
            )
            # Same broadcast obligation as every other status writer. Load-bearing
            # since sessions validate against the durable status: without it a
            # sibling worker keeps its cached handle until its next request, and
            # /events subscribers never learn the workspace is gone.
            await publish_status_change(workspace_id, "deleted")
            return True
        return False

    except Exception as e:
        logger.error(f"Error deleting workspace {workspace_id}: {e}")
        raise


async def batch_update_sort_order(
    user_id: str,
    items: List[Tuple[str, int]],
    conn=None,
) -> None:
    """
    Batch-update sort_order for multiple workspaces in a single query.

    Args:
        user_id: User ID (for ownership check)
        items: List of (workspace_id, sort_order) tuples
        conn: Optional database connection to reuse
    """
    if not items:
        return

    try:
        # Build VALUES list for the update
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
            logger.warning(f"batch_update_sort_order: 0/{len(items)} rows updated for user {user_id}")
        else:
            logger.info(f"Batch-updated sort_order for {updated}/{len(items)} workspaces (user {user_id})")

    except Exception as e:
        logger.error(f"Error batch-updating sort_order for user {user_id}: {e}")
        raise


async def get_running_workspace_ids_for_user(user_id: str) -> List[str]:
    """Ids of a user's running workspaces, for user-level best-effort fan-out
    (e.g. pushing merged vault secrets to live sandboxes on mutation)."""
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
    """
    Get workspaces by status (for cleanup tasks).

    Args:
        status: Status to filter by
        limit: Maximum number of results
        conn: Optional database connection to reuse

    Returns:
        List of workspace dicts
    """
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


# ---------------------------------------------------------------------------
# Preview server command persistence
# ---------------------------------------------------------------------------


async def save_preview_command(
    workspace_id: str, port: int, command: str
) -> None:
    """Store a preview server command in ``artifacts.preview_servers``."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE workspaces
                    SET artifacts = jsonb_set(
                        COALESCE(artifacts, '{}'::jsonb),
                        '{preview_servers}',
                        COALESCE(artifacts->'preview_servers', '{}'::jsonb)
                            || jsonb_build_object(%s::text, %s::text),
                        true
                    )
                    WHERE workspace_id = %s
                    """,
                    (str(port), command, workspace_id),
                )
    except Exception:
        logger.debug("Failed to persist preview command", exc_info=True)


async def get_preview_command(
    workspace_id: str, port: int
) -> Optional[str]:
    """Read a preview server command from ``artifacts.preview_servers``."""
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
