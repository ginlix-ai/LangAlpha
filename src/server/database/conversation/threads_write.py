"""Write paths for conversation_threads: creation, status/metadata updates, deletion."""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.contracts.status import (
    RAW_TERMINAL_SNAPSHOT_STATUSES,
    TERMINAL_STATUSES,
)
from src.server.database import pool
from src.server.database.conversation import _sql, errors
from src.server.utils.pg_sanitize import normalize_uuid

logger = logging.getLogger(__name__)


# Cache key helpers — single source of truth for key format.
# Invalidation sites import these instead of hardcoding the prefix.
_EXISTS_TTL = 86400  # 24h — freshness via explicit invalidation


def ws_exists_key(workspace_id: str) -> str:
    return f"ws_exists:{workspace_id}"


def thread_exists_key(thread_id: str) -> str:
    return f"thread_exists:{thread_id}"


async def calculate_next_thread_index(workspace_id: str, conn=None) -> int:
    """
    Calculate the next thread_index for a workspace (0-based).

    Uses MAX(thread_index) + 1 instead of COUNT(*) to correctly handle
    gaps from deleted threads and avoid unique constraint violations.

    Args:
        workspace_id: Workspace ID
        conn: Optional database connection to reuse
    """
    try:
        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT COALESCE(MAX(thread_index), -1) + 1 as next_index
                    FROM conversation_threads
                    WHERE workspace_id = %s
                """,
                    (workspace_id,),
                )
                result = await cur.fetchone()
                return result["next_index"]
        else:
            async with pool.get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(
                        """
                        SELECT COALESCE(MAX(thread_index), -1) + 1 as next_index
                        FROM conversation_threads
                        WHERE workspace_id = %s
                    """,
                        (workspace_id,),
                    )
                    result = await cur.fetchone()
                    return result["next_index"]

    except Exception as e:
        logger.error(f"Error calculating thread index: {e}")
        return 0


async def create_thread(
    conversation_thread_id: str,
    workspace_id: str,
    current_status: str,
    msg_type: Optional[str] = None,
    thread_index: Optional[int] = None,
    title: Optional[str] = None,
    external_id: Optional[str] = None,
    platform: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    conn=None,
) -> Dict[str, Any]:
    """
    Create a thread entry (thread_index auto-calculated if not provided).

    `platform` records the user surface the chat originated from. Conventions:
      - "web"                   — main ChatAgent page
      - "market_view:<SYMBOL>"  — MarketView side panel, symbol uppercased
      - "telegram" | "slack" | "discord" | "feishu" — channel integrations
      - NULL                    — no user surface (system-initiated threads)
    `metadata` is the general thread annotation column. Known keys:
      - "origin": {"type": "agent"|"automation"|"system", "id": "<initiator id>"}
        — who initiated the thread ('agent' id = dispatching flash thread,
        'automation' id = automation). Absent origin key = user-initiated.
    `external_id` is only set by channel integrations and combined with platform
    is unique-indexed for dedup. Web-originated threads have external_id NULL.

    A concurrent create that loses the `(platform, external_id)` dedup index
    raises ``errors.ExternalIdConflictError`` (routes map it to a 409 / an in-stream
    ``external_id_conflict`` SSE error); it is never silently resolved to the
    winning thread here — the channel client regenerates under a fresh external
    key. The winner already resolves upstream via ``lookup_thread_by_external_id``.
    """
    columns = [
        "conversation_thread_id",
        "workspace_id",
        "current_status",
        "msg_type",
        "thread_index",
        "title",
    ]
    base_params = [conversation_thread_id, workspace_id, current_status, msg_type]
    # thread_index is appended per-attempt (may be recalculated on retry)

    extra_params: List[Any] = []
    if platform:
        columns.append("platform")
        extra_params.append(platform)
    if external_id:
        columns.append("external_id")
        extra_params.append(external_id)
    if metadata:
        columns.append("metadata")
        extra_params.append(Json(metadata))

    col_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    returning_str = f"{col_str}, created_at, updated_at"

    sql = f"""
        INSERT INTO conversation_threads ({col_str})
        VALUES ({placeholders})
        RETURNING {returning_str}
    """

    max_retries = 3
    for attempt in range(max_retries):
        # Calculate thread_index if not provided, or recalculate on retry
        if thread_index is None or attempt > 0:
            thread_index = await calculate_next_thread_index(workspace_id, conn=conn)

        params = tuple(base_params + [thread_index, title] + extra_params)

        try:
            if conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(sql, params)
                    result = await cur.fetchone()
                    logger.info(
                        f"[conversation_db] create_thread thread_id={conversation_thread_id} thread_index={thread_index} workspace_id={workspace_id}"
                    )
                    return dict(result)
            else:
                async with pool.get_db_connection() as conn_new:
                    async with conn_new.cursor(row_factory=dict_row) as cur:
                        await cur.execute(sql, params)
                        result = await cur.fetchone()
                        return dict(result)

        except psycopg.errors.UniqueViolation as e:
            # A collision on the external-id dedup index can't be fixed by
            # retrying — (platform, external_id) is fixed for this insert, so a
            # concurrent create won the race. Surface a typed conflict (routes map
            # it to a 409 / an in-stream ``external_id_conflict`` SSE error) that
            # channel clients recover from by regenerating under a fresh external
            # key. Only the per-workspace thread_index constraint is the retryable
            # race handled below (recalc thread_index + reinsert).
            if (
                errors._unique_violation_constraint(e) == errors._EXTERNAL_ID_INDEX
                and platform
                and external_id
            ):
                raise errors.ExternalIdConflictError(
                    platform=platform, external_id=external_id
                )
            if attempt == max_retries - 1:
                logger.error(
                    f"thread_index conflict after {max_retries} attempts for workspace {workspace_id}"
                )
                raise
            logger.warning(
                f"thread_index conflict (attempt {attempt + 1}/{max_retries}), retrying for workspace {workspace_id}"
            )
            continue

        except Exception as e:
            logger.error(f"Error creating thread: {e}")
            raise


async def update_thread_external_id(
    thread_id: str, platform: str, external_id: str
) -> Optional[Dict[str, Any]]:
    """Stamp ``platform`` + ``external_id`` onto an already-created thread.

    Post-creation counterpart to passing external linkage into ``create_thread``:
    lets a client attach a channel identity to an existing thread. A blind
    ``UPDATE ... WHERE conversation_thread_id = %s`` mirroring
    ``update_thread_fields`` — ownership is enforced by the route
    (``require_thread_owner``) before this is called, or intentionally skipped for
    the privileged service backfill, so no ownership join is re-checked here.

    Re-stamping the same values is idempotent: the row already holds them, so the
    unique index sees no conflicting peer. If ANOTHER thread already holds this
    ``(platform, external_id)`` the UPDATE violates
    ``idx_conversation_threads_external``; that is caught and re-raised as
    ``errors.ExternalIdConflictError`` instead of escaping as a 500.

    Returns the updated thread dict, or ``None`` when no matching thread was found.
    """
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    UPDATE conversation_threads
                    SET platform = %s, external_id = %s, updated_at = NOW()
                    WHERE conversation_thread_id = %s
                    RETURNING conversation_thread_id, workspace_id, current_status, msg_type, thread_index, title, platform, metadata, is_shared, is_pinned, archived_at, created_at, updated_at
                """,
                    (platform, external_id, thread_id),
                )
                result = await cur.fetchone()
                if result:
                    logger.info(
                        f"[conversation_db] update_thread_external_id "
                        f"thread_id={thread_id} platform={platform} external_id={external_id}"
                    )
                    return dict(result)
                return None

    except psycopg.errors.UniqueViolation as e:
        # Another thread already holds this (platform, external_id). Surface a
        # typed conflict the router maps to 409 rather than a raw 500. Guard on
        # the index name so an unrelated unique violation still bubbles as-is.
        if errors._unique_violation_constraint(e) == errors._EXTERNAL_ID_INDEX:
            logger.info(
                f"[conversation_db] update_thread_external_id conflict "
                f"platform={platform} external_id={external_id}"
            )
            raise errors.ExternalIdConflictError(platform=platform, external_id=external_id)
        logger.error(f"Error updating thread external_id: {e}")
        raise

    except Exception as e:
        logger.error(f"Error updating thread external_id: {e}")
        raise


async def update_thread_status(
    conversation_thread_id: str,
    status: str,
    *,
    checkpoint_id: str | None = None,
    conn=None,
) -> bool:
    """
    Update thread status (completed, interrupted, error, timeout, etc.).

    Args:
        conversation_thread_id: Thread ID
        status: New status
        checkpoint_id: Optional latest checkpoint ID to store for branch tracking
        conn: Optional database connection to reuse
    """
    try:
        if checkpoint_id:
            sql = """
                UPDATE conversation_threads
                SET current_status = %s, latest_checkpoint_id = %s, updated_at = NOW()
                WHERE conversation_thread_id = %s
            """
            params = (status, checkpoint_id, conversation_thread_id)
        else:
            sql = """
                UPDATE conversation_threads
                SET current_status = %s, updated_at = NOW()
                WHERE conversation_thread_id = %s
            """
            params = (status, conversation_thread_id)

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql, params)
                logger.info(
                    f"[conversation_db] update_thread_status thread_id={conversation_thread_id} status={status}"
                )
                return True
        else:
            async with pool.get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(sql, params)
                    logger.info(
                        f"[conversation_db] update_thread_status thread_id={conversation_thread_id} status={status}"
                    )
                    return True

    except Exception as e:
        logger.error(f"Error updating thread status: {e}")
        return False


async def update_thread_checkpoint_id(
    conversation_thread_id: str, checkpoint_id: str, conn=None
) -> bool:
    """Update the latest checkpoint ID for a thread without changing status."""
    try:
        sql = """
            UPDATE conversation_threads
            SET latest_checkpoint_id = %s, updated_at = NOW()
            WHERE conversation_thread_id = %s
        """
        params = (checkpoint_id, conversation_thread_id)

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql, params)
                return True
        else:
            async with pool.get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(sql, params)
                    return True
    except Exception as e:
        logger.error(f"Error updating thread checkpoint_id: {e}")
        return False


async def advance_thread_checkpoint_id(
    conversation_thread_id: str,
    *,
    from_checkpoint_id: Optional[str],
    to_checkpoint_id: str,
) -> bool:
    """CAS tip advance for root-namespace ui appends after a turn finalized.

    Not the turn-lifecycle writer (``database/runs/lifecycle.py`` owns that):
    this only moves the tip when it still points at the exact checkpoint the
    append built on, so a concurrent turn or branch switch keeps ownership.
    """
    try:
        sql = """
            UPDATE conversation_threads
            SET latest_checkpoint_id = %s, updated_at = NOW()
            WHERE conversation_thread_id = %s
              AND latest_checkpoint_id IS NOT DISTINCT FROM %s
        """
        params = (to_checkpoint_id, conversation_thread_id, from_checkpoint_id)
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql, params)
                return cur.rowcount > 0
    except Exception as e:
        logger.error(f"Error advancing thread checkpoint_id: {e}")
        return False


async def ensure_thread_exists(
    workspace_id: str,
    conversation_thread_id: str,
    user_id: str,
    initial_query: str,
    initial_status: str = "in_progress",
    msg_type: Optional[str] = None,
    external_id: Optional[str] = None,
    platform: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Ensure conversation_threads row exists before workflow starts.

    Uses a single database connection for all operations to reduce connection churn.
    Workspace must already exist (created via POST /workspaces).

    Args:
        workspace_id: Workspace ID (must exist)
        conversation_thread_id: Thread ID to create/resume
        user_id: User ID for logging
        initial_query: Initial query text (used as thread title)
        initial_status: Initial thread status
        msg_type: Message type (e.g., 'ptc')
        external_id: Optional external thread identifier (e.g. "chat_id:topic_id")
        platform: Optional platform identifier (e.g. "telegram", "slack")
        metadata: Optional thread metadata (see create_thread; e.g. origin
            provenance). Applied only when this call creates the row —
            resume never rewrites it.

    Returns:
        True if this call created the thread row, False if it already existed.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()

    # Pre-flight cache reads — no DB connection needed on full cache hit
    ws_key = ws_exists_key(workspace_id)
    thread_key = thread_exists_key(conversation_thread_id)
    ws_cached = False
    thread_cached = False
    if cache.enabled and cache.client:
        try:
            ws_cached = (await cache.client.get(ws_key)) == b"1"
        except Exception:
            pass
        try:
            thread_cached = (await cache.client.get(thread_key)) == b"1"
        except Exception:
            pass

    # Fast path: both cached and thread exists → just update status, no pool checkout
    # needed for existence checks
    if ws_cached and thread_cached:
        await update_thread_status(
            conversation_thread_id, initial_status
        )
        logger.debug(
            f"Resumed thread {conversation_thread_id}, updated status to {initial_status}"
        )
        return False

    # Slow path: at least one cache miss — need a connection.
    #
    # Existence stamps are collected here and written after the connection is
    # released. A blocking cache pool queues instead of failing, so a Redis
    # stall inside this block would hold a Postgres connection for the whole
    # acquire timeout — turning Redis contention into Postgres exhaustion for
    # endpoints that never touch Redis. The stamps are pure cache warming and
    # have no ordering relationship to the transaction.
    stamp_exists: list[str] = []
    async with pool.get_db_connection() as conn:
        # Step 1: Verify workspace exists
        if not ws_cached:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT workspace_id FROM workspaces WHERE workspace_id = %s
                """,
                    (workspace_id,),
                )
                workspace = await cur.fetchone()

            if not workspace:
                raise ValueError(
                    f"Workspace {workspace_id} does not exist. Create it first via POST /workspaces"
                )
            stamp_exists.append(ws_key)

        # Step 2: Check if thread already exists
        thread_exists = thread_cached
        if not thread_cached:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT conversation_thread_id FROM conversation_threads WHERE conversation_thread_id = %s
                """,
                    (conversation_thread_id,),
                )
                thread_exists = await cur.fetchone()

        # Step 3: Create thread if it doesn't exist
        created = False
        if not thread_exists:
            # Use initial query as thread title (truncate to 255 chars)
            title = initial_query[:255] if initial_query else None
            await create_thread(
                conversation_thread_id=conversation_thread_id,
                workspace_id=workspace_id,
                current_status=initial_status,
                msg_type=msg_type,
                thread_index=None,  # Will be calculated inside create_thread using same conn
                title=title,
                external_id=external_id,
                platform=platform,
                metadata=metadata,
                conn=conn,
            )
            # Cache the new thread's existence
            stamp_exists.append(thread_key)
            created = True
        else:
            # Thread exists (resume scenario), update status
            await update_thread_status(
                conversation_thread_id, initial_status, conn=conn
            )
            # Cache thread existence on resume too (in case cache was cold)
            if not thread_cached:
                stamp_exists.append(thread_key)
            logger.debug(
                f"Resumed thread {conversation_thread_id}, updated status to {initial_status}"
            )

    if stamp_exists and cache.enabled and cache.client:
        for key in stamp_exists:
            try:
                await cache.client.set(key, b"1", ex=_EXISTS_TTL)
            except Exception:
                pass
    return created


async def truncate_thread_from_turn(
    conversation_thread_id: str,
    from_turn_index: int,
    preserve_query_at_fork: bool = False,
    conn=None,
) -> int:
    """Delete queries and responses at turn_index >= from_turn_index.

    Used by edit/regenerate/retry to clear stale turns before the normal
    persistence flow creates fresh records. Usages are NOT affected
    (no FK constraints after migration).

    Args:
        preserve_query_at_fork: If True, keep the query at from_turn_index
            (used by regenerate — user message unchanged, only response regenerated).
            Queries at turn_index > from_turn_index are still deleted.

    Returns:
        Total number of deleted rows (queries + responses).
    """

    async def _execute(conn):
        # Explicit transaction required (autocommit is ON by default)
        async with conn.transaction():
            async with conn.cursor() as cur:
                # Always delete all responses at fork turn and beyond
                await cur.execute(
                    """
                    DELETE FROM conversation_responses
                    WHERE conversation_thread_id = %s AND turn_index >= %s
                """,
                    (conversation_thread_id, from_turn_index),
                )
                deleted_responses = cur.rowcount

                # For regenerate: keep query at fork turn, delete only later turns
                # For edit: delete query at fork turn and beyond
                query_op = ">" if preserve_query_at_fork else ">="
                await cur.execute(
                    f"""
                    DELETE FROM conversation_queries
                    WHERE conversation_thread_id = %s AND turn_index {query_op} %s
                """,
                    (conversation_thread_id, from_turn_index),
                )
                deleted_queries = cur.rowcount

                return deleted_queries + deleted_responses

    try:
        if conn:
            return await _execute(conn)
        else:
            async with pool.get_db_connection() as conn:
                return await _execute(conn)
    except Exception as e:
        logger.error(
            f"Error truncating thread {conversation_thread_id} from turn {from_turn_index}: {e}"
        )
        raise


async def delete_thread(conversation_thread_id: str, conn=None) -> bool:
    """Delete thread (CASCADE to queries, responses).

    ``conn`` pins the delete to the caller's session — the mutation fence's
    locked connection, so the destructive statement dies with the lock."""

    async def _execute(conn) -> bool:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                DELETE FROM conversation_threads
                WHERE conversation_thread_id = %s
            """,
                (conversation_thread_id,),
            )

            logger.info(f"Deleted thread: {conversation_thread_id}")
            return True

    try:
        if conn:
            return await _execute(conn)
        async with pool.get_db_connection() as conn:
            return await _execute(conn)
    except Exception as e:
        logger.error(f"Error deleting thread: {e}")
        raise


_UNSET = object()

# Archiving marks the latest attempt seen, in the same statement as the
# archive flag. An archived row is absent from the feed snapshot, and below
# the unseen cutoff that absence is indistinguishable from truncation — so a
# committed archive with an uncommitted stamp would resurrect the very
# ambiguity the cutoff exists to bound. GREATEST ignores a NULL subquery, so
# a live-LIKE latest attempt (in_progress/interrupted, filtered out below)
# simply leaves the cursor alone: on unarchive its unseen dot legitimately
# reflects work that settled while archived.
_ARCHIVE_SEEN_STAMP = f"""last_seen_run_seq = GREATEST(
        last_seen_run_seq,
        (SELECT la.run_seq
         FROM (
             SELECT cr.run_seq, cr.status
             FROM conversation_responses cr
             WHERE cr.conversation_thread_id = %s
             ORDER BY cr.run_seq DESC
             LIMIT 1
         ) la
         WHERE la.status IN (
             {_sql.sql_literals(RAW_TERMINAL_SNAPSHOT_STATUSES)}
         ))
    )"""


async def update_thread_fields(
    conversation_thread_id: str,
    *,
    title: Any = _UNSET,
    is_pinned: Any = _UNSET,
    archived: Any = _UNSET,
) -> Optional[Dict[str, Any]]:
    """Dynamic update of user-editable thread fields (title, pin, archive).

    Only title bumps ``updated_at`` — pin/archive must not reorder the
    recency sort (same reasoning as ``stamp_thread_seen``). Archiving also
    advances the seen cursor in the SAME statement (see ``_ARCHIVE_SEEN_STAMP``).
    Returns the updated row, or None when the thread doesn't exist / nothing
    to set.
    """
    sets: List[str] = []
    params: List[Any] = []
    if title is not _UNSET:
        sets.append("title = %s")
        params.append(title)
        sets.append("updated_at = NOW()")
    if is_pinned is not _UNSET:
        sets.append("is_pinned = %s")
        params.append(bool(is_pinned))
    if archived is not _UNSET:
        if archived:
            sets.append("archived_at = NOW()")
            sets.append(_ARCHIVE_SEEN_STAMP)
            params.append(conversation_thread_id)
        else:
            sets.append("archived_at = NULL")
    if not sets:
        return None

    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    UPDATE conversation_threads
                    SET {", ".join(sets)}
                    WHERE conversation_thread_id = %s
                    RETURNING conversation_thread_id, workspace_id, current_status, msg_type, thread_index, title, platform, metadata, is_shared, is_pinned, archived_at, last_seen_run_seq, created_at, updated_at
                """,
                    (*params, conversation_thread_id),
                )

                result = await cur.fetchone()
                if result:
                    logger.info(
                        f"[conversation_db] update_thread_fields thread_id={conversation_thread_id} set={sets}"
                    )
                    return dict(result)
                return None

    except Exception as e:
        logger.error(f"Error updating thread fields: {e}")
        raise


async def update_thread_title_cas(
    conversation_thread_id: str, title: str, expected_title: Optional[str]
) -> Optional[Dict[str, Any]]:
    """Compare-and-swap title update: applies only while the title still holds
    *expected_title* (the creation-time first-query stamp), so a concurrent
    manual rename or delete always wins over the background title generator.

    Returns the updated row, or None when the CAS lost (renamed/deleted).
    """
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    UPDATE conversation_threads
                    SET title = %s, updated_at = NOW()
                    WHERE conversation_thread_id = %s
                      AND title IS NOT DISTINCT FROM %s
                    RETURNING conversation_thread_id, workspace_id, current_status, msg_type, thread_index, title, platform, metadata, is_pinned, archived_at, created_at, updated_at
                """,
                    (title, conversation_thread_id, expected_title),
                )

                result = await cur.fetchone()
                if result:
                    logger.info(
                        f"[conversation_db] update_thread_title_cas thread_id={conversation_thread_id} title={title}"
                    )
                    return dict(result)
                return None

    except Exception as e:
        logger.error(f"Error CAS-updating thread title: {e}")
        raise


async def stamp_thread_seen(
    conversation_thread_id: str, run_id: str
) -> Optional[Dict[str, Any]]:
    """Causal seen stamp: advance the cursor to the OBSERVED run's seq only.

    The observed run must belong to this thread and be terminal — a
    "stamp whatever is latest" design would let a delayed /seen sweep a run
    that started AND settled after the observation (a dot the user never
    saw, silently cleared). GREATEST keeps the stamp idempotent and
    monotonic under multi-tab races. Returns
    ``{last_seen_run_seq, latest_run_seq}`` cursors, or None when the run
    doesn't qualify (missing / wrong thread / still live).
    """
    run_id = normalize_uuid(run_id)
    if run_id is None:
        return None
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                WITH observed AS (
                    SELECT run_seq FROM conversation_responses
                    WHERE conversation_response_id = %s
                      AND conversation_thread_id = %s
                      AND status IN ({_sql.sql_literals(TERMINAL_STATUSES)})
                )
                UPDATE conversation_threads ct
                SET last_seen_run_seq =
                        GREATEST(ct.last_seen_run_seq, observed.run_seq)
                FROM observed
                WHERE ct.conversation_thread_id = %s
                RETURNING ct.last_seen_run_seq,
                    (SELECT COALESCE(MAX(run_seq), 0)
                     FROM conversation_responses
                     WHERE conversation_thread_id = %s) AS latest_run_seq
                """,
                (
                    run_id,
                    conversation_thread_id,
                    conversation_thread_id,
                    conversation_thread_id,
                ),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def update_thread_sharing(
    conversation_thread_id: str,
    is_shared: bool,
    share_token: Optional[str] = None,
    share_permissions: Optional[Dict[str, Any]] = None,
    shared_at: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """
    Update sharing settings for a thread.

    Args:
        conversation_thread_id: Thread ID
        is_shared: Whether the thread is publicly shared
        share_token: Opaque share token (set on first enable)
        share_permissions: Permission dict e.g. {"allow_files": false, "allow_download": false}
        shared_at: Timestamp of last enable
    """
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                sets = ["is_shared = %s", "updated_at = NOW()"]
                params: list = [is_shared]

                if share_token is not None:
                    sets.append("share_token = %s")
                    params.append(share_token)

                if share_permissions is not None:
                    sets.append("share_permissions = %s")
                    params.append(Json(share_permissions))

                if shared_at is not None:
                    sets.append("shared_at = %s")
                    params.append(shared_at)

                params.append(conversation_thread_id)

                await cur.execute(
                    f"""
                    UPDATE conversation_threads
                    SET {", ".join(sets)}
                    WHERE conversation_thread_id = %s
                    RETURNING conversation_thread_id, workspace_id, share_token,
                              is_shared, share_permissions, shared_at,
                              current_status, msg_type, title, created_at, updated_at
                    """,
                    tuple(params),
                )

                result = await cur.fetchone()
                return dict(result) if result else None

    except Exception as e:
        logger.error(f"Error updating thread sharing: {e}")
        raise
