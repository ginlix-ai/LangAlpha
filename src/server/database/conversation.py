"""
Database utility functions for query-response logging.

Provides functions for creating, retrieving, and managing conversation history,
threads, queries, and responses in PostgreSQL.
"""

import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone
from uuid import UUID
from contextlib import asynccontextmanager
import psycopg
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

# Module-level connection pool cache for conversation database operations
# This ensures we reuse connections across operations, reducing connection overhead
_conversation_db_pool_cache = {}


def get_db_connection_string() -> str:
    """
    Get PostgreSQL connection string from environment variables.

    Database credentials are stored in .env file.
    Uses minimal connection string matching LangGraph pool configuration.

    Environment variables:
        DB_HOST: PostgreSQL host (default: localhost)
        DB_PORT: PostgreSQL port (default: 5432)
        DB_NAME: Database name (default: postgres)
        DB_USER: Database user (default: postgres)
        DB_PASSWORD: Database password (default: postgres)
    """
    import os

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")

    sslmode = "require" if "supabase.com" in db_host else "disable"
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={sslmode}"


def _on_reconnect_failed(pool):
    """Callback when conversation DB pool fails to reconnect after reconnect_timeout."""
    logger.critical(
        f"[ConversationDB] Connection pool failed to reconnect after "
        f"reconnect_timeout. Pool stats: {pool.get_stats()}"
    )


async def _configure_postgres_connection(conn):
    """
    Configure PostgreSQL connection for Supabase compatibility.

    Sets properties AT CONNECTION CREATION (before pool manages it).
    Critical: Do not modify connections after pool acquisition.
    """
    conn.prepare_threshold = 0  # Disable prepared statements
    await conn.set_autocommit(True)  # Set autocommit at creation
    logger.debug("Configured conversation DB connection with prepare_threshold=0, autocommit=True")


def get_or_create_pool() -> AsyncConnectionPool:
    """
    Get or create the shared connection pool for conversation database operations.

    Uses module-level cache to ensure pool is reused across operations.
    Configured with minimal settings matching LangGraph pool for stability.

    Returns:
        AsyncConnectionPool instance
    """
    db_uri = get_db_connection_string()

    if db_uri not in _conversation_db_pool_cache:
        # Create pool with minimal configuration matching LangGraph pool
        _conversation_db_pool_cache[db_uri] = AsyncConnectionPool(
            conninfo=db_uri,
            min_size=1,
            max_size=10,
            configure=_configure_postgres_connection,
            check=AsyncConnectionPool.check_connection,
            open=False,
            reconnect_failed=_on_reconnect_failed,
            kwargs={
                "connect_timeout": 10,
                "keepalives": 1,
                "keepalives_idle": 60,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            },
        )

    return _conversation_db_pool_cache[db_uri]


@asynccontextmanager
async def get_db_connection():
    """
    Shared database connection context manager using connection pooling.

    Provides async connection with consistent configuration:
    - Uses connection pool for efficient connection reuse
    - Prepared statements disabled (prepare_threshold=0)
    - Autocommit mode enabled (configured at pool creation)

    IMPORTANT:
    - Pool must be opened during server startup (in app.py lifespan)
    - Use row_factory per-cursor, not on connection:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT * FROM table")
    - Do NOT modify connection after acquisition - causes pool to discard it.
    """
    pool = get_or_create_pool()

    # Pool should already be open from startup
    # If not, this indicates a configuration error
    if pool.closed:
        raise RuntimeError(
            "Conversation database pool is not open. "
            "Pool must be opened during server startup in app.py lifespan."
        )

    # Get connection from pool - do not modify after acquisition
    async with pool.connection() as conn:
        try:
            yield conn
        finally:
            # Ensure connection is in proper state before returning to pool
            # This prevents "closing returned connection: ACTIVE/INTRANS" warnings
            # when CancelledError or other exceptions interrupt async context cleanup
            import psycopg.pq

            status = conn.info.transaction_status
            if status != psycopg.pq.TransactionStatus.IDLE:
                logger.warning(
                    f"Connection not in IDLE state (status: {status.name}). "
                    "This can happen when async context cleanup is interrupted. "
                    "Attempting to clean up connection state."
                )
                try:
                    if status == psycopg.pq.TransactionStatus.ACTIVE:
                        # Query in progress - cancel it to prevent pool warnings
                        # ACTIVE means a query is executing but hasn't completed
                        logger.debug("Connection in ACTIVE state, cancelling pending query")
                        # Cancel the query on the server side
                        await conn.cancel()
                        # Give the cancellation a moment to process
                        import asyncio
                        await asyncio.sleep(0.01)
                        # Now rollback to clean state
                        await conn.rollback()
                    elif status in (
                        psycopg.pq.TransactionStatus.INTRANS,
                        psycopg.pq.TransactionStatus.INERROR
                    ):
                        # Transaction in progress or error - rollback
                        logger.debug(f"Connection in {status.name} state, rolling back")
                        await conn.rollback()

                    # Verify we're now idle
                    final_status = conn.info.transaction_status
                    if final_status == psycopg.pq.TransactionStatus.IDLE:
                        logger.debug("Connection successfully reset to IDLE state")
                    else:
                        logger.warning(
                            f"Connection still not IDLE after cleanup (status: {final_status.name})"
                        )
                except Exception as cleanup_error:
                    logger.error(
                        f"Error during connection state cleanup: {cleanup_error}",
                        exc_info=True
                    )


# ==================== Legacy Conversation History Operations ====================
# NOTE: conversation_history table has been removed. Use workspaces table instead.
# These functions are kept as stubs for backward compatibility during migration.


# ==================== Thread Operations ====================

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
                await cur.execute("""
                    SELECT COALESCE(MAX(thread_index), -1) + 1 as next_index
                    FROM conversation_threads
                    WHERE workspace_id = %s
                """, (workspace_id,))
                result = await cur.fetchone()
                return result['next_index']
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("""
                        SELECT COALESCE(MAX(thread_index), -1) + 1 as next_index
                        FROM conversation_threads
                        WHERE workspace_id = %s
                    """, (workspace_id,))
                    result = await cur.fetchone()
                    return result['next_index']

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
    conn=None
) -> Dict[str, Any]:
    """
    Create a thread entry (thread_index auto-calculated if not provided).

    Args:
        conversation_thread_id: Thread ID
        workspace_id: Workspace ID
        current_status: Initial status
        msg_type: Message type
        thread_index: Optional thread index (calculated if not provided)
        title: Optional thread title
        external_id: Optional external thread identifier (e.g. "chat_id:topic_id")
        platform: Optional platform identifier (e.g. "telegram", "slack")
        conn: Optional database connection to reuse
    """
    # Build SQL dynamically — only include external_id/platform for platform callers
    columns = [
        "conversation_thread_id", "workspace_id", "current_status",
        "msg_type", "thread_index", "title",
    ]
    base_params = [conversation_thread_id, workspace_id, current_status, msg_type]
    # thread_index is appended per-attempt (may be recalculated on retry)

    if external_id and platform:
        columns.extend(["external_id", "platform"])
        extra_params = [external_id, platform]
    else:
        extra_params = []

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
                    logger.info(f"[conversation_db] create_thread thread_id={conversation_thread_id} thread_index={thread_index} workspace_id={workspace_id}")
                    return dict(result)
            else:
                async with get_db_connection() as conn_new:
                    async with conn_new.cursor(row_factory=dict_row) as cur:
                        await cur.execute(sql, params)
                        result = await cur.fetchone()
                        return dict(result)

        except psycopg.errors.UniqueViolation:
            if attempt == max_retries - 1:
                logger.error(f"thread_index conflict after {max_retries} attempts for workspace {workspace_id}")
                raise
            logger.warning(f"thread_index conflict (attempt {attempt + 1}/{max_retries}), retrying for workspace {workspace_id}")
            continue

        except Exception as e:
            logger.error(f"Error creating thread: {e}")
            raise


async def lookup_thread_by_external_id(
    platform: str, external_id: str, user_id: str
) -> Optional[str]:
    """Look up thread_id by platform + external_id, scoped to user's workspaces.

    Returns the conversation_thread_id if found, None otherwise.
    """
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT ct.conversation_thread_id
                    FROM conversation_threads ct
                    JOIN workspaces w ON ct.workspace_id = w.workspace_id
                    WHERE ct.platform = %s
                      AND ct.external_id = %s
                      AND w.user_id = %s
                    ORDER BY ct.updated_at DESC
                    LIMIT 1
                """, (platform, external_id, user_id))
                result = await cur.fetchone()
                if result:
                    thread_id = str(result["conversation_thread_id"])
                    logger.info(
                        f"[conversation_db] lookup_thread_by_external_id "
                        f"platform={platform} external_id={external_id} -> {thread_id}"
                    )
                    return thread_id
                return None
    except Exception as e:
        logger.error(f"Error looking up thread by external_id: {e}")
        return None


async def update_thread_status(conversation_thread_id: str, status: str, conn=None) -> bool:
    """
    Update thread status (completed, interrupted, error, timeout, etc.).

    Args:
        conversation_thread_id: Thread ID
        status: New status
        conn: Optional database connection to reuse
    """
    try:
        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    UPDATE conversation_threads
                    SET current_status = %s, updated_at = NOW()
                    WHERE conversation_thread_id = %s
                """, (status, conversation_thread_id))
                logger.info(f"[conversation_db] update_thread_status thread_id={conversation_thread_id} status={status}")
                return True
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("""
                        UPDATE conversation_threads
                        SET current_status = %s, updated_at = NOW()
                        WHERE conversation_thread_id = %s
                    """, (status, conversation_thread_id))
                    logger.info(f"[conversation_db] update_thread_status thread_id={conversation_thread_id} status={status}")
                    return True

    except Exception as e:
        logger.error(f"Error updating thread status: {e}")
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
) -> None:
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
    """
    async with get_db_connection() as conn:
        # Step 1: Verify workspace exists
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT workspace_id FROM workspaces WHERE workspace_id = %s
            """, (workspace_id,))
            workspace = await cur.fetchone()

        if not workspace:
            raise ValueError(f"Workspace {workspace_id} does not exist. Create it first via POST /workspaces")

        # Step 2: Check if thread already exists (for resume scenarios)
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT conversation_thread_id FROM conversation_threads WHERE conversation_thread_id = %s
            """, (conversation_thread_id,))
            thread_exists = await cur.fetchone()

        # Step 3: Create thread if it doesn't exist
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
                conn=conn
            )
        else:
            # Thread exists (resume scenario), update status
            await update_thread_status(conversation_thread_id, initial_status, conn=conn)
            logger.info(f"Resumed thread {conversation_thread_id}, updated status to {initial_status}")


async def get_workspace_threads(
    workspace_id: str,
    limit: int = 20,
    offset: int = 0,
    sort_by: str = "updated_at",
    sort_order: str = "desc"
) -> Tuple[List[Dict[str, Any]], int]:
    """Get threads for a workspace with pagination."""
    # Validate sort parameters
    valid_sort_fields = ["created_at", "updated_at", "thread_index"]
    if sort_by not in valid_sort_fields:
        sort_by = "updated_at"

    if sort_order.lower() not in ["asc", "desc"]:
        sort_order = "desc"

    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Get total count
                await cur.execute("""
                    SELECT COUNT(*) as total
                    FROM conversation_threads
                    WHERE workspace_id = %s
                """, (workspace_id,))

                total_result = await cur.fetchone()
                total_count = total_result['total']

                # Get threads
                query = f"""
                    SELECT
                        conversation_thread_id, workspace_id, current_status, msg_type, thread_index,
                        title, is_shared, created_at, updated_at
                    FROM conversation_threads
                    WHERE workspace_id = %s
                    ORDER BY {sort_by} {sort_order.upper()}
                    LIMIT %s OFFSET %s
                """
                await cur.execute(query, (workspace_id, limit, offset))

                threads = await cur.fetchall()
                return [dict(row) for row in threads], total_count

    except Exception as e:
        logger.error(f"Error getting threads for workspace: {e}")
        raise


async def get_threads_for_user(
    user_id: str,
    limit: int = 20,
    offset: int = 0,
    sort_by: str = "updated_at",
    sort_order: str = "desc",
) -> Tuple[List[Dict[str, Any]], int]:
    """Get all threads for a user across all workspaces."""
    sort_fields = {
        "created_at": "t.created_at",
        "updated_at": "t.updated_at",
        "thread_index": "t.thread_index",
    }
    if sort_by not in sort_fields:
        sort_by = "updated_at"

    if sort_order.lower() not in ["asc", "desc"]:
        sort_order = "desc"

    order_by = sort_fields[sort_by]

    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT COUNT(*) as total
                    FROM conversation_threads t
                    JOIN workspaces w ON t.workspace_id = w.workspace_id
                    WHERE w.user_id = %s AND w.status != 'deleted'
                    """,
                    (user_id,),
                )
                total_result = await cur.fetchone()
                total_count = total_result["total"] if total_result else 0

                query = f"""
                    SELECT
                        t.conversation_thread_id, t.workspace_id, t.current_status, t.msg_type, t.thread_index,
                        t.title, t.is_shared, t.created_at, t.updated_at,
                        fq.content AS first_query_content
                    FROM conversation_threads t
                    JOIN workspaces w ON t.workspace_id = w.workspace_id
                    LEFT JOIN LATERAL (
                        SELECT q.content
                        FROM conversation_queries q
                        WHERE q.conversation_thread_id = t.conversation_thread_id
                        ORDER BY q.turn_index ASC
                        LIMIT 1
                    ) fq ON TRUE
                    WHERE w.user_id = %s AND w.status != 'deleted'
                    ORDER BY {order_by} {sort_order.upper()}
                    LIMIT %s OFFSET %s
                """
                await cur.execute(query, (user_id, limit, offset))
                threads = await cur.fetchall()
                return [dict(row) for row in threads], total_count

    except Exception as e:
        logger.error(f"Error getting threads for user: {e}")
        raise


# ==================== Query Operations ====================

async def get_next_turn_index(conversation_thread_id: str, conn=None) -> int:
    """
    Calculate the next turn_index for a thread (0-based).

    Args:
        conversation_thread_id: Thread ID
        conn: Optional database connection to reuse
    """
    try:
        if conn:
            # Reuse provided connection
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT COUNT(*) as count
                    FROM conversation_queries
                    WHERE conversation_thread_id = %s
                """, (conversation_thread_id,))
                result = await cur.fetchone()
                return result['count']
        else:
            # Acquire new connection (backward compatibility)
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("""
                        SELECT COUNT(*) as count
                        FROM conversation_queries
                        WHERE conversation_thread_id = %s
                    """, (conversation_thread_id,))
                    result = await cur.fetchone()
                    return result['count']

    except Exception as e:
        logger.error(f"Error calculating turn index: {e}")
        return 0


async def create_query(
    conversation_query_id: str,
    conversation_thread_id: str,
    turn_index: int,
    content: str,
    query_type: str,
    feedback_action: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    created_at: Optional[datetime] = None,
    conn=None,
    idempotent: bool = True
) -> Dict[str, Any]:
    """
    Create a query entry.

    Args:
        conversation_query_id: Query ID
        conversation_thread_id: Thread ID
        turn_index: Turn index
        content: Query content
        query_type: Query type
        feedback_action: Optional feedback action
        metadata: Optional metadata
        created_at: Optional timestamp
        conn: Optional database connection to reuse
        idempotent: If True, use ON CONFLICT DO UPDATE for safe retries
    """
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    try:
        if conn:
            # Reuse provided connection
            async with conn.cursor(row_factory=dict_row) as cur:
                if idempotent:
                    # Idempotent: ON CONFLICT DO UPDATE for safe retries
                    await cur.execute("""
                        INSERT INTO conversation_queries (
                            conversation_query_id, conversation_thread_id, turn_index, content, type,
                            feedback_action, metadata, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (conversation_thread_id, turn_index) DO UPDATE
                        SET content = EXCLUDED.content,
                            type = EXCLUDED.type,
                            feedback_action = EXCLUDED.feedback_action,
                            metadata = EXCLUDED.metadata,
                            created_at = EXCLUDED.created_at
                        RETURNING conversation_query_id, conversation_thread_id, turn_index, content, type,
                                  feedback_action, metadata, created_at
                    """, (conversation_query_id, conversation_thread_id, turn_index, content, query_type,
                          feedback_action, Json(metadata or {}), created_at))
                else:
                    # Non-idempotent: fail on conflict
                    await cur.execute("""
                        INSERT INTO conversation_queries (
                            conversation_query_id, conversation_thread_id, turn_index, content, type,
                            feedback_action, metadata, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING conversation_query_id, conversation_thread_id, turn_index, content, type,
                                  feedback_action, metadata, created_at
                    """, (conversation_query_id, conversation_thread_id, turn_index, content, query_type,
                          feedback_action, Json(metadata or {}), created_at))
                result = await cur.fetchone()
                logger.info(f"[conversation_db] create_query query_id={conversation_query_id} thread_id={conversation_thread_id} turn_index={turn_index} type={query_type}")
                return dict(result)
        else:
            # Acquire new connection (backward compatibility)
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    if idempotent:
                        # Idempotent: ON CONFLICT DO UPDATE for safe retries
                        await cur.execute("""
                            INSERT INTO conversation_queries (
                                conversation_query_id, conversation_thread_id, turn_index, content, type,
                                feedback_action, metadata, created_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (conversation_thread_id, turn_index) DO UPDATE
                            SET content = EXCLUDED.content,
                                type = EXCLUDED.type,
                                feedback_action = EXCLUDED.feedback_action,
                                metadata = EXCLUDED.metadata,
                                created_at = EXCLUDED.created_at
                            RETURNING conversation_query_id, conversation_thread_id, turn_index, content, type,
                                      feedback_action, metadata, created_at
                        """, (conversation_query_id, conversation_thread_id, turn_index, content, query_type,
                              feedback_action, Json(metadata or {}), created_at))
                    else:
                        # Non-idempotent: fail on conflict
                        await cur.execute("""
                            INSERT INTO conversation_queries (
                                conversation_query_id, conversation_thread_id, turn_index, content, type,
                                feedback_action, metadata, created_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING conversation_query_id, conversation_thread_id, turn_index, content, type,
                                      feedback_action, metadata, created_at
                        """, (conversation_query_id, conversation_thread_id, turn_index, content, query_type,
                              feedback_action, Json(metadata or {}), created_at))
                    result = await cur.fetchone()
                    logger.info(f"[conversation_db] create_query query_id={conversation_query_id} thread_id={conversation_thread_id} turn_index={turn_index} type={query_type}")
                    return dict(result)

    except Exception as e:
        logger.error(f"Error creating query: {e}")
        raise


async def get_queries_for_thread(
    conversation_thread_id: str,
    limit: Optional[int] = None,
    offset: int = 0
) -> Tuple[List[Dict[str, Any]], int]:
    """Get queries for a thread."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Get total count
                await cur.execute("""
                    SELECT COUNT(*) as total
                    FROM conversation_queries
                    WHERE conversation_thread_id = %s
                """, (conversation_thread_id,))

                total_result = await cur.fetchone()
                total_count = total_result['total']

                # Get queries
                if limit:
                    await cur.execute("""
                        SELECT
                            conversation_query_id, conversation_thread_id, turn_index, content, type,
                            feedback_action, metadata, created_at
                        FROM conversation_queries
                        WHERE conversation_thread_id = %s
                        ORDER BY turn_index ASC
                        LIMIT %s OFFSET %s
                    """, (conversation_thread_id, limit, offset))
                else:
                    await cur.execute("""
                        SELECT
                            conversation_query_id, conversation_thread_id, turn_index, content, type,
                            feedback_action, metadata, created_at
                        FROM conversation_queries
                        WHERE conversation_thread_id = %s
                        ORDER BY turn_index ASC
                    """, (conversation_thread_id,))

                queries = await cur.fetchall()
                return [dict(row) for row in queries], total_count

    except Exception as e:
        logger.error(f"Error getting queries for thread: {e}")
        raise


# ==================== Response Operations ====================

async def create_response(
    conversation_response_id: str,
    conversation_thread_id: str,
    turn_index: int,
    status: str,
    interrupt_reason: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    warnings: Optional[List[str]] = None,
    errors: Optional[List[str]] = None,
    execution_time: Optional[float] = None,
    created_at: Optional[datetime] = None,
    sse_events: Optional[Any] = None,
    conn=None,
    idempotent: bool = True
) -> Dict[str, Any]:
    """
    Create a response entry.

    Args:
        conversation_response_id: Response ID
        conversation_thread_id: Thread ID
        turn_index: Turn index
        status: Status
        interrupt_reason: Optional interrupt reason
        metadata: Optional metadata
        warnings: Optional warnings
        errors: Optional errors
        execution_time: Optional execution time
        created_at: Optional timestamp
        sse_events: Optional SSE events data
        conn: Optional database connection to reuse
        idempotent: If True, use ON CONFLICT DO UPDATE for safe retries
    """
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    try:
        if conn:
            # Reuse provided connection
            async with conn.cursor(row_factory=dict_row) as cur:
                if idempotent:
                    # Idempotent: ON CONFLICT DO UPDATE for safe retries
                    await cur.execute("""
                        INSERT INTO conversation_responses (
                            conversation_response_id, conversation_thread_id, turn_index, status,
                            interrupt_reason, metadata,
                            warnings, errors, execution_time, created_at,
                            sse_events
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (conversation_thread_id, turn_index) DO UPDATE
                        SET status = EXCLUDED.status,
                            interrupt_reason = EXCLUDED.interrupt_reason,
                            metadata = EXCLUDED.metadata,
                            warnings = EXCLUDED.warnings,
                            errors = EXCLUDED.errors,
                            execution_time = EXCLUDED.execution_time,
                            created_at = EXCLUDED.created_at,
                            sse_events = EXCLUDED.sse_events
                        RETURNING conversation_response_id, conversation_thread_id, turn_index, status,
                                  interrupt_reason, metadata,
                                  warnings, errors, execution_time, created_at,
                                  sse_events
                    """, (
                        conversation_response_id, conversation_thread_id, turn_index,
                        status, interrupt_reason,
                        Json(metadata or {}),
                        warnings or [],
                        errors or [],
                        execution_time,
                        created_at,
                        Json(sse_events) if sse_events else None
                    ))
                else:
                    # Non-idempotent: fail on conflict
                    await cur.execute("""
                        INSERT INTO conversation_responses (
                            conversation_response_id, conversation_thread_id, turn_index, status,
                            interrupt_reason, metadata,
                            warnings, errors, execution_time, created_at,
                            sse_events
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING conversation_response_id, conversation_thread_id, turn_index, status,
                                  interrupt_reason, metadata,
                                  warnings, errors, execution_time, created_at,
                                  sse_events
                    """, (
                        conversation_response_id, conversation_thread_id, turn_index,
                        status, interrupt_reason,
                        Json(metadata or {}),
                        warnings or [],
                        errors or [],
                        execution_time,
                        created_at,
                        Json(sse_events) if sse_events else None
                    ))
                result = await cur.fetchone()
                logger.info(f"[conversation_db] create_response response_id={conversation_response_id} thread_id={conversation_thread_id} turn_index={turn_index} status={status}")
                return dict(result)
        else:
            # Acquire new connection (backward compatibility)
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    if idempotent:
                        # Idempotent: ON CONFLICT DO UPDATE for safe retries
                        await cur.execute("""
                            INSERT INTO conversation_responses (
                                conversation_response_id, conversation_thread_id, turn_index, status,
                                interrupt_reason, metadata,
                                warnings, errors, execution_time, created_at,
                                sse_events
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (conversation_thread_id, turn_index) DO UPDATE
                            SET status = EXCLUDED.status,
                                interrupt_reason = EXCLUDED.interrupt_reason,
                                metadata = EXCLUDED.metadata,
                                warnings = EXCLUDED.warnings,
                                errors = EXCLUDED.errors,
                                execution_time = EXCLUDED.execution_time,
                                created_at = EXCLUDED.created_at,
                                sse_events = EXCLUDED.sse_events
                            RETURNING conversation_response_id, conversation_thread_id, turn_index, status,
                                      interrupt_reason, metadata,
                                      warnings, errors, execution_time, created_at,
                                      sse_events
                        """, (
                            conversation_response_id, conversation_thread_id, turn_index,
                            status, interrupt_reason,
                            Json(metadata or {}),
                            warnings or [],
                            errors or [],
                            execution_time,
                            created_at,
                            Json(sse_events) if sse_events else None
                        ))
                    else:
                        # Non-idempotent: fail on conflict
                        await cur.execute("""
                            INSERT INTO conversation_responses (
                                conversation_response_id, conversation_thread_id, turn_index, status,
                                interrupt_reason, metadata,
                                warnings, errors, execution_time, created_at,
                                sse_events
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING conversation_response_id, conversation_thread_id, turn_index, status,
                                      interrupt_reason, metadata,
                                      warnings, errors, execution_time, created_at,
                                      sse_events
                        """, (
                            conversation_response_id, conversation_thread_id, turn_index,
                            status, interrupt_reason,
                            Json(metadata or {}),
                            warnings or [],
                            errors or [],
                            execution_time,
                            created_at,
                            Json(sse_events) if sse_events else None
                        ))
                    result = await cur.fetchone()
                    logger.info(f"[conversation_db] create_response response_id={conversation_response_id} thread_id={conversation_thread_id} turn_index={turn_index} status={status}")
                    return dict(result)

    except Exception as e:
        logger.error(f"Error creating response: {e}")
        raise


async def update_sse_events(
    conversation_response_id: str,
    sse_events: List[Dict[str, Any]],
    conn=None,
) -> bool:
    """
    Update sse_events for an existing response.

    Used by post-interrupt subagent result collector to replace incomplete
    subagent events with the full set captured by middleware.

    Args:
        conversation_response_id: The response ID to update
        sse_events: Updated SSE events list
        conn: Optional database connection to reuse

    Returns:
        True if the row was updated, False if not found
    """
    try:
        if conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE conversation_responses
                    SET sse_events = %s
                    WHERE conversation_response_id = %s
                    """,
                    (Json(sse_events), conversation_response_id),
                )
                updated = cur.rowcount > 0
        else:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE conversation_responses
                        SET sse_events = %s
                        WHERE conversation_response_id = %s
                        """,
                        (Json(sse_events), conversation_response_id),
                    )
                    updated = cur.rowcount > 0

        if updated:
            logger.info(
                f"[conversation_db] update_sse_events response_id={conversation_response_id} "
                f"events={len(sse_events)}"
            )
        else:
            logger.warning(
                f"[conversation_db] update_sse_events: no row found for response_id={conversation_response_id}"
            )
        return updated

    except Exception as e:
        logger.error(f"Error updating sse_events: {e}")
        raise


async def get_responses_for_thread(
    conversation_thread_id: str,
    limit: Optional[int] = None,
    offset: int = 0
) -> Tuple[List[Dict[str, Any]], int]:
    """Get responses for a thread."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Get total count
                await cur.execute("""
                    SELECT COUNT(*) as total
                    FROM conversation_responses
                    WHERE conversation_thread_id = %s
                """, (conversation_thread_id,))

                total_result = await cur.fetchone()
                total_count = total_result['total']

                # Get responses
                if limit:
                    await cur.execute("""
                        SELECT
                            conversation_response_id, conversation_thread_id, turn_index, status,
                            interrupt_reason, metadata,
                            warnings, errors, execution_time, created_at,
                            sse_events
                        FROM conversation_responses
                        WHERE conversation_thread_id = %s
                        ORDER BY turn_index ASC
                        LIMIT %s OFFSET %s
                    """, (conversation_thread_id, limit, offset))
                else:
                    await cur.execute("""
                        SELECT
                            conversation_response_id, conversation_thread_id, turn_index, status,
                            interrupt_reason, metadata,
                            warnings, errors, execution_time, created_at,
                            sse_events
                        FROM conversation_responses
                        WHERE conversation_thread_id = %s
                        ORDER BY turn_index ASC
                    """, (conversation_thread_id,))

                responses = await cur.fetchall()
                return [dict(row) for row in responses], total_count

    except Exception as e:
        logger.error(f"Error getting responses for thread: {e}")
        raise


# ==================== Query-Response Pair Operations ====================

async def get_query_response_pairs(
    conversation_thread_id: str,
    limit: Optional[int] = None,
    offset: int = 0
) -> Tuple[List[Dict[str, Any]], int]:
    """Get query-response pairs for a thread (joined data)."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Get total count
                await cur.execute("""
                    SELECT COUNT(*) as total
                    FROM conversation_queries
                    WHERE conversation_thread_id = %s
                """, (conversation_thread_id,))

                total_result = await cur.fetchone()
                total_count = total_result['total']

                # Get joined query-response pairs
                if limit:
                    await cur.execute("""
                        SELECT
                            q.conversation_query_id, q.conversation_thread_id, q.turn_index, q.content as query_content,
                            q.type as query_type, q.feedback_action, q.metadata as query_metadata,
                            q.created_at as query_created_at,
                            r.conversation_response_id, r.status, r.interrupt_reason,
                            r.metadata as response_metadata,
                            r.warnings, r.errors, r.execution_time,
                            r.created_at as response_created_at

                        FROM conversation_queries q
                        LEFT JOIN conversation_responses r ON q.conversation_thread_id = r.conversation_thread_id AND q.turn_index = r.turn_index
                        WHERE q.conversation_thread_id = %s
                        ORDER BY q.turn_index ASC
                        LIMIT %s OFFSET %s
                    """, (conversation_thread_id, limit, offset))
                else:
                    await cur.execute("""
                        SELECT
                            q.conversation_query_id, q.conversation_thread_id, q.turn_index, q.content as query_content,
                            q.type as query_type, q.feedback_action, q.metadata as query_metadata,
                            q.created_at as query_created_at,
                            r.conversation_response_id, r.status, r.interrupt_reason,
                            r.metadata as response_metadata,
                            r.warnings, r.errors, r.execution_time,
                            r.created_at as response_created_at

                        FROM conversation_queries q
                        LEFT JOIN conversation_responses r ON q.conversation_thread_id = r.conversation_thread_id AND q.turn_index = r.turn_index
                        WHERE q.conversation_thread_id = %s
                        ORDER BY q.turn_index ASC
                    """, (conversation_thread_id,))

                pairs = await cur.fetchall()
                return [dict(row) for row in pairs], total_count

    except Exception as e:
        logger.error(f"Error getting query-response pairs for thread: {e}")
        raise


# ==================== Extended Operations for API v2 ====================

async def get_thread_with_summary(conversation_thread_id: str) -> Optional[Dict[str, Any]]:
    """Get thread with enriched summary data (pair count, costs, etc.)."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Get thread basic info
                await cur.execute("""
                    SELECT conversation_thread_id, workspace_id, current_status, thread_index, created_at, updated_at
                    FROM conversation_threads
                    WHERE conversation_thread_id = %s
                """, (conversation_thread_id,))

                thread = await cur.fetchone()
                if not thread:
                    return None

                thread = dict(thread)

                # Get aggregated pair data
                await cur.execute("""
                    SELECT
                        COUNT(q.turn_index) as pair_count,
                        COALESCE(SUM((u.token_usage->>'total_cost')::float), 0) as total_cost,
                        COALESCE(SUM(r.execution_time), 0) as total_execution_time,
                        MAX(q.type) as last_query_type,
                        BOOL_OR(COALESCE(array_length(r.errors, 1), 0) > 0) as has_errors
                    FROM conversation_queries q
                    LEFT JOIN conversation_responses r ON q.conversation_thread_id = r.conversation_thread_id AND q.turn_index = r.turn_index
                    LEFT JOIN conversation_usages u ON r.conversation_response_id = u.conversation_response_id
                    WHERE q.conversation_thread_id = %s
                """, (conversation_thread_id,))

                stats = await cur.fetchone()
                if stats:
                    thread.update(dict(stats))

                return thread

    except Exception as e:
        logger.error(f"Error getting thread with summary: {e}")
        raise


async def delete_thread(conversation_thread_id: str) -> bool:
    """Delete thread (CASCADE to queries, responses)."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    DELETE FROM conversation_threads
                    WHERE conversation_thread_id = %s
                """, (conversation_thread_id,))

                logger.info(f"Deleted thread: {conversation_thread_id}")
                return True

    except Exception as e:
        logger.error(f"Error deleting thread: {e}")
        raise


async def update_thread_title(conversation_thread_id: str, title: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Update thread title.

    Args:
        conversation_thread_id: Thread ID
        title: New title (can be None to clear title)

    Returns:
        Updated thread dict, or None if thread not found
    """
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    UPDATE conversation_threads
                    SET title = %s, updated_at = NOW()
                    WHERE conversation_thread_id = %s
                    RETURNING conversation_thread_id, workspace_id, current_status, msg_type, thread_index, title, created_at, updated_at
                """, (title, conversation_thread_id))

                result = await cur.fetchone()
                if result:
                    logger.info(f"[conversation_db] update_thread_title thread_id={conversation_thread_id} title={title}")
                    return dict(result)
                return None

    except Exception as e:
        logger.error(f"Error updating thread title: {e}")
        raise


async def get_thread_by_id(conversation_thread_id: str) -> Optional[Dict[str, Any]]:
    """
    Get thread by ID.

    Args:
        conversation_thread_id: Thread ID

    Returns:
        Thread dict or None if not found
    """
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT conversation_thread_id, workspace_id, current_status,
                           msg_type, thread_index, title,
                           share_token, is_shared, share_permissions, shared_at,
                           created_at, updated_at
                    FROM conversation_threads
                    WHERE conversation_thread_id = %s
                """, (conversation_thread_id,))

                result = await cur.fetchone()
                return dict(result) if result else None

    except Exception as e:
        logger.error(f"Error getting thread by id: {e}")
        raise


async def get_thread_by_share_token(share_token: str) -> Optional[Dict[str, Any]]:
    """
    Get a shared thread by its public share token.

    Returns thread info + workspace_id + workspace name only if is_shared = TRUE.
    """
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT
                        t.conversation_thread_id,
                        t.workspace_id,
                        t.current_status,
                        t.msg_type,
                        t.title,
                        t.share_token,
                        t.is_shared,
                        t.share_permissions,
                        t.shared_at,
                        t.created_at,
                        t.updated_at,
                        w.name AS workspace_name
                    FROM conversation_threads t
                    JOIN workspaces w ON w.workspace_id = t.workspace_id
                    WHERE t.share_token = %s AND t.is_shared = TRUE
                """, (share_token,))

                result = await cur.fetchone()
                return dict(result) if result else None

    except Exception as e:
        logger.error(f"Error getting thread by share token: {e}")
        raise


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
        async with get_db_connection() as conn:
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
                    SET {', '.join(sets)}
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


async def get_user_stats(user_id: str) -> Dict[str, Any]:
    """Get aggregated user statistics."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Get workspace count
                await cur.execute("""
                    SELECT COUNT(*) as total_workspaces
                    FROM workspaces
                    WHERE user_id = %s
                """, (user_id,))
                ws_count = (await cur.fetchone())['total_workspaces']

                # Get thread statistics via workspaces
                await cur.execute("""
                    SELECT
                        COUNT(DISTINCT t.conversation_thread_id) as total_threads,
                        COUNT(DISTINCT q.conversation_query_id) as total_queries,
                        COUNT(DISTINCT r.conversation_response_id) as total_responses,
                        COALESCE(SUM((u.token_usage->>'total_cost')::float), 0) as total_cost,
                        COALESCE(SUM(r.execution_time), 0) as total_execution_time,
                        MIN(t.created_at) as first_activity,
                        MAX(t.updated_at) as last_activity
                    FROM workspaces w
                    LEFT JOIN conversation_threads t ON w.workspace_id = t.workspace_id
                    LEFT JOIN conversation_queries q ON t.conversation_thread_id = q.conversation_thread_id
                    LEFT JOIN conversation_responses r ON t.conversation_thread_id = r.conversation_thread_id
                    LEFT JOIN conversation_usages u ON r.conversation_response_id = u.conversation_response_id
                    WHERE w.user_id = %s
                """, (user_id,))
                stats = await cur.fetchone()

                # Get status breakdown
                await cur.execute("""
                    SELECT
                        t.current_status,
                        COUNT(*) as count
                    FROM workspaces w
                    JOIN conversation_threads t ON w.workspace_id = t.workspace_id
                    WHERE w.user_id = %s
                    GROUP BY t.current_status
                """, (user_id,))
                status_rows = await cur.fetchall()
                by_status = {row['current_status']: row['count'] for row in status_rows}

                return {
                    'user_id': user_id,
                    'total_workspaces': ws_count,
                    'total_threads': stats['total_threads'] or 0,
                    'total_queries': stats['total_queries'] or 0,
                    'total_responses': stats['total_responses'] or 0,
                    'total_cost': float(stats['total_cost'] or 0),
                    'total_execution_time': float(stats['total_execution_time'] or 0),
                    'date_range': {
                        'first_activity': stats['first_activity'],
                        'last_activity': stats['last_activity']
                    },
                    'by_status': by_status
                }

    except Exception as e:
        logger.error(f"Error getting user stats: {e}")
        raise


async def get_workspace_stats(workspace_id: str) -> Dict[str, Any]:
    """Get aggregated workspace statistics."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Get thread and pair statistics
                await cur.execute("""
                    SELECT
                        COUNT(DISTINCT t.conversation_thread_id) as total_threads,
                        COUNT(DISTINCT q.conversation_query_id) as total_pairs,
                        COALESCE(SUM((u.token_usage->>'total_cost')::float), 0) as total_cost,
                        COALESCE(SUM(r.execution_time), 0) as total_execution_time
                    FROM conversation_threads t
                    LEFT JOIN conversation_queries q ON t.conversation_thread_id = q.conversation_thread_id
                    LEFT JOIN conversation_responses r ON t.conversation_thread_id = r.conversation_thread_id
                    LEFT JOIN conversation_usages u ON r.conversation_response_id = u.conversation_response_id
                    WHERE t.workspace_id = %s
                """, (workspace_id,))
                stats = await cur.fetchone()

                # Get status breakdown
                await cur.execute("""
                    SELECT
                        current_status,
                        COUNT(*) as count
                    FROM conversation_threads
                    WHERE workspace_id = %s
                    GROUP BY current_status
                """, (workspace_id,))
                status_rows = await cur.fetchall()
                by_status = {row['current_status']: row['count'] for row in status_rows}

                # Get cost breakdown by model
                await cur.execute("""
                    SELECT
                        u.token_usage
                    FROM conversation_threads t
                    JOIN conversation_responses r ON t.conversation_thread_id = r.conversation_thread_id
                    JOIN conversation_usages u ON r.conversation_response_id = u.conversation_response_id
                    WHERE t.workspace_id = %s AND u.token_usage IS NOT NULL
                """, (workspace_id,))

                responses = await cur.fetchall()
                cost_by_model = {}
                for row in responses:
                    token_usage = row['token_usage']
                    if token_usage and 'by_model' in token_usage:
                        for model, usage in token_usage['by_model'].items():
                            if model not in cost_by_model:
                                cost_by_model[model] = {
                                    'input_tokens': 0,
                                    'output_tokens': 0,
                                    'total_tokens': 0,
                                    'cost': 0.0
                                }
                            cost_by_model[model]['input_tokens'] += usage.get('input_tokens', 0)
                            cost_by_model[model]['output_tokens'] += usage.get('output_tokens', 0)
                            cost_by_model[model]['total_tokens'] += usage.get('total_tokens', 0)
                            cost_by_model[model]['cost'] += usage.get('cost', 0.0)

                return {
                    'workspace_id': workspace_id,
                    'total_threads': stats['total_threads'] or 0,
                    'total_pairs': stats['total_pairs'] or 0,
                    'total_cost': float(stats['total_cost'] or 0),
                    'total_execution_time': float(stats['total_execution_time'] or 0),
                    'by_status': by_status,
                    'cost_breakdown': {
                        'by_model': cost_by_model
                    }
                }

    except Exception as e:
        logger.error(f"Error getting workspace stats: {e}")
        raise




# ========== Usage Tracking Functions ==========

async def create_usage_record(
    usage_data: Dict[str, Any],
    conn: Optional[AsyncConnection] = None
) -> bool:
    """
    Create a usage record in conversation_usages table.

    Args:
        usage_data: Usage data dict with structure:
            {
                "conversation_usage_id": str,
                "conversation_response_id": str,
                "user_id": str,
                "conversation_thread_id": str,
                "workspace_id": str,
                "msg_type": str,
                "status": str,
                "token_usage": dict (JSONB),
                "infrastructure_usage": dict (JSONB, optional),
                "token_credits": float,
                "infrastructure_credits": float,
                "total_credits": float,
                "created_at": datetime
            }
        conn: Optional connection (for transactions)

    Returns:
        True if successful

    Raises:
        psycopg.Error: On database errors
    """
    async def _create(cur):
        await cur.execute("""
            INSERT INTO conversation_usages (
                conversation_usage_id,
                conversation_response_id,
                user_id,
                conversation_thread_id,
                workspace_id,
                msg_type,
                status,
                token_usage,
                infrastructure_usage,
                token_credits,
                infrastructure_credits,
                total_credits,
                is_byok,
                created_at
            ) VALUES (
                %(conversation_usage_id)s,
                %(conversation_response_id)s,
                %(user_id)s,
                %(conversation_thread_id)s,
                %(workspace_id)s,
                %(msg_type)s,
                %(status)s,
                %(token_usage)s,
                %(infrastructure_usage)s,
                %(token_credits)s,
                %(infrastructure_credits)s,
                %(total_credits)s,
                %(is_byok)s,
                %(created_at)s
            )
        """, {
            "conversation_usage_id": usage_data["conversation_usage_id"],
            "conversation_response_id": usage_data["conversation_response_id"],
            "user_id": usage_data["user_id"],
            "conversation_thread_id": usage_data["conversation_thread_id"],
            "workspace_id": usage_data["workspace_id"],
            "msg_type": usage_data.get("msg_type", "ptc"),
            "status": usage_data.get("status", "completed"),
            "token_usage": Json(usage_data.get("token_usage")),
            "infrastructure_usage": Json(usage_data.get("infrastructure_usage")),
            "token_credits": usage_data["token_credits"],
            "infrastructure_credits": usage_data["infrastructure_credits"],
            "total_credits": usage_data["total_credits"],
            "is_byok": usage_data.get("is_byok", False),
            "created_at": usage_data["created_at"]
        })

    if conn:
        async with conn.cursor() as cur:
            await _create(cur)
    else:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await _create(cur)

    return True


async def update_usage_record(
    conversation_response_id: str,
    token_usage: Dict[str, Any],
    conn: Optional[AsyncConnection] = None,
) -> bool:
    """
    Update the token_usage JSONB on an existing conversation_usages record.

    Used by the subagent result collector to merge subagent token costs
    into the parent turn's usage record after subagents complete.

    Args:
        conversation_response_id: The response ID whose usage to update
        token_usage: Updated token_usage dict (replaces existing value)
        conn: Optional connection (for transactions)

    Returns:
        True if a row was updated, False if not found
    """
    async def _update(cur):
        await cur.execute(
            """
            UPDATE conversation_usages
            SET token_usage = %s
            WHERE conversation_response_id = %s
            """,
            (Json(token_usage), conversation_response_id),
        )
        return cur.rowcount > 0

    try:
        if conn:
            async with conn.cursor() as cur:
                updated = await _update(cur)
        else:
            async with get_db_connection() as conn_new:
                async with conn_new.cursor() as cur:
                    updated = await _update(cur)

        if updated:
            logger.info(
                f"[conversation_db] update_usage_record response_id={conversation_response_id}"
            )
        else:
            logger.warning(
                f"[conversation_db] update_usage_record: no row found for "
                f"response_id={conversation_response_id}"
            )
        return updated

    except Exception as e:
        logger.error(f"Error updating usage record: {e}")
        raise


async def get_user_total_credits(
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get total credits spent by a user (fast, no JOINs needed).

    Args:
        user_id: User identifier
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)

    Returns:
        Dict with structure:
        {
            "user_id": str,
            "total_credits": float,
            "token_credits": float,
            "infrastructure_credits": float,
            "workflow_count": int,
            "start_date": str or None,
            "end_date": str or None
        }
    """
    # Build date filter
    date_filter = ""
    params = {"user_id": user_id}

    if start_date:
        date_filter += " AND created_at >= %(start_date)s"
        params["start_date"] = start_date
    if end_date:
        date_filter += " AND created_at < %(end_date)s"
        params["end_date"] = end_date

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT
                    %(user_id)s as user_id,
                    COALESCE(SUM(total_credits), 0) as total_credits,
                    COALESCE(SUM(token_credits), 0) as token_credits,
                    COALESCE(SUM(infrastructure_credits), 0) as infrastructure_credits,
                    COUNT(DISTINCT conversation_thread_id) as workflow_count
                FROM conversation_usages
                WHERE user_id = %(user_id)s
                {date_filter}
            """, params)

            row = await cur.fetchone()

            return {
                "user_id": user_id,
                "total_credits": float(row["total_credits"]) if row["total_credits"] else 0.0,
                "token_credits": float(row["token_credits"]) if row["token_credits"] else 0.0,
                "infrastructure_credits": float(row["infrastructure_credits"]) if row["infrastructure_credits"] else 0.0,
                "workflow_count": row["workflow_count"],
                "start_date": start_date,
                "end_date": end_date
            }


async def get_user_credit_history(
    user_id: str,
    days: int = 30,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Get time-series credit history for a user.

    Args:
        user_id: User identifier
        days: Number of days to look back (default: 30)
        limit: Maximum number of records (default: 100)

    Returns:
        List of usage records ordered by created_at DESC
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    conversation_usage_id,
                    conversation_response_id,
                    conversation_thread_id,
                    workspace_id,
                    turn_index,
                    token_credits,
                    infrastructure_credits,
                    total_credits,
                    created_at,
                    metadata
                FROM conversation_usages
                WHERE user_id = %s
                  AND created_at >= NOW() - INTERVAL '%s days'
                ORDER BY created_at DESC
                LIMIT %s
            """, (user_id, days, limit))

            rows = await cur.fetchall()
            return [dict(row) for row in rows]


async def get_response_usage(conversation_response_id: str) -> Optional[Dict[str, Any]]:
    """
    Get usage record for a specific response.

    Args:
        conversation_response_id: Response identifier

    Returns:
        Usage record dict or None if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    conversation_usage_id,
                    conversation_response_id,
                    user_id,
                    conversation_thread_id,
                    workspace_id,
                    msg_type,
                    status,
                    token_usage,
                    infrastructure_usage,
                    token_credits,
                    infrastructure_credits,
                    total_credits,
                    created_at
                FROM conversation_usages
                WHERE conversation_response_id = %s
            """, (conversation_response_id,))

            row = await cur.fetchone()
            return dict(row) if row else None


async def get_thread_credits(conversation_thread_id: str) -> Dict[str, Any]:
    """
    Get total credits for a thread (across all query-response pairs).

    Args:
        conversation_thread_id: Thread identifier

    Returns:
        Dict with structure:
        {
            "conversation_thread_id": str,
            "total_credits": float,
            "token_credits": float,
            "infrastructure_credits": float,
            "pair_count": int
        }
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    %(conversation_thread_id)s as conversation_thread_id,
                    COALESCE(SUM(total_credits), 0) as total_credits,
                    COALESCE(SUM(token_credits), 0) as token_credits,
                    COALESCE(SUM(infrastructure_credits), 0) as infrastructure_credits,
                    COUNT(*) as pair_count
                FROM conversation_usages
                WHERE conversation_thread_id = %(conversation_thread_id)s
            """, {"conversation_thread_id": conversation_thread_id})

            row = await cur.fetchone()

            return {
                "conversation_thread_id": conversation_thread_id,
                "total_credits": float(row["total_credits"]) if row["total_credits"] else 0.0,
                "token_credits": float(row["token_credits"]) if row["token_credits"] else 0.0,
                "infrastructure_credits": float(row["infrastructure_credits"]) if row["infrastructure_credits"] else 0.0,
                "pair_count": row["pair_count"]
            }


async def get_workspace_credits(workspace_id: str) -> Dict[str, Any]:
    """
    Get total credits for a workspace (across all threads and pairs).

    Args:
        workspace_id: Workspace identifier

    Returns:
        Dict with structure:
        {
            "workspace_id": str,
            "total_credits": float,
            "token_credits": float,
            "infrastructure_credits": float,
            "thread_count": int,
            "pair_count": int
        }
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    %(workspace_id)s as workspace_id,
                    COALESCE(SUM(total_credits), 0) as total_credits,
                    COALESCE(SUM(token_credits), 0) as token_credits,
                    COALESCE(SUM(infrastructure_credits), 0) as infrastructure_credits,
                    COUNT(DISTINCT conversation_thread_id) as thread_count,
                    COUNT(*) as pair_count
                FROM conversation_usages
                WHERE workspace_id = %(workspace_id)s
            """, {"workspace_id": workspace_id})

            row = await cur.fetchone()

            return {
                "workspace_id": workspace_id,
                "total_credits": float(row["total_credits"]) if row["total_credits"] else 0.0,
                "token_credits": float(row["token_credits"]) if row["token_credits"] else 0.0,
                "infrastructure_credits": float(row["infrastructure_credits"]) if row["infrastructure_credits"] else 0.0,
                "thread_count": row["thread_count"],
                "pair_count": row["pair_count"]
            }
