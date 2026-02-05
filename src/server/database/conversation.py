"""
Database utility functions for query-response logging.

Provides functions for creating, retrieving, and managing conversation history,
threads, queries, and responses in PostgreSQL.
"""

import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
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
            open=False
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

    Args:
        workspace_id: Workspace ID
        conn: Optional database connection to reuse
    """
    try:
        if conn:
            # Reuse provided connection
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT COUNT(*) as count
                    FROM conversation_thread
                    WHERE workspace_id = %s
                """, (workspace_id,))
                result = await cur.fetchone()
                return result['count']
        else:
            # Acquire new connection (backward compatibility)
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("""
                        SELECT COUNT(*) as count
                        FROM conversation_thread
                        WHERE workspace_id = %s
                    """, (workspace_id,))
                    result = await cur.fetchone()
                    return result['count']

    except Exception as e:
        logger.error(f"Error calculating thread index: {e}")
        return 0


async def create_thread(
    thread_id: str,
    workspace_id: str,
    current_status: str,
    msg_type: Optional[str] = None,
    thread_index: Optional[int] = None,
    title: Optional[str] = None,
    conn=None
) -> Dict[str, Any]:
    """
    Create a thread entry (thread_index auto-calculated if not provided).

    Args:
        thread_id: Thread ID
        workspace_id: Workspace ID
        current_status: Initial status
        msg_type: Message type
        thread_index: Optional thread index (calculated if not provided)
        title: Optional thread title
        conn: Optional database connection to reuse
    """
    # Calculate thread_index if not provided
    if thread_index is None:
        thread_index = await calculate_next_thread_index(workspace_id, conn=conn)

    try:
        if conn:
            # Reuse provided connection
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    INSERT INTO conversation_thread (thread_id, workspace_id, current_status, msg_type, thread_index, title)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING thread_id, workspace_id, current_status, msg_type, thread_index, title, created_at, updated_at
                """, (thread_id, workspace_id, current_status, msg_type, thread_index, title))
                result = await cur.fetchone()
                logger.info(f"[conversation_db] create_thread thread_id={thread_id} thread_index={thread_index} workspace_id={workspace_id}")
                return dict(result)
        else:
            # Acquire new connection (backward compatibility)
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("""
                        INSERT INTO conversation_thread (thread_id, workspace_id, current_status, msg_type, thread_index, title)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING thread_id, workspace_id, current_status, msg_type, thread_index, title, created_at, updated_at
                    """, (thread_id, workspace_id, current_status, msg_type, thread_index, title))
                    result = await cur.fetchone()
                    return dict(result)

    except Exception as e:
        logger.error(f"Error creating thread: {e}")
        raise


async def update_thread_status(thread_id: str, status: str, conn=None) -> bool:
    """
    Update thread status (completed, interrupted, error, timeout, etc.).

    Args:
        thread_id: Thread ID
        status: New status
        conn: Optional database connection to reuse
    """
    try:
        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    UPDATE conversation_thread
                    SET current_status = %s, updated_at = NOW()
                    WHERE thread_id = %s
                """, (status, thread_id))
                logger.info(f"[conversation_db] update_thread_status thread_id={thread_id} status={status}")
                return True
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("""
                        UPDATE conversation_thread
                        SET current_status = %s, updated_at = NOW()
                        WHERE thread_id = %s
                    """, (status, thread_id))
                    logger.info(f"[conversation_db] update_thread_status thread_id={thread_id} status={status}")
                    return True

    except Exception as e:
        logger.error(f"Error updating thread status: {e}")
        return False


async def ensure_thread_exists(
    workspace_id: str,
    thread_id: str,
    user_id: str,
    initial_query: str,
    initial_status: str = "in_progress",
    msg_type: Optional[str] = None,
) -> None:
    """
    Ensure conversation_thread exists before workflow starts.

    Uses a single database connection for all operations to reduce connection churn.
    Workspace must already exist (created via POST /workspaces).

    Args:
        workspace_id: Workspace ID (must exist)
        thread_id: Thread ID to create/resume
        user_id: User ID for logging
        initial_query: Initial query text (used as thread title)
        initial_status: Initial thread status
        msg_type: Message type (e.g., 'ptc')
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
                SELECT thread_id FROM conversation_thread WHERE thread_id = %s
            """, (thread_id,))
            thread_exists = await cur.fetchone()

        # Step 3: Create thread if it doesn't exist
        if not thread_exists:
            # Use initial query as thread title (truncate to 255 chars)
            title = initial_query[:255] if initial_query else None
            await create_thread(
                thread_id=thread_id,
                workspace_id=workspace_id,
                current_status=initial_status,
                msg_type=msg_type,
                thread_index=None,  # Will be calculated inside create_thread using same conn
                title=title,
                conn=conn
            )
        else:
            # Thread exists (resume scenario), update status
            await update_thread_status(thread_id, initial_status, conn=conn)
            logger.info(f"Resumed thread {thread_id}, updated status to {initial_status}")


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
                    FROM conversation_thread
                    WHERE workspace_id = %s
                """, (workspace_id,))

                total_result = await cur.fetchone()
                total_count = total_result['total']

                # Get threads
                query = f"""
                    SELECT
                        thread_id, workspace_id, current_status, msg_type, thread_index,
                        title, created_at, updated_at
                    FROM conversation_thread
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
                    FROM conversation_thread t
                    JOIN workspaces w ON t.workspace_id = w.workspace_id
                    WHERE w.user_id = %s AND w.status != 'deleted'
                    """,
                    (user_id,),
                )
                total_result = await cur.fetchone()
                total_count = total_result["total"] if total_result else 0

                query = f"""
                    SELECT
                        t.thread_id, t.workspace_id, t.current_status, t.msg_type, t.thread_index,
                        t.title, t.created_at, t.updated_at,
                        fq.content AS first_query_content
                    FROM conversation_thread t
                    JOIN workspaces w ON t.workspace_id = w.workspace_id
                    LEFT JOIN LATERAL (
                        SELECT q.content
                        FROM conversation_query q
                        WHERE q.thread_id = t.thread_id
                        ORDER BY q.pair_index ASC
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


async def get_thread_messages(
    thread_id: str,
    limit: Optional[int] = None,
    offset: int = 0,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], List[Dict[str, Any]], int]:
    """Get all messages for a single thread (chronologically ordered by pair_index)."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT thread_id, workspace_id, thread_index, current_status, msg_type, created_at, updated_at
                    FROM conversation_thread
                    WHERE thread_id = %s
                    """,
                    (thread_id,),
                )
                thread = await cur.fetchone()
                if not thread:
                    return None, None, [], 0

                thread = dict(thread)

                await cur.execute(
                    """
                    SELECT workspace_id, user_id, name, description, status,
                           created_at, updated_at
                    FROM workspaces
                    WHERE workspace_id = %s
                    """,
                    (thread["workspace_id"],),
                )
                workspace = await cur.fetchone()
                if not workspace:
                    return None, thread, [], 0

                workspace = dict(workspace)

                await cur.execute(
                    """
                    SELECT COUNT(*) as total
                    FROM conversation_query
                    WHERE thread_id = %s
                    """,
                    (thread_id,),
                )
                total_result = await cur.fetchone()
                total_count = total_result["total"] if total_result else 0

                if limit:
                    await cur.execute(
                        """
                        SELECT
                            t.thread_id,
                            t.thread_index,
                            q.pair_index,
                            q.query_id,
                            q.content as query_content,
                            q.type as query_type,
                            q.feedback_action,
                            q.metadata as query_metadata,
                            q.timestamp as query_timestamp,
                            r.response_id,
                            r.status,
                            r.interrupt_reason,
                            r.agent_messages,
                            r.execution_time,
                            r.warnings,
                            r.errors,
                            r.timestamp as response_timestamp
                        FROM conversation_query q
                        JOIN conversation_thread t ON q.thread_id = t.thread_id
                        LEFT JOIN conversation_response r ON q.thread_id = r.thread_id AND q.pair_index = r.pair_index
                        WHERE q.thread_id = %s
                        ORDER BY q.pair_index ASC
                        LIMIT %s OFFSET %s
                        """,
                        (thread_id, limit, offset),
                    )
                else:
                    await cur.execute(
                        """
                        SELECT
                            t.thread_id,
                            t.thread_index,
                            q.pair_index,
                            q.query_id,
                            q.content as query_content,
                            q.type as query_type,
                            q.feedback_action,
                            q.metadata as query_metadata,
                            q.timestamp as query_timestamp,
                            r.response_id,
                            r.status,
                            r.interrupt_reason,
                            r.agent_messages,
                            r.execution_time,
                            r.warnings,
                            r.errors,
                            r.timestamp as response_timestamp
                        FROM conversation_query q
                        JOIN conversation_thread t ON q.thread_id = t.thread_id
                        LEFT JOIN conversation_response r ON q.thread_id = r.thread_id AND q.pair_index = r.pair_index
                        WHERE q.thread_id = %s
                        ORDER BY q.pair_index ASC
                        """,
                        (thread_id,),
                    )

                messages = await cur.fetchall()
                return workspace, thread, [dict(row) for row in messages], total_count

    except Exception as e:
        logger.error(f"Error getting thread messages: {e}")
        raise


# ==================== Query Operations ====================

async def get_next_pair_index(thread_id: str, conn=None) -> int:
    """
    Calculate the next pair_index for a thread (0-based).

    Args:
        thread_id: Thread ID
        conn: Optional database connection to reuse
    """
    try:
        if conn:
            # Reuse provided connection
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT COUNT(*) as count
                    FROM conversation_query
                    WHERE thread_id = %s
                """, (thread_id,))
                result = await cur.fetchone()
                return result['count']
        else:
            # Acquire new connection (backward compatibility)
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("""
                        SELECT COUNT(*) as count
                        FROM conversation_query
                        WHERE thread_id = %s
                    """, (thread_id,))
                    result = await cur.fetchone()
                    return result['count']

    except Exception as e:
        logger.error(f"Error calculating pair index: {e}")
        return 0


async def create_query(
    query_id: str,
    thread_id: str,
    pair_index: int,
    content: str,
    query_type: str,
    feedback_action: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    timestamp: Optional[datetime] = None,
    conn=None,
    idempotent: bool = True
) -> Dict[str, Any]:
    """
    Create a query entry.

    Args:
        query_id: Query ID
        thread_id: Thread ID
        pair_index: Pair index
        content: Query content
        query_type: Query type
        feedback_action: Optional feedback action
        metadata: Optional metadata
        timestamp: Optional timestamp
        conn: Optional database connection to reuse
        idempotent: If True, use ON CONFLICT DO UPDATE for safe retries
    """
    if timestamp is None:
        timestamp = datetime.now()

    try:
        if conn:
            # Reuse provided connection
            async with conn.cursor(row_factory=dict_row) as cur:
                if idempotent:
                    # Idempotent: ON CONFLICT DO UPDATE for safe retries
                    await cur.execute("""
                        INSERT INTO conversation_query (
                            query_id, thread_id, pair_index, content, type,
                            feedback_action, metadata, timestamp
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (thread_id, pair_index) DO UPDATE
                        SET content = EXCLUDED.content,
                            type = EXCLUDED.type,
                            feedback_action = EXCLUDED.feedback_action,
                            metadata = EXCLUDED.metadata,
                            timestamp = EXCLUDED.timestamp
                        RETURNING query_id, thread_id, pair_index, content, type,
                                  feedback_action, metadata, timestamp
                    """, (query_id, thread_id, pair_index, content, query_type,
                          feedback_action, Json(metadata or {}), timestamp))
                else:
                    # Non-idempotent: fail on conflict
                    await cur.execute("""
                        INSERT INTO conversation_query (
                            query_id, thread_id, pair_index, content, type,
                            feedback_action, metadata, timestamp
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING query_id, thread_id, pair_index, content, type,
                                  feedback_action, metadata, timestamp
                    """, (query_id, thread_id, pair_index, content, query_type,
                          feedback_action, Json(metadata or {}), timestamp))
                result = await cur.fetchone()
                logger.info(f"[conversation_db] create_query query_id={query_id} thread_id={thread_id} pair_index={pair_index} type={query_type}")
                return dict(result)
        else:
            # Acquire new connection (backward compatibility)
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    if idempotent:
                        # Idempotent: ON CONFLICT DO UPDATE for safe retries
                        await cur.execute("""
                            INSERT INTO conversation_query (
                                query_id, thread_id, pair_index, content, type,
                                feedback_action, metadata, timestamp
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (thread_id, pair_index) DO UPDATE
                            SET content = EXCLUDED.content,
                                type = EXCLUDED.type,
                                feedback_action = EXCLUDED.feedback_action,
                                metadata = EXCLUDED.metadata,
                                timestamp = EXCLUDED.timestamp
                            RETURNING query_id, thread_id, pair_index, content, type,
                                      feedback_action, metadata, timestamp
                        """, (query_id, thread_id, pair_index, content, query_type,
                              feedback_action, Json(metadata or {}), timestamp))
                    else:
                        # Non-idempotent: fail on conflict
                        await cur.execute("""
                            INSERT INTO conversation_query (
                                query_id, thread_id, pair_index, content, type,
                                feedback_action, metadata, timestamp
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING query_id, thread_id, pair_index, content, type,
                                      feedback_action, metadata, timestamp
                        """, (query_id, thread_id, pair_index, content, query_type,
                              feedback_action, Json(metadata or {}), timestamp))
                    result = await cur.fetchone()
                    logger.info(f"[conversation_db] create_query query_id={query_id} thread_id={thread_id} pair_index={pair_index} type={query_type}")
                    return dict(result)

    except Exception as e:
        logger.error(f"Error creating query: {e}")
        raise


async def get_queries_for_thread(
    thread_id: str,
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
                    FROM conversation_query
                    WHERE thread_id = %s
                """, (thread_id,))

                total_result = await cur.fetchone()
                total_count = total_result['total']

                # Get queries
                if limit:
                    await cur.execute("""
                        SELECT
                            query_id, thread_id, pair_index, content, type,
                            feedback_action, metadata, timestamp
                        FROM conversation_query
                        WHERE thread_id = %s
                        ORDER BY pair_index ASC
                        LIMIT %s OFFSET %s
                    """, (thread_id, limit, offset))
                else:
                    await cur.execute("""
                        SELECT
                            query_id, thread_id, pair_index, content, type,
                            feedback_action, metadata, timestamp
                        FROM conversation_query
                        WHERE thread_id = %s
                        ORDER BY pair_index ASC
                    """, (thread_id,))

                queries = await cur.fetchall()
                return [dict(row) for row in queries], total_count

    except Exception as e:
        logger.error(f"Error getting queries for thread: {e}")
        raise


# ==================== Response Operations ====================

async def create_response(
    response_id: str,
    thread_id: str,
    pair_index: int,
    status: str,
    interrupt_reason: Optional[str] = None,
    agent_messages: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    state_snapshot: Optional[Dict[str, Any]] = None,
    warnings: Optional[List[str]] = None,
    errors: Optional[List[str]] = None,
    execution_time: Optional[float] = None,
    timestamp: Optional[datetime] = None,
    streaming_chunks: Optional[Any] = None,
    conn=None,
    idempotent: bool = True
) -> Dict[str, Any]:
    """
    Create a response entry.

    Args:
        response_id: Response ID
        thread_id: Thread ID
        pair_index: Pair index
        status: Status
        interrupt_reason: Optional interrupt reason
        agent_messages: Optional agent messages
        metadata: Optional metadata
        state_snapshot: Optional state snapshot
        warnings: Optional warnings
        errors: Optional errors
        execution_time: Optional execution time
        timestamp: Optional timestamp
        conn: Optional database connection to reuse
        idempotent: If True, use ON CONFLICT DO UPDATE for safe retries
    """
    if timestamp is None:
        timestamp = datetime.now()

    try:
        if conn:
            # Reuse provided connection
            async with conn.cursor(row_factory=dict_row) as cur:
                if idempotent:
                    # Idempotent: ON CONFLICT DO UPDATE for safe retries
                    await cur.execute("""
                        INSERT INTO conversation_response (
                            response_id, thread_id, pair_index, status,
                            interrupt_reason, agent_messages, metadata,
                            state_snapshot, warnings, errors, execution_time, timestamp,
                            streaming_chunks
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (thread_id, pair_index) DO UPDATE
                        SET status = EXCLUDED.status,
                            interrupt_reason = EXCLUDED.interrupt_reason,
                            agent_messages = EXCLUDED.agent_messages,
                            metadata = EXCLUDED.metadata,
                            state_snapshot = EXCLUDED.state_snapshot,
                            warnings = EXCLUDED.warnings,
                            errors = EXCLUDED.errors,
                            execution_time = EXCLUDED.execution_time,
                            timestamp = EXCLUDED.timestamp,
                            streaming_chunks = EXCLUDED.streaming_chunks
                        RETURNING response_id, thread_id, pair_index, status,
                                  interrupt_reason, agent_messages, metadata,
                                  state_snapshot, warnings, errors, execution_time, timestamp,
                                  streaming_chunks
                    """, (
                        response_id, thread_id, pair_index,
                        status, interrupt_reason,
                        Json(agent_messages) if agent_messages else None,
                        Json(metadata or {}),
                        Json(state_snapshot) if state_snapshot else None,
                        warnings or [],
                        errors or [],
                        execution_time,
                        timestamp,
                        Json(streaming_chunks) if streaming_chunks else None
                    ))
                else:
                    # Non-idempotent: fail on conflict
                    await cur.execute("""
                        INSERT INTO conversation_response (
                            response_id, thread_id, pair_index, status,
                            interrupt_reason, agent_messages, metadata,
                            state_snapshot, warnings, errors, execution_time, timestamp,
                            streaming_chunks
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING response_id, thread_id, pair_index, status,
                                  interrupt_reason, agent_messages, metadata,
                                  state_snapshot, warnings, errors, execution_time, timestamp,
                                  streaming_chunks
                    """, (
                        response_id, thread_id, pair_index,
                        status, interrupt_reason,
                        Json(agent_messages) if agent_messages else None,
                        Json(metadata or {}),
                        Json(state_snapshot) if state_snapshot else None,
                        warnings or [],
                        errors or [],
                        execution_time,
                        timestamp,
                        Json(streaming_chunks) if streaming_chunks else None
                    ))
                result = await cur.fetchone()
                logger.info(f"[conversation_db] create_response response_id={response_id} thread_id={thread_id} pair_index={pair_index} status={status}")
                return dict(result)
        else:
            # Acquire new connection (backward compatibility)
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    if idempotent:
                        # Idempotent: ON CONFLICT DO UPDATE for safe retries
                        await cur.execute("""
                            INSERT INTO conversation_response (
                                response_id, thread_id, pair_index, status,
                                interrupt_reason, agent_messages, metadata,
                                state_snapshot, warnings, errors, execution_time, timestamp,
                                streaming_chunks
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (thread_id, pair_index) DO UPDATE
                            SET status = EXCLUDED.status,
                                interrupt_reason = EXCLUDED.interrupt_reason,
                                agent_messages = EXCLUDED.agent_messages,
                                metadata = EXCLUDED.metadata,
                                state_snapshot = EXCLUDED.state_snapshot,
                                warnings = EXCLUDED.warnings,
                                errors = EXCLUDED.errors,
                                execution_time = EXCLUDED.execution_time,
                                timestamp = EXCLUDED.timestamp,
                                streaming_chunks = EXCLUDED.streaming_chunks
                            RETURNING response_id, thread_id, pair_index, status,
                                      interrupt_reason, agent_messages, metadata,
                                      state_snapshot, warnings, errors, execution_time, timestamp,
                                      streaming_chunks
                        """, (
                            response_id, thread_id, pair_index,
                            status, interrupt_reason,
                            Json(agent_messages) if agent_messages else None,
                            Json(metadata or {}),
                            Json(state_snapshot) if state_snapshot else None,
                            warnings or [],
                            errors or [],
                            execution_time,
                            timestamp,
                            Json(streaming_chunks) if streaming_chunks else None
                        ))
                    else:
                        # Non-idempotent: fail on conflict
                        await cur.execute("""
                            INSERT INTO conversation_response (
                                response_id, thread_id, pair_index, status,
                                interrupt_reason, agent_messages, metadata,
                                state_snapshot, warnings, errors, execution_time, timestamp,
                                streaming_chunks
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING response_id, thread_id, pair_index, status,
                                      interrupt_reason, agent_messages, metadata,
                                      state_snapshot, warnings, errors, execution_time, timestamp,
                                      streaming_chunks
                        """, (
                            response_id, thread_id, pair_index,
                            status, interrupt_reason,
                            Json(agent_messages) if agent_messages else None,
                            Json(metadata or {}),
                            Json(state_snapshot) if state_snapshot else None,
                            warnings or [],
                            errors or [],
                            execution_time,
                            timestamp,
                            Json(streaming_chunks) if streaming_chunks else None
                        ))
                    result = await cur.fetchone()
                    logger.info(f"[conversation_db] create_response response_id={response_id} thread_id={thread_id} pair_index={pair_index} status={status}")
                    return dict(result)

    except Exception as e:
        logger.error(f"Error creating response: {e}")
        raise


async def get_responses_for_thread(
    thread_id: str,
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
                    FROM conversation_response
                    WHERE thread_id = %s
                """, (thread_id,))

                total_result = await cur.fetchone()
                total_count = total_result['total']

                # Get responses
                if limit:
                    await cur.execute("""
                        SELECT
                            response_id, thread_id, pair_index, status,
                            interrupt_reason, agent_messages, metadata,
                            state_snapshot, warnings, errors, execution_time, timestamp,
                            streaming_chunks
                        FROM conversation_response
                        WHERE thread_id = %s
                        ORDER BY pair_index ASC
                        LIMIT %s OFFSET %s
                    """, (thread_id, limit, offset))
                else:
                    await cur.execute("""
                        SELECT
                            response_id, thread_id, pair_index, status,
                            interrupt_reason, agent_messages, metadata,
                            state_snapshot, warnings, errors, execution_time, timestamp,
                            streaming_chunks
                        FROM conversation_response
                        WHERE thread_id = %s
                        ORDER BY pair_index ASC
                    """, (thread_id,))

                responses = await cur.fetchall()
                return [dict(row) for row in responses], total_count

    except Exception as e:
        logger.error(f"Error getting responses for thread: {e}")
        raise


# ==================== Query-Response Pair Operations ====================

async def get_query_response_pairs(
    thread_id: str,
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
                    FROM conversation_query
                    WHERE thread_id = %s
                """, (thread_id,))

                total_result = await cur.fetchone()
                total_count = total_result['total']

                # Get joined query-response pairs
                if limit:
                    await cur.execute("""
                        SELECT
                            q.query_id, q.thread_id, q.pair_index, q.content as query_content,
                            q.type as query_type, q.feedback_action, q.metadata as query_metadata,
                            q.timestamp as query_timestamp,
                            r.response_id, r.status, r.interrupt_reason,
                            r.agent_messages, r.metadata as response_metadata,
                            r.state_snapshot, r.warnings, r.errors, r.execution_time,
                            r.timestamp as response_timestamp

                        FROM conversation_query q
                        LEFT JOIN conversation_response r ON q.thread_id = r.thread_id AND q.pair_index = r.pair_index
                        WHERE q.thread_id = %s
                        ORDER BY q.pair_index ASC
                        LIMIT %s OFFSET %s
                    """, (thread_id, limit, offset))
                else:
                    await cur.execute("""
                        SELECT
                            q.query_id, q.thread_id, q.pair_index, q.content as query_content,
                            q.type as query_type, q.feedback_action, q.metadata as query_metadata,
                            q.timestamp as query_timestamp,
                            r.response_id, r.status, r.interrupt_reason,
                            r.agent_messages, r.metadata as response_metadata,
                            r.state_snapshot, r.warnings, r.errors, r.execution_time,
                            r.timestamp as response_timestamp

                        FROM conversation_query q
                        LEFT JOIN conversation_response r ON q.thread_id = r.thread_id AND q.pair_index = r.pair_index
                        WHERE q.thread_id = %s
                        ORDER BY q.pair_index ASC
                    """, (thread_id,))

                pairs = await cur.fetchall()
                return [dict(row) for row in pairs], total_count

    except Exception as e:
        logger.error(f"Error getting query-response pairs for thread: {e}")
        raise


# ==================== Extended Operations for API v2 ====================

async def get_thread_with_summary(thread_id: str) -> Optional[Dict[str, Any]]:
    """Get thread with enriched summary data (pair count, costs, etc.)."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Get thread basic info
                await cur.execute("""
                    SELECT thread_id, workspace_id, current_status, thread_index, created_at, updated_at
                    FROM conversation_thread
                    WHERE thread_id = %s
                """, (thread_id,))

                thread = await cur.fetchone()
                if not thread:
                    return None

                thread = dict(thread)

                # Get aggregated pair data
                await cur.execute("""
                    SELECT
                        COUNT(q.pair_index) as pair_count,
                        COALESCE(SUM((u.token_usage->>'total_cost')::float), 0) as total_cost,
                        COALESCE(SUM(r.execution_time), 0) as total_execution_time,
                        MAX(q.type) as last_query_type,
                        BOOL_OR(COALESCE(array_length(r.errors, 1), 0) > 0) as has_errors
                    FROM conversation_query q
                    LEFT JOIN conversation_response r ON q.thread_id = r.thread_id AND q.pair_index = r.pair_index
                    LEFT JOIN conversation_usage u ON r.response_id = u.response_id
                    WHERE q.thread_id = %s
                """, (thread_id,))

                stats = await cur.fetchone()
                if stats:
                    thread.update(dict(stats))

                return thread

    except Exception as e:
        logger.error(f"Error getting thread with summary: {e}")
        raise


async def get_response_full_detail(thread_id: str, pair_index: int) -> Optional[Dict[str, Any]]:
    """Get complete response details including state_snapshot and agent_messages."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT
                        response_id, thread_id, pair_index, status,
                        interrupt_reason, agent_messages, metadata,
                        state_snapshot, warnings, errors, execution_time, timestamp
                    FROM conversation_response
                    WHERE thread_id = %s AND pair_index = %s
                """, (thread_id, pair_index))

                response = await cur.fetchone()
                return dict(response) if response else None

    except Exception as e:
        logger.error(f"Error getting response full detail: {e}")
        raise


async def get_response_by_id(response_id: str) -> Optional[Dict[str, Any]]:
    """Get complete response details by response_id (admin/debug endpoint)."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT
                        response_id, thread_id, pair_index, status,
                        interrupt_reason, agent_messages, metadata,
                        state_snapshot, warnings, errors, execution_time, timestamp
                    FROM conversation_response
                    WHERE response_id = %s
                """, (response_id,))

                response = await cur.fetchone()
                return dict(response) if response else None

    except Exception as e:
        logger.error(f"Error getting response by ID: {e}")
        raise


async def delete_thread(thread_id: str) -> bool:
    """Delete thread (CASCADE to queries, responses)."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    DELETE FROM conversation_thread
                    WHERE thread_id = %s
                """, (thread_id,))

                logger.info(f"Deleted thread: {thread_id}")
                return True

    except Exception as e:
        logger.error(f"Error deleting thread: {e}")
        raise


async def update_thread_title(thread_id: str, title: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Update thread title.

    Args:
        thread_id: Thread ID
        title: New title (can be None to clear title)

    Returns:
        Updated thread dict, or None if thread not found
    """
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    UPDATE conversation_thread
                    SET title = %s, updated_at = NOW()
                    WHERE thread_id = %s
                    RETURNING thread_id, workspace_id, current_status, msg_type, thread_index, title, created_at, updated_at
                """, (title, thread_id))

                result = await cur.fetchone()
                if result:
                    logger.info(f"[conversation_db] update_thread_title thread_id={thread_id} title={title}")
                    return dict(result)
                return None

    except Exception as e:
        logger.error(f"Error updating thread title: {e}")
        raise


async def get_thread_by_id(thread_id: str) -> Optional[Dict[str, Any]]:
    """
    Get thread by ID.

    Args:
        thread_id: Thread ID

    Returns:
        Thread dict or None if not found
    """
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT thread_id, workspace_id, current_status, msg_type, thread_index, title, created_at, updated_at
                    FROM conversation_thread
                    WHERE thread_id = %s
                """, (thread_id,))

                result = await cur.fetchone()
                return dict(result) if result else None

    except Exception as e:
        logger.error(f"Error getting thread by id: {e}")
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
                        COUNT(DISTINCT t.thread_id) as total_threads,
                        COUNT(DISTINCT q.query_id) as total_queries,
                        COUNT(DISTINCT r.response_id) as total_responses,
                        COALESCE(SUM((u.token_usage->>'total_cost')::float), 0) as total_cost,
                        COALESCE(SUM(r.execution_time), 0) as total_execution_time,
                        MIN(t.created_at) as first_activity,
                        MAX(t.updated_at) as last_activity
                    FROM workspaces w
                    LEFT JOIN conversation_thread t ON w.workspace_id = t.workspace_id
                    LEFT JOIN conversation_query q ON t.thread_id = q.thread_id
                    LEFT JOIN conversation_response r ON t.thread_id = r.thread_id
                    LEFT JOIN conversation_usage u ON r.response_id = u.response_id
                    WHERE w.user_id = %s
                """, (user_id,))
                stats = await cur.fetchone()

                # Get status breakdown
                await cur.execute("""
                    SELECT
                        t.current_status,
                        COUNT(*) as count
                    FROM workspaces w
                    JOIN conversation_thread t ON w.workspace_id = t.workspace_id
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
                        COUNT(DISTINCT t.thread_id) as total_threads,
                        COUNT(DISTINCT q.query_id) as total_pairs,
                        COALESCE(SUM((u.token_usage->>'total_cost')::float), 0) as total_cost,
                        COALESCE(SUM(r.execution_time), 0) as total_execution_time
                    FROM conversation_thread t
                    LEFT JOIN conversation_query q ON t.thread_id = q.thread_id
                    LEFT JOIN conversation_response r ON t.thread_id = r.thread_id
                    LEFT JOIN conversation_usage u ON r.response_id = u.response_id
                    WHERE t.workspace_id = %s
                """, (workspace_id,))
                stats = await cur.fetchone()

                # Get status breakdown
                await cur.execute("""
                    SELECT
                        current_status,
                        COUNT(*) as count
                    FROM conversation_thread
                    WHERE workspace_id = %s
                    GROUP BY current_status
                """, (workspace_id,))
                status_rows = await cur.fetchall()
                by_status = {row['current_status']: row['count'] for row in status_rows}

                # Get cost breakdown by model
                await cur.execute("""
                    SELECT
                        u.token_usage
                    FROM conversation_thread t
                    JOIN conversation_response r ON t.thread_id = r.thread_id
                    JOIN conversation_usage u ON r.response_id = u.response_id
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




# ============================================================================
# Filesystem Persistence Functions
# ============================================================================


async def ensure_filesystem(workspace_id: str) -> str:
    """
    Create filesystem if doesn't exist (lazy creation).

    Args:
        workspace_id: Workspace ID to create filesystem for

    Returns:
        filesystem_id (same as workspace_id)
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Use ON CONFLICT DO NOTHING for idempotent insert
            await cur.execute("""
                INSERT INTO workspace_filesystems (filesystem_id, workspace_id)
                VALUES (%s, %s)
                ON CONFLICT (workspace_id) DO NOTHING
            """, (workspace_id, workspace_id))

            logger.debug(f"Ensured filesystem exists for workspace {workspace_id}")
            return workspace_id


async def upsert_file(
    filesystem_id: str,
    file_path: str,
    content: Optional[str],
    line_count: int,
    updated_in_thread_id: str,
    updated_in_pair_index: int,
    created_in_thread_id: Optional[str] = None,
    created_in_pair_index: Optional[int] = None
) -> str:
    """
    Insert or update file in filesystem.

    Args:
        filesystem_id: Filesystem ID (same as workspace_id)
        file_path: Full file path (e.g., /report/tesla.md)
        content: File contents (None = soft-deleted file)
        line_count: Number of lines in file
        updated_in_thread_id: Thread that last modified this file
        updated_in_pair_index: Pair index when last modified
        created_in_thread_id: Thread that created the file (optional)
        created_in_pair_index: Pair index when created (optional)

    Returns:
        file_id
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Check if file exists
            await cur.execute("""
                SELECT file_id, created_in_thread_id, created_in_pair_index
                FROM workspace_files
                WHERE filesystem_id = %s AND file_path = %s
            """, (filesystem_id, file_path))

            existing = await cur.fetchone()

            if existing:
                # Update existing file
                await cur.execute("""
                    UPDATE workspace_files
                    SET content = %s,
                        line_count = %s,
                        updated_in_thread_id = %s,
                        updated_in_pair_index = %s,
                        updated_at = NOW()
                    WHERE file_id = %s
                """, (content, line_count, updated_in_thread_id, updated_in_pair_index, existing['file_id']))

                logger.debug(f"Updated file {file_path} in filesystem {filesystem_id}")
                return existing['file_id']
            else:
                # Insert new file
                import uuid
                file_id = str(uuid.uuid4())

                await cur.execute("""
                    INSERT INTO workspace_files (
                        file_id, filesystem_id, file_path, content, line_count,
                        created_in_thread_id, created_in_pair_index,
                        updated_in_thread_id, updated_in_pair_index
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    file_id, filesystem_id, file_path, content, line_count,
                    created_in_thread_id or updated_in_thread_id,
                    created_in_pair_index if created_in_pair_index is not None else updated_in_pair_index,
                    updated_in_thread_id, updated_in_pair_index
                ))

                logger.debug(f"Created file {file_path} in filesystem {filesystem_id}")
                return file_id


async def update_file_metadata(
    file_id: str,
    content: Optional[str] = None,
    line_count: Optional[int] = None,
    updated_in_thread_id: Optional[str] = None,
    updated_in_pair_index: Optional[int] = None,
) -> None:
    """
    Update file metadata and content after an operation.

    Only updates fields that are provided (not None).
    Use content="" (empty string) to clear content, content=None to leave unchanged.

    Args:
        file_id: File ID to update
        content: Current file content (optional, None = no change)
        line_count: New line count (optional)
        updated_in_thread_id: Thread that performed the update (optional)
        updated_in_pair_index: Pair index when updated (optional)
    """
    # Build dynamic SET clause based on provided values
    updates = []
    params = []

    # Note: content=None means "don't update", content="" means "set to empty"
    # We use a sentinel to distinguish between "not provided" and "set to None/empty"
    if content is not None:
        updates.append("content = %s")
        params.append(content)

    if line_count is not None:
        updates.append("line_count = %s")
        params.append(line_count)

    if updated_in_thread_id is not None:
        updates.append("updated_in_thread_id = %s")
        params.append(updated_in_thread_id)

    if updated_in_pair_index is not None:
        updates.append("updated_in_pair_index = %s")
        params.append(updated_in_pair_index)

    if not updates:
        return  # Nothing to update

    updates.append("updated_at = NOW()")
    params.append(file_id)

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                UPDATE workspace_files
                SET {', '.join(updates)}
                WHERE file_id = %s
            """, tuple(params))

            logger.debug(f"Updated file metadata for file_id={file_id}")


async def get_file_content(file_id: str) -> Optional[str]:
    """
    Get current content for a file.

    Args:
        file_id: File ID

    Returns:
        File content string, or None if not found or content is NULL
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT content FROM workspace_files WHERE file_id = %s
            """, (file_id,))
            row = await cur.fetchone()
            return row['content'] if row else None


async def get_files_for_workspace(workspace_id: str) -> Dict[str, dict]:
    """
    Get all current files in workspace filesystem.

    Args:
        workspace_id: Workspace ID

    Returns:
        Dict mapping file_path to file info dict
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT f.*
                FROM workspace_files f
                JOIN workspace_filesystems fs ON f.filesystem_id = fs.filesystem_id
                WHERE fs.workspace_id = %s AND f.content IS NOT NULL
            """, (workspace_id,))

            rows = await cur.fetchall()

            return {
                row['file_path']: dict(row)
                for row in rows
            }


async def log_file_operation(
    file_id: str,
    operation: str,
    thread_id: str,
    pair_index: int,
    agent: str,
    tool_call_id: Optional[str] = None,
    operation_index: Optional[int] = None,
    old_string: Optional[str] = None,
    new_string: Optional[str] = None,
    timestamp: Optional[datetime] = None
) -> str:
    """
    Log file operation to audit trail.

    Args:
        file_id: File ID
        operation: Operation type (write_file, edit_file, delete)
        thread_id: Thread where operation occurred
        pair_index: Query-response pair index
        agent: Agent that performed operation
        tool_call_id: LangChain tool call ID (optional)
        operation_index: Sequential index per file (0, 1, 2, ...) for guaranteed ordering (optional)
        old_string: For edit_file: string being replaced (optional)
        new_string: For edit_file: replacement string (optional)
        timestamp: Operation timestamp (optional, defaults to NOW())

    Returns:
        operation_id
    """
    import uuid
    operation_id = str(uuid.uuid4())

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            if timestamp:
                await cur.execute("""
                    INSERT INTO workspace_file_operations (
                        operation_id, file_id, operation, thread_id, pair_index,
                        agent, tool_call_id, operation_index, old_string, new_string, timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    operation_id, file_id, operation, thread_id, pair_index,
                    agent, tool_call_id, operation_index, old_string, new_string, timestamp
                ))
            else:
                await cur.execute("""
                    INSERT INTO workspace_file_operations (
                        operation_id, file_id, operation, thread_id, pair_index,
                        agent, tool_call_id, operation_index, old_string, new_string
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    operation_id, file_id, operation, thread_id, pair_index,
                    agent, tool_call_id, operation_index, old_string, new_string
                ))

            logger.debug(f"Logged {operation} operation for file {file_id} (index: {operation_index})")
            return operation_id


async def get_max_operation_index_for_file(file_id: str, conn=None) -> int:
    """
    Get the maximum operation_index for a file from the database.

    This is used to ensure operation_index increments correctly across threads.

    Args:
        file_id: File ID to query
        conn: Optional existing connection to reuse (for transactions)

    Returns:
        Maximum operation_index for the file, or -1 if no operations exist yet
    """
    async def _query(connection):
        async with connection.cursor() as cur:
            await cur.execute("""
                SELECT COALESCE(MAX(operation_index), -1) as max_index
                FROM workspace_file_operations
                WHERE file_id = %s
            """, (file_id,))
            result = await cur.fetchone()
            return result[0] if result else -1

    if conn is not None:
        # Reuse existing connection (for transactions)
        return await _query(conn)
    else:
        # Create new connection
        async with get_db_connection() as connection:
            return await _query(connection)


async def get_operations_for_thread(thread_id: str, pair_index: int) -> List[dict]:
    """
    Get all file operations for a specific thread and pair_index.

    Normalizes the data for frontend consumption:
    - write_file: Uses new_string as content (full file content)
    - edit_file: Returns only old_string/new_string as diffs (no content field)

    Args:
        thread_id: Thread ID
        pair_index: Query-response pair index

    Returns:
        List of operation dicts with file info
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT op.*, f.file_path, f.line_count
                FROM workspace_file_operations op
                JOIN workspace_files f ON op.file_id = f.file_id
                WHERE op.thread_id = %s AND op.pair_index = %s
                ORDER BY op.timestamp ASC
            """, (thread_id, pair_index))

            rows = await cur.fetchall()

            # Normalize: For write_file, use new_string as content
            operations = []
            for row in rows:
                op = dict(row)
                if op['operation'] == 'write_file':
                    # For write_file: new_string contains full content
                    op['content'] = op['new_string']
                    # Clear new_string to avoid confusion (it's not a diff)
                    op['new_string'] = None
                else:  # edit_file
                    # For edit_file: only return diffs (old_string/new_string)
                    # Set content to None to avoid sending large file content
                    op['content'] = None

                operations.append(op)

            return operations


async def get_file_snapshot_before_thread(workspace_id: str, thread_index: int) -> Dict[str, dict]:
    """
    Get complete file state as it was BEFORE the given thread_index started.

    This returns files that existed and were last updated in threads with
    thread_index < the specified thread_index.

    Args:
        workspace_id: Workspace ID
        thread_index: Thread index to get snapshot before

    Returns:
        Dict mapping file_path to file info
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Get all files that were last updated before this thread
            await cur.execute("""
                SELECT DISTINCT ON (f.file_path)
                    f.file_id,
                    f.file_path,
                    f.content,
                    f.line_count,
                    f.updated_in_thread_id,
                    f.updated_in_pair_index
                FROM workspace_files f
                JOIN workspace_filesystems fs ON f.filesystem_id = fs.filesystem_id
                JOIN conversation_thread t_updated ON f.updated_in_thread_id = t_updated.thread_id
                WHERE fs.workspace_id = %s
                  AND t_updated.thread_index < %s
                  AND f.content IS NOT NULL
                ORDER BY f.file_path, t_updated.thread_index DESC
            """, (workspace_id, thread_index))

            rows = await cur.fetchall()

            return {
                row['file_path']: {
                    'file_id': row['file_id'],
                    'content': row['content'],
                    'line_count': row['line_count'],
                    'updated_in_thread_id': row['updated_in_thread_id'],
                    'updated_in_pair_index': row['updated_in_pair_index']
                }
                for row in rows
            }


# ========== Usage Tracking Functions ==========

async def create_usage_record(
    usage_data: Dict[str, Any],
    conn: Optional[AsyncConnection] = None
) -> bool:
    """
    Create a usage record in conversation_usage table.

    Args:
        usage_data: Usage data dict with structure:
            {
                "usage_id": str,
                "response_id": str,
                "user_id": str,
                "thread_id": str,
                "workspace_id": str,
                "msg_type": str,
                "status": str,
                "token_usage": dict (JSONB),
                "infrastructure_usage": dict (JSONB, optional),
                "token_credits": float,
                "infrastructure_credits": float,
                "total_credits": float,
                "timestamp": datetime
            }
        conn: Optional connection (for transactions)

    Returns:
        True if successful

    Raises:
        psycopg.Error: On database errors
    """
    async def _create(cur):
        await cur.execute("""
            INSERT INTO conversation_usage (
                usage_id,
                response_id,
                user_id,
                thread_id,
                workspace_id,
                msg_type,
                status,
                token_usage,
                infrastructure_usage,
                token_credits,
                infrastructure_credits,
                total_credits,
                timestamp
            ) VALUES (
                %(usage_id)s,
                %(response_id)s,
                %(user_id)s,
                %(thread_id)s,
                %(workspace_id)s,
                %(msg_type)s,
                %(status)s,
                %(token_usage)s,
                %(infrastructure_usage)s,
                %(token_credits)s,
                %(infrastructure_credits)s,
                %(total_credits)s,
                %(timestamp)s
            )
        """, {
            "usage_id": usage_data["usage_id"],
            "response_id": usage_data["response_id"],
            "user_id": usage_data["user_id"],
            "thread_id": usage_data["thread_id"],
            "workspace_id": usage_data["workspace_id"],
            "msg_type": usage_data.get("msg_type", "chat"),
            "status": usage_data.get("status", "completed"),
            "token_usage": Json(usage_data.get("token_usage")),
            "infrastructure_usage": Json(usage_data.get("infrastructure_usage")),
            "token_credits": usage_data["token_credits"],
            "infrastructure_credits": usage_data["infrastructure_credits"],
            "total_credits": usage_data["total_credits"],
            "timestamp": usage_data["timestamp"]
        })

    if conn:
        async with conn.cursor() as cur:
            await _create(cur)
    else:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await _create(cur)

    return True


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
        date_filter += " AND timestamp >= %(start_date)s"
        params["start_date"] = start_date
    if end_date:
        date_filter += " AND timestamp < %(end_date)s"
        params["end_date"] = end_date

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT
                    %(user_id)s as user_id,
                    COALESCE(SUM(total_credits), 0) as total_credits,
                    COALESCE(SUM(token_credits), 0) as token_credits,
                    COALESCE(SUM(infrastructure_credits), 0) as infrastructure_credits,
                    COUNT(DISTINCT thread_id) as workflow_count
                FROM conversation_usage
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
        List of usage records ordered by timestamp DESC
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    usage_id,
                    response_id,
                    thread_id,
                    workspace_id,
                    pair_index,
                    token_credits,
                    infrastructure_credits,
                    total_credits,
                    timestamp,
                    metadata
                FROM conversation_usage
                WHERE user_id = %s
                  AND timestamp >= NOW() - INTERVAL '%s days'
                ORDER BY timestamp DESC
                LIMIT %s
            """, (user_id, days, limit))

            rows = await cur.fetchall()
            return [dict(row) for row in rows]


async def get_response_usage(response_id: str) -> Optional[Dict[str, Any]]:
    """
    Get usage record for a specific response.

    Args:
        response_id: Response identifier

    Returns:
        Usage record dict or None if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    usage_id,
                    response_id,
                    user_id,
                    thread_id,
                    workspace_id,
                    msg_type,
                    status,
                    token_usage,
                    infrastructure_usage,
                    token_credits,
                    infrastructure_credits,
                    total_credits,
                    timestamp,
                    created_at,
                    updated_at
                FROM conversation_usage
                WHERE response_id = %s
            """, (response_id,))

            row = await cur.fetchone()
            return dict(row) if row else None


async def get_thread_credits(thread_id: str) -> Dict[str, Any]:
    """
    Get total credits for a thread (across all query-response pairs).

    Args:
        thread_id: Thread identifier

    Returns:
        Dict with structure:
        {
            "thread_id": str,
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
                    %(thread_id)s as thread_id,
                    COALESCE(SUM(total_credits), 0) as total_credits,
                    COALESCE(SUM(token_credits), 0) as token_credits,
                    COALESCE(SUM(infrastructure_credits), 0) as infrastructure_credits,
                    COUNT(*) as pair_count
                FROM conversation_usage
                WHERE thread_id = %(thread_id)s
            """, {"thread_id": thread_id})

            row = await cur.fetchone()

            return {
                "thread_id": thread_id,
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
                    COUNT(DISTINCT thread_id) as thread_count,
                    COUNT(*) as pair_count
                FROM conversation_usage
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
