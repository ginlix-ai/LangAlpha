"""Shared psycopg connection pool for the app-data database.

One module-level pool per connection string, configured to match the
LangGraph checkpointer pool (prepare_threshold=0, autocommit at creation).
Every app-data module reaches Postgres through ``get_db_connection``.
"""

import logging
from contextlib import asynccontextmanager

import anyio
from psycopg_pool import AsyncConnectionPool

from src.config.env import DB_SSLMODE
from src.config.settings import get_conversation_pool_max

logger = logging.getLogger(__name__)

# Module-level connection pool cache for conversation database operations
# This ensures we reuse connections across operations, reducing connection overhead
_conversation_db_pool_cache = {}

# Warn once per process, not once per connection — plaintext is the norm for a
# local Postgres and a per-connection warning would drown the log.
_warned_plaintext = False


def _warn_if_plaintext(conn) -> None:
    """Surface an unencrypted session so `prefer` can't downgrade silently."""
    global _warned_plaintext
    if _warned_plaintext or DB_SSLMODE != "prefer" or conn.pgconn.ssl_in_use:
        return
    _warned_plaintext = True
    logger.warning(
        "Postgres session is unencrypted — the server did not offer TLS "
        "(DB_SSLMODE=prefer). Set DB_SSLMODE=require to fail closed instead."
    )


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
        DB_SSLMODE: TLS mode (default: prefer)
    """
    import os

    from urllib.parse import quote_plus

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")

    return f"postgresql://{quote_plus(db_user)}:{quote_plus(db_password)}@{db_host}:{db_port}/{db_name}?sslmode={DB_SSLMODE}"


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
    _warn_if_plaintext(conn)
    logger.debug(
        "Configured conversation DB connection with prepare_threshold=0, autocommit=True"
    )


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
        pool_max = get_conversation_pool_max()
        logger.info(
            f"Creating PostgreSQL connection pool for conversations (max_size={pool_max})"
        )
        # Create pool with minimal configuration matching LangGraph pool
        _conversation_db_pool_cache[db_uri] = AsyncConnectionPool(
            conninfo=db_uri,
            min_size=1,
            max_size=pool_max,
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
    async with pool.connection(timeout=10) as conn:
        try:
            yield conn
        finally:
            # Reached when CancelledError or another exception interrupts async
            # context cleanup: bring the connection back to a state the pool can
            # reuse, or failing that stop the server working for a caller that
            # is already gone.
            import psycopg.pq
            from psycopg import capabilities

            status = conn.info.transaction_status
            if status != psycopg.pq.TransactionStatus.IDLE:
                logger.warning(
                    f"Connection not in IDLE state (status: {status.name}). "
                    "This can happen when async context cleanup is interrupted. "
                    "Attempting to clean up connection state."
                )
                try:
                    if status == psycopg.pq.TransactionStatus.ACTIVE:
                        # The query is still on the wire with its result
                        # unconsumed, so nothing can be sent on this connection
                        # from here - the pool closes and replaces ACTIVE
                        # connections. All that is left worth doing is telling
                        # the server to stop working for a caller that is gone.
                        #
                        # Shielded because an anyio parent scope (every SSE
                        # response body runs in one) re-delivers the caller's
                        # cancellation at every await, which would kill this at
                        # its first one - and CancelledError is not an
                        # Exception, so the handler below would never see it.
                        #
                        # Only cancel_safe on libpq 17+ honours its timeout;
                        # older builds fall back to a blocking PQcancel with no
                        # deadline, which must never run inside a shield. That
                        # timeout is the only bound here - any await added to
                        # this branch needs one of its own.
                        if capabilities.has_cancel_safe():
                            logger.debug(
                                "Connection in ACTIVE state, cancelling pending query "
                                "and returning it to the pool for replacement"
                            )
                            with anyio.CancelScope(shield=True):
                                await conn.cancel_safe(timeout=2.0)
                        else:
                            logger.debug(
                                "Connection in ACTIVE state, returning it to the pool "
                                "for replacement (libpq too old for a bounded cancel)"
                            )
                    elif status in (
                        psycopg.pq.TransactionStatus.INTRANS,
                        psycopg.pq.TransactionStatus.INERROR,
                    ):
                        # Transaction in progress or error - rollback
                        logger.debug(f"Connection in {status.name} state, rolling back")
                        await conn.rollback()

                        final_status = conn.info.transaction_status
                        if final_status == psycopg.pq.TransactionStatus.IDLE:
                            logger.debug("Connection successfully reset to IDLE state")
                        else:
                            logger.warning(
                                f"Connection still not IDLE after rollback "
                                f"(status: {final_status.name})"
                            )
                except Exception as cleanup_error:
                    logger.error(
                        f"Error during connection state cleanup: {cleanup_error}",
                        exc_info=True,
                    )


# ==================== Legacy Conversation History Operations ====================
# NOTE: conversation_history table has been removed. Use workspaces table instead.
# These functions are kept as stubs for backward compatibility during migration.


# ==================== Thread Operations ====================
