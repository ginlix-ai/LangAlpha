"""
FastAPI application setup, initialization, and middleware configuration.

This module contains:
- Application lifespan management (startup/shutdown)
- Global state initialization (agent_config, session_service, checkpointer)
- Middleware setup (CORS, request ID)
- Router registration
"""

# ============================================================================
# Imports and Global Variables
# ============================================================================
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from urllib.parse import urlparse
from uuid import uuid4

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.config.logging_config import configure_logging
from src.config.settings import (
    get_allowed_origins,
)
from src.server.services.background_task_manager import BackgroundTaskManager
from src.server.services.background_registry_store import BackgroundRegistryStore

logger = logging.getLogger(__name__)
INTERNAL_SERVER_ERROR_DETAIL = "Internal Server Error"

# Global variables
agent_config = None  # PTC Agent configuration (loaded from config files)
session_service = None  # PTC Session service instance
workspace_manager = None  # Workspace manager instance
checkpointer = None  # PTC Agent LangGraph checkpointer for state persistence
graph = None  # Most recently used LangGraph (for persistence snapshots)


# ============================================================================
# Lifespan Context Manager
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize resources when server starts, cleanup when stops."""
    global agent_config, session_service, workspace_manager, checkpointer

    # Configure logging based on environment settings (first thing on startup)
    configure_logging()


    # Initialize and open conversation database pool
    from src.server.database.conversation_db import get_or_create_pool
    conv_pool = get_or_create_pool()
    # Extract connection details from pool
    conninfo = conv_pool._conninfo if hasattr(conv_pool, '_conninfo') else "unknown"
    try:
        # Parse basic connection info (format: postgresql://user:pass@host:port/dbname?sslmode=...)
        import re
        match = re.search(r'@([^:]+):(\d+)/([^?]+)', conninfo)
        if match:
            db_host, db_port, db_name = match.groups()
            await conv_pool.open()
            # Validate pool is ready with a simple health check
            async with conv_pool.connection() as conn:
                await conn.execute("SELECT 1")
            logger.info(f"Conversation DB: Connected to {db_host}:{db_port}/{db_name}")
        else:
            await conv_pool.open()
            # Validate pool is ready with a simple health check
            async with conv_pool.connection() as conn:
                await conn.execute("SELECT 1")
            logger.info("Conversation DB: Connected successfully")
    except Exception as e:
        if match:
            logger.error(f"Conversation DB: Failed to connect to {db_host}:{db_port}/{db_name} - {e}")
        else:
            logger.error(f"Conversation DB: Failed to connect - {e}")
        raise

    # Initialize Redis cache
    try:
        from src.utils.cache.redis_cache import init_cache

        logger.info("Initializing Redis cache client...")
        await init_cache()
        logger.info("Redis cache client initialized")

    except Exception as e:
        logger.warning(f"Redis cache initialization failed: {e}")
        logger.warning("Server will continue without caching")

    # Start BackgroundTaskManager cleanup task
    try:
        manager = BackgroundTaskManager.get_instance()
        await manager.start_cleanup_task()
    except Exception as e:
        logger.warning(
            f"Failed to start BackgroundTaskManager cleanup task: {e}")

    # Initialize PTC Agent configuration and session service
    try:
        from ptc_agent.config import load_from_files, ConfigContext

        logger.info("Loading PTC Agent configuration...")
        agent_config = await load_from_files(context=ConfigContext.SDK)
        agent_config.validate_api_keys()
        logger.info("PTC Agent configuration loaded successfully")

        # Initialize session service
        from src.server.services.session_manager import SessionService
        session_service = SessionService.get_instance(
            config=agent_config,
            idle_timeout=1800,  # 30 minutes
            cleanup_interval=300,  # 5 minutes
        )
        await session_service.start_cleanup_task()
        logger.info("PTC Session Service initialized")

        # Initialize workspace manager
        from src.server.services.workspace_manager import WorkspaceManager
        workspace_manager = WorkspaceManager.get_instance(
            config=agent_config,
            idle_timeout=1800,  # 30 minutes
            cleanup_interval=300,  # 5 minutes
        )
        await workspace_manager.start_cleanup_task()
        logger.info("Workspace Manager initialized")

        # Initialize PTC Agent checkpointer for state persistence
        from src.server.utils.checkpointer import get_checkpointer, open_checkpointer_pool
        checkpointer = get_checkpointer(
            memory_type=os.getenv("MEMORY_DB_TYPE", "postgres")
        )
        await open_checkpointer_pool(checkpointer)
        logger.info("PTC Agent checkpointer initialized")

    except FileNotFoundError as e:
        logger.warning(f"PTC Agent config not found: {e}")
        logger.warning("PTC Agent endpoints will not be available")
    except Exception as e:
        logger.warning(f"Failed to initialize PTC Agent: {e}")
        logger.warning("PTC Agent endpoints may not work correctly")

    yield  # Server is running

    # Shutdown
    logger.info("Application shutdown started...")

    # 0. Cancel background subagent tasks
    try:
        registry_store = BackgroundRegistryStore.get_instance()
        await registry_store.cancel_all(force=True)
    except Exception as e:
        logger.warning(f"Error cancelling background subagent tasks: {e}")

    # 0a. Shutdown Workspace Manager (stop cleanup task, clear cache)
    if workspace_manager is not None:
        try:
            logger.info("Shutting down Workspace Manager...")
            await workspace_manager.shutdown()
            logger.info("Workspace Manager shutdown complete")
        except Exception as e:
            logger.warning(f"Error during Workspace Manager shutdown: {e}")

    # 0b. Shutdown PTC Session Service (stop sandboxes)
    if session_service is not None:
        try:
            logger.info("Shutting down PTC Session Service...")
            await session_service.shutdown()
            logger.info("PTC Session Service shutdown complete")
        except Exception as e:
            logger.warning(f"Error during PTC Session Service shutdown: {e}")

    # 0c. Close PTC Agent checkpointer pool
    if checkpointer is not None:
        try:
            from src.server.utils.checkpointer import close_checkpointer_pool
            logger.info("Closing PTC Agent checkpointer pool...")
            await close_checkpointer_pool(checkpointer)
            logger.info("PTC Agent checkpointer pool closed")
        except Exception as e:
            logger.warning(f"Error closing PTC Agent checkpointer pool: {e}")

    # 1. FIRST: Gracefully shutdown background workflows
    try:
        manager = BackgroundTaskManager.get_instance()
        await manager.shutdown(timeout=50.0)  # Leave 10s for pool cleanup
    except Exception as e:
        logger.error(f"Error during BackgroundTaskManager shutdown: {e}")

    # 2. THEN: Close database pools
    # Close conversation database pool
    try:
        from src.server.database.conversation_db import get_or_create_pool
        conv_pool = get_or_create_pool()
        if not conv_pool.closed:
            logger.info("Closing conversation database pool...")
            await conv_pool.close()
            logger.info("Conversation database pool closed successfully")
    except Exception as e:
        logger.warning(f"Error closing conversation database pool: {e}")


    # 3. FINALLY: Close Redis cache connection
    try:
        from src.utils.cache.redis_cache import close_cache
        logger.info("Closing Redis cache client...")
        await close_cache()
        logger.info("Redis cache client closed")
    except Exception as e:
        logger.warning(f"Error closing Redis cache: {e}")

    logger.info("Application shutdown complete")


# ============================================================================
# FastAPI App Initialization and Middleware Setup
# ============================================================================
app = FastAPI(
    version="0.1.0",
    lifespan=lifespan,
)


class RequestIDMiddleware:
    """Add request ID for tracing without using BaseHTTPMiddleware"""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Let OPTIONS requests pass through immediately for CORS preflight
        if scope.get("method") == "OPTIONS":
            await self.app(scope, receive, send)
            return

        trace_id = str(uuid4())
        scope["state"] = {"trace_id": trace_id}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append((b"x-trace-id", trace_id.encode()))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_wrapper)

# Register request ID middleware first (will be executed after CORS)
# Note: In FastAPI, middleware is executed in reverse order (last added = first executed)
# So we add RequestIDMiddleware first, then CORS, so CORS executes first
app.add_middleware(RequestIDMiddleware)

# Add CORS middleware LAST (will be executed FIRST)
# This ensures CORS headers are properly set for all requests including OPTIONS preflight
# Allowed origins loaded from config.yaml
from src.config.settings import get_allowed_origins

allowed_origins = get_allowed_origins()

logger.info(f"Allowed origins: {allowed_origins}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  # Restrict to specific origins
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE",
                   "OPTIONS"],  # Use the configured list of methods
    allow_headers=["*"
                   ],  # Now allow all headers, but can be restricted further
)


# ============================================================================
# Router Registration
# ============================================================================
# Import routers
from src.server.app.workflow import router as workflow_router
from src.server.app.conversation import workspaces_threads_router, conversations_router, threads_router, messages_router
from src.server.app.cache import router as cache_router
from src.server.app.utilities import health_router
from src.server.app.chat import router as chat_router  # Main chat endpoint (v1)
from src.server.app.workspaces import router as workspaces_router
from src.server.app.workspace_files import router as workspace_files_router
from src.server.app.market_data import router as market_data_router
from src.server.app.users import router as users_router
from src.server.app.watchlist import router as watchlist_router
from src.server.app.portfolio import router as portfolio_router

# Include all routers
app.include_router(chat_router)  # /api/v1/chat/* - Main chat endpoint
app.include_router(workflow_router)  # /api/v1/workflow/* - Workflow state management
app.include_router(workspaces_router)  # /api/v1/workspaces/* - Workspace CRUD
app.include_router(workspace_files_router)  # /api/v1/workspaces/{id}/files/* - Live file access
app.include_router(workspaces_threads_router)  # /api/v1/workspaces/{id}/threads|messages - Thread management
app.include_router(conversations_router)  # /api/v1/conversations/* - User conversations + messages
app.include_router(threads_router)  # /api/v1/threads/* - Thread utilities (replay)
app.include_router(messages_router)  # /api/v1/messages/* - Message detail endpoints
app.include_router(cache_router)  # /api/v1/cache/* - Cache management
app.include_router(market_data_router)  # /api/v1/market-data/* - Market data proxy
app.include_router(users_router)  # /api/v1/users/* - User management
app.include_router(watchlist_router)  # /api/v1/users/me/watchlist/* - Watchlist management
app.include_router(portfolio_router)  # /api/v1/users/me/portfolio/* - Portfolio management
app.include_router(health_router)  # /health - Health check
