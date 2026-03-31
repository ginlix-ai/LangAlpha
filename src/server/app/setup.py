"""
FastAPI application setup, initialization, and middleware configuration.

This module contains:
- Application lifespan management (startup/shutdown)
- Global state initialization (agent_config, session_service, checkpointer)
- Middleware setup (CORS, request ID)
- Router registration
"""

# ============================================================================
# Windows Event Loop Fix (must be before any async imports)
# ============================================================================
# On Windows, Python 3.8+ defaults to ProactorEventLoop, which is incompatible
# with psycopg's async mode. Set WindowsSelectorEventLoopPolicy before any
# async code runs to avoid "ProactorEventLoop" errors when opening connection pools.
import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ============================================================================
# Imports and Global Variables
# ============================================================================
import logging
import os
import certifi

os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

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
store = None  # LangGraph Store for cross-turn metadata persistence
graph = None  # Most recently used LangGraph (for persistence snapshots)


# ============================================================================
# Lifespan Context Manager
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize resources when server starts, cleanup when stops."""
    global agent_config, session_service, workspace_manager, checkpointer, store

    # Configure logging based on environment settings (first thing on startup)
    configure_logging()

    # Initialize and open conversation database pool
    from src.server.database.conversation import get_or_create_pool

    conv_pool = get_or_create_pool()
    # Extract connection details from pool
    conninfo = conv_pool._conninfo if hasattr(conv_pool, "_conninfo") else "unknown"
    try:
        # Parse basic connection info (format: postgresql://user:pass@host:port/dbname?sslmode=...)
        import re

        match = re.search(r"@([^:]+):(\d+)/([^?]+)", conninfo)
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
            logger.error(
                f"Conversation DB: Failed to connect to {db_host}:{db_port}/{db_name} - {e}"
            )
        else:
            logger.error(f"Conversation DB: Failed to connect - {e}")
        raise

    # Auto-provision local dev user when Supabase auth is disabled
    from src.config.settings import AUTH_ENABLED, LOCAL_DEV_USER_ID

    if not AUTH_ENABLED:
        from src.server.database.user import get_user, create_user_from_auth

        # Only provision if name is missing or user doesn't exist
        existing = await get_user(LOCAL_DEV_USER_ID)
        if not existing or not existing.get("name"):
            await create_user_from_auth(
                user_id=LOCAL_DEV_USER_ID,
                name="Local User",
            )
            logger.info(f"[auth] Local dev user provisioned: {LOCAL_DEV_USER_ID}")

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
        logger.warning(f"Failed to start BackgroundTaskManager cleanup task: {e}")

    # Initialize PTC Agent configuration and session service
    try:
        from ptc_agent.config import load_from_files, ConfigContext

        logger.info("Loading PTC Agent configuration...")
        agent_config = await load_from_files(context=ConfigContext.SDK)
        agent_config.validate_api_keys()
        logger.info("PTC Agent configuration loaded successfully")

        # Initialize session service
        # Derive idle timeout from Daytona auto-stop so the server cleans up
        # *before* Daytona kills the sandbox (10-min buffer, 5-min floor).
        daytona_auto_stop = agent_config.daytona.auto_stop_interval  # seconds
        server_idle_timeout = max(daytona_auto_stop - 600, 300)

        from src.server.services.session_manager import SessionService

        session_service = SessionService.get_instance(
            config=agent_config,
            idle_timeout=server_idle_timeout,
            cleanup_interval=300,  # 5 minutes
        )
        await session_service.start_cleanup_task()
        logger.info("PTC Session Service initialized")

        # Initialize workspace manager
        from src.server.services.workspace_manager import WorkspaceManager

        workspace_manager = WorkspaceManager.get_instance(
            config=agent_config,
            idle_timeout=server_idle_timeout,
            cleanup_interval=300,  # 5 minutes
        )
        await workspace_manager.start_cleanup_task()
        logger.info("Workspace Manager initialized")

        # Initialize PTC Agent checkpointer for state persistence
        from src.server.utils.checkpointer import (
            get_checkpointer,
            open_checkpointer_pool,
            get_store,
            setup_store,
        )

        checkpointer = get_checkpointer(
            memory_type=os.getenv("MEMORY_DB_TYPE", "postgres"),
            db_host=os.getenv("DB_HOST", "localhost"),
            db_port=os.getenv("DB_PORT", "5432"),
            db_name=os.getenv("DB_NAME", "postgres"),
            db_user=os.getenv("DB_USER", "postgres"),
            db_password=os.getenv("DB_PASSWORD", "postgres"),
        )
        await open_checkpointer_pool(checkpointer)
        # Validate checkpointer pool is ready with a health check
        if checkpointer and hasattr(checkpointer, "conn"):
            pool = checkpointer.conn
            async with pool.connection() as conn:
                await conn.execute("SELECT 1")
        logger.info("PTC Agent checkpointer initialized")

        # Initialize LangGraph Store (shares pool with checkpointer)
        try:
            store = get_store(checkpointer)
            if store:
                await setup_store(store)
                logger.info("LangGraph Store initialized")
        except Exception as e:
            logger.warning(f"LangGraph Store setup failed: {e}")
            logger.warning("Offloaded ID dedup will use in-memory fallback")
            store = None

    except FileNotFoundError as e:
        logger.warning(f"PTC Agent config not found: {e}")
        logger.warning("PTC Agent endpoints will not be available")
    except Exception as e:
        logger.warning(f"Failed to initialize PTC Agent: {e}")
        logger.warning("PTC Agent endpoints may not work correctly")

    # Start SharedWSConnectionManager (shared upstream WS to ginlix-data)
    try:
        from src.server.services.shared_ws_manager import DEFAULT_WS_FEEDS, SharedWSConnectionManager

        for market, interval, tier in DEFAULT_WS_FEEDS:
            ws = SharedWSConnectionManager.get_instance(market, interval, tier)
            await ws.start()
        logger.info("SharedWSConnectionManager instances started")
    except Exception as e:
        logger.warning(f"Failed to start SharedWSConnectionManager: {e}")

    # Start AutomationScheduler (polling loop for time-based triggers)
    try:
        from src.server.services.automation_scheduler import AutomationScheduler

        automation_scheduler = AutomationScheduler.get_instance()
        await automation_scheduler.start()
        logger.info("AutomationScheduler started")
    except Exception as e:
        logger.warning(f"Failed to start AutomationScheduler: {e}")
        logger.warning("Scheduled automations will not run")

    # Start PriceMonitorService (real-time price condition triggers)
    try:
        from src.server.services.price_monitor import PriceMonitorService

        price_monitor = PriceMonitorService.get_instance()
        await price_monitor.start()
        logger.info("PriceMonitorService started")
    except Exception as e:
        logger.warning(f"Failed to start PriceMonitorService: {e}")
        logger.warning("Price-triggered automations will not run")

    # Start MarketInsightService (schedule-based market news gathering)
    try:
        from src.server.services.insight_service import InsightService

        insight_service_inst = InsightService.get_instance()
        await insight_service_inst.start()
    except Exception as e:
        logger.warning(f"Failed to start MarketInsightService: {e}")

    yield  # Server is running

    # Shutdown
    logger.info("Application shutdown started...")

    # 0. Shutdown MarketInsightService
    try:
        from src.server.services.insight_service import InsightService

        insight_svc = InsightService.get_instance()
        await insight_svc.shutdown()
    except Exception as e:
        logger.warning(f"Error shutting down MarketInsightService: {e}")

    # 0.5. Shutdown PriceMonitorService (before scheduler so executions can drain)
    try:
        from src.server.services.price_monitor import PriceMonitorService

        price_mon = PriceMonitorService.get_instance()
        await price_mon.stop()
    except Exception as e:
        logger.warning(f"Error shutting down PriceMonitorService: {e}")

    # 1. Shutdown AutomationScheduler
    try:
        from src.server.services.automation_scheduler import AutomationScheduler

        scheduler = AutomationScheduler.get_instance()
        await scheduler.shutdown()
    except Exception as e:
        logger.warning(f"Error shutting down AutomationScheduler: {e}")

    # 1.5. Shutdown SharedWSConnectionManager
    try:
        from src.server.services.shared_ws_manager import SharedWSConnectionManager

        for ws in SharedWSConnectionManager.all_instances():
            await ws.stop()
    except Exception as e:
        logger.warning(f"Error shutting down SharedWSConnectionManager: {e}")

    # 2. Cancel background subagent tasks
    try:
        registry_store = BackgroundRegistryStore.get_instance()
        await registry_store.cancel_all(force=True)
    except Exception as e:
        logger.warning(f"Error cancelling background subagent tasks: {e}")

    # 3. Shutdown Workspace Manager (stop cleanup task, clear cache)
    if workspace_manager is not None:
        try:
            logger.info("Shutting down Workspace Manager...")
            await workspace_manager.shutdown()
            logger.info("Workspace Manager shutdown complete")
        except Exception as e:
            logger.warning(f"Error during Workspace Manager shutdown: {e}")

    # 4. Shutdown PTC Session Service (stop sandboxes)
    if session_service is not None:
        try:
            logger.info("Shutting down PTC Session Service...")
            await session_service.shutdown()
            logger.info("PTC Session Service shutdown complete")
        except Exception as e:
            logger.warning(f"Error during PTC Session Service shutdown: {e}")

    # 5. Close PTC Agent checkpointer pool
    if checkpointer is not None:
        try:
            from src.server.utils.checkpointer import close_checkpointer_pool

            logger.info("Closing PTC Agent checkpointer pool...")
            await close_checkpointer_pool(checkpointer)
            logger.info("PTC Agent checkpointer pool closed")
        except Exception as e:
            logger.warning(f"Error closing PTC Agent checkpointer pool: {e}")

    # 6. Gracefully shutdown background workflows
    try:
        manager = BackgroundTaskManager.get_instance()
        await manager.shutdown()  # Uses shutdown_timeout from config.yaml
    except Exception as e:
        logger.error(f"Error during BackgroundTaskManager shutdown: {e}")

    # 7. Close database pools
    try:
        from src.server.database.conversation import get_or_create_pool

        conv_pool = get_or_create_pool()
        if not conv_pool.closed:
            logger.info("Closing conversation database pool...")
            await conv_pool.close()
            logger.info("Conversation database pool closed successfully")
    except Exception as e:
        logger.warning(f"Error closing conversation database pool: {e}")

    # 8. Close Redis cache connection
    try:
        from src.utils.cache.redis_cache import close_cache

        logger.info("Closing Redis cache client...")
        await close_cache()
        logger.info("Redis cache client closed")
    except Exception as e:
        logger.warning(f"Error closing Redis cache: {e}")

    # 9. Close usage-limits HTTP client
    try:
        from src.server.dependencies.usage_limits import close_http_client

        await close_http_client()
        logger.info("Usage limits HTTP client closed")
    except Exception as e:
        logger.warning(f"Error closing usage limits HTTP client: {e}")

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


# Register GZip compression middleware (compresses JSON responses >= 1KB)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Register request ID middleware (will be executed after CORS)
# Note: In FastAPI, middleware is executed in reverse order (last added = first executed)
# So we add RequestIDMiddleware first, then CORS, so CORS executes first
app.add_middleware(RequestIDMiddleware)

# Add CORS middleware LAST (will be executed FIRST)
# This ensures CORS headers are properly set for all requests including OPTIONS preflight
# Allowed origins loaded from config.yaml
allowed_origins = get_allowed_origins()

logger.info(f"Allowed origins: {allowed_origins}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  # Restrict to specific origins
    allow_credentials=True,
    allow_methods=[
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "OPTIONS",
    ],  # Use the configured list of methods
    allow_headers=["*"],  # Now allow all headers, but can be restricted further
)


# ============================================================================
# Router Registration
# ============================================================================
# Import routers
from src.server.app.threads import router as threads_router
from src.server.app.sessions import router as sessions_router
from src.server.app.cache import router as cache_router
from src.server.app.utilities import health_router
from src.server.app.workspaces import router as workspaces_router
from src.server.app.workspace_files import router as workspace_files_router
from src.server.app.workspace_sandbox import router as workspace_sandbox_router
from src.server.app.workspace_sandbox import preview_redirect_router
from src.server.app.market_data import router as market_data_router
from src.server.app.users import router as users_router
from src.server.app.watchlist import router as watchlist_router
from src.server.app.portfolio import router as portfolio_router
from src.server.app.infoflow import router as infoflow_router
from src.server.app.news import router as news_router
from src.server.app.calendar import router as calendar_router
from src.server.app.sec_proxy import router as sec_proxy_router
from src.server.app.api_keys import router as api_keys_router
from src.server.app.automations import router as automations_router
from src.server.app.insights import router as insights_router
from src.server.app.oauth import router as oauth_router
from src.server.app.public import router as public_router
from src.server.app.skills import router as skills_router
from src.server.app.vault import router as vault_router

# Conditionally import ginlix-data WS proxy (only when GINLIX_DATA_WS_URL is set)
from src.config.settings import GINLIX_DATA_ENABLED

if GINLIX_DATA_ENABLED:
    from src.server.app.market_data_ws import router as market_data_ws_router

    logger.info("ginlix-data WS proxy enabled")
else:
    # Register a minimal status endpoint so the frontend preflight check
    # gets a clean 200 instead of a noisy 404.
    from fastapi import APIRouter as _APIRouter

    market_data_ws_router = _APIRouter()

    @market_data_ws_router.get("/ws/v1/market-data/status")
    async def market_data_ws_status_disabled():
        return {"enabled": False}

    logger.info("ginlix-data WS proxy disabled (GINLIX_DATA_URL not set)")

# Include all routers
app.include_router(threads_router)  # /api/v1/threads/* - Thread CRUD, messages, control
app.include_router(sessions_router)  # /api/v1/sessions - Active session stats
app.include_router(workspaces_router)  # /api/v1/workspaces/* - Workspace CRUD
app.include_router(
    workspace_files_router
)  # /api/v1/workspaces/{id}/files/* - Live file access
app.include_router(
    workspace_sandbox_router
)  # /api/v1/workspaces/{id}/sandbox/* - Sandbox stats & packages
app.include_router(cache_router)  # /api/v1/cache/* - Cache management
app.include_router(market_data_router)  # /api/v1/market-data/* - Market data proxy
app.include_router(users_router)  # /api/v1/users/* - User management
app.include_router(
    watchlist_router
)  # /api/v1/users/me/watchlist/* - Watchlist management
app.include_router(
    portfolio_router
)  # /api/v1/users/me/portfolio/* - Portfolio management
app.include_router(
    infoflow_router
)  # /api/v1/infoflow/* - InfoFlow content feed (kept for PopularCard)
app.include_router(news_router)  # /api/v1/news - News feed (general + ticker-filtered)
app.include_router(calendar_router)  # /api/v1/calendar/* - Economic & earnings calendars
app.include_router(sec_proxy_router)  # /api/v1/sec-proxy/* - SEC EDGAR document proxy
app.include_router(
    api_keys_router
)  # /api/v1/users/me/api-keys + /api/v1/models - BYOK & model config
app.include_router(
    automations_router
)  # /api/v1/automations/* - Scheduled automation triggers
app.include_router(insights_router)  # /api/v1/insights/* - AI market insights
app.include_router(oauth_router)  # /api/v1/oauth/* - OAuth provider connections (Codex)
app.include_router(
    public_router
)  # /api/v1/public/* - Public shared thread access (no auth)
app.include_router(skills_router)  # /api/v1/skills - Available agent skills
app.include_router(
    vault_router
)  # /api/v1/workspaces/{id}/vault/secrets - Per-workspace secret storage
app.include_router(health_router)  # /health - Health check
app.include_router(
    preview_redirect_router
)  # /api/v1/preview/{workspace_id}/{port} - Unauthenticated preview URL redirect

app.include_router(
    market_data_ws_router
)  # /ws/v1/market-data/* - Real-time WS proxy (or just status endpoint when disabled)
